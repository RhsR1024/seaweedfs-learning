// Package operation 实现 SeaweedFS 的大文件分块操作
// 本文件包含分块文件（Chunked File）的数据结构和读取逻辑
//
// 分块文件说明:
//   SeaweedFS 支持将大文件分割成多个小块（chunks）存储在不同的 Volume Server 上
//   这种机制支持：
//   - 超大文件存储：突破单个 Volume 的大小限制
//   - 并行上传下载：可以并发处理多个分块
//   - 断点续传：支持只重传失败的分块
//   - 流式处理：支持 Seek 和 Range 请求
//
// Chunk Manifest 结构:
//   当文件被分块存储时，SeaweedFS 会创建一个 manifest 文件，记录：
//   - 文件名和 MIME 类型
//   - 总文件大小
//   - 各分块的 fid、偏移量和大小
//
// 典型使用场景:
//   - 视频流媒体：支持 Range 请求实现视频拖拽
//   - 大文件下载：断点续传和并行下载
//   - 分布式存储：分块分散在多个节点，提高可用性
package operation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

var (
	// ErrRangeRequestsNotSupported 远程服务器不支持 Range 请求
	// 当请求的分块所在的服务器没有设置 Accept-Ranges 头时返回
	ErrRangeRequestsNotSupported = errors.New("Range requests are not supported by the remote server")
	// ErrInvalidRange 无效的范围请求
	// 当尝试读取超出文件末尾的数据时返回
	ErrInvalidRange = errors.New("Invalid range")
)

// ChunkInfo 分块信息结构
// 描述单个分块的存储位置和大小
//
// 字段:
//   - Fid: 分块的文件 ID（格式："volumeId,fileKey"）
//   - Offset: 分块在原始文件中的字节偏移量
//   - Size: 分块的大小（字节）
//
// 示例:
//
//	{
//	  "fid": "3,01e3b0756f",
//	  "offset": 8388608,    // 8MB
//	  "size": 4194304       // 4MB
//	}
type ChunkInfo struct {
	Fid    string `json:"fid"`
	Offset int64  `json:"offset"`
	Size   int64  `json:"size"`
}

// ChunkList 分块列表类型
// 实现了 sort.Interface，支持按偏移量排序
type ChunkList []*ChunkInfo

// ChunkManifest 分块清单结构
// 记录整个分块文件的元数据
//
// 字段:
//   - Name: 原始文件名
//   - Mime: MIME 类型（如 "video/mp4"）
//   - Size: 完整文件的总大小
//   - Chunks: 分块列表（按偏移量排序）
//
// JSON 格式示例:
//
//	{
//	  "name": "video.mp4",
//	  "mime": "video/mp4",
//	  "size": 104857600,
//	  "chunks": [
//	    {"fid": "3,01e3b0756f", "offset": 0, "size": 8388608},
//	    {"fid": "3,01e3b07570", "offset": 8388608, "size": 8388608},
//	    ...
//	  ]
//	}
type ChunkManifest struct {
	Name   string    `json:"name,omitempty"`
	Mime   string    `json:"mime,omitempty"`
	Size   int64     `json:"size,omitempty"`
	Chunks ChunkList `json:"chunks,omitempty"`
}

// ChunkedFileReader 分块文件读取器
// 提供可 Seek 的流式读取接口，支持从任意位置读取大文件
//
// 字段:
//   - totalSize: 完整文件的总大小
//   - chunkList: 分块信息列表（按偏移量排序）
//   - master: Master Server 地址（用于查询分块位置）
//   - pos: 当前读取位置（字节偏移）
//   - pr/pw: 管道读写器（用于流式数据传输）
//   - mutex: 并发保护锁
//   - grpcDialOption: gRPC 连接选项
//
// 特性:
//   - 支持 io.Reader、io.Seeker、io.ReaderAt、io.WriterTo 接口
//   - 自动处理跨分块读取
//   - 延迟加载分块数据（按需读取）
type ChunkedFileReader struct {
	totalSize      int64
	chunkList      []*ChunkInfo
	master         pb.ServerAddress
	pos            int64
	pr             *io.PipeReader
	pw             *io.PipeWriter
	mutex          sync.Mutex
	grpcDialOption grpc.DialOption
}

// Len 返回分块数量（实现 sort.Interface）
func (s ChunkList) Len() int { return len(s) }

// Less 比较两个分块的偏移量（实现 sort.Interface）
func (s ChunkList) Less(i, j int) bool { return s[i].Offset < s[j].Offset }

// Swap 交换两个分块的位置（实现 sort.Interface）
func (s ChunkList) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

// LoadChunkManifest 加载并解析分块清单
// 从存储的 manifest 数据中恢复 ChunkManifest 对象
//
// 参数:
//   - buffer: manifest 数据（JSON 格式，可能被压缩）
//   - isCompressed: 是否已压缩
//
// 返回值:
//   - *ChunkManifest: 解析后的清单对象
//   - error: 解析错误
//
// 工作流程:
//  1. 如果数据被压缩，先解压缩
//  2. JSON 反序列化为 ChunkManifest
//  3. 按偏移量对分块排序（确保顺序正确）
//  4. 返回清单对象
//
// 用途:
//   - 读取分块文件：加载 manifest 获取分块位置
//   - 继续上传：恢复之前的上传进度
//   - 删除操作：找到所有分块进行删除
func LoadChunkManifest(buffer []byte, isCompressed bool) (*ChunkManifest, error) {
	if isCompressed {
		var err error
		if buffer, err = util.DecompressData(buffer); err != nil {
			glog.V(0).Infof("fail to decompress chunk manifest: %v", err)
		}
	}
	cm := ChunkManifest{}
	if e := json.Unmarshal(buffer, &cm); e != nil {
		return nil, e
	}
	sort.Sort(cm.Chunks) // 按偏移量排序，确保读取顺序正确
	return &cm, nil
}

// Marshal 将分块清单序列化为 JSON 字节数组
// 用于存储 manifest 到 SeaweedFS
//
// 返回值:
//   - []byte: JSON 格式的 manifest 数据
//   - error: 序列化错误
func (cm *ChunkManifest) Marshal() ([]byte, error) {
	return json.Marshal(cm)
}

// DeleteChunks 删除分块清单中的所有分块
// 清理操作，当删除分块文件时，需要删除所有分块
//
// 参数:
//   - masterFn: 获取 Master Server 地址的函数
//   - usePublicUrl: 是否使用公开 URL
//   - grpcDialOption: gRPC 连接选项
//
// 返回值:
//   - error: 任何分块删除失败时返回错误
//
// 工作流程:
//  1. 收集所有分块的 fid
//  2. 批量删除（DeleteFileIds）
//  3. 检查删除结果，返回第一个错误
//
// 注意:
//   - 批量删除提高效率
//   - 任何一个失败都会返回错误（部分删除）
//   - 调用者需要处理部分删除的情况
func (cm *ChunkManifest) DeleteChunks(masterFn GetMasterFn, usePublicUrl bool, grpcDialOption grpc.DialOption) error {
	var fileIds []string
	for _, ci := range cm.Chunks {
		fileIds = append(fileIds, ci.Fid)
	}
	results := DeleteFileIds(masterFn, usePublicUrl, grpcDialOption, fileIds)

	// 检查删除结果
	for _, result := range results {
		if result.Error != "" {
			glog.V(0).Infof("delete file %+v: %v", result.FileId, result.Error)
			return fmt.Errorf("chunk delete %v: %v", result.FileId, result.Error)
		}
	}

	return nil
}

// readChunkNeedle 从指定 URL 读取分块数据
// 支持 Range 请求，用于断点续传和流式读取
//
// 参数:
//   - fileUrl: 分块的完整 URL
//   - w: 数据输出目标
//   - offset: 分块内的起始偏移量（0 表示从头读取）
//   - jwt: JWT 认证令牌（可选）
//
// 返回值:
//   - written: 实际写入的字节数
//   - e: 读取错误
//
// HTTP 状态码处理:
//   - 200 OK: 完整内容（offset 必须为 0，否则返回错误）
//   - 206 Partial Content: 部分内容（Range 请求成功）
//   - 416 Range Not Satisfiable: 范围无效
//   - 其他: 返回错误
//
// Range 请求格式:
//   Range: bytes=<offset>-
//   例如: Range: bytes=1024- （从 1024 字节开始读取到末尾）
func readChunkNeedle(fileUrl string, w io.Writer, offset int64, jwt string) (written int64, e error) {
	req, err := http.NewRequest(http.MethodGet, fileUrl, nil)
	if err != nil {
		return written, err
	}
	if offset > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
	}

	resp, err := util_http.Do(req)
	if err != nil {
		return written, err
	}
	defer util_http.CloseResponse(resp)

	switch resp.StatusCode {
	case http.StatusRequestedRangeNotSatisfiable:
		return written, ErrInvalidRange
	case http.StatusOK:
		if offset > 0 {
			return written, ErrRangeRequestsNotSupported
		}
	case http.StatusPartialContent:
		break
	default:
		return written, fmt.Errorf("Read chunk needle error: [%d] %s", resp.StatusCode, fileUrl)

	}
	return io.Copy(w, resp.Body)
}

// NewChunkedFileReader 创建分块文件读取器
// 初始化一个可从任意位置读取分块大文件的读取器
//
// 参数:
//   - chunkList: 分块信息列表（包含每个分块的 fid、偏移量和大小）
//   - master: Master Server 地址（用于查询分块所在的 Volume Server）
//   - grpcDialOption: gRPC 连接选项
//
// 返回值:
//   - *ChunkedFileReader: 初始化完成的读取器
//
// 工作流程:
//  1. 计算所有分块的总大小
//  2. 按偏移量对分块排序（确保读取顺序正确）
//  3. 初始化读取器状态（位置从 0 开始）
//
// 使用示例:
//
//	reader := NewChunkedFileReader(manifest.Chunks, masterAddr, grpcOpt)
//	defer reader.Close()
//	data := make([]byte, 1024)
//	n, err := reader.Read(data)
//
// 特性:
//   - 延迟加载：分块数据在实际读取时才从 Volume Server 获取
//   - 支持 Seek：可以跳转到任意位置
//   - 自动跨块：透明处理跨分块的读取请求
func NewChunkedFileReader(chunkList []*ChunkInfo, master pb.ServerAddress, grpcDialOption grpc.DialOption) *ChunkedFileReader {
	var totalSize int64
	for _, chunk := range chunkList {
		totalSize += chunk.Size
	}
	sort.Sort(ChunkList(chunkList))
	return &ChunkedFileReader{
		totalSize:      totalSize,
		chunkList:      chunkList,
		master:         master,
		grpcDialOption: grpcDialOption,
	}
}

// Seek 设置下次读取的位置（实现 io.Seeker 接口）
// 支持从文件开头、当前位置或末尾计算新位置
//
// 参数:
//   - offset: 偏移量（字节数）
//   - whence: 基准位置
//   - io.SeekStart (0): 从文件开头计算
//   - io.SeekCurrent (1): 从当前位置计算
//   - io.SeekEnd (2): 从文件末尾计算（offset 通常为负数）
//
// 返回值:
//   - int64: 新的绝对位置
//   - error: 如果新位置超出文件末尾，返回 ErrInvalidRange
//
// 使用示例:
//
//	// 跳到文件开头
//	reader.Seek(0, io.SeekStart)
//
//	// 向前跳 1MB
//	reader.Seek(1024*1024, io.SeekCurrent)
//
//	// 跳到文件末尾前 100 字节
//	reader.Seek(-100, io.SeekEnd)
//
// 注意:
//   - 如果位置发生变化，会关闭当前的读取管道
//   - 新位置的数据会在下次 Read 时延迟加载
//   - 即使返回错误，位置也会被更新
func (cf *ChunkedFileReader) Seek(offset int64, whence int) (int64, error) {
	var err error
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		offset += cf.pos
	case io.SeekEnd:
		offset = cf.totalSize + offset
	}
	if offset > cf.totalSize {
		err = ErrInvalidRange
	}
	if cf.pos != offset {
		cf.Close()
	}
	cf.pos = offset
	return cf.pos, err
}

// WriteTo 将分块文件数据写入到指定的 Writer（实现 io.WriterTo 接口）
// 从当前位置开始，将所有剩余数据写入目标
//
// 参数:
//   - w: 数据输出目标（如 HTTP ResponseWriter、文件等）
//
// 返回值:
//   - n: 实际写入的字节数
//   - err: 读取或写入错误
//
// 工作流程:
//  1. 根据当前位置找到对应的分块
//  2. 计算分块内的起始偏移
//  3. 依次读取并写入每个分块的数据
//  4. 更新读取位置
//
// 关键特性:
//   - 自动处理跨分块读取
//   - 支持从分块中间开始读取（用于 Seek 后的读取）
//   - 每次读取分块时通过 Master 查询 Volume Server 地址
//   - 使用 Range 请求实现部分读取
//
// 错误处理:
//   - 如果当前位置无效，返回 ErrInvalidRange
//   - 如果查询分块位置失败，返回 lookup 错误
//   - 如果读取分块数据失败，返回读取错误
//
// 性能考虑:
//   - 每个分块的 URL 是动态查询的（支持分块迁移）
//   - 数据直接流式传输，不在内存中缓存整个文件
func (cf *ChunkedFileReader) WriteTo(w io.Writer) (n int64, err error) {
	chunkIndex := -1
	chunkStartOffset := int64(0)
	for i, ci := range cf.chunkList {
		if cf.pos >= ci.Offset && cf.pos < ci.Offset+ci.Size {
			chunkIndex = i
			chunkStartOffset = cf.pos - ci.Offset
			break
		}
	}
	if chunkIndex < 0 {
		return n, ErrInvalidRange
	}
	for ; chunkIndex < len(cf.chunkList); chunkIndex++ {
		ci := cf.chunkList[chunkIndex]
		// if we need read date from local volume server first?
		fileUrl, jwt, lookupError := LookupFileId(func(_ context.Context) pb.ServerAddress {
			return cf.master
		}, cf.grpcDialOption, ci.Fid)
		if lookupError != nil {
			return n, lookupError
		}
		if wn, e := readChunkNeedle(fileUrl, w, chunkStartOffset, jwt); e != nil {
			return n, e
		} else {
			n += wn
			cf.pos += wn
		}

		chunkStartOffset = 0
	}
	return n, nil
}

// ReadAt 从指定位置读取数据（实现 io.ReaderAt 接口）
// 支持随机访问，不影响其他并发读取操作
//
// 参数:
//   - p: 数据缓冲区
//   - off: 读取的起始位置（绝对偏移量）
//
// 返回值:
//   - n: 实际读取的字节数
//   - err: 读取错误
//
// 使用示例:
//
//	data := make([]byte, 1024)
//	n, err := reader.ReadAt(data, 8192) // 从 8KB 位置读取 1KB 数据
//
// 注意:
//   - 会先 Seek 到指定位置，然后读取
//   - 会修改内部的读取位置状态
//   - 适用于需要随机访问文件内容的场景
func (cf *ChunkedFileReader) ReadAt(p []byte, off int64) (n int, err error) {
	cf.Seek(off, 0)
	return cf.Read(p)
}

// Read 读取数据到缓冲区（实现 io.Reader 接口）
// 从当前位置读取数据，支持流式读取
//
// 参数:
//   - p: 数据缓冲区
//
// 返回值:
//   - int: 实际读取的字节数
//   - error: 读取错误（包括 io.EOF）
//
// 工作流程:
//  1. 获取或创建管道读取器
//  2. 从管道读取数据
//  3. 内部协程会从 Volume Server 获取分块数据并写入管道
//
// 使用示例:
//
//	data := make([]byte, 4096)
//	for {
//	    n, err := reader.Read(data)
//	    if err == io.EOF {
//	        break
//	    }
//	    // 处理 data[:n]
//	}
//
// 特性:
//   - 延迟加载：数据在首次 Read 时才开始从服务器获取
//   - 自动缓冲：使用管道实现生产者-消费者模式
//   - 流式处理：支持大文件的流式读取，无需全部加载到内存
func (cf *ChunkedFileReader) Read(p []byte) (int, error) {
	return cf.getPipeReader().Read(p)
}

// Close 关闭分块文件读取器（实现 io.Closer 接口）
// 释放相关资源，包括关闭管道连接
//
// 返回值:
//   - error: 关闭时的错误（如果有）
//
// 注意:
//   - 线程安全：使用互斥锁保护
//   - 幂等操作：多次调用不会产生错误
//   - 使用 defer 确保资源释放
//
// 使用示例:
//
//	reader := NewChunkedFileReader(chunks, master, opt)
//	defer reader.Close()
//	// 使用 reader...
func (cf *ChunkedFileReader) Close() (e error) {
	cf.mutex.Lock()
	defer cf.mutex.Unlock()
	return cf.closePipe()
}

// closePipe 关闭内部管道
// 释放管道读写器资源
//
// 返回值:
//   - error: 关闭时的最后一个错误（如果有）
//
// 工作流程:
//  1. 关闭管道读取端（如果存在）
//  2. 置空读取端指针
//  3. 关闭管道写入端（如果存在）
//  4. 置空写入端指针
//
// 注意:
//   - 此方法不加锁，调用者需要确保线程安全
//   - 关闭管道会导致正在进行的 WriteTo 操作失败
//   - 返回的是最后一个错误，可能丢失第一个错误
func (cf *ChunkedFileReader) closePipe() (e error) {
	if cf.pr != nil {
		if err := cf.pr.Close(); err != nil {
			e = err
		}
	}
	cf.pr = nil
	if cf.pw != nil {
		if err := cf.pw.Close(); err != nil {
			e = err
		}
	}
	cf.pw = nil
	return e
}

// getPipeReader 获取或创建管道读取器
// 使用管道实现生产者-消费者模式的流式读取
//
// 返回值:
//   - io.Reader: 管道读取端
//
// 工作流程:
//  1. 加锁保护并发访问
//  2. 如果管道已存在且有效，直接返回
//  3. 否则关闭旧管道，创建新管道
//  4. 启动协程从 Volume Server 获取数据并写入管道
//  5. 返回管道读取端
//
// 生产者-消费者模式:
//   - 生产者（协程）：调用 WriteTo 从服务器获取数据
//   - 消费者（调用者）：通过返回的 Reader 读取数据
//   - 管道作为缓冲区，实现异步数据传输
//
// 线程安全:
//   - 使用互斥锁保护管道的创建和访问
//   - 生产者协程独立运行，通过管道传输数据
//
// 错误处理:
//   - 生产者协程的错误通过 CloseWithError 传递给消费者
//   - 消费者在 Read 时会收到这些错误
func (cf *ChunkedFileReader) getPipeReader() io.Reader {
	cf.mutex.Lock()
	defer cf.mutex.Unlock()
	if cf.pr != nil && cf.pw != nil {
		return cf.pr
	}
	cf.closePipe()
	cf.pr, cf.pw = io.Pipe()
	go func(pw *io.PipeWriter) {
		_, e := cf.WriteTo(pw)
		pw.CloseWithError(e)
	}(cf.pw)
	return cf.pr
}
