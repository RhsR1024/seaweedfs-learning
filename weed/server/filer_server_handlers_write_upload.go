// Package weed_server 中的 filer_server_handlers_write_upload.go 提供上传过程中与 Volume Server 交互的细节实现
//
// 核心功能：
//   1. 数据流拆分：将大文件拆分为多个 chunk 并发上传
//   2. 小文件内联：对于极小文件（< SaveToFilerLimit），直接存储在元数据中，避免 chunk 开销
//   3. 服务端加密：支持 SSE-C、SSE-KMS、SSE-S3 三种加密模式
//   4. 并发控制：使用缓冲池和信号量限制并发上传数量，防止内存溢出
//   5. 错误重试：上传失败时自动重试，并清理失败的 chunk
//
// 数据流向：
//   HTTP 请求 → uploadRequestToChunks → uploadReaderToChunks → dataToChunkWithSSE → doUpload → Volume Server
//
// 关键设计：
//   - 使用 sync.Pool 复用 Buffer，减少 GC 压力
//   - 使用信号量（bytesBufferLimitChan）限制并发数，避免内存占用过高
//   - 使用 TeeReader 一边读取一边计算 MD5，无需二次读取
//   - 对于 SSE 加密，在 chunk 级别保存加密元数据（IV、KeyMD5 等）
package weed_server

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"hash"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"slices"

	"encoding/json"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// bufPool 是 bytes.Buffer 的对象池
// 用于复用 Buffer 对象，避免在大文件上传时重复分配内存，降低 GC 压力
// 每个 goroutine 从池中获取 Buffer，使用完后归还
var bufPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

// uploadRequestToChunks 解析 HTTP 请求并将 payload 拆分为 chunk
//
// 参数:
//   - ctx: 上下文
//   - w: HTTP 响应写入器（当前未使用，保留以便扩展）
//   - r: HTTP 请求对象，用于解析 query 参数和请求头
//   - reader: 数据源（通常是请求 Body）
//   - chunkSize: 每个 chunk 的最大字节数
//   - fileName: 文件名
//   - contentType: MIME 类型
//   - contentLength: 内容总长度（当前未使用，保留以便扩展）
//   - so: 存储选项（副本策略、数据中心等）
//
// 返回:
//   - fileChunks: 上传成功的 chunk 列表
//   - md5Hash: 完整数据的 MD5 哈希对象
//   - chunkOffset: 最终的偏移量（用于追加写入）
//   - uploadErr: 上传过程中的错误
//   - smallContent: 小文件内联数据（如果文件小于 SaveToFilerLimit）
//
// 支持的 query 参数:
//   - offset: 写入的起始偏移量（字节）
//   - op=append: 追加模式（不能同时指定 offset）
//
// 错误情况:
//   - offset 参数无效（非数字或负数）
//   - append 模式下同时指定了 offset
func (fs *FilerServer) uploadRequestToChunks(ctx context.Context, w http.ResponseWriter, r *http.Request, reader io.Reader, chunkSize int32, fileName, contentType string, contentLength int64, so *operation.StorageOption) (fileChunks []*filer_pb.FileChunk, md5Hash hash.Hash, chunkOffset int64, uploadErr error, smallContent []byte) {
	query := r.URL.Query()

	// 【检查是否为追加模式】
	// 追加模式会在文件末尾写入数据，不需要指定 offset
	isAppend := isAppend(r)

	// 【解析 offset 参数】
	// offset 用于指定写入的起始位置（字节偏移量）
	// 例如：offset=1024 表示从文件的第 1024 字节开始写入
	if query.Has("offset") {
		offset := query.Get("offset")
		offsetInt, err := strconv.ParseInt(offset, 10, 64)
		if err != nil || offsetInt < 0 {
			// offset 必须是非负整数
			err = fmt.Errorf("invalid 'offset': '%s'", offset)
			return nil, nil, 0, err, nil
		}
		if isAppend && offsetInt > 0 {
			// 追加模式下不允许指定 offset（追加总是从文件末尾开始）
			err = fmt.Errorf("cannot set offset when op=append")
			return nil, nil, 0, err, nil
		}
		chunkOffset = offsetInt
	}

	// 调用实际的上传逻辑
	return fs.uploadReaderToChunks(ctx, r, reader, chunkOffset, chunkSize, fileName, contentType, isAppend, so)
}

// uploadReaderToChunks 实际执行读取与并发上传
//
// 参数:
//   - ctx: 上下文
//   - r: HTTP 请求对象（用于提取 SSE 加密头信息，可为 nil）
//   - reader: 数据源
//   - startOffset: 起始偏移量
//   - chunkSize: 每个 chunk 的最大字节数
//   - fileName: 文件名
//   - contentType: MIME 类型
//   - isAppend: 是否为追加模式
//   - so: 存储选项
//
// 返回:
//   - fileChunks: 上传成功的 chunk 列表
//   - md5Hash: 完整数据的 MD5 哈希对象
//   - chunkOffset: 最终的偏移量
//   - uploadErr: 上传错误
//   - smallContent: 小文件内联数据
//
// 工作原理:
//   1. 使用 TeeReader 一边读取数据一边计算 MD5
//   2. 将数据按 chunkSize 拆分为多个块
//   3. 对于第一个 chunk，如果小于 SaveToFilerLimit，直接内联到元数据中
//   4. 对于其他 chunk，使用 goroutine 并发上传
//   5. 使用信号量（bytesBufferLimitChan）限制并发数为 4，避免内存占用过高
//   6. 任一 chunk 上传失败则中止所有上传，并清理已上传的 chunk
//
// 并发控制:
//   - bytesBufferCounter=4：最多同时处理 4 个 chunk
//   - wg：等待所有 goroutine 完成
//   - fileChunksLock：保护 fileChunks 列表的并发写入
//   - uploadErrLock：保护 uploadErr 的并发写入
func (fs *FilerServer) uploadReaderToChunks(ctx context.Context, r *http.Request, reader io.Reader, startOffset int64, chunkSize int32, fileName, contentType string, isAppend bool, so *operation.StorageOption) (fileChunks []*filer_pb.FileChunk, md5Hash hash.Hash, chunkOffset int64, uploadErr error, smallContent []byte) {

	// 【步骤 1：初始化 MD5 计算和读取器】
	// 使用 TeeReader 在读取数据的同时计算 MD5，避免二次读取
	md5Hash = md5.New()
	chunkOffset = startOffset
	var partReader = io.NopCloser(io.TeeReader(reader, md5Hash))

	// 【步骤 2：初始化并发控制】
	var wg sync.WaitGroup                                               // 等待所有上传 goroutine 完成
	var bytesBufferCounter int64 = 4                                    // 最多同时处理 4 个 chunk
	bytesBufferLimitChan := make(chan struct{}, bytesBufferCounter)     // 信号量，限制并发数
	var fileChunksLock sync.Mutex                                        // 保护 fileChunks 列表
	var uploadErrLock sync.Mutex                                         // 保护 uploadErr 变量

	// 【步骤 3：循环读取并上传 chunk】
	for {

		// 【步骤 3.1：获取信号量】
		// 限制同时占用的缓冲区数量，避免内存占用过高
		// 如果已有 4 个 goroutine 在运行，这里会阻塞等待
		bytesBufferLimitChan <- struct{}{}

		// 【步骤 3.2：检查是否有上传失败】
		// 任一 chunk 上传失败即可提前结束，避免浪费资源
		// uploadErr 可能被多个协程修改，因此必须加锁
		uploadErrLock.Lock()
		if uploadErr != nil {
			<-bytesBufferLimitChan  // 释放信号量
			uploadErrLock.Unlock()
			break
		}
		uploadErrLock.Unlock()

		// 【步骤 3.3：从对象池获取 Buffer】
		// 复用 Buffer 对象，避免频繁分配和 GC
		bytesBuffer := bufPool.Get().(*bytes.Buffer)

		// 【步骤 3.4：读取一个 chunk 的数据】
		// LimitReader 确保最多读取 chunkSize 字节
		limitedReader := io.LimitReader(partReader, int64(chunkSize))

		// 清空 Buffer（如果是复用的可能有旧数据）
		bytesBuffer.Reset()

		// 从 limitedReader 读取数据到 bytesBuffer
		dataSize, err := bytesBuffer.ReadFrom(limitedReader)

		// data, err := io.ReadAll(limitedReader) // 旧实现，保留以便排查
		// 【步骤 3.5：处理读取错误或结束】
		if err != nil || dataSize == 0 {
			bufPool.Put(bytesBuffer)         // 归还 Buffer
			<-bytesBufferLimitChan           // 释放信号量
			if err != nil {
				// 记录读取错误
				uploadErrLock.Lock()
				if uploadErr == nil {
					uploadErr = err
				}
				uploadErrLock.Unlock()
			}
			break  // 读取完成或出错，退出循环
		}

		// 【步骤 3.6：小文件内联优化】
		// 对于第一个 chunk 且非追加模式，如果数据量小于 SaveToFilerLimit，
		// 直接将数据内联到元数据中，避免创建 chunk 的开销
		if chunkOffset == 0 && !isAppend {
			if dataSize < fs.option.SaveToFilerLimit {
				chunkOffset += dataSize
				smallContent = make([]byte, dataSize)
				bytesBuffer.Read(smallContent)
				bufPool.Put(bytesBuffer)
				<-bytesBufferLimitChan
				stats.FilerHandlerCounter.WithLabelValues(stats.ContentSaveToFiler).Inc()
				break  // 小文件已内联，无需继续上传
			}
		} else {
			// 非第一个 chunk 或追加模式，记录自动分块统计
			stats.FilerHandlerCounter.WithLabelValues(stats.AutoChunk).Inc()
		}

		// 【步骤 3.7：启动 goroutine 上传 chunk】
		wg.Add(1)
		go func(offset int64, buf *bytes.Buffer) {
			defer func() {
				bufPool.Put(buf)          // 归还 Buffer 到对象池
				<-bytesBufferLimitChan    // 释放信号量，允许下一个 chunk 开始处理
				wg.Done()                 // 标记此 goroutine 完成
			}()

			// 将 Buffer 中的数据上传为 chunk
			// dataToChunkWithSSE 会处理：
			//   1. 从 Master 请求分配 fid
			//   2. 将数据上传到 Volume Server
			//   3. 提取并保存 SSE 加密元数据（如果有）
			chunks, toChunkErr := fs.dataToChunkWithSSE(ctx, r, fileName, contentType, buf.Bytes(), offset, so)
			if toChunkErr != nil {
				// 上传失败，记录错误（只记录第一个错误）
				uploadErrLock.Lock()
				if uploadErr == nil {
					uploadErr = toChunkErr
				}
				uploadErrLock.Unlock()
			}
			if chunks != nil {
				// 上传成功，将 chunk 添加到列表中
				fileChunksLock.Lock()
				fileChunksSize := len(fileChunks) + len(chunks)
				for _, chunk := range chunks {
					fileChunks = append(fileChunks, chunk)
					glog.V(4).InfofCtx(ctx, "uploaded %s chunk %d to %s [%d,%d)", fileName, fileChunksSize, chunk.FileId, offset, offset+int64(chunk.Size))
				}
				fileChunksLock.Unlock()
			}
		}(chunkOffset, bytesBuffer)

		// 【步骤 3.8：更新偏移量】
		// 重置下一个 chunk 需要用到的偏移
		chunkOffset = chunkOffset + dataSize

		// 【步骤 3.9：检查是否读取完成】
		// 如果最后一个 chunk 没有填满 chunkSize，说明已经读到末尾，直接退出
		if dataSize < int64(chunkSize) {
			break
		}
	}

	// 【步骤 4：等待所有 goroutine 完成】
	wg.Wait()

	// 【步骤 5：处理上传失败情况】
	if uploadErr != nil {
		glog.V(0).InfofCtx(ctx, "upload file %s error: %v", fileName, uploadErr)
		// 记录需要清理的 chunk
		for _, chunk := range fileChunks {
			glog.V(4).InfofCtx(ctx, "purging failed uploaded %s chunk %s [%d,%d)", fileName, chunk.FileId, chunk.Offset, chunk.Offset+int64(chunk.Size))
		}
		// 删除已上传的 chunk（因为整体上传失败）
		fs.filer.DeleteUncommittedChunks(ctx, fileChunks)
		return nil, md5Hash, 0, uploadErr, nil
	}

	// 【步骤 6：排序 chunk 列表】
	// 按 offset 排序，确保 chunk 顺序正确
	// 虽然 chunk 是并发上传的，但最终需要按文件偏移量排序
	slices.SortFunc(fileChunks, func(a, b *filer_pb.FileChunk) int {
		return int(a.Offset - b.Offset)
	})
	return fileChunks, md5Hash, chunkOffset, nil, smallContent
}

// doUpload 调用 operation.UploadResult 完成实际的 HTTP 上传
//
// 参数:
//   - ctx: 上下文
//   - urlLocation: Volume Server 的上传 URL（由 Master 分配返回）
//   - limitedReader: 数据源
//   - fileName: 文件名
//   - contentType: MIME 类型
//   - pairMap: 额外的 HTTP 头信息（Key-Value 对）
//   - auth: JWT 认证令牌
//
// 返回:
//   - *operation.UploadResult: 上传结果（包含 fid、size 等信息）
//   - error: 上传错误
//   - []byte: 内联数据（用于极小文件优化，通常为空）
//
// 功能:
//   - 封装 HTTP 上传逻辑
//   - 记录上传时长和重试次数统计
//   - 支持 JWT 认证和自定义 HTTP 头
func (fs *FilerServer) doUpload(ctx context.Context, urlLocation string, limitedReader io.Reader, fileName string, contentType string, pairMap map[string]string, auth security.EncodedJwt) (*operation.UploadResult, error, []byte) {

	// 【统计指标：上传计数】
	stats.FilerHandlerCounter.WithLabelValues(stats.ChunkUpload).Inc()

	// 【统计指标：上传时长】
	start := time.Now()
	defer func() {
		stats.FilerRequestHistogram.WithLabelValues(stats.ChunkUpload).Observe(time.Since(start).Seconds())
	}()

	// 【构建上传选项】
	uploadOption := &operation.UploadOption{
		UploadUrl:         urlLocation,      // Volume Server 地址
		Filename:          fileName,         // 文件名
		Cipher:            fs.option.Cipher, // 是否启用加密
		IsInputCompressed: false,            // 数据未压缩
		MimeType:          contentType,      // MIME 类型
		PairMap:           pairMap,          // 额外的 HTTP 头
		Jwt:               auth,             // JWT 认证令牌
	}

	// 【创建上传器】
	uploader, err := operation.NewUploader()
	if err != nil {
		return nil, err, []byte{}
	}

	// 【执行上传】
	// Upload 会自动处理重试逻辑
	uploadResult, err, data := uploader.Upload(ctx, limitedReader, uploadOption)

	// 【统计指标：重试次数】
	if uploadResult != nil && uploadResult.RetryCount > 0 {
		stats.FilerHandlerCounter.WithLabelValues(stats.ChunkUploadRetry).Add(float64(uploadResult.RetryCount))
	}

	return uploadResult, err, data
}

// dataToChunk 直接将内存数据写入 Volume，产生新的 chunk 元信息
//
// 参数:
//   - ctx: 上下文
//   - fileName: 文件名
//   - contentType: MIME 类型
//   - data: 要上传的数据（字节数组）
//   - chunkOffset: chunk 在文件中的偏移量
//   - so: 存储选项
//
// 返回:
//   - []*filer_pb.FileChunk: 生成的 chunk 列表（通常只有一个）
//   - error: 上传错误
//
// 功能:
//   - 常用于 append 或内联写入场景
//   - 不处理 SSE 加密，内部调用 dataToChunkWithSSE(r=nil)
func (fs *FilerServer) dataToChunk(ctx context.Context, fileName, contentType string, data []byte, chunkOffset int64, so *operation.StorageOption) ([]*filer_pb.FileChunk, error) {
	return fs.dataToChunkWithSSE(ctx, nil, fileName, contentType, data, chunkOffset, so)
}

// dataToChunkWithSSE 在 dataToChunk 基础上，增加对 SSE 加密请求头的解析和透传
//
// 参数:
//   - ctx: 上下文
//   - r: HTTP 请求对象（用于提取 SSE 加密头信息，可为 nil）
//   - fileName: 文件名
//   - contentType: MIME 类型
//   - data: 要上传的数据
//   - chunkOffset: chunk 在文件中的偏移量
//   - so: 存储选项
//
// 返回:
//   - []*filer_pb.FileChunk: 生成的 chunk 列表
//   - error: 上传错误
//
// 支持的 SSE 加密模式:
//   1. SSE-C (Server-Side Encryption with Customer-Provided Keys)
//      - 客户端提供加密密钥（通过请求头传递）
//      - 每个 chunk 保存独立的 IV 和 KeyMD5
//      - 请求头：x-amz-server-side-encryption-customer-algorithm、SeaweedFS-SSE-IV、x-amz-server-side-encryption-customer-key-MD5
//
//   2. SSE-KMS (Server-Side Encryption with KMS)
//      - 使用 KMS（密钥管理服务）托管密钥
//      - chunk 保存 KMS 密钥 ID 和相关元数据
//      - 请求头：SeaweedFS-SSE-KMS-Key
//
//   3. SSE-S3 (Server-Side Encryption with S3-Managed Keys)
//      - 使用 S3 托管的服务器端密钥
//      - 请求头：SeaweedFS-SSE-S3-Key
//
// 工作流程:
//   1. 从 Master 请求分配 fid
//   2. 将数据上传到 Volume Server
//   3. 如果请求包含 SSE 头，提取加密元数据并保存到 chunk 结构中
//   4. 返回包含加密信息的 chunk 对象
//
// 重试逻辑:
//   - 使用 util.Retry 自动重试失败的上传
//   - 失败的 chunk 会被记录到 failedFileChunks 中
func (fs *FilerServer) dataToChunkWithSSE(ctx context.Context, r *http.Request, fileName, contentType string, data []byte, chunkOffset int64, so *operation.StorageOption) ([]*filer_pb.FileChunk, error) {
	// 【步骤 1：创建数据读取器】
	dataReader := util.NewBytesReader(data)

	// 【步骤 2：初始化变量】
	// 如果分配文件 ID 失败，重试以获取不同的 file id
	var fileId, urlLocation string
	var auth security.EncodedJwt
	var uploadErr error
	var uploadResult *operation.UploadResult
	var failedFileChunks []*filer_pb.FileChunk

	// 【步骤 3：重试上传逻辑】
	err := util.Retry("filerDataToChunk", func() error {
		// 【步骤 3.1：从 Master 请求分配 fid】
		// 每个 chunk 都单独分配 fid，避免并发覆盖
		fileId, urlLocation, auth, uploadErr = fs.assignNewFileInfo(ctx, so)
		if uploadErr != nil {
			glog.V(4).InfofCtx(ctx, "retry later due to assign error: %v", uploadErr)
			stats.FilerHandlerCounter.WithLabelValues(stats.ChunkAssignRetry).Inc()
			return uploadErr
		}

		// 【步骤 3.2：将 chunk 上传至对应的 Volume Server】
		uploadResult, uploadErr, _ = fs.doUpload(ctx, urlLocation, dataReader, fileName, contentType, nil, auth)
		if uploadErr != nil {
			glog.V(4).InfofCtx(ctx, "retry later due to upload error: %v", uploadErr)
			stats.FilerHandlerCounter.WithLabelValues(stats.ChunkDoUploadRetry).Inc()
			// 记录失败的 chunk 信息，以便清理
			fid, _ := filer_pb.ToFileIdObject(fileId)
			fileChunk := filer_pb.FileChunk{
				FileId: fileId,
				Offset: chunkOffset,
				Fid:    fid,
			}
			failedFileChunks = append(failedFileChunks, &fileChunk)
			return uploadErr
		}
		return nil
	})
	if err != nil {
		glog.ErrorfCtx(ctx, "upload error: %v", err)
		return failedFileChunks, err
	}

	// 【步骤 4：检查上传结果】
	// 如果上传的数据大小为 0，说明是空 chunk，直接返回
	if uploadResult.Size == 0 {
		return nil, nil
	}

	// 【步骤 5：提取 SSE 加密元数据】
	// 从请求头中提取 SSE 相关元数据（如算法、Key ID 等）
	var sseType filer_pb.SSEType = filer_pb.SSEType_NONE
	var sseMetadata []byte

	if r != nil {

		// 【情况 1：SSE-KMS 加密】
		// 使用 KMS（密钥管理服务）托管密钥
		sseKMSHeaderValue := r.Header.Get(s3_constants.SeaweedFSSSEKMSKeyHeader)
		if sseKMSHeaderValue != "" {
			sseType = filer_pb.SSEType_SSE_KMS
			// Base64 解码 KMS 密钥信息
			if kmsData, err := base64.StdEncoding.DecodeString(sseKMSHeaderValue); err == nil {
				sseMetadata = kmsData
				glog.V(4).InfofCtx(ctx, "Storing SSE-KMS metadata for chunk %s at offset %d", fileId, chunkOffset)
			} else {
				glog.V(1).InfofCtx(ctx, "Failed to decode SSE-KMS metadata for chunk %s: %v", fileId, err)
			}
		} else if r.Header.Get(s3_constants.AmzServerSideEncryptionCustomerAlgorithm) != "" {
			// 【情况 2：SSE-C 加密】
			// 客户端提供加密密钥，为每个 chunk 构造独立元数据
			sseType = filer_pb.SSEType_SSE_C

			// 从请求头拿到 SSE-C 的密钥、IV 等信息
			sseIVHeader := r.Header.Get(s3_constants.SeaweedFSSSEIVHeader)
			keyMD5Header := r.Header.Get(s3_constants.AmzServerSideEncryptionCustomerKeyMD5)

			if sseIVHeader != "" && keyMD5Header != "" {
				// Base64 解码 IV（初始化向量）
				if ivData, err := base64.StdEncoding.DecodeString(sseIVHeader); err == nil {
					// SSE-C 的偏移必须与 chunkOffset 对齐，以便解密时生成正确 IV
					// 构造 SSE-C 元数据结构
					ssecMetadataStruct := struct {
						Algorithm  string `json:"algorithm"`   // 加密算法（AES256）
						IV         string `json:"iv"`          // 初始化向量（Base64 编码）
						KeyMD5     string `json:"keyMD5"`      // 密钥 MD5（用于验证）
						PartOffset int64  `json:"partOffset"`  // chunk 偏移量（用于计算 IV）
					}{
						Algorithm:  "AES256",
						IV:         base64.StdEncoding.EncodeToString(ivData),
						KeyMD5:     keyMD5Header,
						PartOffset: chunkOffset,
					}
					// 序列化为 JSON
					if ssecMetadata, serErr := json.Marshal(ssecMetadataStruct); serErr == nil {
						sseMetadata = ssecMetadata
					} else {
						glog.V(1).InfofCtx(ctx, "Failed to serialize SSE-C metadata for chunk %s: %v", fileId, serErr)
					}
				} else {
					glog.V(1).InfofCtx(ctx, "Failed to decode SSE-C IV for chunk %s: %v", fileId, err)
				}
			} else {
				glog.V(4).InfofCtx(ctx, "SSE-C chunk %s missing IV or KeyMD5 header", fileId)
			}
		} else if r.Header.Get(s3_constants.SeaweedFSSSES3Key) != "" {
			// 【情况 3：SSE-S3 加密】
			// 使用服务器托管密钥
			sseType = filer_pb.SSEType_SSE_S3

			// 从请求头提取 SSE-S3 所需的元数据
			sseS3Header := r.Header.Get(s3_constants.SeaweedFSSSES3Key)
			if sseS3Header != "" {
				if s3Data, err := base64.StdEncoding.DecodeString(sseS3Header); err == nil {
					// SSE-S3 也保持 chunk 级别的 metadata，以统一处理逻辑
					glog.V(4).InfofCtx(ctx, "Storing SSE-S3 metadata for chunk %s at offset %d", fileId, chunkOffset)
					sseMetadata = s3Data
				} else {
					glog.V(1).InfofCtx(ctx, "Failed to decode SSE-S3 metadata for chunk %s: %v", fileId, err)
				}
			}
		}
	}

	// 【步骤 6：创建 chunk 对象】
	// 如果存在 SSE 元数据，创建包含加密信息的 chunk
	var chunk *filer_pb.FileChunk
	if sseType != filer_pb.SSEType_NONE {
		chunk = uploadResult.ToPbFileChunkWithSSE(fileId, chunkOffset, time.Now().UnixNano(), sseType, sseMetadata)
	} else {
		chunk = uploadResult.ToPbFileChunk(fileId, chunkOffset, time.Now().UnixNano())
	}

	return []*filer_pb.FileChunk{chunk}, nil
}
