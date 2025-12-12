// Package needle 提供 SeaweedFS 的 Needle 存储结构和相关操作
// 本文件实现 Needle 的分页读取功能（按需读取）
//
// ============================================================================
// 分页读取的应用场景
// ============================================================================
//
// 分页读取主要用于以下场景：
//
// 1. 大文件的 Range 请求（HTTP Range Header）
//    - 客户端只请求文件的一部分数据
//    - 不需要将整个文件加载到内存
//
// 2. 视频/音频流媒体
//    - 播放器按需请求数据块
//    - 支持快进、跳转等操作
//
// 3. 断点续传
//    - 从指定偏移量继续下载
//
// 4. 内存优化
//    - 避免大文件一次性加载
//    - 降低内存峰值使用
//
// ============================================================================
// Needle 文件布局（用于理解偏移量计算）
// ============================================================================
//
// +-------------------+
// |  NeedleHeader     |  <- volumeOffset 指向这里
// |  (16 bytes)       |
// +-------------------+
// |  DataSize         |  <- 4 bytes
// |  (uint32)         |
// +-------------------+
// |  Data             |  <- needleOffset 相对于这里
// |  (DataSize bytes) |
// +-------------------+
// |  Metadata         |
// |  (flags, name...) |
// +-------------------+
// |  Checksum         |
// |  (4 bytes)        |
// +-------------------+
// |  Padding          |
// +-------------------+
//
// ============================================================================
package needle

import (
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// ReadNeedleData 从 Needle 中读取指定范围的数据（分页读取）
//
// 这个方法用于在不加载整个 Needle 数据到内存的情况下，
// 读取 Needle 中特定偏移量和长度的数据。
//
// 适用场景：
// - HTTP Range 请求（如视频播放时的跳转）
// - 大文件的部分下载
// - 内存受限环境下的数据访问
//
// 参数:
//   - r: 后端存储文件接口（支持随机读取）
//   - volumeOffset: Needle 在 Volume 文件中的起始偏移（指向 NeedleHeader）
//   - data: 数据缓冲区，读取的数据将写入此处
//   - needleOffset: 在 Needle 数据部分内的偏移量（从 Data 字段开始计算）
//
// 返回:
//   - count: 实际读取的字节数
//   - err: 读取错误（如 EOF、I/O 错误等）
//
// 偏移量计算示例：
//
//	假设要读取 Needle 数据的第 100 字节开始的 50 字节：
//	- volumeOffset = Needle 在文件中的位置（如 1000）
//	- needleOffset = 100（从 Data 字段开始的偏移）
//	- len(data) = 50（要读取的字节数）
//	实际读取位置 = 1000 + 16（Header）+ 4（DataSize）+ 100 = 1120
func (n *Needle) ReadNeedleData(r backend.BackendStorageFile, volumeOffset int64, data []byte, needleOffset int64) (count int, err error) {
	// 计算可读取的数据大小
	// 确保不超过：1. 缓冲区大小  2. Needle 剩余数据
	sizeToRead := min(int64(len(data)), int64(n.DataSize)-needleOffset)

	// 如果没有数据可读（超出范围或已到末尾）
	if sizeToRead <= 0 {
		return 0, io.EOF
	}

	// 计算实际的文件读取偏移量
	// startOffset = volumeOffset + NeedleHeaderSize + DataSizeSize + needleOffset
	// 其中：
	//   - NeedleHeaderSize = 16（Cookie + NeedleId + Size）
	//   - DataSizeSize = 4（存储 DataSize 的 4 字节）
	//   - needleOffset = 用户请求的数据偏移
	startOffset := volumeOffset + NeedleHeaderSize + DataSizeSize + needleOffset

	// 执行随机读取
	count, err = r.ReadAt(data[:sizeToRead], startOffset)

	// 处理 EOF 情况：如果读取了预期数量的数据，忽略 EOF 错误
	// 这是因为 ReadAt 在读到文件末尾时会同时返回数据和 EOF
	if err == io.EOF && int64(count) == sizeToRead {
		err = nil
	}

	// 记录错误日志（包含详细的调试信息）
	if err != nil {
		fileSize, _, _ := r.GetStat()
		glog.Errorf("%s read %d %d size %d at offset %d fileSize %d: %v",
			r.Name(),      // 文件名
			n.Id,          // Needle ID
			needleOffset,  // Needle 内偏移
			sizeToRead,    // 请求读取大小
			volumeOffset,  // Volume 内偏移
			fileSize,      // 文件总大小
			err)           // 错误信息
	}
	return
}

// ReadNeedleMeta 读取 Needle 的元数据（不包含实际数据）
//
// 这个方法用于只需要元数据而不需要实际文件内容的场景，如：
// - 检查文件是否存在
// - 获取文件大小、名称、MIME 类型等
// - 验证文件完整性
//
// 读取的元数据包括（取决于 Needle 版本）：
// - DataSize: 数据大小
// - Flags: 标志位
// - Name: 文件名
// - Mime: MIME 类型
// - LastModified: 最后修改时间
// - Ttl: 生存时间
// - Pairs: 额外的键值对
// - Checksum: 校验和
// - AppendAtNs: 追加时间戳（Version3）
//
// 参数:
//   - r: 后端存储文件接口
//   - offset: Needle 在 Volume 文件中的偏移量
//   - size: Needle 的大小（来自索引）
//   - version: Needle 版本（Version1/Version2/Version3）
//
// 返回:
//   - err: 读取错误
func (n *Needle) ReadNeedleMeta(r backend.BackendStorageFile, offset int64, size Size, version Version) (err error) {
	// 使用 defer + recover 捕获可能的 panic
	// 这是一个防御性编程措施，防止切片越界等导致程序崩溃
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic occurred: %+v", r)
		}
	}()

	// ========== 步骤 1：读取 Header + DataSize ==========
	// 先读取固定长度的头部信息
	bytes := make([]byte, NeedleHeaderSize+DataSizeSize)

	count, err := r.ReadAt(bytes, offset)

	// 处理 EOF：如果读取了预期数量，忽略 EOF
	if err == io.EOF && count == NeedleHeaderSize+DataSizeSize {
		err = nil
	}
	if count != NeedleHeaderSize+DataSizeSize || err != nil {
		return err
	}

	// ========== 步骤 2：解析 Header ==========
	// ParseNeedleHeader 解析 Cookie、Id、Size
	n.ParseNeedleHeader(bytes)

	// 验证大小一致性
	// 检查从头部读取的 Size 是否与索引记录的 size 匹配
	if n.Size != size {
		// 特殊情况：32 位偏移且在有效范围内时报错
		if OffsetSize == 4 && offset < int64(MaxPossibleVolumeSize) {
			return ErrorSizeMismatch
		}
	}

	// ========== 步骤 3：解析 DataSize ==========
	// DataSize 是实际数据的长度（不包含元数据）
	n.DataSize = util.BytesToUint32(bytes[NeedleHeaderSize : NeedleHeaderSize+DataSizeSize])

	// ========== 步骤 4：计算元数据的偏移和大小 ==========
	// startOffset: 元数据开始位置
	// 对于有效的 size，跳过 Header + DataSize + Data
	startOffset := offset + NeedleHeaderSize
	if size.IsValid() {
		startOffset = offset + NeedleHeaderSize + DataSizeSize + int64(n.DataSize)
	}

	// 计算 Needle 的实际总大小（包含 padding）
	dataSize := GetActualSize(size, version)

	// stopOffset: Needle 结束位置
	stopOffset := offset + dataSize

	// metaSize: 元数据部分的大小
	metaSize := stopOffset - startOffset

	// ========== 步骤 5：读取元数据 ==========
	metaSlice := make([]byte, int(metaSize))

	count, err = r.ReadAt(metaSlice, startOffset)
	if err != nil && int64(count) == metaSize {
		err = nil
	}
	if err != nil {
		return err
	}

	// ========== 步骤 6：解析元数据 ==========
	var index int
	if size.IsValid() {
		// 解析 Version2/Version3 的非数据元数据
		// 包括 Flags、Name、Mime、LastModified、Ttl、Pairs
		index, err = n.readNeedleDataVersion2NonData(metaSlice)
	}

	// 解析 Needle 尾部（Checksum、AppendAtNs、Padding）
	err = n.readNeedleTail(metaSlice[index:], version)
	return err
}

// min 返回两个 int64 中的较小值
//
// 用于计算实际可读取的数据量，确保不超出边界
func min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}
