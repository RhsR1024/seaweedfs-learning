// Package idx 提供 SeaweedFS 索引文件（.idx）的操作功能
// 本文件实现了索引文件的顺序遍历功能
//
// ============================================================================
// 索引文件遍历的重要性
// ============================================================================
//
// 遍历索引文件是 SeaweedFS 中多个关键操作的基础：
//
// 1. Volume 加载：启动时遍历 .idx 文件，将索引加载到内存
// 2. Vacuum 压缩：遍历索引，识别和清理已删除的 Needle
// 3. 数据恢复：从索引重建 Needle 映射
// 4. EC 编码：遍历索引生成 .ecx 排序索引文件
// 5. 数据验证：检查索引与数据文件的一致性
//
// ============================================================================
// 性能优化
// ============================================================================
//
// - 批量读取：每次读取 RowsToRead（1024）个条目，减少 I/O 次数
// - 流式处理：使用回调函数处理每个条目，避免一次性加载全部数据
// - 支持断点续读：可以从指定位置开始遍历
//
// ============================================================================
package idx

import (
	"io"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// WalkIndexFile 遍历索引文件，对每个条目调用回调函数
//
// 这是 SeaweedFS 索引处理的核心函数，用于顺序扫描 .idx 文件。
// 通过回调函数处理每个索引条目，支持中途终止遍历。
//
// 工作原理：
// 1. 从 startFrom 位置开始读取索引文件
// 2. 每次批量读取 RowsToRead 个条目（提高 I/O 效率）
// 3. 解析每个条目，调用回调函数处理
// 4. 回调返回错误时停止遍历
//
// 参数:
//   - r: 索引文件的 ReaderAt 接口，支持随机读取
//   - startFrom: 起始条目编号（从 0 开始）
//   - fn: 回调函数，处理每个索引条目
//     函数签名：func(key NeedleId, offset Offset, size Size) error
//     返回 nil 继续遍历，返回错误停止遍历
//
// 返回:
//   - error: 遍历过程中的错误（文件读取错误或回调函数返回的错误）
//
// 使用示例：
//
//	// 统计索引文件中的条目数量
//	count := 0
//	err := WalkIndexFile(idxFile, 0, func(key NeedleId, offset Offset, size Size) error {
//	    count++
//	    return nil
//	})
//
//	// 查找特定 NeedleId
//	var foundOffset Offset
//	targetId := types.NeedleId(12345)
//	err := WalkIndexFile(idxFile, 0, func(key NeedleId, offset Offset, size Size) error {
//	    if key == targetId {
//	        foundOffset = offset
//	        return errors.New("found") // 找到后停止遍历
//	    }
//	    return nil
//	})
func WalkIndexFile(r io.ReaderAt, startFrom uint64, fn func(key types.NeedleId, offset types.Offset, size types.Size) error) error {
	// 计算起始读取位置（字节偏移）
	// startFrom 是条目编号，需要乘以每个条目的大小
	readerOffset := int64(startFrom * types.NeedleMapEntrySize)

	// 分配批量读取缓冲区
	// 每次读取 RowsToRead（1024）个条目，共 16KB
	// 这是 I/O 效率和内存使用的平衡点
	bytes := make([]byte, types.NeedleMapEntrySize*RowsToRead)

	// 首次读取
	count, e := r.ReadAt(bytes, readerOffset)

	// 处理空文件或读取位置超出文件末尾的情况
	if count == 0 && e == io.EOF {
		return nil // 文件为空或已到末尾，正常结束
	}

	// 记录读取日志（调试级别 3）
	glog.V(3).Infof("readerOffset %d count %d err: %v", readerOffset, count, e)

	// 更新读取位置
	readerOffset += int64(count)

	// 声明循环中使用的变量
	var (
		key    types.NeedleId // 当前条目的 NeedleId
		offset types.Offset   // 当前条目的偏移量
		size   types.Size     // 当前条目的大小
		i      int            // 缓冲区内的当前位置
	)

	// 主循环：持续读取直到文件结束或发生错误
	// 循环条件：有数据读取（count > 0）且没有严重错误（或只是 EOF）
	for count > 0 && e == nil || e == io.EOF {
		// 内循环：处理当前批次中的所有完整条目
		// 条件：缓冲区中还有完整的条目（至少 NeedleMapEntrySize 字节）
		for i = 0; i+types.NeedleMapEntrySize <= count; i += types.NeedleMapEntrySize {
			// 解析当前条目
			// IdxFileEntry 从字节数组中提取 NeedleId、Offset、Size
			key, offset, size = IdxFileEntry(bytes[i : i+types.NeedleMapEntrySize])

			// 调用回调函数处理当前条目
			if e = fn(key, offset, size); e != nil {
				// 回调返回错误，停止遍历
				// 这可以用于提前终止（如找到目标后）
				return e
			}
		}

		// 检查是否已到文件末尾
		if e == io.EOF {
			return nil // 正常结束
		}

		// 读取下一批数据
		count, e = r.ReadAt(bytes, readerOffset)
		glog.V(3).Infof("readerOffset %d count %d err: %v", readerOffset, count, e)
		readerOffset += int64(count)
	}

	// 返回最后的错误（如果不是 EOF）
	return e
}

// IdxFileEntry 从字节数组中解析单个索引条目
//
// 索引条目的二进制格式（16 字节）：
//
//	+----------------+------------+----------+
//	|   NeedleId     |   Offset   |   Size   |
//	|   8 bytes      |   4 bytes  |  4 bytes |
//	+----------------+------------+----------+
//	|     uint64     |   uint32   |  uint32  |
//	| (big-endian)   |(big-endian)|(big-endian)|
//	+----------------+------------+----------+
//
// 字段说明：
//   - NeedleId: Needle 的唯一标识符，用于查找和定位
//   - Offset: Needle 在 .dat 文件中的位置（以 8 字节为单位）
//     实际字节偏移 = Offset * 8（NeedlePaddingSize）
//   - Size: Needle 的原始数据大小
//     Size = 0 表示该 Needle 已被删除（墓碑标记）
//
// 参数:
//   - bytes: 16 字节的原始数据
//
// 返回:
//   - key: NeedleId
//   - offset: 文件偏移量
//   - size: 数据大小
func IdxFileEntry(bytes []byte) (key types.NeedleId, offset types.Offset, size types.Size) {
	// 解析 NeedleId（前 8 字节）
	// BytesToNeedleId 将大端序字节转换为 uint64
	key = types.BytesToNeedleId(bytes[:types.NeedleIdSize])

	// 解析 Offset（中间 4 字节）
	// BytesToOffset 将大端序字节转换为 Offset 类型
	offset = types.BytesToOffset(bytes[types.NeedleIdSize : types.NeedleIdSize+types.OffsetSize])

	// 解析 Size（最后 4 字节）
	// BytesToSize 将大端序字节转换为 Size 类型
	// 注意：Size = 0 是删除标记（tombstone）
	size = types.BytesToSize(bytes[types.NeedleIdSize+types.OffsetSize : types.NeedleIdSize+types.OffsetSize+types.SizeSize])

	return
}

// 常量定义
const (
	// RowsToRead 每次批量读取的条目数量
	// 1024 个条目 × 16 字节/条目 = 16KB
	//
	// 选择 1024 的原因：
	// 1. 16KB 是常见的文件系统块大小倍数，I/O 效率高
	// 2. 不会占用过多内存
	// 3. 减少系统调用次数，提高顺序读取性能
	RowsToRead = 1024
)
