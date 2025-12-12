package erasure_coding

import (
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// Interval 表示数据在 EC 分片中的一个连续区间
// 用于定位 Needle 数据在分片文件中的位置
type Interval struct {
	BlockIndex          int        // 块索引（在大块或小块序列中的位置）
	InnerBlockOffset    int64      // 块内偏移量（数据在该块内的起始位置）
	Size                types.Size // 该区间的数据大小
	IsLargeBlock        bool       // 是否为大块（true=大块，false=小块）
	LargeBlockRowsCount int        // 大块行数（用于计算小块的偏移量）
}

// LocateData 定位数据在 EC 分片中的位置
// 将一段连续的数据映射到多个区间（可能跨越多个块）
//
// 参数:
//   - largeBlockLength: 大块长度（通常为 1GB）
//   - smallBlockLength: 小块长度（通常为 1MB）
//   - shardDatSize: 单个分片的数据大小
//   - offset: 数据在原始文件中的偏移量
//   - size: 数据大小
// 返回值:
//   - intervals: 数据区间列表（可能跨越多个块）
// 工作原理:
//   EC 编码采用分段策略，文件被分成大块和小块
//   大块用于主体数据，小块用于尾部数据
//   此函数将原始文件中的数据位置映射到分片文件中的具体位置
func LocateData(largeBlockLength, smallBlockLength int64, shardDatSize int64, offset int64, size types.Size) (intervals []Interval) {
	// 定位起始偏移量所在的块
	blockIndex, isLargeBlock, nLargeBlockRows, innerBlockOffset := locateOffset(largeBlockLength, smallBlockLength, shardDatSize, offset)

	// 循环处理数据，可能跨越多个块
	for size > 0 {
		interval := Interval{
			BlockIndex:          blockIndex,
			InnerBlockOffset:    innerBlockOffset,
			IsLargeBlock:        isLargeBlock,
			LargeBlockRowsCount: int(nLargeBlockRows),
		}

		// 计算当前块的剩余空间
		blockRemaining := largeBlockLength - innerBlockOffset
		if !isLargeBlock {
			blockRemaining = smallBlockLength - innerBlockOffset
		}

		// 如果剩余数据可以完全放入当前块
		if int64(size) <= blockRemaining {
			interval.Size = size
			intervals = append(intervals, interval)
			return
		}

		// 当前块放不下所有数据，填满当前块
		interval.Size = types.Size(blockRemaining)
		intervals = append(intervals, interval)

		// 更新剩余大小和块索引
		size -= interval.Size
		blockIndex += 1

		// 检查是否从大块区域切换到小块区域
		if isLargeBlock && blockIndex == interval.LargeBlockRowsCount*DataShardsCount {
			isLargeBlock = false
			blockIndex = 0
		}
		innerBlockOffset = 0 // 新块从头开始

	}
	return
}

// locateOffset 定位偏移量在大块或小块区域中的位置
// 参数:
//   - largeBlockLength: 大块长度
//   - smallBlockLength: 小块长度
//   - shardDatSize: 分片数据大小
//   - offset: 原始文件中的偏移量
// 返回值:
//   - blockIndex: 块索引
//   - isLargeBlock: 是否在大块区域
//   - nLargeBlockRows: 大块行数
//   - innerBlockOffset: 块内偏移量
func locateOffset(largeBlockLength, smallBlockLength int64, shardDatSize int64, offset int64) (blockIndex int, isLargeBlock bool, nLargeBlockRows int64, innerBlockOffset int64) {
	// 计算一行大块的总大小（所有数据分片的大块大小之和）
	// 例如：10 个数据分片 × 1GB = 10GB
	largeRowSize := largeBlockLength * DataShardsCount
	// 计算大块行数
	nLargeBlockRows = (shardDatSize - 1) / largeBlockLength

	// 判断偏移量是否在大块区域
	if offset < nLargeBlockRows*largeRowSize {
		isLargeBlock = true
		blockIndex, innerBlockOffset = locateOffsetWithinBlocks(largeBlockLength, offset)
		return
	}

	// 偏移量在小块区域
	isLargeBlock = false
	offset -= nLargeBlockRows * largeRowSize // 减去大块区域的大小
	blockIndex, innerBlockOffset = locateOffsetWithinBlocks(smallBlockLength, offset)
	return
}

// locateOffsetWithinBlocks 在块序列中定位偏移量
// 参数:
//   - blockLength: 块长度
//   - offset: 偏移量
// 返回值:
//   - blockIndex: 块索引
//   - innerBlockOffset: 块内偏移量
func locateOffsetWithinBlocks(blockLength int64, offset int64) (blockIndex int, innerBlockOffset int64) {
	blockIndex = int(offset / blockLength)
	innerBlockOffset = offset % blockLength
	return
}

// ToShardIdAndOffset 将区间转换为分片 ID 和分片文件中的偏移量
// 参数:
//   - largeBlockSize: 大块大小
//   - smallBlockSize: 小块大小
// 返回值:
//   - ShardId: 分片 ID（0-9 为数据分片）
//   - int64: 分片文件中的偏移量
// 工作原理:
//   EC 编码时，数据按行交错存储到各个分片
//   例如：第 0 行的数据分布在分片 0-9 中，第 1 行的数据也分布在分片 0-9 中
//   此函数将块索引转换为具体的分片 ID 和该分片中的偏移量
func (interval Interval) ToShardIdAndOffset(largeBlockSize, smallBlockSize int64) (ShardId, int64) {
	// 初始偏移量为块内偏移量
	ecFileOffset := interval.InnerBlockOffset
	// 计算行索引（第几行）
	rowIndex := interval.BlockIndex / DataShardsCount

	if interval.IsLargeBlock {
		// 大块区域：每行占用 largeBlockSize
		ecFileOffset += int64(rowIndex) * largeBlockSize
	} else {
		// 小块区域：需要加上所有大块的大小，再加上小块行的大小
		ecFileOffset += int64(interval.LargeBlockRowsCount)*largeBlockSize + int64(rowIndex)*smallBlockSize
	}

	// 分片 ID = 块索引 % 数据分片数
	// 例如：blockIndex=0 -> 分片 0，blockIndex=10 -> 分片 0，blockIndex=5 -> 分片 5
	ecFileIndex := interval.BlockIndex % DataShardsCount
	return ShardId(ecFileIndex), ecFileOffset
}
