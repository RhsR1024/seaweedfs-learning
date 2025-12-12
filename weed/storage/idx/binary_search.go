// Package idx 提供 SeaweedFS 索引文件（.idx）的操作功能
// 本文件实现了在索引数据中进行二分查找的算法
//
// ============================================================================
// 索引文件格式
// ============================================================================
//
// .idx 文件是 SeaweedFS Volume 的索引文件，每个条目包含：
// - NeedleId (8 字节): Needle 的唯一标识符
// - Offset (4 字节): Needle 在 .dat 文件中的偏移量（以 8 字节为单位）
// - Size (4 字节): Needle 的大小
//
// 总计每个条目 16 字节（types.NeedleMapEntrySize）
//
// ============================================================================
// 二分查找应用场景
// ============================================================================
//
// 1. 在排序后的 .ecx 文件中查找 Needle
// 2. 在内存中查找满足特定条件的索引条目
// 3. 辅助数据压缩和垃圾回收等操作
//
// ============================================================================
package idx

import (
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// FirstInvalidIndex 在索引数据中查找第一个不满足条件的条目位置
//
// 算法说明：
// 这是一个变体的二分查找算法，用于找到第一个使 lessThanOrEqualToFn 返回 false 的位置。
// 等价于在排序数组中找到"第一个大于给定值的元素位置"。
//
// 使用场景：
// 1. 查找所有偏移量小于等于某个值的 Needle（用于 Vacuum 压缩）
// 2. 查找所有 NeedleId 小于等于某个值的条目
// 3. 数据分区和范围查询
//
// 算法复杂度：O(log n)，其中 n = len(bytes) / NeedleMapEntrySize
//
// 参数:
//   - bytes: 索引数据的字节数组，必须是 NeedleMapEntrySize 的整数倍
//   - lessThanOrEqualToFn: 判断函数，返回 true 表示当前条目满足条件（应该在结果左侧）
//     函数签名：func(key NeedleId, offset Offset, size Size) (bool, error)
//
// 返回:
//   - int: 第一个不满足条件的索引（如果所有条目都满足，返回条目总数）
//   - error: lessThanOrEqualToFn 返回的错误
//
// 示例：
//
//	假设有以下排序后的索引条目（按 NeedleId 排序）：
//	  索引 0: NeedleId=100
//	  索引 1: NeedleId=200
//	  索引 2: NeedleId=300
//	  索引 3: NeedleId=400
//
//	查找第一个 NeedleId > 250 的位置：
//	  lessThanOrEqualToFn = func(key, ...) { return key <= 250 }
//	  结果：返回 2（索引 2 的 NeedleId=300 > 250）
func FirstInvalidIndex(bytes []byte, lessThanOrEqualToFn func(key types.NeedleId, offset types.Offset, size types.Size) (bool, error)) (int, error) {
	// 计算索引条目的数量
	// 每个条目 16 字节（NeedleMapEntrySize）
	entryCount := len(bytes) / types.NeedleMapEntrySize

	// 初始化二分查找的边界
	// left: 搜索范围的左边界（包含）
	// right: 搜索范围的右边界（包含）
	left, right := 0, entryCount-1

	// index: 结果索引，初始化为"所有条目都满足条件"的情况
	// 如果找不到不满足条件的条目，返回 entryCount（即超出最后一个有效索引）
	index := right + 1

	// 标准二分查找循环
	for left <= right {
		// 计算中间位置
		// 使用 left + (right-left)>>1 而不是 (left+right)/2，避免整数溢出
		mid := left + (right-left)>>1

		// 计算中间条目在字节数组中的起始位置
		loc := mid * types.NeedleMapEntrySize

		// ========== 解析索引条目 ==========
		// 从字节数组中提取 NeedleId、Offset、Size
		//
		// 条目布局（16 字节）：
		// +--------+--------+------+
		// |NeedleId| Offset | Size |
		// | 8 bytes| 4 bytes|4bytes|
		// +--------+--------+------+

		// 提取 NeedleId（前 8 字节）
		key := types.BytesToNeedleId(bytes[loc : loc+types.NeedleIdSize])

		// 提取 Offset（接下来 4 字节）
		offset := types.BytesToOffset(bytes[loc+types.NeedleIdSize : loc+types.NeedleIdSize+types.OffsetSize])

		// 提取 Size（最后 4 字节）
		size := types.BytesToSize(bytes[loc+types.NeedleIdSize+types.OffsetSize : loc+types.NeedleIdSize+types.OffsetSize+types.SizeSize])

		// ========== 调用判断函数 ==========
		// res = true: 当前条目满足条件，继续在右半部分搜索
		// res = false: 当前条目不满足条件，记录位置并在左半部分搜索
		res, err := lessThanOrEqualToFn(key, offset, size)
		if err != nil {
			// 判断函数返回错误，终止搜索
			return -1, err
		}

		if res {
			// 当前条目满足条件，第一个不满足的在右边
			// 将搜索范围缩小到右半部分
			left = mid + 1
		} else {
			// 当前条目不满足条件，可能是第一个不满足的
			// 记录当前位置，继续在左半部分搜索更小的索引
			index = mid
			right = mid - 1
		}
	}

	// 返回第一个不满足条件的索引
	// 可能的返回值范围：0 ~ entryCount
	// 返回 entryCount 表示所有条目都满足条件
	return index, nil
}
