// Package erasure_coding 纠删码模块测试
// 本文件测试 EC Volume 的 Needle 定位和读取功能
//
// ============================================================================
// EC Volume 读取流程
// ============================================================================
//
// 1. 从 .ecx 文件（排序索引）查找 Needle 位置
// 2. 使用 LocateData 计算数据在 EC 分片中的位置
// 3. 从对应分片读取数据
//
// .ecx 文件特点：
// - 按 NeedleId 排序，支持二分查找
// - 每个条目包含：NeedleId(8) + Offset(4) + Size(4) = 16 字节
// - 相比普通 .idx 文件，.ecx 文件查询效率更高
//
// ============================================================================
package erasure_coding

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// TestPositioning 测试 EC Volume 的 Needle 定位功能
//
// 这个测试需要一个实际的 .ecx 文件（389.ecx），验证：
// 1. SearchNeedleFromSortedIndex 能正确从排序索引中查找 Needle
// 2. LocateData 能正确计算 Needle 在 EC 分片中的位置
// 3. ToShardIdAndOffset 能正确计算分片 ID 和偏移量
//
// 注意：这个测试依赖外部文件 "389.ecx"，在没有该文件的环境中会失败
func TestPositioning(t *testing.T) {
	// ========== 步骤 1：打开 .ecx 排序索引文件 ==========
	// .ecx 文件是 EC Volume 的核心索引文件
	// 文件名格式：{volumeId}.ecx
	// 内容：按 NeedleId 排序的 (NeedleId, Offset, Size) 条目
	ecxFile, err := os.OpenFile("389.ecx", os.O_RDONLY, 0)
	if err != nil {
		t.Errorf("failed to open ecx file: %v", err)
	}
	defer ecxFile.Close()

	// 获取文件大小，用于计算条目数量
	stat, _ := ecxFile.Stat()
	fileSize := stat.Size()

	// ========== 步骤 2：定义测试用例 ==========
	// 测试多个已知的 Needle，验证定位结果
	tests := []struct {
		needleId string // Needle ID（十六进制字符串）
		offset   int64  // 预期的偏移量
		size     int    // 预期的大小
	}{
		// 测试用例 1：Needle ID = 0f0edb92
		// 位于原始数据偏移 31300679656 处，大小 1167 字节
		{needleId: "0f0edb92", offset: 31300679656, size: 1167},

		// 测试用例 2：Needle ID = 0ef7d7f8
		// 位于原始数据偏移 11513014944 处，大小 66044 字节
		{needleId: "0ef7d7f8", offset: 11513014944, size: 66044},
	}

	// ========== 步骤 3：执行测试 ==========
	for _, test := range tests {
		// 将十六进制字符串解析为 NeedleId
		needleId, _ := types.ParseNeedleId(test.needleId)

		// 从排序索引文件中二分查找 Needle
		// SearchNeedleFromSortedIndex 使用二分查找，时间复杂度 O(log n)
		// 参数：
		//   - ecxFile: 排序索引文件
		//   - fileSize: 文件大小（用于计算条目数）
		//   - needleId: 要查找的 Needle ID
		//   - nil: 版本参数（可选）
		offset, size, err := SearchNeedleFromSortedIndex(ecxFile, fileSize, needleId, nil)
		assert.Equal(t, nil, err, "SearchNeedleFromSortedIndex")

		// 打印查找结果
		// ToActualOffset() 将逻辑偏移转换为文件中的实际字节偏移
		fmt.Printf("offset: %d size: %d\n", offset.ToActualOffset(), size)
	}

	// ========== 步骤 4：测试完整的数据定位流程 ==========
	// 使用另一个 Needle ID 测试从索引查找到分片定位的完整流程
	needleId, _ := types.ParseNeedleId("0f087622")

	// 查找 Needle 在原始数据中的位置
	offset, size, err := SearchNeedleFromSortedIndex(ecxFile, fileSize, needleId, nil)
	assert.Equal(t, nil, err, "SearchNeedleFromSortedIndex")
	fmt.Printf("offset: %d size: %d\n", offset.ToActualOffset(), size)

	// ========== 步骤 5：计算 EC 分片位置 ==========
	// shardEcdFileSize：单个 EC 分片文件的大小（约 1GB）
	// 这个值用于计算数据在分片中的确切位置
	var shardEcdFileSize int64 = 1118830592 // 约 1GB

	// 计算 Needle 的实际大小（包含元数据开销）
	// GetActualSize 考虑了 Needle 版本和元数据字段的影响
	actualSize := needle.GetActualSize(size, needle.GetCurrentVersion())

	// 使用 LocateData 计算数据在 EC 分片中的位置
	// 返回一系列 Interval，描述数据如何分布在各分片中
	intervals := LocateData(
		ErasureCodingLargeBlockSize,  // 大块大小：1GB
		ErasureCodingSmallBlockSize,  // 小块大小：1MB
		shardEcdFileSize,             // 单个分片文件大小
		offset.ToActualOffset(),      // 原始数据偏移量
		types.Size(actualSize),       // 数据大小
	)

	// ========== 步骤 6：打印分片位置信息 ==========
	// 遍历每个数据区间，输出其在 EC 分片中的具体位置
	for _, interval := range intervals {
		// ToShardIdAndOffset 将 Interval 转换为具体的分片 ID 和偏移量
		// shardId: 数据所在的分片编号（0-13）
		// shardOffset: 数据在该分片文件中的字节偏移
		shardId, shardOffset := interval.ToShardIdAndOffset(
			ErasureCodingLargeBlockSize,
			ErasureCodingSmallBlockSize,
		)
		fmt.Printf("interval: %+v, shardId: %d, shardOffset: %d\n", interval, shardId, shardOffset)
	}
}
