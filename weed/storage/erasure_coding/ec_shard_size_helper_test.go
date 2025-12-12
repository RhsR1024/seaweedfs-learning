// Package erasure_coding 纠删码模块的测试文件
// 本文件测试 EC Shard 大小辅助函数的正确性
package erasure_coding

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

// TestShardSizeHelpers 测试 Shard 大小设置和获取辅助函数
//
// 测试背景：
// 在 SeaweedFS 的纠删码实现中，每个 Volume 可以被编码为多个 Shard（分片）。
// EcIndexBits 是一个位图字段，用于标识哪些 Shard 存在于当前节点。
// ShardSizes 是一个数组，按顺序存储存在的 Shard 的大小。
//
// 测试场景：
// 使用 EcIndexBits = 37（二进制：100101）表示 Shard 0、2、5 存在
// 验证 SetShardSize 和 GetShardSize 的正确映射关系
func TestShardSizeHelpers(t *testing.T) {
	// 创建一个测试消息，其中 Shard 0、2、5 存在
	// EcIndexBits = 0b100101 = 37
	// 位图解析：
	//   - 位 0（值 1）：Shard 0 存在
	//   - 位 1（值 0）：Shard 1 不存在
	//   - 位 2（值 1）：Shard 2 存在
	//   - 位 3（值 0）：Shard 3 不存在
	//   - 位 4（值 0）：Shard 4 不存在
	//   - 位 5（值 1）：Shard 5 存在
	msg := &master_pb.VolumeEcShardInformationMessage{
		Id:          123,             // Volume ID
		EcIndexBits: 37,              // 二进制: 100101, 表示 Shard 0, 2, 5 存在
	}

	// ========== 测试 SetShardSize 函数 ==========
	// 为存在的 Shard 设置大小，应该成功
	if !SetShardSize(msg, 0, 1000) {
		t.Error("Failed to set size for shard 0")
	}
	if !SetShardSize(msg, 2, 2000) {
		t.Error("Failed to set size for shard 2")
	}
	if !SetShardSize(msg, 5, 5000) {
		t.Error("Failed to set size for shard 5")
	}

	// 测试为不存在的 Shard 设置大小，应该失败
	// Shard 1 对应的位图位为 0，表示不存在
	if SetShardSize(msg, 1, 1500) {
		t.Error("Should not be able to set size for non-present shard 1")
	}

	// ========== 验证 ShardSizes 切片结构 ==========
	// ShardSizes 数组长度应该等于存在的 Shard 数量（3 个）
	// 数组按 Shard ID 顺序存储：[Shard0_size, Shard2_size, Shard5_size]
	if len(msg.ShardSizes) != 3 {
		t.Errorf("Expected ShardSizes length 3, got %d", len(msg.ShardSizes))
	}

	// ========== 测试 GetShardSize 函数 ==========
	// 获取存在的 Shard 的大小
	if size, found := GetShardSize(msg, 0); !found || size != 1000 {
		t.Errorf("Expected shard 0 size 1000, got %d (found: %v)", size, found)
	}
	if size, found := GetShardSize(msg, 2); !found || size != 2000 {
		t.Errorf("Expected shard 2 size 2000, got %d (found: %v)", size, found)
	}
	if size, found := GetShardSize(msg, 5); !found || size != 5000 {
		t.Errorf("Expected shard 5 size 5000, got %d (found: %v)", size, found)
	}

	// 测试获取不存在的 Shard 的大小，应该返回 found=false
	if size, found := GetShardSize(msg, 1); found {
		t.Errorf("Should not find shard 1, but got size %d", size)
	}

	// ========== 验证数组顺序正确性 ==========
	// 最终验证：直接访问 ShardSizes 切片，确认顺序
	if len(msg.ShardSizes) != 3 {
		t.Errorf("Expected 3 shard sizes in slice, got %d", len(msg.ShardSizes))
	}

	// ShardSizes 按 Shard ID 升序排列：
	// 索引 0 -> Shard 0 -> 1000
	// 索引 1 -> Shard 2 -> 2000
	// 索引 2 -> Shard 5 -> 5000
	expectedSizes := []int64{1000, 2000, 5000}
	for i, expectedSize := range expectedSizes {
		if i < len(msg.ShardSizes) && msg.ShardSizes[i] != expectedSize {
			t.Errorf("Expected ShardSizes[%d] = %d, got %d", i, expectedSize, msg.ShardSizes[i])
		}
	}
}

// TestShardBitsHelpers 测试 ShardBits 位图操作辅助函数
//
// ShardBits 是一个封装了位图操作的类型，提供以下功能：
// 1. ShardIdToIndex: 将 Shard ID 映射到 ShardSizes 数组索引
// 2. IndexToShardId: 将 ShardSizes 数组索引映射回 Shard ID
// 3. EachSetIndex: 遍历所有存在的 Shard ID
//
// 这些函数对于正确访问紧凑存储的 ShardSizes 数组至关重要
func TestShardBitsHelpers(t *testing.T) {
	// 测试 EcIndexBits = 37（二进制：100101，Shard 0、2、5 存在）
	shardBits := ShardBits(37)

	// ========== 测试 ShardIdToIndex：Shard ID -> 数组索引 ==========
	// 映射关系：
	//   Shard 0 -> 索引 0（第一个存在的 Shard）
	//   Shard 2 -> 索引 1（第二个存在的 Shard）
	//   Shard 5 -> 索引 2（第三个存在的 Shard）
	if index, found := shardBits.ShardIdToIndex(0); !found || index != 0 {
		t.Errorf("Expected shard 0 at index 0, got %d (found: %v)", index, found)
	}
	if index, found := shardBits.ShardIdToIndex(2); !found || index != 1 {
		t.Errorf("Expected shard 2 at index 1, got %d (found: %v)", index, found)
	}
	if index, found := shardBits.ShardIdToIndex(5); !found || index != 2 {
		t.Errorf("Expected shard 5 at index 2, got %d (found: %v)", index, found)
	}

	// 测试不存在的 Shard，应该返回 found=false
	// Shard 1 在位图中为 0，不存在
	if index, found := shardBits.ShardIdToIndex(1); found {
		t.Errorf("Should not find shard 1, but got index %d", index)
	}

	// ========== 测试 IndexToShardId：数组索引 -> Shard ID ==========
	// 这是 ShardIdToIndex 的逆操作
	// 索引 0 -> Shard 0
	// 索引 1 -> Shard 2
	// 索引 2 -> Shard 5
	if shardId, found := shardBits.IndexToShardId(0); !found || shardId != 0 {
		t.Errorf("Expected index 0 to be shard 0, got %d (found: %v)", shardId, found)
	}
	if shardId, found := shardBits.IndexToShardId(1); !found || shardId != 2 {
		t.Errorf("Expected index 1 to be shard 2, got %d (found: %v)", shardId, found)
	}
	if shardId, found := shardBits.IndexToShardId(2); !found || shardId != 5 {
		t.Errorf("Expected index 2 to be shard 5, got %d (found: %v)", shardId, found)
	}

	// 测试无效索引，应该返回 found=false
	// 只有 3 个 Shard 存在，索引 3 无效
	if shardId, found := shardBits.IndexToShardId(3); found {
		t.Errorf("Should not find shard for index 3, but got shard %d", shardId)
	}

	// ========== 测试 EachSetIndex：遍历所有存在的 Shard ==========
	// EachSetIndex 按 Shard ID 升序遍历所有置位的位
	var collectedShards []ShardId
	shardBits.EachSetIndex(func(shardId ShardId) {
		collectedShards = append(collectedShards, shardId)
	})

	// 预期结果：按顺序收集 Shard 0、2、5
	expectedShards := []ShardId{0, 2, 5}
	if len(collectedShards) != len(expectedShards) {
		t.Errorf("Expected EachSetIndex to collect %v, got %v", expectedShards, collectedShards)
	}
	for i, expected := range expectedShards {
		if i >= len(collectedShards) || collectedShards[i] != expected {
			t.Errorf("Expected EachSetIndex to collect %v, got %v", expectedShards, collectedShards)
			break
		}
	}
}
