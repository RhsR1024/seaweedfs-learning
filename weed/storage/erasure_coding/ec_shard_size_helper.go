package erasure_coding

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

// GetShardSize 从 VolumeEcShardInformationMessage 中获取指定分片的大小
// 使用优化的存储格式，通过分片 ID 查找对应的大小值
//
// 参数:
//   - msg: Volume EC 分片信息消息
//   - shardId: 分片 ID
// 返回值:
//   - size: 分片大小（字节）
//   - found: 是否找到该分片
// 说明:
//   ShardSizes 数组按照 EcIndexBits 中设置的位顺序存储分片大小
//   例如：如果 EcIndexBits = 0b1101（分片 0,2,3 存在），
//        则 ShardSizes[0] 对应分片 0，ShardSizes[1] 对应分片 2，ShardSizes[2] 对应分片 3
func GetShardSize(msg *master_pb.VolumeEcShardInformationMessage, shardId ShardId) (size int64, found bool) {
	if msg == nil || msg.ShardSizes == nil {
		return 0, false
	}

	// 将 EcIndexBits 转换为 ShardBits 类型
	shardBits := ShardBits(msg.EcIndexBits)
	// 将分片 ID 转换为 ShardSizes 数组中的索引
	index, found := shardBits.ShardIdToIndex(shardId)
	if !found || index >= len(msg.ShardSizes) {
		return 0, false
	}

	return msg.ShardSizes[index], true
}

// SetShardSize 在 VolumeEcShardInformationMessage 中设置指定分片的大小
// 使用优化的存储格式，通过分片 ID 定位并设置大小值
//
// 参数:
//   - msg: Volume EC 分片信息消息
//   - shardId: 分片 ID
//   - size: 分片大小（字节）
// 返回值:
//   - bool: 是否成功设置（如果分片不在 EcIndexBits 中则返回 false）
func SetShardSize(msg *master_pb.VolumeEcShardInformationMessage, shardId ShardId, size int64) bool {
	if msg == nil {
		return false
	}

	shardBits := ShardBits(msg.EcIndexBits)
	index, found := shardBits.ShardIdToIndex(shardId)
	if !found {
		return false
	}

	// 如果需要，初始化 ShardSizes 数组
	expectedLength := shardBits.ShardIdCount()
	if msg.ShardSizes == nil {
		msg.ShardSizes = make([]int64, expectedLength)
	} else if len(msg.ShardSizes) != expectedLength {
		// 调整数组大小以匹配预期长度
		newSizes := make([]int64, expectedLength)
		copy(newSizes, msg.ShardSizes)
		msg.ShardSizes = newSizes
	}

	if index >= len(msg.ShardSizes) {
		return false
	}

	msg.ShardSizes[index] = size
	return true
}

// InitializeShardSizes 根据 EcIndexBits 初始化 ShardSizes 数组
// 确保数组长度与实际存在的分片数量匹配
//
// 参数:
//   - msg: Volume EC 分片信息消息
// 说明:
//   这个函数通常在创建新的 VolumeEcShardInformationMessage 时调用
//   以确保 ShardSizes 数组有正确的长度
func InitializeShardSizes(msg *master_pb.VolumeEcShardInformationMessage) {
	if msg == nil {
		return
	}

	shardBits := ShardBits(msg.EcIndexBits)
	expectedLength := shardBits.ShardIdCount()

	if msg.ShardSizes == nil || len(msg.ShardSizes) != expectedLength {
		msg.ShardSizes = make([]int64, expectedLength)
	}
}
