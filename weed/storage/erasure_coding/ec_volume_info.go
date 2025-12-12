package erasure_coding

import (
	"math/bits"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// EcVolumeInfo 在 Master 服务器中使用的 EC Volume 信息数据结构
// 用于跟踪 EC Volume 的分片分布和元数据
type EcVolumeInfo struct {
	VolumeId    needle.VolumeId // Volume ID
	Collection  string          // 集合名称
	ShardBits   ShardBits       // 分片位图，标记哪些分片存在（使用位图优化存储）
	DiskType    string          // 磁盘类型
	DiskId      uint32          // 磁盘 ID（该 EC Volume 所在的磁盘）
	ExpireAtSec uint64          // 过期时间（Unix 时间戳），从 EC Volume 创建时计算
	ShardSizes  []int64         // 优化存储：按 ShardBits 中设置位的顺序存储分片大小
}

// AddShardId 向 EC Volume 信息添加一个分片 ID
// 如果分片实际被添加（之前不存在），会调整 ShardSizes 数组大小
func (ecInfo *EcVolumeInfo) AddShardId(id ShardId) {
	oldBits := ecInfo.ShardBits
	ecInfo.ShardBits = ecInfo.ShardBits.AddShardId(id)

	// 如果分片实际被添加，调整 ShardSizes 数组大小
	if oldBits != ecInfo.ShardBits {
		ecInfo.resizeShardSizes(oldBits)
	}
}

// RemoveShardId 从 EC Volume 信息移除一个分片 ID
// 如果分片实际被移除（之前存在），会调整 ShardSizes 数组大小
func (ecInfo *EcVolumeInfo) RemoveShardId(id ShardId) {
	oldBits := ecInfo.ShardBits
	ecInfo.ShardBits = ecInfo.ShardBits.RemoveShardId(id)

	// 如果分片实际被移除，调整 ShardSizes 数组大小
	if oldBits != ecInfo.ShardBits {
		ecInfo.resizeShardSizes(oldBits)
	}
}

// SetShardSize 设置指定分片的大小
// 参数:
//   - id: 分片 ID
//   - size: 分片大小（字节）
func (ecInfo *EcVolumeInfo) SetShardSize(id ShardId, size int64) {
	ecInfo.ensureShardSizesInitialized()
	if index, found := ecInfo.ShardBits.ShardIdToIndex(id); found && index < len(ecInfo.ShardSizes) {
		ecInfo.ShardSizes[index] = size
	}
}

// GetShardSize 获取指定分片的大小
// 参数:
//   - id: 分片 ID
// 返回值:
//   - int64: 分片大小（字节）
//   - bool: 是否找到该分片
func (ecInfo *EcVolumeInfo) GetShardSize(id ShardId) (int64, bool) {
	if index, found := ecInfo.ShardBits.ShardIdToIndex(id); found && index < len(ecInfo.ShardSizes) {
		return ecInfo.ShardSizes[index], true
	}
	return 0, false
}

// GetTotalSize 获取所有分片的总大小
// 返回值:
//   - int64: 总大小（字节）
func (ecInfo *EcVolumeInfo) GetTotalSize() int64 {
	var total int64
	for _, size := range ecInfo.ShardSizes {
		total += size
	}
	return total
}

// HasShardId 检查是否包含指定的分片 ID
func (ecInfo *EcVolumeInfo) HasShardId(id ShardId) bool {
	return ecInfo.ShardBits.HasShardId(id)
}

// ShardIds 返回所有分片 ID 列表
func (ecInfo *EcVolumeInfo) ShardIds() (ret []ShardId) {
	return ecInfo.ShardBits.ShardIds()
}

// ShardIdCount 返回分片数量
func (ecInfo *EcVolumeInfo) ShardIdCount() (count int) {
	return ecInfo.ShardBits.ShardIdCount()
}

// Minus 计算两个 EC Volume 信息的差集
// 返回在 ecInfo 中存在但在 other 中不存在的分片
//
// 参数:
//   - other: 另一个 EC Volume 信息
// 返回值:
//   - *EcVolumeInfo: 差集结果
func (ecInfo *EcVolumeInfo) Minus(other *EcVolumeInfo) *EcVolumeInfo {
	ret := &EcVolumeInfo{
		VolumeId:    ecInfo.VolumeId,
		Collection:  ecInfo.Collection,
		ShardBits:   ecInfo.ShardBits.Minus(other.ShardBits),
		DiskType:    ecInfo.DiskType,
		DiskId:      ecInfo.DiskId,
		ExpireAtSec: ecInfo.ExpireAtSec,
	}

	// 为结果初始化优化的 ShardSizes 数组
	ret.ensureShardSizesInitialized()

	// 复制保留分片的大小信息
	retIndex := 0
	for shardId := ShardId(0); shardId < ShardId(MaxShardCount) && retIndex < len(ret.ShardSizes); shardId++ {
		if ret.ShardBits.HasShardId(shardId) {
			if size, exists := ecInfo.GetShardSize(shardId); exists {
				ret.ShardSizes[retIndex] = size
			}
			retIndex++
		}
	}

	return ret
}

// ToVolumeEcShardInformationMessage 将 EcVolumeInfo 转换为 protobuf 消息格式
// 用于在 gRPC 通信中传输 EC Volume 信息
//
// 返回值:
//   - *master_pb.VolumeEcShardInformationMessage: protobuf 消息
func (ecInfo *EcVolumeInfo) ToVolumeEcShardInformationMessage() (ret *master_pb.VolumeEcShardInformationMessage) {
	t := &master_pb.VolumeEcShardInformationMessage{
		Id:          uint32(ecInfo.VolumeId),
		EcIndexBits: uint32(ecInfo.ShardBits),
		Collection:  ecInfo.Collection,
		DiskType:    ecInfo.DiskType,
		ExpireAtSec: ecInfo.ExpireAtSec,
		DiskId:      ecInfo.DiskId,
	}

	// 直接设置优化的 ShardSizes 数组
	t.ShardSizes = make([]int64, len(ecInfo.ShardSizes))
	copy(t.ShardSizes, ecInfo.ShardSizes)

	return t
}

// ShardBits 使用位图表示分片 ID 的存在状态
// 使用 uint32（32 位）以支持未来可能的扩展（虽然当前最多使用 14 位）
type ShardBits uint32

// AddShardId 向位图添加一个分片 ID
// 参数:
//   - id: 分片 ID
// 返回值:
//   - ShardBits: 更新后的位图
func (b ShardBits) AddShardId(id ShardId) ShardBits {
	if id >= MaxShardCount {
		return b // 拒绝超出范围的分片 ID
	}
	// 使用按位或操作设置对应的位
	return b | (1 << id)
}

// RemoveShardId 从位图移除一个分片 ID
// 参数:
//   - id: 分片 ID
// 返回值:
//   - ShardBits: 更新后的位图
func (b ShardBits) RemoveShardId(id ShardId) ShardBits {
	if id >= MaxShardCount {
		return b // 拒绝超出范围的分片 ID
	}
	// 使用按位清除操作清除对应的位
	return b &^ (1 << id)
}

// HasShardId 检查位图中是否包含指定的分片 ID
// 参数:
//   - id: 分片 ID
// 返回值:
//   - bool: 是否包含该分片
func (b ShardBits) HasShardId(id ShardId) bool {
	if id >= MaxShardCount {
		return false // 超出范围的分片 ID 永远不存在
	}
	return b&(1<<id) > 0
}

// ShardIds 返回位图中所有设置的分片 ID 列表
// 返回值:
//   - []ShardId: 分片 ID 列表（按升序）
func (b ShardBits) ShardIds() (ret []ShardId) {
	for i := ShardId(0); i < ShardId(MaxShardCount); i++ {
		if b.HasShardId(i) {
			ret = append(ret, i)
		}
	}
	return
}

// ToUint32Slice 将位图转换为 uint32 切片
// 返回值:
//   - []uint32: 分片 ID 列表（uint32 类型）
func (b ShardBits) ToUint32Slice() (ret []uint32) {
	for i := uint32(0); i < uint32(MaxShardCount); i++ {
		if b.HasShardId(ShardId(i)) {
			ret = append(ret, i)
		}
	}
	return
}

// ShardIdCount 返回位图中设置的分片数量
// 使用高效的位操作算法（Brian Kernighan's Algorithm）
// 返回值:
//   - int: 分片数量
func (b ShardBits) ShardIdCount() (count int) {
	// 每次迭代清除最低位的 1，直到 b 为 0
	for count = 0; b > 0; count++ {
		b &= b - 1
	}
	return
}

// Minus 计算两个位图的差集（b 中有但 other 中没有的）
// 参数:
//   - other: 另一个位图
// 返回值:
//   - ShardBits: 差集结果
func (b ShardBits) Minus(other ShardBits) ShardBits {
	return b &^ other
}

// Plus 计算两个位图的并集
// 参数:
//   - other: 另一个位图
// 返回值:
//   - ShardBits: 并集结果
func (b ShardBits) Plus(other ShardBits) ShardBits {
	return b | other
}

// MinusParityShards 从位图中移除所有校验分片
// 假设使用默认的 10+4 EC 布局，校验分片 ID 为 10-13
// 返回值:
//   - ShardBits: 移除校验分片后的位图（仅包含数据分片）
func (b ShardBits) MinusParityShards() ShardBits {
	// 移除校验分片（ID 10-13）
	for i := DataShardsCount; i < TotalShardsCount; i++ {
		b = b.RemoveShardId(ShardId(i))
	}
	return b
}

// ShardIdToIndex 将分片 ID 转换为其在 ShardSizes 数组中的索引位置
// 这是优化存储的关键方法，将稀疏的分片 ID 映射到紧凑的数组索引
//
// 参数:
//   - shardId: 分片 ID
// 返回值:
//   - index: 数组索引位置
//   - found: 是否找到该分片
// 示例:
//   如果 ShardBits = 0b1101（分片 0,2,3 存在）
//   - ShardIdToIndex(0) -> (0, true)  // 第 1 个设置的位
//   - ShardIdToIndex(2) -> (1, true)  // 第 2 个设置的位
//   - ShardIdToIndex(3) -> (2, true)  // 第 3 个设置的位
//   - ShardIdToIndex(1) -> (-1, false) // 未设置
func (b ShardBits) ShardIdToIndex(shardId ShardId) (index int, found bool) {
	if !b.HasShardId(shardId) {
		return -1, false
	}

	// 创建一个掩码，包含 shardId 之前的所有位
	mask := uint32((1 << shardId) - 1)
	// 使用高效的位操作统计 shardId 之前设置的位数
	index = bits.OnesCount32(uint32(b) & mask)
	return index, true
}

// EachSetIndex 遍历所有设置的分片 ID，对每个分片调用提供的函数
// 使用高效的位操作，只遍历实际设置的位
//
// 参数:
//   - fn: 对每个分片 ID 执行的函数
func (b ShardBits) EachSetIndex(fn func(shardId ShardId)) {
	bitsValue := uint32(b)
	for bitsValue != 0 {
		// 找到最低位设置的位的位置
		shardId := ShardId(bits.TrailingZeros32(bitsValue))
		fn(shardId)
		// 清除最低位设置的位
		bitsValue &= bitsValue - 1
	}
}

// IndexToShardId 将 ShardSizes 数组中的索引位置转换为对应的分片 ID
// 这是 ShardIdToIndex 的反向操作
//
// 参数:
//   - index: 数组索引位置
// 返回值:
//   - shardId: 分片 ID
//   - found: 是否有效索引
func (b ShardBits) IndexToShardId(index int) (shardId ShardId, found bool) {
	if index < 0 {
		return 0, false
	}

	currentIndex := 0
	for i := ShardId(0); i < ShardId(MaxShardCount); i++ {
		if b.HasShardId(i) {
			if currentIndex == index {
				return i, true
			}
			currentIndex++
		}
	}
	return 0, false // 索引超出范围
}

// ensureShardSizesInitialized 确保 ShardSizes 数组已初始化并具有正确的长度
// 这是 EcVolumeInfo 的辅助方法
func (ecInfo *EcVolumeInfo) ensureShardSizesInitialized() {
	expectedLength := ecInfo.ShardBits.ShardIdCount()
	if ecInfo.ShardSizes == nil {
		ecInfo.ShardSizes = make([]int64, expectedLength)
	} else if len(ecInfo.ShardSizes) != expectedLength {
		// 调整大小并保留现有数据
		ecInfo.resizeShardSizes(ecInfo.ShardBits)
	}
}

// resizeShardSizes 调整 ShardSizes 数组大小并保留现有数据
// 在添加或移除分片时调用
//
// 参数:
//   - prevShardBits: 之前的分片位图（用于定位旧数据）
func (ecInfo *EcVolumeInfo) resizeShardSizes(prevShardBits ShardBits) {
	expectedLength := ecInfo.ShardBits.ShardIdCount()
	newSizes := make([]int64, expectedLength)

	// 根据当前 ShardBits 将现有大小复制到新位置
	if len(ecInfo.ShardSizes) > 0 {
		newIndex := 0
		for shardId := ShardId(0); shardId < ShardId(MaxShardCount) && newIndex < expectedLength; shardId++ {
			if ecInfo.ShardBits.HasShardId(shardId) {
				// 尝试从旧数组中找到此分片的大小
				if oldIndex, found := prevShardBits.ShardIdToIndex(shardId); found && oldIndex < len(ecInfo.ShardSizes) {
					newSizes[newIndex] = ecInfo.ShardSizes[oldIndex]
				}
				newIndex++
			}
		}
	}

	ecInfo.ShardSizes = newSizes
}
