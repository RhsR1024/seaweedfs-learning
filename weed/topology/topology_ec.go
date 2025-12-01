// Package topology 实现 SeaweedFS 的拓扑结构管理
// 本文件实现 EC（Erasure Coding，纠删码）分片的位置管理
//
// EC 分片机制：
//   - 将一个 Volume 分成多个 Shard（分片）
//   - 每个 Shard 存储在不同的 DataNode 上
//   - 即使部分 Shard 丢失，也能通过纠删码算法恢复数据
//   - 大幅降低存储成本（相比多副本）
//
// 默认 EC 配置（10+4）：
//   - 10 个数据分片（Data Shards）
//   - 4 个校验分片（Parity Shards）
//   - 任意丢失 4 个分片仍可恢复
//   - 存储开销：1.4 倍（相比 3 副本的 3 倍）
package topology

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// EcShardLocations 存储一个 EC Volume 的所有分片位置信息
// 记录每个分片（Shard）存储在哪些 DataNode 上
//
// 数据结构：
//   - Collection: 该 Volume 所属的集合（用于逻辑分组）
//   - Locations: 二维数组，下标是 Shard ID（0-31），值是存储该分片的 DataNode 列表
//
// 示例：
//   假设 Volume 3 的 EC 配置为 10+4（14 个分片）：
//   - Locations[0] = [dn1, dn2]  // Shard 0 在 dn1 和 dn2 上有副本
//   - Locations[1] = [dn3]       // Shard 1 只在 dn3 上
//   - Locations[13] = [dn4]      // Shard 13（最后一个校验分片）在 dn4 上
//
// MaxShardCount (32) 说明：
//   - 支持自定义 EC 比例（如 16+16）
//   - 标准配置只使用前 14 个位置（10+4）
type EcShardLocations struct {
	Collection string                                   // 集合名称
	Locations  [erasure_coding.MaxShardCount][]*DataNode // 每个 Shard 的位置列表
}

// SyncDataNodeEcShards 同步 DataNode 的 EC 分片信息（全量同步）
// Volume Server 心跳时调用，上报其拥有的所有 EC 分片
//
// 参数：
//   - shardInfos: DataNode 上报的所有 EC 分片信息（protobuf 格式）
//   - dn: 上报的 DataNode
//
// 返回：
//   - newShards: 新增的分片列表（需要在拓扑中注册）
//   - deletedShards: 已删除的分片列表（需要从拓扑中移除）
//
// 工作流程：
//   1. 将 protobuf 格式的分片信息转换为内存结构 EcVolumeInfo
//   2. 调用 DataNode.UpdateEcShards() 计算差异（新增和删除）
//   3. 在拓扑中注册新增的分片
//   4. 从拓扑中移除已删除的分片
//
// 使用场景：
//   - Volume Server 首次心跳时，全量上报 EC 分片
//   - Volume Server 重启后，重新同步 EC 分片状态
func (t *Topology) SyncDataNodeEcShards(shardInfos []*master_pb.VolumeEcShardInformationMessage, dn *DataNode) (newShards, deletedShards []*erasure_coding.EcVolumeInfo) {
	// 【步骤 1：转换为内存结构】
	// 将 protobuf 消息转换为 Go 内存结构
	var shards []*erasure_coding.EcVolumeInfo
	for _, shardInfo := range shardInfos {
		// 直接创建 EcVolumeInfo，使用优化的格式
		ecVolumeInfo := &erasure_coding.EcVolumeInfo{
			VolumeId:    needle.VolumeId(shardInfo.Id),           // Volume ID
			Collection:  shardInfo.Collection,                    // 集合名称
			ShardBits:   erasure_coding.ShardBits(shardInfo.EcIndexBits), // 分片位图（标记拥有哪些分片）
			DiskType:    shardInfo.DiskType,                      // 磁盘类型（hdd/ssd）
			DiskId:      shardInfo.DiskId,                        // 磁盘 ID
			ExpireAtSec: shardInfo.ExpireAtSec,                   // 过期时间
			ShardSizes:  shardInfo.ShardSizes,                    // 每个分片的大小
		}

		shards = append(shards, ecVolumeInfo)
	}

	// 【步骤 2：计算差异】
	// 比较新旧状态，找出新增和删除的分片
	newShards, deletedShards = dn.UpdateEcShards(shards)

	// 【步骤 3：在拓扑中注册新增的分片】
	for _, v := range newShards {
		t.RegisterEcShards(v, dn)
	}

	// 【步骤 4：从拓扑中移除已删除的分片】
	for _, v := range deletedShards {
		t.UnRegisterEcShards(v, dn)
	}

	return
}

// IncrementalSyncDataNodeEcShards 增量同步 DataNode 的 EC 分片信息
// Volume Server 心跳时调用，仅上报变化的分片（新增和删除）
//
// 参数：
//   - newEcShards: 新增的 EC 分片信息列表
//   - deletedEcShards: 已删除的 EC 分片信息列表
//   - dn: 上报的 DataNode
//
// 工作流程：
//   1. 将 protobuf 格式的新增/删除分片信息转换为内存结构
//   2. 调用 DataNode.DeltaUpdateEcShards() 增量更新 DataNode 状态
//   3. 在拓扑中注册新增的分片
//   4. 从拓扑中移除已删除的分片
//
// 优点：
//   - 相比全量同步，减少网络传输和计算开销
//   - 适合频繁的心跳更新
//
// 使用场景：
//   - Volume Server 定期心跳时，仅上报变化
//   - EC Volume 重新平衡后，上报分片迁移
func (t *Topology) IncrementalSyncDataNodeEcShards(newEcShards, deletedEcShards []*master_pb.VolumeEcShardInformationMessage, dn *DataNode) {
	// 【步骤 1：转换新增的分片信息】
	var newShards, deletedShards []*erasure_coding.EcVolumeInfo
	for _, shardInfo := range newEcShards {
		// 直接创建 EcVolumeInfo，使用优化的格式
		ecVolumeInfo := &erasure_coding.EcVolumeInfo{
			VolumeId:    needle.VolumeId(shardInfo.Id),
			Collection:  shardInfo.Collection,
			ShardBits:   erasure_coding.ShardBits(shardInfo.EcIndexBits),
			DiskType:    shardInfo.DiskType,
			DiskId:      shardInfo.DiskId,
			ExpireAtSec: shardInfo.ExpireAtSec,
			ShardSizes:  shardInfo.ShardSizes,
		}

		newShards = append(newShards, ecVolumeInfo)
	}

	// 【步骤 2：转换已删除的分片信息】
	for _, shardInfo := range deletedEcShards {
		// 直接创建 EcVolumeInfo，使用优化的格式
		ecVolumeInfo := &erasure_coding.EcVolumeInfo{
			VolumeId:    needle.VolumeId(shardInfo.Id),
			Collection:  shardInfo.Collection,
			ShardBits:   erasure_coding.ShardBits(shardInfo.EcIndexBits),
			DiskType:    shardInfo.DiskType,
			DiskId:      shardInfo.DiskId,
			ExpireAtSec: shardInfo.ExpireAtSec,
			ShardSizes:  shardInfo.ShardSizes,
		}

		deletedShards = append(deletedShards, ecVolumeInfo)
	}

	// 【步骤 3：增量更新 DataNode 状态】
	dn.DeltaUpdateEcShards(newShards, deletedShards)

	// 【步骤 4：在拓扑中注册新增的分片】
	for _, v := range newShards {
		t.RegisterEcShards(v, dn)
	}

	// 【步骤 5：从拓扑中移除已删除的分片】
	for _, v := range deletedShards {
		t.UnRegisterEcShards(v, dn)
	}
}

// NewEcShardLocations 创建新的 EC 分片位置管理器
//
// 参数：
//   - collection: 集合名称（用于逻辑分组）
//
// 返回：
//   - *EcShardLocations: 初始化完成的分片位置管理器
func NewEcShardLocations(collection string) *EcShardLocations {
	return &EcShardLocations{
		Collection: collection,
	}
}

// AddShard 添加一个分片到指定 DataNode 的位置记录
// 如果该分片在该 DataNode 上已存在，则不重复添加
//
// 参数：
//   - shardId: 分片 ID（0-31，标准 10+4 配置使用 0-13）
//   - dn: 存储该分片的 DataNode
//
// 返回：
//   - added: 是否成功添加（重复添加返回 false）
//
// 边界检查：
//   - 防止 shardId 超出范围导致 panic
func (loc *EcShardLocations) AddShard(shardId erasure_coding.ShardId, dn *DataNode) (added bool) {
	// 防御性边界检查，防止 shardId 超出数组范围
	if int(shardId) >= erasure_coding.MaxShardCount {
		return false
	}

	// 获取该分片当前的 DataNode 列表
	dataNodes := loc.Locations[shardId]

	// 检查 DataNode 是否已存在，避免重复添加
	for _, n := range dataNodes {
		if n.Id() == dn.Id() {
			return false // 已存在，不重复添加
		}
	}

	// 添加 DataNode 到该分片的位置列表
	loc.Locations[shardId] = append(dataNodes, dn)
	return true
}

// DeleteShard 从指定 DataNode 的位置记录中删除一个分片
// 如果该分片在该 DataNode 上不存在，则返回 false
//
// 参数：
//   - shardId: 分片 ID
//   - dn: 存储该分片的 DataNode
//
// 返回：
//   - deleted: 是否成功删除（不存在返回 false）
func (loc *EcShardLocations) DeleteShard(shardId erasure_coding.ShardId, dn *DataNode) (deleted bool) {
	// 防御性边界检查，防止 shardId 超出数组范围
	if int(shardId) >= erasure_coding.MaxShardCount {
		return false
	}

	// 获取该分片当前的 DataNode 列表
	dataNodes := loc.Locations[shardId]

	// 查找 DataNode 在列表中的位置
	foundIndex := -1
	for index, n := range dataNodes {
		if n.Id() == dn.Id() {
			foundIndex = index
			break
		}
	}

	// 未找到，返回 false
	if foundIndex < 0 {
		return false
	}

	// 从列表中删除该 DataNode（使用切片拼接）
	loc.Locations[shardId] = append(dataNodes[:foundIndex], dataNodes[foundIndex+1:]...)
	return true
}

// RegisterEcShards 在拓扑中注册 EC 分片
// DataNode 上报新的 EC 分片时调用
//
// 参数：
//   - ecShardInfos: EC 分片信息（包含 VolumeId 和分片列表）
//   - dn: 拥有这些分片的 DataNode
//
// 工作流程：
//   1. 根据 VolumeId 查找或创建 EcShardLocations
//   2. 将每个分片添加到对应的位置记录
//
// 线程安全：
//   - 使用写锁保护 ecShardMap
func (t *Topology) RegisterEcShards(ecShardInfos *erasure_coding.EcVolumeInfo, dn *DataNode) {
	t.ecShardMapLock.Lock()
	defer t.ecShardMapLock.Unlock()

	// 查找或创建该 Volume 的分片位置记录
	locations, found := t.ecShardMap[ecShardInfos.VolumeId]
	if !found {
		// 首次注册该 Volume 的分片，创建新的位置管理器
		locations = NewEcShardLocations(ecShardInfos.Collection)
		t.ecShardMap[ecShardInfos.VolumeId] = locations
	}

	// 将所有分片添加到位置记录
	// ecShardInfos.ShardIds() 返回该 DataNode 拥有的分片 ID 列表
	for _, shardId := range ecShardInfos.ShardIds() {
		locations.AddShard(shardId, dn)
	}
}

// UnRegisterEcShards 从拓扑中注销 EC 分片
// DataNode 删除 EC 分片或下线时调用
//
// 参数：
//   - ecShardInfos: EC 分片信息（包含 VolumeId 和分片列表）
//   - dn: 原本拥有这些分片的 DataNode
//
// 工作流程：
//   1. 根据 VolumeId 查找 EcShardLocations
//   2. 从每个分片的位置记录中删除该 DataNode
//
// 线程安全：
//   - 使用写锁保护 ecShardMap
func (t *Topology) UnRegisterEcShards(ecShardInfos *erasure_coding.EcVolumeInfo, dn *DataNode) {
	glog.Infof("removing ec shard info:%+v", ecShardInfos)
	t.ecShardMapLock.Lock()
	defer t.ecShardMapLock.Unlock()

	// 查找该 Volume 的分片位置记录
	locations, found := t.ecShardMap[ecShardInfos.VolumeId]
	if !found {
		return // Volume 不存在，直接返回
	}

	// 从所有分片的位置记录中删除该 DataNode
	for _, shardId := range ecShardInfos.ShardIds() {
		locations.DeleteShard(shardId, dn)
	}
}

// LookupEcShards 查询 EC Volume 的所有分片位置
// 客户端读取 EC Volume 时调用，获取每个分片的存储位置
//
// 参数：
//   - vid: Volume ID
//
// 返回：
//   - locations: 分片位置信息（包含每个分片在哪些 DataNode 上）
//   - found: 是否找到该 Volume 的分片信息
//
// 使用场景：
//   - 客户端读取 EC Volume 时，查询分片位置
//   - EC Volume 重新平衡前，检查当前分片分布
//
// 线程安全：
//   - 使用读锁保护 ecShardMap
func (t *Topology) LookupEcShards(vid needle.VolumeId) (locations *EcShardLocations, found bool) {
	t.ecShardMapLock.RLock()
	defer t.ecShardMapLock.RUnlock()

	locations, found = t.ecShardMap[vid]

	return
}

// ListEcServersByCollection 列出指定集合的所有 EC Volume Server
// 返回存储该集合 EC 分片的所有 DataNode 地址（去重）
//
// 参数：
//   - collection: 集合名称
//
// 返回：
//   - dataNodes: DataNode 地址列表（已去重）
//
// 使用场景：
//   - 查询某个集合的 EC Volume 分布在哪些服务器上
//   - EC Volume 重新平衡时，确定候选 DataNode
//
// 线程安全：
//   - 使用读锁保护 ecShardMap
func (t *Topology) ListEcServersByCollection(collection string) (dataNodes []pb.ServerAddress) {
	t.ecShardMapLock.RLock()
	defer t.ecShardMapLock.RUnlock()

	// 使用 map 去重（同一个 DataNode 可能存储多个分片）
	dateNodeMap := make(map[pb.ServerAddress]bool)

	// 遍历所有 EC Volume
	for _, ecVolumeLocation := range t.ecShardMap {
		if ecVolumeLocation.Collection == collection {
			// 遍历该 Volume 的所有分片位置
			for _, locations := range ecVolumeLocation.Locations {
				// 遍历每个分片的 DataNode 列表
				for _, loc := range locations {
					dateNodeMap[loc.ServerAddress()] = true
				}
			}
		}
	}

	// 将 map 的 key 转换为列表
	for k, _ := range dateNodeMap {
		dataNodes = append(dataNodes, k)
	}

	return
}

// DeleteEcCollection 删除指定集合的所有 EC 分片信息
// 删除集合时调用，清理拓扑中的相关数据
//
// 参数：
//   - collection: 要删除的集合名称
//
// 工作流程：
//   1. 找出属于该集合的所有 Volume ID
//   2. 从 ecShardMap 中删除这些 Volume 的分片信息
//
// 线程安全：
//   - 使用写锁保护 ecShardMap
//
// 注意：
//   - 这只是删除拓扑中的元数据，不会删除实际的分片文件
//   - 实际的分片文件需要通过其他接口删除
func (t *Topology) DeleteEcCollection(collection string) {
	t.ecShardMapLock.Lock()
	defer t.ecShardMapLock.Unlock()

	// 【步骤 1：收集要删除的 Volume ID】
	var vids []needle.VolumeId
	for vid, ecVolumeLocation := range t.ecShardMap {
		if ecVolumeLocation.Collection == collection {
			vids = append(vids, vid)
		}
	}

	// 【步骤 2：从 map 中删除这些 Volume 的分片信息】
	for _, vid := range vids {
		delete(t.ecShardMap, vid)
	}
}
