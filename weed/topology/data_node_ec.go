// Package topology 提供纠删码（EC）相关的 DataNode 方法
// 纠删码是 SeaweedFS 的冷数据存储方案，将一个 Volume 拆分为 14 个分片：
//   - 10 个数据分片（Data Shards）
//   - 4 个校验分片（Parity Shards）
// 可以分散存储在不同的 DataNode 上，实现更高的存储效率和数据安全性
package topology

import (
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// GetEcShards 获取 DataNode 上所有 EC 分片信息
// 聚合所有 Disk 子节点的 EC 分片
//
// 返回：
//   - []*erasure_coding.EcVolumeInfo: EC 分片信息列表
//     每个 EcVolumeInfo 包含：
//     - VolumeId: 原 Volume ID
//     - ShardBits: 位图，标识该节点持有哪些分片（0-13）
//     - DiskType: 磁盘类型
//
// 线程安全：使用读锁保护并发访问
func (dn *DataNode) GetEcShards() (ret []*erasure_coding.EcVolumeInfo) {
	dn.RLock()
	for _, c := range dn.children {
		disk := c.(*Disk)                    // 类型断言：子节点是 Disk
		ret = append(ret, disk.GetEcShards()...) // 聚合所有 Disk 的 EC 分片
	}
	dn.RUnlock()
	return ret
}

// UpdateEcShards 更新 DataNode 的 EC 分片信息
// 对比当前状态和上报状态，识别新增/删除的 EC 分片
//
// 参数：
//   - actualShards: Volume server 上报的实际 EC 分片列表（来自心跳）
//
// 返回：
//   - newShards: 新增的 EC 分片列表
//   - deletedShards: 已删除的 EC 分片列表
//
// 工作流程：
//   1. 对比现有分片和实际分片
//   2. 识别新增和删除的分片（通过位图运算）
//   3. 更新磁盘使用统计
//   4. 如果有变化，更新内部状态
//
// 线程安全：内部调用的方法使用锁保护
func (dn *DataNode) UpdateEcShards(actualShards []*erasure_coding.EcVolumeInfo) (newShards, deletedShards []*erasure_coding.EcVolumeInfo) {
	// 【步骤 1：构建实际 EC 分片的 map，便于快速查找】
	actualEcShardMap := make(map[needle.VolumeId]*erasure_coding.EcVolumeInfo)
	for _, ecShards := range actualShards {
		actualEcShardMap[ecShards.VolumeId] = ecShards
	}

	// 【步骤 2：获取 DataNode 当前记录的所有 EC 分片】
	existingEcShards := dn.GetEcShards()

	// 【步骤 3：对比现有分片和实际分片，识别变化】
	for _, ecShards := range existingEcShards {

		var newShardCount, deletedShardCount int
		disk := dn.getOrCreateDisk(ecShards.DiskType)

		vid := ecShards.VolumeId
		if actualEcShards, ok := actualEcShardMap[vid]; !ok {
			// 情况 1：整个 EC Volume 已被删除（实际分片中不存在）
			deletedShards = append(deletedShards, ecShards)
			deletedShardCount += ecShards.ShardIdCount()
		} else {
			// 情况 2：EC Volume 存在，但可能有部分分片变化
			// 使用位图运算识别新增和删除的分片

			// 计算新增的分片：actual - existing
			// 例如：actual=1101, existing=1001，则 a=0100（新增了分片 2）
			a := actualEcShards.Minus(ecShards)
			if a.ShardIdCount() > 0 {
				newShards = append(newShards, a)
				newShardCount += a.ShardIdCount()
			}

			// 计算删除的分片：existing - actual
			// 例如：existing=1101, actual=1001，则 d=0100（删除了分片 2）
			d := ecShards.Minus(actualEcShards)
			if d.ShardIdCount() > 0 {
				deletedShards = append(deletedShards, d)
				deletedShardCount += d.ShardIdCount()
			}
		}

		// 如果分片数量有净变化，更新磁盘使用统计
		if (newShardCount - deletedShardCount) != 0 {
			disk.UpAdjustDiskUsageDelta(types.ToDiskType(ecShards.DiskType), &DiskUsageCounts{
				ecShardCount: int64(newShardCount - deletedShardCount), // 净变化量
			})
		}

	}

	// 【步骤 4：处理完全新增的 EC Volume】
	// 如果实际分片中有 DataNode 之前不存在的 Volume，说明是新增的
	for _, ecShards := range actualShards {
		if dn.HasEcShards(ecShards.VolumeId) {
			continue // 已存在，跳过（前面已经处理过）
		}

		// 完全新增的 EC Volume
		newShards = append(newShards, ecShards)

		// 更新磁盘使用统计
		disk := dn.getOrCreateDisk(ecShards.DiskType)
		disk.UpAdjustDiskUsageDelta(types.ToDiskType(ecShards.DiskType), &DiskUsageCounts{
			ecShardCount: int64(ecShards.ShardIdCount()), // 新增分片数
		})
	}

	// 【步骤 5：如果有变化，更新内部状态】
	if len(newShards) > 0 || len(deletedShards) > 0 {
		dn.doUpdateEcShards(actualShards) // 用实际分片列表替换内部状态
	}

	return
}

// HasEcShards 检查 DataNode 是否持有指定 Volume 的 EC 分片
//
// 参数：
//   - volumeId: Volume ID
//
// 返回：
//   - bool: 是否持有该 Volume 的 EC 分片
//
// 说明：
//   - 遍历所有 Disk 子节点，检查是否有该 Volume 的 EC 分片
//   - 只要有一个 Disk 持有分片就返回 true
//
// 线程安全：使用读锁保护并发访问
func (dn *DataNode) HasEcShards(volumeId needle.VolumeId) (found bool) {
	dn.RLock()
	defer dn.RUnlock()
	for _, c := range dn.children {
		disk := c.(*Disk)
		_, found = disk.ecShards[volumeId] // 直接访问 Disk 的 ecShards map
		if found {
			return // 找到即返回
		}
	}
	return
}

// doUpdateEcShards 内部方法：完全替换 DataNode 的 EC 分片状态
// 用实际分片列表覆盖所有 Disk 的 EC 分片记录
//
// 参数：
//   - actualShards: 新的 EC 分片列表（来自 Volume server 心跳）
//
// 工作流程：
//   1. 清空所有 Disk 的 ecShards map
//   2. 根据 actualShards 重新填充各 Disk 的 ecShards
//
// 说明：
//   - 这是一个破坏性操作，会清除所有现有的 EC 分片记录
//   - 由 UpdateEcShards 调用，仅在检测到变化时执行
//
// 线程安全：使用写锁保护并发访问
func (dn *DataNode) doUpdateEcShards(actualShards []*erasure_coding.EcVolumeInfo) {
	dn.Lock()
	// 【步骤 1：清空所有 Disk 的 EC 分片】
	for _, c := range dn.children {
		disk := c.(*Disk)
		disk.ecShards = make(map[needle.VolumeId]*erasure_coding.EcVolumeInfo) // 重新初始化 map
	}
	// 【步骤 2：根据实际分片列表重新填充】
	for _, shard := range actualShards {
		disk := dn.getOrCreateDisk(shard.DiskType) // 获取或创建对应的 Disk
		disk.ecShards[shard.VolumeId] = shard      // 添加 EC 分片记录
	}
	dn.Unlock()
}

// DeltaUpdateEcShards 增量更新 DataNode 的 EC 分片
// 根据新增和删除的分片列表进行增量更新，不影响未变化的分片
//
// 参数：
//   - newShards: 新增的 EC 分片列表
//   - deletedShards: 删除的 EC 分片列表
//
// 说明：
//   - 这是 UpdateEcShards 的轻量级替代方案
//   - 适用于已知具体变化的场景，避免全量对比
//   - 会自动更新磁盘使用统计
//
// 使用场景：
//   - EC 编码完成，新增分片
//   - EC 分片迁移或删除
func (dn *DataNode) DeltaUpdateEcShards(newShards, deletedShards []*erasure_coding.EcVolumeInfo) {

	// 处理新增的 EC 分片
	for _, newShard := range newShards {
		dn.AddOrUpdateEcShard(newShard) // 添加或合并分片（使用位图运算）
	}

	// 处理删除的 EC 分片
	for _, deletedShard := range deletedShards {
		dn.DeleteEcShard(deletedShard) // 删除分片（使用位图运算）
	}

}

// AddOrUpdateEcShard 添加或更新单个 EC 分片
// 将操作委托给对应的 Disk 节点
//
// 参数：
//   - s: EC 分片信息
//
// 说明：
//   - 根据分片的 DiskType 自动选择或创建对应的 Disk
//   - 实际的位图合并在 Disk.AddOrUpdateEcShard 中完成
//   - 会自动更新磁盘使用统计
func (dn *DataNode) AddOrUpdateEcShard(s *erasure_coding.EcVolumeInfo) {
	disk := dn.getOrCreateDisk(s.DiskType) // 获取或创建对应类型的 Disk
	disk.AddOrUpdateEcShard(s)              // 委托给 Disk 处理（位图合并）
}

// DeleteEcShard 删除单个 EC 分片
// 将操作委托给对应的 Disk 节点
//
// 参数：
//   - s: 要删除的 EC 分片信息
//
// 说明：
//   - 根据分片的 DiskType 找到对应的 Disk
//   - 实际的位图差集运算在 Disk.DeleteEcShard 中完成
//   - 会自动更新磁盘使用统计
func (dn *DataNode) DeleteEcShard(s *erasure_coding.EcVolumeInfo) {
	disk := dn.getOrCreateDisk(s.DiskType) // 获取或创建对应类型的 Disk
	disk.DeleteEcShard(s)                   // 委托给 Disk 处理（位图差集）
}

// HasVolumesById 检查 DataNode 是否有指定的 Volume（普通 Volume 或 EC 分片）
//
// 参数：
//   - volumeId: Volume ID
//
// 返回：
//   - bool: 是否存在该 Volume
//
// 说明：
//   - 会检查所有 Disk 子节点
//   - 同时检查普通 Volume 和 EC 分片
//   - 只要有一个 Disk 持有该 Volume 就返回 true
//
// 线程安全：使用读锁保护并发访问
func (dn *DataNode) HasVolumesById(volumeId needle.VolumeId) (hasVolumeId bool) {

	dn.RLock()
	defer dn.RUnlock()
	// 遍历所有 Disk 子节点
	for _, c := range dn.children {
		disk := c.(*Disk)
		if disk.HasVolumesById(volumeId) { // Disk.HasVolumesById 会检查普通 Volume 和 EC 分片
			return true // 找到即返回
		}
	}
	return false

}
