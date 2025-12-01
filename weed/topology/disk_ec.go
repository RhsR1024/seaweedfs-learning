// Package topology 提供纠删码（EC）相关的 Disk 方法
// 这些方法管理单个磁盘上的 EC 分片
package topology

import (
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// GetEcShards 获取 Disk 上所有 EC 分片信息
//
// 返回：
//   - []*erasure_coding.EcVolumeInfo: EC 分片信息列表
//
// 线程安全：使用读锁保护并发访问
func (d *Disk) GetEcShards() (ret []*erasure_coding.EcVolumeInfo) {
	d.RLock()
	for _, ecVolumeInfo := range d.ecShards {
		ret = append(ret, ecVolumeInfo)
	}
	d.RUnlock()
	return ret
}

// AddOrUpdateEcShard 添加或更新 EC 分片
// 使用位图合并的方式，支持增量添加分片
//
// 参数：
//   - s: EC 分片信息，包含 VolumeId 和 ShardBits（位图）
//
// 工作原理：
//   1. 如果 Volume 不存在，直接添加
//   2. 如果 Volume 已存在，使用位图运算合并分片
//      例如：existing=1001, new=0100，合并后=1101
//   3. 更新磁盘使用统计
//
// 线程安全：使用 ecShardsLock 保护并发访问
func (d *Disk) AddOrUpdateEcShard(s *erasure_coding.EcVolumeInfo) {
	d.ecShardsLock.Lock()
	defer d.ecShardsLock.Unlock()

	delta := 0 // 分片数量变化
	if existing, ok := d.ecShards[s.VolumeId]; !ok {
		// EC Volume 不存在，直接添加
		d.ecShards[s.VolumeId] = s
		delta = s.ShardBits.ShardIdCount() // 新增的分片数
	} else {
		// EC Volume 已存在，合并分片位图
		oldCount := existing.ShardBits.ShardIdCount()                 // 合并前的分片数
		existing.ShardBits = existing.ShardBits.Plus(s.ShardBits)     // 位图合并（OR 运算）
		delta = existing.ShardBits.ShardIdCount() - oldCount          // 净增加的分片数
	}

	if delta == 0 {
		return // 没有变化，无需更新统计
	}

	// 向上传播统计变化：Disk → DataNode → Rack → DataCenter → Topology
	d.UpAdjustDiskUsageDelta(types.ToDiskType(string(d.Id())), &DiskUsageCounts{
		ecShardCount: int64(delta), // EC 分片数变化
	})
}

// DeleteEcShard 删除 EC 分片
// 使用位图差集运算，支持增量删除分片
//
// 参数：
//   - s: 要删除的 EC 分片信息
//
// 工作原理：
//   1. 从现有分片位图中减去要删除的分片
//      例如：existing=1101, delete=0100，结果=1001
//   2. 如果所有分片都被删除（位图为 0），移除整个 EC Volume
//   3. 更新磁盘使用统计
//
// 线程安全：使用 ecShardsLock 保护并发访问
func (d *Disk) DeleteEcShard(s *erasure_coding.EcVolumeInfo) {
	d.ecShardsLock.Lock()
	defer d.ecShardsLock.Unlock()

	if existing, ok := d.ecShards[s.VolumeId]; ok {
		oldCount := existing.ShardBits.ShardIdCount()             // 删除前的分片数
		existing.ShardBits = existing.ShardBits.Minus(s.ShardBits) // 位图差集（AND NOT 运算）
		delta := existing.ShardBits.ShardIdCount() - oldCount      // 净减少的分片数（负数）

		if delta != 0 {
			// 向上传播统计变化（delta 是负数）
			d.UpAdjustDiskUsageDelta(types.ToDiskType(string(d.Id())), &DiskUsageCounts{
				ecShardCount: int64(delta),
			})
		}

		// 如果所有分片都被删除，移除整个 EC Volume 记录
		if existing.ShardBits.ShardIdCount() == 0 {
			delete(d.ecShards, s.VolumeId)
		}
	}
}

// HasVolumesById 检查 Disk 是否有指定的 Volume（普通 Volume 或 EC Volume）
//
// 参数：
//   - id: Volume ID
//
// 返回：
//   - bool: 是否存在该 Volume
//
// 说明：
//   - 会同时检查普通 Volume 和 EC 分片
//   - 只要有一个 EC 分片存在，就返回 true
//
// 线程安全：
//   - 使用 RLock 保护普通 Volume 的访问
//   - 使用 ecShardsLock.RLock 保护 EC 分片的访问
func (d *Disk) HasVolumesById(id needle.VolumeId) (hasVolumeId bool) {

	// 【步骤 1：检查普通 Volume】
	d.RLock()
	_, ok := d.volumes[id]
	if ok {
		hasVolumeId = true
	}
	d.RUnlock()

	if hasVolumeId {
		return // 找到普通 Volume，直接返回
	}

	// 【步骤 2：检查 EC 分片】
	d.ecShardsLock.RLock()
	_, ok = d.ecShards[id]
	if ok {
		hasVolumeId = true
	}
	d.ecShardsLock.RUnlock()

	return
}
