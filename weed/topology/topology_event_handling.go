// Package topology 实现 SeaweedFS 的拓扑结构管理
// 本文件实现拓扑事件处理，包括：
//   - 定期刷新可写 Volume 列表
//   - 标记已满/拥挤的 Volume
//   - 处理 DataNode 下线
//   - 定期执行 Vacuum 清理
//
// 关键概念：
//   - 已满 Volume（Full Volume）：达到 volumeSizeLimit，标记为只读
//   - 拥挤 Volume（Crowded Volume）：接近满（如 90%），触发预创建新 Volume
//   - Vacuum 清理：回收被删除 Needle 占用的空间
package topology

import (
	"math/rand/v2"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage"
)

// StartRefreshWritableVolumes 启动后台任务，定期刷新可写 Volume 状态
// 在 Master Server 启动时调用，包含三个并发的 goroutine：
//   1. 定期检查死节点和已满 Volume
//   2. 定期执行 Vacuum 清理
//   3. 处理 Volume 状态变更通知（已满/拥挤）
//
// 参数：
//   - grpcDialOption: gRPC 连接选项
//   - garbageThreshold: 垃圾回收阈值（0.0-1.0），如 0.3 表示删除率达到 30% 时执行 Vacuum
//   - concurrentVacuumLimitPerVolumeServer: 每个 Volume Server 并发 Vacuum 数量限制
//   - growThreshold: Volume 增长阈值（0.0-1.0），如 0.9 表示 90% 满时预创建新 Volume
//   - preallocate: Volume 预分配大小
//
// 设计思想：
//   - 使用 channel 解耦事件生产和消费
//   - 添加随机延迟，避免所有节点同时执行相同操作
//   - 只有 Leader 执行维护任务，避免重复操作
func (t *Topology) StartRefreshWritableVolumes(grpcDialOption grpc.DialOption, garbageThreshold float64, concurrentVacuumLimitPerVolumeServer int, growThreshold float64, preallocate int64) {
	// 【Goroutine 1：定期检查死节点和已满 Volume】
	go func() {
		for {
			// 只有 Leader 节点执行检查，避免重复操作
			if t.IsLeader() {
				// 计算心跳新鲜度阈值
				// 如果节点的 LastSeen 时间早于此阈值，视为死节点
				// 3 倍心跳间隔是一个保守的判断（允许 2 次心跳丢失）
				freshThreshHold := time.Now().Unix() - 3*t.pulse // 3 times of sleep interval

				// 递归遍历拓扑树，执行：
				//   1. 标记超过 volumeSizeLimit 的 Volume 为只读（Full）
				//   2. 标记接近满（growThreshold）的 Volume 为拥挤（Crowded）
				//   3. 检测死节点（超过 freshThreshHold 未心跳）
				//   4. 检查副本数是否满足策略要求
				t.CollectDeadNodeAndFullVolumes(freshThreshHold, t.volumeSizeLimit, growThreshold)
			}

			// 随机延迟，避免所有 Master 节点同时执行
			// 基础延迟：pulse * 1000 毫秒
			// 随机因子：0-100%，使得实际延迟在 [pulse, 2*pulse] 秒之间
			time.Sleep(time.Duration(float32(t.pulse*1e3)*(1+rand.Float32())) * time.Millisecond)
		}
	}()

	// 【Goroutine 2：定期执行 Vacuum 清理】
	go func(garbageThreshold float64) {
		for {
			// 只有 Leader 节点执行 Vacuum
			if t.IsLeader() {
				// 检查是否禁用 Vacuum
				if !t.isDisableVacuum {
					// 执行 Vacuum 清理，回收被删除 Needle 占用的空间
					// 参数说明：
					//   - grpcDialOption: gRPC 连接选项
					//   - garbageThreshold: 垃圾率阈值（删除 Needle 数 / 总 Needle 数）
					//   - concurrentVacuumLimitPerVolumeServer: 每个 Volume Server 并发限制
					//   - 0: VolumeId 限制（0 表示所有 Volume）
					//   - "": Collection 限制（空表示所有 Collection）
					//   - preallocate: 预分配大小
					//   - true: 强制 Vacuum（忽略垃圾率阈值）
					t.Vacuum(grpcDialOption, garbageThreshold, concurrentVacuumLimitPerVolumeServer, 0, "", preallocate, true)
				}
			} else {
				// 非 Leader 节点，重置 Prometheus 指标
				// 避免非 Leader 节点上报过期的副本不匹配指标
				stats.MasterReplicaPlacementMismatch.Reset()
			}

			// Vacuum 间隔：14 分钟 + 随机 0-120 秒
			// 避免所有节点同时执行 Vacuum，降低系统负载峰值
			time.Sleep(14*time.Minute + time.Duration(120*rand.Float32())*time.Second)
		}
	}(garbageThreshold)

	// 【Goroutine 3：处理 Volume 状态变更通知】
	go func() {
		for {
			select {
			// 处理已满 Volume 通知
			// 由 CollectDeadNodeAndFullVolumes 发送到 chanFullVolumes
			case fv := <-t.chanFullVolumes:
				t.SetVolumeCapacityFull(fv)

			// 处理拥挤 Volume 通知
			// 由 CollectDeadNodeAndFullVolumes 发送到 chanCrowdedVolumes
			case cv := <-t.chanCrowdedVolumes:
				t.SetVolumeCrowded(cv)
			}
		}
	}()
}

// SetVolumeCapacityFull 将 Volume 标记为已满（只读）
// 当 Volume 大小达到 volumeSizeLimit 时调用
//
// 参数：
//   - volumeInfo: Volume 的详细信息（包含 ID、Collection、副本策略等）
//
// 返回：
//   - bool: 是否成功标记
//
// 工作流程：
//   1. 根据 Volume 信息获取对应的 VolumeLayout
//   2. 在 VolumeLayout 中将该 Volume 标记为已满
//   3. 更新所有拥有该 Volume 的 DataNode 的活跃 Volume 计数（-1）
//
// 副作用：
//   - Volume 变为只读，不再接受新的文件上传
//   - DataNode 的活跃 Volume 计数减少，影响负载均衡
//   - 统计信息向上传播到 Rack → DataCenter → Topology
func (t *Topology) SetVolumeCapacityFull(volumeInfo storage.VolumeInfo) bool {
	// 【步骤 1：获取 VolumeLayout】
	// VolumeLayout 按 Collection、副本策略、TTL、磁盘类型分组管理 Volume
	diskType := types.ToDiskType(volumeInfo.DiskType)
	vl := t.GetVolumeLayout(volumeInfo.Collection, volumeInfo.ReplicaPlacement, volumeInfo.Ttl, diskType)

	// 【步骤 2：在 VolumeLayout 中标记 Volume 为已满】
	if !vl.SetVolumeCapacityFull(volumeInfo.Id) {
		return false // Volume 不存在或已经是只读状态
	}

	// 【步骤 3：更新 DataNode 的活跃 Volume 计数】
	vl.accessLock.RLock()
	defer vl.accessLock.RUnlock()

	// 查找该 Volume 的所有位置
	vidLocations, found := vl.vid2location[volumeInfo.Id]
	if !found {
		return false // Volume 位置信息不存在（不应该发生）
	}

	// 遍历所有拥有该 Volume 的 DataNode
	for _, dn := range vidLocations.list {
		// 只有当 Volume 之前是可写状态时，才减少活跃计数
		// 避免重复减少（如果多次调用此函数）
		if !volumeInfo.ReadOnly {
			// 获取 DataNode 的磁盘节点
			disk := dn.getOrCreateDisk(volumeInfo.DiskType)

			// 更新磁盘使用统计：活跃 Volume 计数 -1
			// 这个更新会向上传播到 Rack → DataCenter → Topology
			disk.UpAdjustDiskUsageDelta(types.ToDiskType(volumeInfo.DiskType), &DiskUsageCounts{
				activeVolumeCount: -1, // 活跃 Volume 数量减 1
			})
		}
	}

	return true
}

// SetVolumeCrowded 将 Volume 标记为拥挤
// 当 Volume 大小接近 volumeSizeLimit（如 90%）时调用，触发预创建新 Volume
//
// 参数：
//   - volumeInfo: Volume 的详细信息
//
// 工作流程：
//   1. 根据 Volume 信息获取对应的 VolumeLayout
//   2. 在 VolumeLayout 中将该 Volume 标记为拥挤
//
// 副作用：
//   - 触发 VolumeGrowth 逻辑，预创建新的可写 Volume
//   - 避免 Volume 满了之后才创建新 Volume，导致写入延迟
func (t *Topology) SetVolumeCrowded(volumeInfo storage.VolumeInfo) {
	diskType := types.ToDiskType(volumeInfo.DiskType)
	vl := t.GetVolumeLayout(volumeInfo.Collection, volumeInfo.ReplicaPlacement, volumeInfo.Ttl, diskType)

	// 在 VolumeLayout 中标记为拥挤
	// VolumeLayout 会检测到拥挤状态，并触发创建新 Volume
	vl.SetVolumeCrowded(volumeInfo.Id)
}

// UnRegisterDataNode 注销 DataNode（节点下线）
// 当 DataNode 长时间未心跳或主动下线时调用
//
// 参数：
//   - dn: 要注销的 DataNode
//
// 工作流程：
//   1. 标记 DataNode 为正在终止状态（IsTerminating = true）
//   2. 在 VolumeLayout 中标记该节点上的所有 Volume 为不可用
//   3. 注销该节点上的所有 EC 分片
//   4. 向上传播负的磁盘使用统计（抵消之前的统计）
//   5. 清空 DataNode 的 Volume 和 EC 分片列表
//   6. 从父节点（Rack）的子节点列表中移除
//
// 注意：
//   - 这不会删除实际的 Volume 数据文件
//   - 只是在拓扑中移除该节点的信息
//   - 如果节点恢复并重新心跳，会重新注册
func (t *Topology) UnRegisterDataNode(dn *DataNode) {
	// 【步骤 1：标记节点为正在终止】
	// 防止在注销过程中继续分配 Volume 到该节点
	dn.IsTerminating = true

	// 【步骤 2：标记所有 Volume 为不可用】
	for _, v := range dn.GetVolumes() {
		glog.V(0).Infoln("Removing Volume", v.Id, "from the dead volume server", dn.Id())

		// 获取该 Volume 的 VolumeLayout
		diskType := types.ToDiskType(v.DiskType)
		vl := t.GetVolumeLayout(v.Collection, v.ReplicaPlacement, v.Ttl, diskType)

		// 在 VolumeLayout 中标记该 Volume 在该 DataNode 上不可用
		// 如果有其他副本，客户端仍可访问；如果没有，Volume 完全不可用
		vl.SetVolumeUnavailable(dn, v.Id)
	}

	// 【步骤 3：注销所有 EC 分片】
	// EC Volume 的分片会从拓扑的 ecShardMap 中移除
	for _, s := range dn.GetEcShards() {
		t.UnRegisterEcShards(s, dn)
	}

	// 【步骤 4：向上传播负的磁盘使用统计】
	// 抵消之前该节点向上传播的统计数据
	// 确保 Rack、DataCenter、Topology 的统计准确
	negativeUsages := dn.GetDiskUsages().negative()
	for dt, du := range negativeUsages.usages {
		dn.UpAdjustDiskUsageDelta(dt, du)
	}

	// 【步骤 5：清空 DataNode 的 Volume 和 EC 分片列表】
	// DeltaUpdateVolumes(新增=[], 删除=所有Volume)
	dn.DeltaUpdateVolumes([]storage.VolumeInfo{}, dn.GetVolumes())
	// DeltaUpdateEcShards(新增=[], 删除=所有分片)
	dn.DeltaUpdateEcShards([]*erasure_coding.EcVolumeInfo{}, dn.GetEcShards())

	// 【步骤 6：从父节点中移除】
	// 从 Rack 的子节点列表中删除该 DataNode
	if dn.Parent() != nil {
		dn.Parent().UnlinkChildNode(dn.Id())
	}
}
