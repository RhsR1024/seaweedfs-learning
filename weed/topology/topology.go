// Package topology 实现了 SeaweedFS 的拓扑结构管理
// 拓扑结构采用分层设计: Topology -> DataCenter -> Rack -> DataNode
// 负责管理集群中所有节点、Volume 的分布和状态
package topology

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"

	backoff "github.com/cenkalti/backoff/v4"

	hashicorpRaft "github.com/hashicorp/raft"
	"github.com/seaweedfs/raft"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/sequence"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// Topology 拓扑结构,SeaweedFS 集群的顶层管理结构
// 采用分层树形结构: Topology -> DataCenter -> Rack -> DataNode -> Volume
// 负责整个集群的资源管理、负载均衡和高可用
type Topology struct {
	vacuumLockCounter int64    // Vacuum(清理)锁计数器,用于控制并发清理操作
	NodeImpl                   // 嵌入 NodeImpl,实现节点接口

	collectionMap  *util.ConcurrentReadMap           // Collection 映射表,key: collection名称, value: *Collection
	ecShardMap     map[needle.VolumeId]*EcShardLocations // EC(纠删码) Shard 位置映射
	ecShardMapLock sync.RWMutex                      // EC Shard 映射表的读写锁

	pulse int64 // 心跳间隔(秒)

	volumeSizeLimit  uint64 // Volume 大小限制(字节)
	replicationAsMin bool   // 是否将副本数视为最小值(而非精确值)
	isDisableVacuum  bool   // 是否禁用 Vacuum 清理功能

	Sequence sequence.Sequencer // 序列号生成器,用于生成唯一的文件 ID

	chanFullVolumes    chan storage.VolumeInfo // 已满 Volume 的通知通道
	chanCrowdedVolumes chan storage.VolumeInfo // 拥挤 Volume 的通知通道

	Configuration *Configuration // 配置信息

	// Raft 一致性协议相关
	RaftServer           raft.Server      // SeaweedFS 自研 Raft 实现
	RaftServerAccessLock sync.RWMutex     // Raft 服务器访问锁
	HashicorpRaft        *hashicorpRaft.Raft // Hashicorp Raft 实现
	barrierLock          sync.Mutex       // Barrier 锁
	barrierDone          bool             // Barrier 是否完成

	// UUID 映射管理(用于数据节点唯一标识)
	UuidAccessLock sync.RWMutex        // UUID 映射表访问锁
	UuidMap        map[string][]string // UUID -> 地址列表映射

	LastLeaderChangeTime time.Time // 最后一次 Leader 变更时间
}

// NewTopology 创建新的拓扑结构实例
// 参数:
//   id: 拓扑 ID(通常为 "topo")
//   seq: 序列号生成器,用于生成唯一文件 ID
//   volumeSizeLimit: Volume 大小限制(字节)
//   pulse: 心跳间隔(秒)
//   replicationAsMin: 是否将副本数视为最小值
// 返回:
//   初始化完成的 Topology 实例
func NewTopology(id string, seq sequence.Sequencer, volumeSizeLimit uint64, pulse int, replicationAsMin bool) *Topology {
	t := &Topology{}
	t.id = NodeId(id)
	t.nodeType = "Topology"
	t.NodeImpl.value = t
	t.diskUsages = newDiskUsages()              // 初始化磁盘使用统计
	t.children = make(map[NodeId]Node)          // 初始化子节点映射(DataCenter 节点)
	t.capacityReservations = newCapacityReservations() // 初始化容量预留管理
	t.collectionMap = util.NewConcurrentReadMap()      // 初始化 Collection 映射
	t.ecShardMap = make(map[needle.VolumeId]*EcShardLocations) // 初始化 EC Shard 映射
	t.pulse = int64(pulse)
	t.volumeSizeLimit = volumeSizeLimit
	t.replicationAsMin = replicationAsMin

	t.Sequence = seq

	t.chanFullVolumes = make(chan storage.VolumeInfo)    // 创建已满 Volume 通知通道
	t.chanCrowdedVolumes = make(chan storage.VolumeInfo) // 创建拥挤 Volume 通知通道

	t.Configuration = &Configuration{}

	return t
}

// IsChildLocked 检查拓扑结构中是否有节点被锁定
// 递归检查整个拓扑树: Topology -> DataCenter -> Rack -> DataNode
// 节点锁定时不允许进行某些操作(如 Volume 迁移、重平衡等)
// 返回:
//   bool: 是否有节点被锁定
//   error: 锁定信息(包含被锁定的节点路径)
func (t *Topology) IsChildLocked() (bool, error) {
	// 检查拓扑根节点本身是否被锁定
	if t.IsLocked() {
		return true, errors.New("topology is locked")
	}
	// 遍历所有 DataCenter
	for _, dcNode := range t.Children() {
		if dcNode.IsLocked() {
			return true, fmt.Errorf("topology child %s is locked", dcNode.String())
		}
		// 遍历 DataCenter 下的所有 Rack
		for _, rackNode := range dcNode.Children() {
			if rackNode.IsLocked() {
				return true, fmt.Errorf("dc %s child %s is locked", dcNode.String(), rackNode.String())
			}
			// 遍历 Rack 下的所有 DataNode
			for _, dataNode := range rackNode.Children() {
				if dataNode.IsLocked() {
					return true, fmt.Errorf("rack %s child %s is locked", rackNode.String(), dataNode.Id())
				}
			}
		}
	}
	return false, nil
}

// IsLeader 判断当前节点是否为 Raft 集群的 Leader
// 只有 Leader 节点才能处理写操作(如 Volume 分配、元数据更新等)
// 支持两种 Raft 实现:
//   1. SeaweedFS 自研 Raft
//   2. Hashicorp Raft
// 返回:
//   bool: 是否为 Leader 节点
func (t *Topology) IsLeader() bool {
	t.RaftServerAccessLock.RLock()
	defer t.RaftServerAccessLock.RUnlock()

	if t.RaftServer != nil {
		// 使用 SeaweedFS Raft 实现
		if t.RaftServer.State() == raft.Leader {
			return true
		}
		// 双重检查:即使状态不是 Leader,也检查当前节点名称是否与 Leader 地址匹配
		if leader, err := t.Leader(); err == nil {
			if pb.ServerAddress(t.RaftServer.Name()) == leader {
				return true
			}
		}
	} else if t.HashicorpRaft != nil {
		// 使用 Hashicorp Raft 实现
		if t.HashicorpRaft.State() == hashicorpRaft.Leader {
			return true
		}
	}
	return false
}

// IsLeaderAndCanRead 判断当前节点是否为 Leader 且可以安全读取数据
// Hashicorp Raft 需要执行 Barrier 操作确保读取到最新的已提交数据
// SeaweedFS Raft 则只需检查 Leader 状态
// 返回:
//   bool: 是否为 Leader 且可以安全读取
func (t *Topology) IsLeaderAndCanRead() bool {
	if t.RaftServer != nil {
		return t.IsLeader()
	} else if t.HashicorpRaft != nil {
		// Hashicorp Raft 需要执行 Barrier 确保读取一致性
		return t.IsLeader() && t.DoBarrier()
	} else {
		return false
	}
}

// DoBarrier 执行 Raft Barrier 操作
// Barrier 确保所有已提交的日志都已应用到状态机
// 这样读取操作才能保证读到最新的已提交数据,实现线性一致性读
// Barrier 操作只需要执行一次,完成后设置 barrierDone 标志
// 返回:
//   bool: Barrier 是否成功
func (t *Topology) DoBarrier() bool {
	t.barrierLock.Lock()
	defer t.barrierLock.Unlock()
	if t.barrierDone {
		return true
	}

	glog.V(0).Infof("raft do barrier")
	// 等待所有已提交的日志应用完成,超时时间 2 分钟
	barrier := t.HashicorpRaft.Barrier(2 * time.Minute)
	if err := barrier.Error(); err != nil {
		glog.Errorf("failed to wait for barrier, error %s", err)
		return false

	}

	t.barrierDone = true
	glog.V(0).Infof("raft do barrier success")
	return true
}

// BarrierReset 重置 Barrier 状态
// 当发生 Leader 切换时需要重置 Barrier,新 Leader 需要重新执行 Barrier
func (t *Topology) BarrierReset() {
	t.barrierLock.Lock()
	defer t.barrierLock.Unlock()
	t.barrierDone = false
}

// Leader 获取当前 Raft 集群的 Leader 地址
// 使用指数退避策略重试,最长等待 20 秒
// 如果集群正在进行 Leader 选举,会持续重试直到 Leader 选出
// 返回:
//   pb.ServerAddress: Leader 地址
//   error: 错误信息
func (t *Topology) Leader() (l pb.ServerAddress, err error) {
	exponentialBackoff := backoff.NewExponentialBackOff()
	exponentialBackoff.InitialInterval = 100 * time.Millisecond // 初始重试间隔 100ms
	exponentialBackoff.MaxElapsedTime = 20 * time.Second        // 最长等待 20 秒
	leaderNotSelected := errors.New("leader not selected yet")
	// 使用指数退避重试获取 Leader
	l, err = backoff.RetryWithData(
		func() (l pb.ServerAddress, err error) {
			l, err = t.MaybeLeader()
			if err == nil && l == "" {
				err = leaderNotSelected
			}
			return l, err
		},
		exponentialBackoff)
	if err == leaderNotSelected {
		l = ""
	}
	return l, err
}

// MaybeLeader 尝试获取当前 Raft 集群的 Leader 地址
// 不进行重试,立即返回当前已知的 Leader(可能为空)
// 返回:
//   pb.ServerAddress: Leader 地址(可能为空字符串)
//   error: 错误信息
func (t *Topology) MaybeLeader() (l pb.ServerAddress, err error) {
	t.RaftServerAccessLock.RLock()
	defer t.RaftServerAccessLock.RUnlock()

	if t.RaftServer != nil {
		// 使用 SeaweedFS Raft
		l = pb.ServerAddress(t.RaftServer.Leader())
	} else if t.HashicorpRaft != nil {
		// 使用 Hashicorp Raft
		l = pb.ServerAddress(t.HashicorpRaft.Leader())
	} else {
		err = errors.New("Raft Server not ready yet!")
	}

	return
}

// Lookup 查找指定 Volume 所在的 DataNode 列表
// 首先在普通 Volume 中查找,如果找不到则在 EC Volume 中查找
// 参数:
//   collection: Collection 名称(为空则搜索所有 Collection)
//   vid: Volume ID
// 返回:
//   []*DataNode: 包含该 Volume 的 DataNode 列表
func (t *Topology) Lookup(collection string, vid needle.VolumeId) (dataNodes []*DataNode) {
	// 如果 collection 为空,遍历所有 collection 查找
	// 注意: 如果 collection 很多可能影响性能
	if collection == "" {
		for _, c := range t.collectionMap.Items() {
			if list := c.(*Collection).Lookup(vid); list != nil {
				return list
			}
		}
	} else {
		// 在指定 collection 中查找
		if c, ok := t.collectionMap.Find(collection); ok {
			return c.(*Collection).Lookup(vid)
		}
	}

	// 如果在普通 Volume 中找不到,尝试在 EC Shard 中查找
	if locations, found := t.LookupEcShards(vid); found {
		for _, loc := range locations.Locations {
			dataNodes = append(dataNodes, loc...)
		}
		return dataNodes
	}

	return nil
}

// NextVolumeId 生成下一个可用的 Volume ID
// Volume ID 全局唯一且递增,通过 Raft 保证一致性
// 只有 Leader 节点才能分配新的 Volume ID
// 返回:
//   needle.VolumeId: 新的 Volume ID
//   error: 错误信息
func (t *Topology) NextVolumeId() (needle.VolumeId, error) {
	// 确保当前节点是 Leader 且可以安全读取
	if !t.IsLeaderAndCanRead() {
		return 0, fmt.Errorf("as leader can not read yet")

	}
	vid := t.GetMaxVolumeId() // 获取当前最大 Volume ID
	next := vid.Next()        // 计算下一个 ID

	t.RaftServerAccessLock.RLock()
	defer t.RaftServerAccessLock.RUnlock()

	// 通过 Raft 同步新的最大 Volume ID
	if t.RaftServer != nil {
		// 使用 SeaweedFS Raft
		if _, err := t.RaftServer.Do(NewMaxVolumeIdCommand(next)); err != nil {
			return 0, err
		}
	} else if t.HashicorpRaft != nil {
		// 使用 Hashicorp Raft
		b, err := json.Marshal(NewMaxVolumeIdCommand(next))
		if err != nil {
			return 0, fmt.Errorf("failed marshal NewMaxVolumeIdCommand: %+v", err)
		}
		// 应用 Raft 日志,超时时间 1 秒
		if future := t.HashicorpRaft.Apply(b, time.Second); future.Error() != nil {
			return 0, future.Error()
		}
	}
	return next, nil
}

// PickForWrite 为写操作选择合适的 Volume 并生成文件 ID
// 这是文件上传的核心入口,负责分配存储位置和生成唯一文件 ID
// 参数:
//   requestedCount: 请求的文件数量(批量上传)
//   option: Volume 增长选项(包含 collection、副本策略、TTL 等)
//   volumeLayout: Volume 布局管理器
// 返回:
//   fileId: 生成的文件 ID(格式: volumeId,fileKey,cookie)
//   count: 实际可用的文件 ID 数量
//   volumeLocationList: Volume 所在的 DataNode 列表
//   shouldGrow: 是否需要创建新 Volume
//   error: 错误信息
func (t *Topology) PickForWrite(requestedCount uint64, option *VolumeGrowOption, volumeLayout *VolumeLayout) (fileId string, count uint64, volumeLocationList *VolumeLocationList, shouldGrow bool, err error) {
	var vid needle.VolumeId
	// 从 VolumeLayout 中选择可写的 Volume
	vid, count, volumeLocationList, shouldGrow, err = volumeLayout.PickForWrite(requestedCount, option)
	if err != nil {
		return "", 0, nil, shouldGrow, fmt.Errorf("failed to find writable volumes for collection:%s replication:%s ttl:%s error: %v", option.Collection, option.ReplicaPlacement.String(), option.Ttl.String(), err)
	}
	if volumeLocationList == nil || volumeLocationList.Length() == 0 {
		return "", 0, nil, shouldGrow, fmt.Errorf("%s available for collection:%s replication:%s ttl:%s", NoWritableVolumes, option.Collection, option.ReplicaPlacement.String(), option.Ttl.String())
	}
	// 使用序列号生成器生成文件 Key
	nextFileId := t.Sequence.NextFileId(requestedCount)
	// 组合生成完整的文件 ID: volumeId,fileKey,cookie
	fileId = needle.NewFileId(vid, nextFileId, rand.Uint32()).String()
	return fileId, count, volumeLocationList, shouldGrow, nil
}

// GetVolumeLayout 获取或创建 VolumeLayout
// VolumeLayout 负责管理具有相同属性(collection、副本策略、TTL、磁盘类型)的 Volume 集合
// 如果 Collection 不存在会自动创建
// 参数:
//   collectionName: Collection 名称
//   rp: 副本放置策略
//   ttl: 生存时间(Time To Live)
//   diskType: 磁盘类型(HDD/SSD)
// 返回:
//   *VolumeLayout: Volume 布局管理器
func (t *Topology) GetVolumeLayout(collectionName string, rp *super_block.ReplicaPlacement, ttl *needle.TTL, diskType types.DiskType) *VolumeLayout {
	return t.collectionMap.Get(collectionName, func() interface{} {
		// 如果 Collection 不存在,创建新的 Collection
		return NewCollection(collectionName, t.volumeSizeLimit, t.replicationAsMin)
	}).(*Collection).GetOrCreateVolumeLayout(rp, ttl, diskType)
}

// ListCollections 列出所有 Collection 名称
// 可以选择包含普通 Volume 的 Collection 和/或 EC Volume 的 Collection
// 参数:
//   includeNormalVolumes: 是否包含普通 Volume 的 Collection
//   includeEcVolumes: 是否包含 EC Volume 的 Collection
// 返回:
//   []string: 排序后的 Collection 名称列表
func (t *Topology) ListCollections(includeNormalVolumes, includeEcVolumes bool) (ret []string) {
	found := make(map[string]bool)

	// 从普通 Volume 中收集 Collection
	if includeNormalVolumes {
		t.collectionMap.RLock()
		for _, c := range t.collectionMap.Items() {
			found[c.(*Collection).Name] = true
		}
		t.collectionMap.RUnlock()
	}

	// 从 EC Volume 中收集 Collection
	if includeEcVolumes {
		t.ecShardMapLock.RLock()
		for _, ecVolumeLocation := range t.ecShardMap {
			found[ecVolumeLocation.Collection] = true
		}
		t.ecShardMapLock.RUnlock()
	}

	// 去重并排序
	for k := range found {
		ret = append(ret, k)
	}
	slices.Sort(ret)

	return ret
}

// FindCollection 查找指定名称的 Collection
// 参数:
//   collectionName: Collection 名称
// 返回:
//   *Collection: Collection 对象
//   bool: 是否找到
func (t *Topology) FindCollection(collectionName string) (*Collection, bool) {
	c, hasCollection := t.collectionMap.Find(collectionName)
	if !hasCollection {
		return nil, false
	}
	return c.(*Collection), hasCollection
}

// DeleteCollection 删除指定的 Collection
// 参数:
//   collectionName: 要删除的 Collection 名称
func (t *Topology) DeleteCollection(collectionName string) {
	t.collectionMap.Delete(collectionName)
}

// DeleteLayout 删除指定的 VolumeLayout
// 如果 Collection 下已经没有任何 VolumeLayout,则同时删除 Collection
// 参数:
//   collectionName: Collection 名称
//   rp: 副本放置策略
//   ttl: 生存时间
//   diskType: 磁盘类型
func (t *Topology) DeleteLayout(collectionName string, rp *super_block.ReplicaPlacement, ttl *needle.TTL, diskType types.DiskType) {
	collection, found := t.FindCollection(collectionName)
	if !found {
		return
	}
	collection.DeleteVolumeLayout(rp, ttl, diskType)
	// 如果 Collection 下已经没有任何 VolumeLayout,删除整个 Collection
	if len(collection.storageType2VolumeLayout.Items()) == 0 {
		t.DeleteCollection(collectionName)
	}
}

// RegisterVolumeLayout 注册 Volume 到拓扑结构
// 当 DataNode 加入集群或新建 Volume 时调用
// 参数:
//   v: Volume 信息
//   dn: 所属的 DataNode
func (t *Topology) RegisterVolumeLayout(v storage.VolumeInfo, dn *DataNode) {
	diskType := types.ToDiskType(v.DiskType)
	vl := t.GetVolumeLayout(v.Collection, v.ReplicaPlacement, v.Ttl, diskType)
	vl.RegisterVolume(&v, dn)         // 将 Volume 注册到 VolumeLayout
	vl.EnsureCorrectWritables(&v)     // 确保可写 Volume 列表正确
}

// UnRegisterVolumeLayout 从拓扑结构中注销 Volume
// 当 DataNode 离线或删除 Volume 时调用
// 参数:
//   v: Volume 信息
//   dn: 所属的 DataNode
func (t *Topology) UnRegisterVolumeLayout(v storage.VolumeInfo, dn *DataNode) {
	glog.Infof("removing volume info: %+v from %v", v, dn.id)
	// 清除副本不匹配的监控指标
	if v.ReplicaPlacement.GetCopyCount() > 1 {
		stats.MasterReplicaPlacementMismatch.WithLabelValues(v.Collection, v.Id.String()).Set(0)
	}
	diskType := types.ToDiskType(v.DiskType)
	volumeLayout := t.GetVolumeLayout(v.Collection, v.ReplicaPlacement, v.Ttl, diskType)
	volumeLayout.UnRegisterVolume(&v, dn)
	// 如果 VolumeLayout 已经为空,删除该 Layout
	if volumeLayout.isEmpty() {
		t.DeleteLayout(v.Collection, v.ReplicaPlacement, v.Ttl, diskType)
	}
}

// DataCenterExists 检查指定的数据中心是否存在
// 空名称被视为存在(表示默认数据中心)
// 参数:
//   dcName: 数据中心名称
// 返回:
//   bool: 是否存在
func (t *Topology) DataCenterExists(dcName string) bool {
	return dcName == "" || t.GetDataCenter(dcName) != nil
}

// GetDataCenter 获取指定名称的数据中心
// 参数:
//   dcName: 数据中心名称
// 返回:
//   *DataCenter: 数据中心对象(如果不存在则返回 nil)
func (t *Topology) GetDataCenter(dcName string) (dc *DataCenter) {
	t.RLock()
	defer t.RUnlock()
	for _, c := range t.children {
		dc = c.(*DataCenter)
		if string(dc.Id()) == dcName {
			return dc
		}
	}
	return dc
}

// GetOrCreateDataCenter 获取或创建指定名称的数据中心
// 如果数据中心不存在,会自动创建并链接到拓扑结构
// 参数:
//   dcName: 数据中心名称
// 返回:
//   *DataCenter: 数据中心对象
func (t *Topology) GetOrCreateDataCenter(dcName string) *DataCenter {
	t.Lock()
	defer t.Unlock()
	// 先查找是否已存在
	for _, c := range t.children {
		dc := c.(*DataCenter)
		if string(dc.Id()) == dcName {
			return dc
		}
	}
	// 不存在则创建新的数据中心
	dc := NewDataCenter(dcName)
	t.doLinkChildNode(dc)
	return dc
}

// ListDataCenters 列出所有数据中心的名称
// 返回:
//   []string: 数据中心名称列表
func (t *Topology) ListDataCenters() (dcs []string) {
	t.RLock()
	defer t.RUnlock()
	for _, c := range t.children {
		dcs = append(dcs, string(c.(*DataCenter).Id()))
	}
	return dcs
}

// ListDCAndRacks 列出所有数据中心及其下属的机架
// 返回拓扑结构的两层视图: DataCenter -> Rack
// 返回:
//   map[NodeId][]NodeId: 数据中心 ID -> 机架 ID 列表的映射
func (t *Topology) ListDCAndRacks() (dcs map[NodeId][]NodeId) {
	t.RLock()
	defer t.RUnlock()
	dcs = make(map[NodeId][]NodeId)
	for _, dcNode := range t.children {
		dcNodeId := dcNode.(*DataCenter).Id()
		for _, rackNode := range dcNode.Children() {
			dcs[dcNodeId] = append(dcs[dcNodeId], rackNode.(*Rack).Id())
		}
	}
	return dcs
}

// SyncDataNodeRegistration 同步 DataNode 的 Volume 注册信息(全量同步)
// 当 DataNode 首次加入集群或重新连接时调用
// 通过比对 Master 已知的 Volume 和 DataNode 上报的 Volume,计算增量变化
// 参数:
//   volumes: DataNode 上报的所有 Volume 信息
//   dn: 数据节点
// 返回:
//   newVolumes: 新增的 Volume 列表
//   deletedVolumes: 删除的 Volume 列表
func (t *Topology) SyncDataNodeRegistration(volumes []*master_pb.VolumeInformationMessage, dn *DataNode) (newVolumes, deletedVolumes []storage.VolumeInfo) {
	// 将 protobuf 消息转换为内存结构 storage.VolumeInfo
	var volumeInfos []storage.VolumeInfo
	for _, v := range volumes {
		if vi, err := storage.NewVolumeInfo(v); err == nil {
			volumeInfos = append(volumeInfos, vi)
		} else {
			glog.V(0).Infof("Fail to convert joined volume information: %v", err)
		}
	}
	// 计算 Volume 的增量变化(新增、删除、修改)
	var changedVolumes []storage.VolumeInfo
	newVolumes, deletedVolumes, changedVolumes = dn.UpdateVolumes(volumeInfos)
	// 注册新增的 Volume
	for _, v := range newVolumes {
		t.RegisterVolumeLayout(v, dn)
	}
	// 注销删除的 Volume
	for _, v := range deletedVolumes {
		t.UnRegisterVolumeLayout(v, dn)
	}
	// 更新修改的 Volume 的可写状态
	for _, v := range changedVolumes {
		diskType := types.ToDiskType(v.DiskType)
		vl := t.GetVolumeLayout(v.Collection, v.ReplicaPlacement, v.Ttl, diskType)
		vl.EnsureCorrectWritables(&v)
	}
	return
}

// IncrementalSyncDataNodeRegistration 增量同步 DataNode 的 Volume 注册信息
// 当 DataNode 创建或删除 Volume 时,只上报变化的部分,提高效率
// 参数:
//   newVolumes: 新增的 Volume 列表
//   deletedVolumes: 删除的 Volume 列表
//   dn: 数据节点
func (t *Topology) IncrementalSyncDataNodeRegistration(newVolumes, deletedVolumes []*master_pb.VolumeShortInformationMessage, dn *DataNode) {
	var newVis, oldVis []storage.VolumeInfo
	// 转换新增的 Volume 信息
	for _, v := range newVolumes {
		vi, err := storage.NewVolumeInfoFromShort(v)
		if err != nil {
			glog.V(0).Infof("NewVolumeInfoFromShort %v: %v", v, err)
			continue
		}
		newVis = append(newVis, vi)
	}
	// 转换删除的 Volume 信息
	for _, v := range deletedVolumes {
		vi, err := storage.NewVolumeInfoFromShort(v)
		if err != nil {
			glog.V(0).Infof("NewVolumeInfoFromShort %v: %v", v, err)
			continue
		}
		oldVis = append(oldVis, vi)
	}
	// 增量更新 DataNode 的 Volume 列表
	dn.DeltaUpdateVolumes(newVis, oldVis)

	// 注册新增的 Volume
	for _, vi := range newVis {
		t.RegisterVolumeLayout(vi, dn)
	}
	// 注销删除的 Volume
	for _, vi := range oldVis {
		t.UnRegisterVolumeLayout(vi, dn)
	}

	return
}

// DataNodeRegistration 将 DataNode 注册到拓扑结构
// 如果 DataNode 已经有父节点,说明已经注册过,直接返回
// 否则根据指定的数据中心和机架名称,将 DataNode 链接到拓扑树中
// 参数:
//   dcName: 数据中心名称
//   rackName: 机架名称
//   dn: 数据节点
func (t *Topology) DataNodeRegistration(dcName, rackName string, dn *DataNode) {
	if dn.Parent() != nil {
		return
	}
	// 将 DataNode 注册到拓扑结构: Topology -> DataCenter -> Rack -> DataNode
	dc := t.GetOrCreateDataCenter(dcName)
	rack := dc.GetOrCreateRack(rackName)
	rack.LinkChildNode(dn)
	glog.Infof("[%s] reLink To topo  ", dn.Id())
}

// DisableVacuum 禁用 Vacuum 清理功能
// Vacuum 用于清理已删除文件占用的空间,可以在维护期间临时禁用
func (t *Topology) DisableVacuum() {
	glog.V(0).Infof("DisableVacuum")
	t.isDisableVacuum = true
}

// EnableVacuum 启用 Vacuum 清理功能
func (t *Topology) EnableVacuum() {
	glog.V(0).Infof("EnableVacuum")
	t.isDisableVacuum = false
}
