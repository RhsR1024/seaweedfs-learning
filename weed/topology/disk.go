package topology

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"

	"github.com/seaweedfs/seaweedfs/weed/storage"
)

// Disk 表示一个物理磁盘或逻辑存储单元
// 在 SeaweedFS 拓扑结构中，Disk 是最底层的存储节点
// 层次关系：Topology → DataCenter → Rack → DataNode → Disk
//
// 职责：
//   - 存储 Volume 和 EC Shard 的元数据（实际数据在文件系统中）
//   - 管理单个磁盘的 Volume 列表
//   - 维护磁盘级别的使用统计
//   - 支持纠删码（EC）和普通 Volume 的混合存储
//
// 说明：
//   - 一个 DataNode 可以有多个 Disk（按磁盘类型区分，如 "hdd", "ssd", "nvme"）
//   - Disk ID 就是磁盘类型字符串
type Disk struct {
	NodeImpl // 继承通用节点实现

	// Volume 存储
	// key: VolumeId, value: Volume 信息（大小、副本策略、Collection 等）
	volumes map[needle.VolumeId]storage.VolumeInfo

	// 纠删码（EC）分片存储
	// key: VolumeId, value: EC Volume 信息（包含 ShardBits，标识持有哪些分片）
	// 一个 EC Volume 被分成 10 个数据分片 + 4 个校验分片，可分布在不同 Disk 上
	ecShards     map[needle.VolumeId]*erasure_coding.EcVolumeInfo
	ecShardsLock sync.RWMutex // EC Shard 的独立读写锁（与 NodeImpl.RWMutex 分离）
}

// NewDisk 创建新的磁盘节点
//
// 参数：
//   - diskType: 磁盘类型字符串（如 "hdd", "ssd", "nvme"）
//     这个字符串既是 Disk 的 ID，也是磁盘类型标识
//
// 返回：
//   - *Disk: 初始化完成的磁盘实例
//
// 初始化内容：
//   - 设置节点 ID 为磁盘类型
//   - 初始化 Volume 和 EC Shard 的存储 map
//   - 初始化磁盘使用统计
func NewDisk(diskType string) *Disk {
	s := &Disk{}
	s.id = NodeId(diskType)                                              // Disk ID 就是磁盘类型（如 "hdd"）
	s.nodeType = "Disk"                                                  // 标记节点类型为 Disk
	s.diskUsages = newDiskUsages()                                       // 初始化磁盘使用统计
	s.volumes = make(map[needle.VolumeId]storage.VolumeInfo, 2)         // 初始化 Volume map，容量 2（会自动扩容）
	s.ecShards = make(map[needle.VolumeId]*erasure_coding.EcVolumeInfo, 2) // 初始化 EC Shard map
	s.NodeImpl.value = s                                                 // 保存对自身的引用，用于类型转换
	return s
}

// DiskUsages 管理多个磁盘类型的使用统计
// 每个节点（Disk/DataNode/Rack/DataCenter/Topology）都有一个 DiskUsages
// 用于聚合和统计不同磁盘类型的使用情况
//
// 说明：
//   - 支持多种磁盘类型（hdd, ssd, nvme 等）
//   - 使用读写锁保护并发访问
//   - 统计信息会向上传播到父节点
type DiskUsages struct {
	sync.RWMutex // 保护 usages map 的并发访问
	// key: 磁盘类型（如 "hdd", "ssd"）
	// value: 该类型磁盘的使用统计
	usages map[types.DiskType]*DiskUsageCounts
}

// newDiskUsages 创建新的磁盘使用统计管理器
func newDiskUsages() *DiskUsages {
	return &DiskUsages{
		usages: make(map[types.DiskType]*DiskUsageCounts),
	}
}

// negative 返回所有统计的负值版本
// 用于节点从拓扑树中移除时，向上传播负的统计增量，从而减少父节点的统计
//
// 返回：
//   - *DiskUsages: 新的 DiskUsages，所有计数都是原值的负数
//
// 使用场景：
//   - NodeImpl.UnlinkChildNode 中，移除子节点时需要减少父节点的统计
func (d *DiskUsages) negative() *DiskUsages {
	d.RLock()
	defer d.RUnlock()
	t := newDiskUsages()
	// 遍历所有磁盘类型，创建负值版本
	for diskType, b := range d.usages {
		a := t.getOrCreateDisk(diskType)
		a.volumeCount = -b.volumeCount             // Volume 数量取负
		a.remoteVolumeCount = -b.remoteVolumeCount // 远程 Volume 数量取负
		a.activeVolumeCount = -b.activeVolumeCount // 活跃 Volume 数量取负
		a.ecShardCount = -b.ecShardCount           // EC 分片数量取负
		a.maxVolumeCount = -b.maxVolumeCount       // 最大 Volume 数量取负
	}
	return t
}

// ToDiskInfo 转换为 protobuf 格式的磁盘信息 map
// 用于 gRPC 通信和集群状态查询
//
// 返回：
//   - map[string]*master_pb.DiskInfo: key 是磁盘类型，value 是磁盘信息
func (d *DiskUsages) ToDiskInfo() map[string]*master_pb.DiskInfo {
	ret := make(map[string]*master_pb.DiskInfo)
	for diskType, diskUsageCounts := range d.usages {
		m := &master_pb.DiskInfo{
			VolumeCount:       diskUsageCounts.volumeCount,       // 总 Volume 数
			MaxVolumeCount:    diskUsageCounts.maxVolumeCount,    // 最大 Volume 数
			// 计算空闲 Volume 槽位数
			// 公式：max - (normal - remote) - ec_volumes
			// 其中：ec_volumes = (ecShardCount + 1) / 10（向上取整）
			FreeVolumeCount:   diskUsageCounts.maxVolumeCount - (diskUsageCounts.volumeCount - diskUsageCounts.remoteVolumeCount) - (diskUsageCounts.ecShardCount+1)/erasure_coding.DataShardsCount,
			ActiveVolumeCount: diskUsageCounts.activeVolumeCount, // 可写 Volume 数
			RemoteVolumeCount: diskUsageCounts.remoteVolumeCount, // 远程 Volume 数（云存储）
		}
		ret[string(diskType)] = m
	}
	return ret
}

// FreeSpace 计算所有磁盘类型的总空闲空间（以 Volume 槽位数计）
//
// 返回：
//   - int64: 总空闲 Volume 槽位数
func (d *DiskUsages) FreeSpace() (freeSpace int64) {
	d.RLock()
	defer d.RUnlock()
	for _, diskUsage := range d.usages {
		freeSpace += diskUsage.FreeSpace() // 累加每种磁盘类型的空闲空间
	}
	return
}

// GetMaxVolumeCount 获取所有磁盘类型的总最大 Volume 数
//
// 返回：
//   - int64: 总最大 Volume 容量
func (d *DiskUsages) GetMaxVolumeCount() (maxVolumeCount int64) {
	d.RLock()
	defer d.RUnlock()
	for _, diskUsage := range d.usages {
		maxVolumeCount += diskUsage.maxVolumeCount // 累加每种磁盘类型的最大容量
	}
	return
}

// DiskUsageCounts 单个磁盘类型的使用统计
// 所有字段使用 int64 类型，支持原子操作
//
// 字段说明：
//   - volumeCount: 总 Volume 数（包括可写和只读）
//   - remoteVolumeCount: 远程 Volume 数（云存储 tiering，不占用本地磁盘）
//   - activeVolumeCount: 活跃（可写）Volume 数
//   - ecShardCount: 纠删码分片总数（10 个数据分片算一个完整 Volume）
//   - maxVolumeCount: 最大 Volume 容量（由磁盘空间和配置决定）
type DiskUsageCounts struct {
	volumeCount       int64 // 总 Volume 数
	remoteVolumeCount int64 // 远程 Volume 数（不占本地空间）
	activeVolumeCount int64 // 可写 Volume 数
	ecShardCount      int64 // EC 分片数
	maxVolumeCount    int64 // 最大 Volume 数
}

// addDiskUsageCounts 原子地添加另一个 DiskUsageCounts 的值
// 用于向上传播统计增量：Disk → DataNode → Rack → DataCenter → Topology
//
// 参数：
//   - b: 要添加的统计增量（可以是负值，表示减少）
//
// 线程安全：使用 atomic 操作保证并发安全
func (a *DiskUsageCounts) addDiskUsageCounts(b *DiskUsageCounts) {
	atomic.AddInt64(&a.volumeCount, b.volumeCount)
	atomic.AddInt64(&a.remoteVolumeCount, b.remoteVolumeCount)
	atomic.AddInt64(&a.activeVolumeCount, b.activeVolumeCount)
	atomic.AddInt64(&a.ecShardCount, b.ecShardCount)
	atomic.AddInt64(&a.maxVolumeCount, b.maxVolumeCount)
}

// FreeSpace 计算空闲 Volume 槽位数
// 考虑了远程 Volume（不占空间）和 EC 分片（10 个分片占 1 个槽位）
//
// 返回：
//   - int64: 空闲 Volume 槽位数
//
// 计算公式：
//   freeSpace = maxVolumeCount + remoteVolumeCount - volumeCount - ecVolumes
//   其中：ecVolumes = (ecShardCount + 9) / 10（向上取整）
func (a *DiskUsageCounts) FreeSpace() int64 {
	// 基础空闲空间 = 最大容量 + 远程卷数 - 总卷数
	// 远程卷不占本地空间，所以加回来
	freeVolumeSlotCount := a.maxVolumeCount + a.remoteVolumeCount - a.volumeCount

	if a.ecShardCount > 0 {
		// 减去 EC 卷占用的空间
		// 每 10 个 EC 分片占 1 个 Volume 槽位，额外 -1 是为了保守估计
		freeVolumeSlotCount = freeVolumeSlotCount - a.ecShardCount/erasure_coding.DataShardsCount - 1
	}
	return freeVolumeSlotCount
}

// minus 计算两个 DiskUsageCounts 的差值
// 返回新的 DiskUsageCounts，不修改原对象
//
// 参数：
//   - b: 要减去的统计
//
// 返回：
//   - *DiskUsageCounts: a - b 的结果
func (a *DiskUsageCounts) minus(b *DiskUsageCounts) *DiskUsageCounts {
	return &DiskUsageCounts{
		volumeCount:       a.volumeCount - b.volumeCount,
		remoteVolumeCount: a.remoteVolumeCount - b.remoteVolumeCount,
		activeVolumeCount: a.activeVolumeCount - b.activeVolumeCount,
		ecShardCount:      a.ecShardCount - b.ecShardCount,
		maxVolumeCount:    a.maxVolumeCount - b.maxVolumeCount,
	}
}

// getOrCreateDisk 获取或创建指定磁盘类型的统计对象
//
// 参数：
//   - diskType: 磁盘类型（如 types.HDD, types.SSD）
//
// 返回：
//   - *DiskUsageCounts: 对应磁盘类型的统计对象
//
// 线程安全：使用写锁保护并发访问
func (du *DiskUsages) getOrCreateDisk(diskType types.DiskType) *DiskUsageCounts {
	du.Lock()
	defer du.Unlock()
	t, found := du.usages[diskType]
	if found {
		return t
	}
	// 不存在则创建新的统计对象
	t = &DiskUsageCounts{}
	du.usages[diskType] = t
	return t
}

// String 返回 Disk 的可读字符串表示
// 格式：Disk:拓扑路径:ID, volumes:Volume列表, ecShards:EC分片列表
//
// 返回：
//   - string: Disk 的详细信息字符串
//
// 线程安全：使用读锁保护并发访问
func (d *Disk) String() string {
	d.RLock()
	defer d.RUnlock()
	return fmt.Sprintf("Disk:%s, volumes:%v, ecShards:%v", d.NodeImpl.String(), d.volumes, d.ecShards)
}

// AddOrUpdateVolume 添加或更新一个 Volume
// 外部接口，内部调用 doAddOrUpdateVolume 完成实际操作
//
// 参数：
//   - v: Volume 信息
//
// 返回：
//   - isNew: 是否是新添加的 Volume
//   - isChanged: Volume 的只读状态是否发生变化
//
// 线程安全：使用写锁保护并发访问
func (d *Disk) AddOrUpdateVolume(v storage.VolumeInfo) (isNew, isChanged bool) {
	d.Lock()
	defer d.Unlock()
	return d.doAddOrUpdateVolume(v)
}

// doAddOrUpdateVolume 内部方法：添加或更新 Volume
// 处理 Volume 的添加、更新和统计传播
//
// 参数：
//   - v: Volume 信息
//
// 返回：
//   - isNew: 是否是新添加的 Volume
//   - isChanged: Volume 的只读状态是否发生变化
//
// 工作流程：
//   1. 如果 Volume 不存在，添加并更新统计
//   2. 如果 Volume 已存在，检查远程状态变化并更新
//   3. 向上传播统计变化
func (d *Disk) doAddOrUpdateVolume(v storage.VolumeInfo) (isNew, isChanged bool) {
	deltaDiskUsage := &DiskUsageCounts{}
	if oldV, ok := d.volumes[v.Id]; !ok {
		// 【情况 1：新 Volume】
		d.volumes[v.Id] = v            // 添加到 volumes map
		deltaDiskUsage.volumeCount = 1 // 总 Volume 数 +1
		if v.IsRemote() {
			deltaDiskUsage.remoteVolumeCount = 1 // 远程 Volume 数 +1（云存储 tiering）
		}
		if !v.ReadOnly {
			deltaDiskUsage.activeVolumeCount = 1 // 可写 Volume 数 +1
		}
		d.UpAdjustMaxVolumeId(v.Id)                                     // 更新最大 VolumeId
		d.UpAdjustDiskUsageDelta(types.ToDiskType(v.DiskType), deltaDiskUsage) // 向上传播统计
		isNew = true
	} else {
		// 【情况 2：Volume 已存在，检查远程状态变化】
		if oldV.IsRemote() != v.IsRemote() {
			// 远程状态发生变化（本地↔云存储）
			if v.IsRemote() {
				deltaDiskUsage.remoteVolumeCount = 1 // 变为远程
			}
			if oldV.IsRemote() {
				deltaDiskUsage.remoteVolumeCount = -1 // 不再是远程
			}
			d.UpAdjustDiskUsageDelta(types.ToDiskType(v.DiskType), deltaDiskUsage) // 向上传播统计变化
		}
		// 检查只读状态是否变化
		isChanged = d.volumes[v.Id].ReadOnly != v.ReadOnly
		d.volumes[v.Id] = v // 更新 Volume 信息
	}
	return
}

// GetVolumes 获取 Disk 上所有的 Volume
//
// 返回：
//   - []storage.VolumeInfo: Volume 信息列表
//
// 线程安全：使用读锁保护并发访问
func (d *Disk) GetVolumes() (ret []storage.VolumeInfo) {
	d.RLock()
	for _, v := range d.volumes {
		ret = append(ret, v)
	}
	d.RUnlock()
	return ret
}

// GetVolumesById 根据 Volume ID 查找 Volume 信息
//
// 参数：
//   - id: Volume ID
//
// 返回：
//   - storage.VolumeInfo: Volume 信息
//   - error: 未找到时返回错误
//
// 线程安全：使用读锁保护并发访问
func (d *Disk) GetVolumesById(id needle.VolumeId) (storage.VolumeInfo, error) {
	d.RLock()
	defer d.RUnlock()
	vInfo, ok := d.volumes[id]
	if ok {
		return vInfo, nil
	} else {
		return storage.VolumeInfo{}, fmt.Errorf("volumeInfo not found")
	}
}

// DeleteVolumeById 根据 Volume ID 删除 Volume
//
// 参数：
//   - id: Volume ID
//
// 说明：
//   - 仅从内存中删除 Volume 记录
//   - 不删除磁盘上的实际文件
//   - 不更新磁盘使用统计（由调用者负责）
//
// 线程安全：使用写锁保护并发访问
func (d *Disk) DeleteVolumeById(id needle.VolumeId) {
	d.Lock()
	defer d.Unlock()
	delete(d.volumes, id)
}

// GetDataCenter 获取 Disk 所属的 DataCenter
// 通过拓扑树向上查找：Disk → DataNode → Rack → DataCenter
//
// 返回：
//   - *DataCenter: 所属的 DataCenter
func (d *Disk) GetDataCenter() *DataCenter {
	dn := d.Parent()        // 父节点是 DataNode
	rack := dn.Parent()     // DataNode 的父节点是 Rack
	dcNode := rack.Parent() // Rack 的父节点是 DataCenter
	dcValue := dcNode.GetValue()
	return dcValue.(*DataCenter) // 类型断言
}

// GetRack 获取 Disk 所属的 Rack
// 通过拓扑树向上查找：Disk → DataNode → Rack
//
// 返回：
//   - *Rack: 所属的 Rack
func (d *Disk) GetRack() *Rack {
	return d.Parent().Parent().(*NodeImpl).value.(*Rack) // 类型断言
}

// GetTopology 获取顶层的 Topology 节点
// 通过拓扑树不断向上查找，直到找到没有父节点的根节点
//
// 返回：
//   - *Topology: 顶层 Topology 节点
func (d *Disk) GetTopology() *Topology {
	p := d.Parent()
	for p.Parent() != nil {
		p = p.Parent() // 不断向上
	}
	t := p.(*Topology) // 类型断言：根节点是 Topology
	return t
}

// ToMap 将 Disk 信息转换为 map 格式
// 用于 JSON 序列化和 HTTP API 响应
//
// 返回：
//   - interface{}: map[string]interface{} 格式的 Disk 信息，包含：
//     - Volumes: Volume 数量
//     - VolumeIds: Volume ID 列表（人类可读）
//     - EcShards: EC 分片数量
//     - Max: 最大 Volume 容量
//     - Free: 空闲 Volume 槽位数
func (d *Disk) ToMap() interface{} {
	ret := make(map[string]interface{})
	diskUsage := d.diskUsages.getOrCreateDisk(types.ToDiskType(string(d.Id())))
	ret["Volumes"] = diskUsage.volumeCount
	ret["VolumeIds"] = d.GetVolumeIds()
	ret["EcShards"] = diskUsage.ecShardCount
	ret["Max"] = diskUsage.maxVolumeCount
	ret["Free"] = d.FreeSpace()
	return ret
}

// FreeSpace 计算 Disk 的空闲 Volume 槽位数
// 考虑了远程 Volume 和 EC 分片的影响
//
// 返回：
//   - int64: 空闲 Volume 槽位数
func (d *Disk) FreeSpace() int64 {
	t := d.diskUsages.getOrCreateDisk(types.ToDiskType(string(d.Id())))
	return t.FreeSpace()
}

// ToDiskInfo 将 Disk 转换为 protobuf 消息格式
// 用于 gRPC 通信，包含详细的磁盘信息和 Volume 列表
//
// 返回：
//   - *master_pb.DiskInfo: protobuf 格式的 Disk 信息，包含：
//     - Type: 磁盘类型（如 "hdd", "ssd"）
//     - VolumeCount: Volume 总数
//     - MaxVolumeCount: 最大 Volume 容量
//     - FreeVolumeCount: 空闲 Volume 槽位数
//     - ActiveVolumeCount: 可写 Volume 数
//     - RemoteVolumeCount: 远程 Volume 数
//     - DiskId: 物理磁盘 ID
//     - VolumeInfos: 所有 Volume 的详细信息列表
//     - EcShardInfos: 所有 EC 分片的详细信息列表
//
// 说明：
//   - DiskId 从第一个 Volume 或 EC 分片中获取
//   - 包含每个 Volume 和 EC 分片的完整信息
func (d *Disk) ToDiskInfo() *master_pb.DiskInfo {
	diskUsage := d.diskUsages.getOrCreateDisk(types.ToDiskType(string(d.Id())))

	// 获取物理磁盘 ID（从第一个 Volume 或 EC 分片中获取）
	var diskId uint32
	volumes := d.GetVolumes()
	ecShards := d.GetEcShards()
	if len(volumes) > 0 {
		diskId = volumes[0].DiskId // 从 Volume 获取
	} else if len(ecShards) > 0 {
		diskId = ecShards[0].DiskId // 从 EC 分片获取
	}

	m := &master_pb.DiskInfo{
		Type:              string(d.Id()),         // 磁盘类型（如 "hdd"）
		VolumeCount:       diskUsage.volumeCount,  // 总 Volume 数
		MaxVolumeCount:    diskUsage.maxVolumeCount, // 最大 Volume 数
		// 计算空闲槽位数：max - (normal - remote) - ec_volumes
		FreeVolumeCount:   diskUsage.maxVolumeCount - (diskUsage.volumeCount - diskUsage.remoteVolumeCount) - (diskUsage.ecShardCount+1)/erasure_coding.DataShardsCount,
		ActiveVolumeCount: diskUsage.activeVolumeCount, // 可写 Volume 数
		RemoteVolumeCount: diskUsage.remoteVolumeCount, // 远程 Volume 数
		DiskId:            diskId,                       // 物理磁盘 ID
	}
	// 添加所有 Volume 的详细信息
	for _, v := range volumes {
		m.VolumeInfos = append(m.VolumeInfos, v.ToVolumeInformationMessage())
	}
	// 添加所有 EC 分片的详细信息
	for _, ecv := range ecShards {
		m.EcShardInfos = append(m.EcShardInfos, ecv.ToVolumeEcShardInformationMessage())
	}
	return m
}

// GetVolumeIds 返回人类可读的 Volume ID 列表
// 限制最多显示 100 个 Volume ID
//
// 返回：
//   - string: Volume ID 列表，格式如 "1,2,3-10,15,20-30"
//
// 说明：
//   - 使用区间表示法压缩连续的 ID
//   - 最多显示 100 个 ID，超过部分用 "..." 表示
//
// 线程安全：使用读锁保护并发访问
func (d *Disk) GetVolumeIds() string {
	d.RLock()
	defer d.RUnlock()
	ids := make([]int, 0, len(d.volumes))

	// 收集所有 Volume ID
	for k := range d.volumes {
		ids = append(ids, int(k))
	}

	// 转换为人类可读格式，最多 100 个
	return util.HumanReadableIntsMax(100, ids...)
}
