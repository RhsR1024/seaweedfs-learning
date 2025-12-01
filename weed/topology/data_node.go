package topology

import (
	"fmt"
	"sync/atomic"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// DataNode 表示一个 Volume Server 节点
// 在 SeaweedFS 拓扑结构中，DataNode 是实际存储数据的物理服务器
// 层次关系：Topology → DataCenter → Rack → DataNode → Disk
//
// 职责：
//   - 存储和管理 Volume（通过其子节点 Disk）
//   - 处理文件的读写请求
//   - 定期向 Master 发送心跳，报告存储状态
//   - 支持多磁盘（每个磁盘是一个 Disk 子节点）
type DataNode struct {
	NodeImpl // 继承通用节点实现

	// 网络配置
	Ip        string // Volume server 的 IP 地址
	Port      int    // HTTP 服务端口（默认 8080）
	GrpcPort  int    // gRPC 服务端口（默认 18080）
	PublicUrl string // 公网访问 URL（用于客户端访问，可能不同于内网 IP）

	// 状态管理
	LastSeen      int64 // 最后一次心跳时间（Unix 时间戳，秒）
	Counter       int   // 节点重启计数器，用于区分相同地址的不同实例（防止旧节点覆盖新节点）
	IsTerminating bool  // 是否正在关闭（为 true 时不再分配新 volume）
}

// NewDataNode 创建新的数据节点
//
// 参数：
//   - id: 数据节点的唯一标识符，通常是 "ip:port" 格式（如 "192.168.1.10:8080"）
//
// 返回：
//   - *DataNode: 初始化完成的数据节点实例
//
// 初始化内容：
//   - 设置节点 ID 和类型
//   - 初始化磁盘使用统计（聚合所有磁盘）
//   - 初始化子节点映射（存储 Disk）
//   - 初始化容量预留管理器
func NewDataNode(id string) *DataNode {
	dn := &DataNode{}
	dn.id = NodeId(id)                              // 设置节点 ID，通常是 "ip:port"
	dn.nodeType = "DataNode"                        // 标记节点类型为 DataNode
	dn.diskUsages = newDiskUsages()                 // 初始化磁盘使用统计，聚合所有 Disk 的使用情况
	dn.children = make(map[NodeId]Node)             // 初始化子节点映射，key 是磁盘类型（如 "hdd", "ssd"），value 是 Disk 节点
	dn.capacityReservations = newCapacityReservations() // 初始化容量预留，防止并发创建 volume 时的竞争条件
	dn.NodeImpl.value = dn                          // 保存对自身的引用，用于类型转换
	return dn
}

// String 返回 DataNode 的可读字符串表示
// 格式：Node:拓扑路径:ID, Ip:IP地址, Port:端口, PublicUrl:公网URL
//
// 返回：
//   - string: DataNode 的详细信息字符串
//
// 线程安全：使用读锁保护并发访问
func (dn *DataNode) String() string {
	dn.RLock()         // 获取读锁
	defer dn.RUnlock() // 函数返回时释放锁
	return fmt.Sprintf("Node:%s, Ip:%s, Port:%d, PublicUrl:%s", dn.NodeImpl.String(), dn.Ip, dn.Port, dn.PublicUrl)
}

// AddOrUpdateVolume 添加或更新一个 Volume
// 外部接口，内部调用 doAddOrUpdateVolume 完成实际操作
//
// 参数：
//   - v: Volume 信息，包含 ID、大小、副本策略、磁盘类型等
//
// 返回：
//   - isNew: 是否是新添加的 Volume
//   - isChangedRO: Volume 的只读状态是否发生变化
//
// 线程安全：使用写锁保护并发访问
func (dn *DataNode) AddOrUpdateVolume(v storage.VolumeInfo) (isNew, isChangedRO bool) {
	dn.Lock()         // 获取写锁
	defer dn.Unlock() // 函数返回时释放锁
	return dn.doAddOrUpdateVolume(v)
}

// getOrCreateDisk 获取或创建指定类型的 Disk 节点
// 每个 DataNode 可以有多个 Disk 子节点，按磁盘类型区分（如 "hdd", "ssd", "nvme"）
//
// 参数：
//   - diskType: 磁盘类型字符串（如 "hdd", "ssd"）
//
// 返回：
//   - *Disk: 获取到或新创建的 Disk 实例
//
// 说明：
//   - 如果 Disk 不存在，会自动创建并链接到 DataNode
//   - 创建新 Disk 时会触发磁盘使用统计的向上传播
func (dn *DataNode) getOrCreateDisk(diskType string) *Disk {
	c, found := dn.children[NodeId(diskType)] // 查找是否已存在该类型的 Disk
	if !found {
		// Disk 不存在，创建新 Disk
		c = NewDisk(diskType)
		// 链接到 DataNode
		// doLinkChildNode 会：
		//   1. 将 disk 添加到 dn.children
		//   2. 设置 disk.parent = dn
		//   3. 向上传播磁盘使用统计到 Rack → DataCenter → Topology
		dn.doLinkChildNode(c)
	}
	disk := c.(*Disk) // 类型断言：子节点必定是 Disk 类型
	return disk
}

// doAddOrUpdateVolume 内部方法：添加或更新 Volume
// 将 Volume 添加到对应的 Disk 节点
//
// 参数：
//   - v: Volume 信息
//
// 返回：
//   - isNew: 是否是新添加的 Volume
//   - isChanged: Volume 的只读状态是否发生变化
//
// 说明：
//   - 根据 v.DiskType 自动选择或创建对应的 Disk
//   - 实际的 Volume 存储和统计更新在 Disk.AddOrUpdateVolume 中完成
func (dn *DataNode) doAddOrUpdateVolume(v storage.VolumeInfo) (isNew, isChanged bool) {
	disk := dn.getOrCreateDisk(v.DiskType) // 获取或创建对应类型的 Disk
	return disk.AddOrUpdateVolume(v)        // 委托给 Disk 处理
}

// UpdateVolumes 检测 Volume server 上的 Volume 变化
// 用于 Master 在收到心跳时，对比当前状态和上报状态，识别新增/删除/变化的 Volume
//
// 参数：
//   - actualVolumes: Volume server 上报的实际 Volume 列表（来自心跳）
//
// 返回：
//   - newVolumes: 新增的 Volume 列表
//   - deletedVolumes: 已删除的 Volume 列表
//   - changedVolumes: 状态发生变化的 Volume 列表（如只读状态改变）
//
// 使用场景：
//   - Volume server 心跳处理
//   - Master 通知其他客户端 Volume 变化
//   - 集群状态同步
//
// 线程安全：使用写锁保护整个对比和更新过程
func (dn *DataNode) UpdateVolumes(actualVolumes []storage.VolumeInfo) (newVolumes, deletedVolumes, changedVolumes []storage.VolumeInfo) {

	// 【步骤 1：构建实际 Volume 的 map，便于快速查找】
	actualVolumeMap := make(map[needle.VolumeId]storage.VolumeInfo)
	for _, v := range actualVolumes {
		actualVolumeMap[v.Id] = v
	}

	dn.Lock()         // 获取写锁，保护并发访问
	defer dn.Unlock() // 函数返回时释放锁

	// 【步骤 2：获取 DataNode 当前记录的所有 Volume】
	existingVolumes := dn.getVolumes()

	// 【步骤 3：查找已删除的 Volume】
	// 遍历现有 Volume，如果在实际列表中不存在，说明已被删除
	for _, v := range existingVolumes {
		vid := v.Id
		if _, ok := actualVolumeMap[vid]; !ok {
			// Volume 已被删除
			glog.V(0).Infoln("Deleting volume id:", vid)

			// 从对应的 Disk 中删除 Volume
			disk := dn.getOrCreateDisk(v.DiskType)
			disk.DeleteVolumeById(vid)
			deletedVolumes = append(deletedVolumes, v)

			// 更新磁盘使用统计
			// 需要减去被删除 Volume 的计数
			deltaDiskUsage := &DiskUsageCounts{}
			deltaDiskUsage.volumeCount = -1 // 总 Volume 数 -1
			if v.IsRemote() {
				deltaDiskUsage.remoteVolumeCount = -1 // 远程 Volume 数 -1（云存储 tiering）
			}
			if !v.ReadOnly {
				deltaDiskUsage.activeVolumeCount = -1 // 可写 Volume 数 -1
			}
			// 向上传播统计变化：Disk → DataNode → Rack → DataCenter → Topology
			disk.UpAdjustDiskUsageDelta(types.ToDiskType(v.DiskType), deltaDiskUsage)
		}
	}

	// 【步骤 4：添加或更新 Volume】
	// 遍历实际 Volume 列表，识别新增和变化的 Volume
	for _, v := range actualVolumes {
		isNew, isChanged := dn.doAddOrUpdateVolume(v)
		if isNew {
			newVolumes = append(newVolumes, v) // 新增的 Volume
		}
		if isChanged {
			changedVolumes = append(changedVolumes, v) // 状态变化的 Volume（如 ReadOnly 改变）
		}
	}
	return
}

// DeltaUpdateVolumes 增量更新 DataNode 的 Volume
// 根据新增和删除的 Volume 列表进行增量更新，不影响未变化的 Volume
//
// 参数：
//   - newVolumes: 新增的 Volume 列表
//   - deletedVolumes: 删除的 Volume 列表
//
// 说明：
//   - 这是 UpdateVolumes 的轻量级替代方案
//   - 适用于已知具体变化的场景，避免全量对比
//   - 会自动更新磁盘使用统计
//
// 线程安全：使用写锁保护并发访问
func (dn *DataNode) DeltaUpdateVolumes(newVolumes, deletedVolumes []storage.VolumeInfo) {
	dn.Lock()
	defer dn.Unlock()

	// 【步骤 1：处理删除的 Volume】
	for _, v := range deletedVolumes {
		disk := dn.getOrCreateDisk(v.DiskType)

		// 检查 Volume 是否存在
		_, err := disk.GetVolumesById(v.Id)
		if err != nil {
			continue // Volume 不存在，跳过
		}
		disk.DeleteVolumeById(v.Id) // 从 Disk 中删除 Volume

		// 更新磁盘使用统计
		deltaDiskUsage := &DiskUsageCounts{}
		deltaDiskUsage.volumeCount = -1 // 总 Volume 数 -1
		if v.IsRemote() {
			deltaDiskUsage.remoteVolumeCount = -1 // 远程 Volume 数 -1
		}
		if !v.ReadOnly {
			deltaDiskUsage.activeVolumeCount = -1 // 可写 Volume 数 -1
		}
		// 向上传播统计变化
		disk.UpAdjustDiskUsageDelta(types.ToDiskType(v.DiskType), deltaDiskUsage)
	}

	// 【步骤 2：处理新增的 Volume】
	for _, v := range newVolumes {
		dn.doAddOrUpdateVolume(v) // 添加或更新 Volume
	}
	return
}

// AdjustMaxVolumeCounts 调整 DataNode 的最大 Volume 容量
// 根据 Volume server 上报的最大容量更新拓扑中的统计
//
// 参数：
//   - maxVolumeCounts: 按磁盘类型分组的最大 Volume 数量 map
//     key: 磁盘类型字符串（如 "hdd", "ssd"）
//     value: 该类型磁盘的最大 Volume 数量
//
// 说明：
//   - Volume server 在启动或配置变更时会上报最大容量
//   - 如果容量没有变化，不进行更新
//   - 如果 Volume server 设置为 0，跳过（可能是临时状态）
//   - 会向上传播容量变化到所有祖先节点
//
// 使用场景：
//   - Volume server 启动时的初始化
//   - 磁盘空间配置变更
//   - Volume server 心跳上报容量更新
func (dn *DataNode) AdjustMaxVolumeCounts(maxVolumeCounts map[string]uint32) {
	for diskType, maxVolumeCount := range maxVolumeCounts {
		if maxVolumeCount == 0 {
			// Volume server 可能将最大容量设为 0（临时状态），跳过
			continue
		}
		dt := types.ToDiskType(diskType)                       // 转换为 DiskType 类型
		currentDiskUsage := dn.diskUsages.getOrCreateDisk(dt) // 获取当前磁盘使用统计
		currentDiskUsageMaxVolumeCount := atomic.LoadInt64(&currentDiskUsage.maxVolumeCount) // 原子读取当前最大容量
		if currentDiskUsageMaxVolumeCount == int64(maxVolumeCount) {
			continue // 容量没有变化，跳过
		}
		// 计算容量变化量并向上传播
		disk := dn.getOrCreateDisk(dt.String())
		disk.UpAdjustDiskUsageDelta(dt, &DiskUsageCounts{
			maxVolumeCount: int64(maxVolumeCount) - currentDiskUsageMaxVolumeCount, // 容量变化量（可正可负）
		})
	}
}

// GetVolumes 获取 DataNode 上所有的 Volume
// 聚合所有 Disk 子节点的 Volume
//
// 返回：
//   - []storage.VolumeInfo: Volume 信息列表
//
// 线程安全：使用读锁保护并发访问
func (dn *DataNode) GetVolumes() (ret []storage.VolumeInfo) {
	dn.RLock()
	for _, c := range dn.children {
		disk := c.(*Disk)
		ret = append(ret, disk.GetVolumes()...) // 聚合所有 Disk 的 Volume
	}
	dn.RUnlock()
	return ret
}

// GetVolumesById 根据 Volume ID 查找 Volume 信息
// 遍历所有 Disk 子节点查找指定的 Volume
//
// 参数：
//   - id: Volume ID
//
// 返回：
//   - storage.VolumeInfo: Volume 信息
//   - error: 未找到时返回错误
//
// 线程安全：使用读锁保护并发访问
func (dn *DataNode) GetVolumesById(id needle.VolumeId) (vInfo storage.VolumeInfo, err error) {
	dn.RLock()
	defer dn.RUnlock()
	found := false
	// 遍历所有 Disk 子节点
	for _, c := range dn.children {
		disk := c.(*Disk)
		vInfo, err = disk.GetVolumesById(id)
		if err == nil {
			found = true
			break // 找到即返回
		}
	}
	if found {
		return vInfo, nil
	} else {
		return storage.VolumeInfo{}, fmt.Errorf("volumeInfo not found")
	}
}

// GetDataCenter 获取 DataNode 所属的 DataCenter
// 通过拓扑树向上查找：DataNode → Rack → DataCenter
//
// 返回：
//   - *DataCenter: 所属的 DataCenter，如果不存在返回 nil
func (dn *DataNode) GetDataCenter() *DataCenter {
	rack := dn.Parent() // 父节点是 Rack
	if rack == nil {
		return nil
	}
	dcNode := rack.Parent() // Rack 的父节点是 DataCenter
	if dcNode == nil {
		return nil
	}
	dcValue := dcNode.GetValue()
	return dcValue.(*DataCenter) // 类型断言
}

// GetDataCenterId 获取 DataNode 所属的 DataCenter ID
//
// 返回：
//   - string: DataCenter ID，如果不存在返回空字符串
func (dn *DataNode) GetDataCenterId() string {
	if dc := dn.GetDataCenter(); dc != nil {
		return string(dc.Id())
	}
	return ""
}

// GetRack 获取 DataNode 所属的 Rack
// DataNode 的直接父节点就是 Rack
//
// 返回：
//   - *Rack: 所属的 Rack
func (dn *DataNode) GetRack() *Rack {
	return dn.Parent().(*NodeImpl).value.(*Rack) // 类型断言
}

// GetTopology 获取顶层的 Topology 节点
// 通过拓扑树不断向上查找，直到找到没有父节点的根节点
//
// 返回：
//   - *Topology: 顶层 Topology 节点
func (dn *DataNode) GetTopology() *Topology {
	p := dn.Parent()
	for p.Parent() != nil {
		p = p.Parent() // 不断向上
	}
	t := p.(*Topology) // 类型断言：根节点是 Topology
	return t
}

// MatchLocation 检查 DataNode 是否匹配指定的 IP 和端口
//
// 参数：
//   - ip: IP 地址
//   - port: 端口号
//
// 返回：
//   - bool: 是否匹配
//
// 使用场景：
//   - 查找特定的 DataNode
//   - 验证 Volume server 位置
func (dn *DataNode) MatchLocation(ip string, port int) bool {
	return dn.Ip == ip && dn.Port == port
}

// Url 返回 DataNode 的 HTTP 访问 URL
// 格式：ip:port
//
// 返回：
//   - string: HTTP URL
func (dn *DataNode) Url() string {
	return util.JoinHostPort(dn.Ip, dn.Port)
}

// ServerAddress 返回 DataNode 的 gRPC 服务地址
//
// 返回：
//   - pb.ServerAddress: 包含 IP、HTTP 端口、gRPC 端口的服务地址
//
// 使用场景：
//   - 建立 gRPC 连接
//   - Master 与 Volume server 通信
func (dn *DataNode) ServerAddress() pb.ServerAddress {
	return pb.NewServerAddress(dn.Ip, dn.Port, dn.GrpcPort)
}

// DataNodeInfo 是 DataNode 的 JSON 序列化结构
// 用于 HTTP API 响应和集群状态展示
type DataNodeInfo struct {
	Url       string `json:"Url"`       // HTTP 访问 URL
	PublicUrl string `json:"PublicUrl"` // 公网访问 URL
	Volumes   int64  `json:"Volumes"`   // 总 Volume 数
	EcShards  int64  `json:"EcShards"`  // 总 EC 分片数
	Max       int64  `json:"Max"`       // 最大 Volume 容量
	VolumeIds string `json:"VolumeIds"` // Volume ID 列表（人类可读格式）
}

// ToInfo 将 DataNode 转换为可序列化的 Info 结构
// 主要用于 HTTP API 的 /cluster/status 等端点
//
// 返回：
//   - DataNodeInfo: 包含 DataNode 统计信息的结构
//
// 说明：
//   - 聚合所有 Disk 的统计数据
//   - Volume ID 列表限制为最多 100 个（人类可读）
func (dn *DataNode) ToInfo() (info DataNodeInfo) {
	info.Url = dn.Url()
	info.PublicUrl = dn.PublicUrl

	// 聚合所有磁盘类型的 Volume 信息
	var volumeCount, ecShardCount, maxVolumeCount int64
	var volumeIds string
	for _, diskUsage := range dn.diskUsages.usages {
		volumeCount += diskUsage.volumeCount       // 累加 Volume 数
		ecShardCount += diskUsage.ecShardCount     // 累加 EC 分片数
		maxVolumeCount += diskUsage.maxVolumeCount // 累加最大容量
	}

	// 收集所有 Disk 的 Volume ID
	for _, disk := range dn.Children() {
		d := disk.(*Disk)
		volumeIds += " " + d.GetVolumeIds() // 空格分隔
	}

	info.Volumes = volumeCount
	info.EcShards = ecShardCount
	info.Max = maxVolumeCount
	info.VolumeIds = volumeIds

	return
}

// ToDataNodeInfo 将 DataNode 转换为 protobuf 消息格式
// 用于 gRPC 通信，包含更详细的磁盘信息
//
// 返回：
//   - *master_pb.DataNodeInfo: protobuf 格式的 DataNode 信息，包含：
//     - Id: DataNode ID
//     - DiskInfos: 按磁盘类型分组的详细磁盘信息
//     - GrpcPort: gRPC 端口
//
// 用途：
//   - Master 节点间的拓扑同步
//   - Volume server 心跳响应
//   - 集群状态查询的 gRPC API
func (dn *DataNode) ToDataNodeInfo() *master_pb.DataNodeInfo {
	m := &master_pb.DataNodeInfo{
		Id:        string(dn.Id()),
		DiskInfos: make(map[string]*master_pb.DiskInfo),
		GrpcPort:  uint32(dn.GrpcPort),
	}
	// 遍历所有 Disk，收集详细信息
	for _, c := range dn.Children() {
		disk := c.(*Disk)
		m.DiskInfos[string(disk.Id())] = disk.ToDiskInfo() // 磁盘类型 → 磁盘详细信息
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
func (dn *DataNode) GetVolumeIds() string {
	dn.RLock()
	defer dn.RUnlock()
	existingVolumes := dn.getVolumes()              // 获取所有 Volume
	ids := make([]int, 0, len(existingVolumes))

	// 收集所有 Volume ID
	for k := range existingVolumes {
		ids = append(ids, int(k))
	}

	// 转换为人类可读格式，最多 100 个
	return util.HumanReadableIntsMax(100, ids...)
}

// getVolumes 内部方法：获取所有 Volume（不加锁）
// 由需要锁保护的方法调用
//
// 返回：
//   - []storage.VolumeInfo: Volume 信息列表
func (dn *DataNode) getVolumes() []storage.VolumeInfo {
	var existingVolumes []storage.VolumeInfo
	for _, c := range dn.children {
		disk := c.(*Disk)
		existingVolumes = append(existingVolumes, disk.GetVolumes()...) // 聚合所有 Disk 的 Volume
	}
	return existingVolumes
}
