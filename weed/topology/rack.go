// Package topology 实现 SeaweedFS 的拓扑结构管理
// 本文件实现 Rack（机架）节点，是 DataCenter 和 DataNode 之间的中间层
package topology

import (
	"slices"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// Rack 表示机架节点
// 在 SeaweedFS 拓扑结构中，Rack 是 DataCenter 和 DataNode 之间的中间层
// 层次关系：Topology → DataCenter → Rack → DataNode → Disk
//
// 设计目的：
//   - 实现机架级别的故障隔离
//   - 支持副本放置策略（如 "010" 表示不同机架放置副本）
//   - 物理上对应数据中心内的一个机架或机柜
//
// 示例拓扑路径：
//   topo:dc1:rack1
//
// 使用场景：
//   - 创建 Volume 时，根据副本策略在不同 Rack 间分配
//   - 统计和展示机架级别的资源使用情况
//   - 物理故障时进行机架级别的隔离
type Rack struct {
	NodeImpl // 继承通用节点实现，提供拓扑树的基本操作
}

// NewRack 创建新的机架节点
//
// 参数：
//   - id: 机架的唯一标识符，通常是物理位置标识（如 "rack1", "row-A-cabinet-3"）
//
// 返回：
//   - *Rack: 初始化完成的机架实例
//
// 初始化内容：
//   - 设置节点 ID 和类型为 "Rack"
//   - 初始化磁盘使用统计（聚合所有子 DataNode）
//   - 初始化子节点映射（存储 DataNode）
//   - 初始化容量预留管理器（防止并发分配竞争）
//
// 示例：
//   rack := NewRack("rack1")
//   dc.LinkChildNode(rack)  // 将 rack 链接到数据中心
func NewRack(id string) *Rack {
	r := &Rack{}
	r.id = NodeId(id)                              // 设置机架 ID
	r.nodeType = "Rack"                            // 标记节点类型为 Rack
	r.diskUsages = newDiskUsages()                 // 初始化磁盘使用统计，聚合所有子 DataNode 的磁盘使用情况
	r.children = make(map[NodeId]Node)             // 初始化子节点映射，key 是 DataNode ID，value 是 DataNode 节点
	r.capacityReservations = newCapacityReservations() // 初始化容量预留，防止并发创建 volume 时的竞争条件
	r.NodeImpl.value = r                          // 保存对自身的引用，用于类型转换
	return r
}

// FindDataNode 在机架中查找指定 IP 和端口的 DataNode
// 遍历机架下的所有 DataNode，查找匹配的节点
//
// 参数：
//   - ip: DataNode 的 IP 地址（如 "192.168.1.10"）
//   - port: DataNode 的端口号（如 8080）
//
// 返回：
//   - *DataNode: 找到的 DataNode，如果不存在则返回 nil
//
// 使用场景：
//   - Volume Server 心跳时，查找对应的 DataNode 进行更新
//   - 检查某个 Volume Server 是否属于该机架
func (r *Rack) FindDataNode(ip string, port int) *DataNode {
	// 遍历机架的所有子节点（DataNode）
	for _, c := range r.Children() {
		dn := c.(*DataNode) // 类型断言：子节点必定是 DataNode 类型
		// 检查 IP 和端口是否匹配
		if dn.MatchLocation(ip, port) {
			return dn // 找到匹配的 DataNode
		}
	}
	return nil // 未找到匹配的 DataNode
}
// GetOrCreateDataNode 获取或创建指定的 DataNode
// 如果 DataNode 已存在则更新心跳时间并返回，否则创建新的 DataNode 并初始化
//
// 参数：
//   - ip: DataNode 的 IP 地址
//   - port: DataNode 的 HTTP 端口
//   - grpcPort: DataNode 的 gRPC 端口
//   - publicUrl: DataNode 的公网访问地址（可选，用于跨网段访问）
//   - maxVolumeCounts: 各磁盘类型的最大 Volume 数量，格式：map["hdd":100, "ssd":50]
//
// 返回：
//   - *DataNode: 获取到或新创建的 DataNode 实例
//
// 工作流程：
//   1. 检查是否已存在匹配的 DataNode（通过 IP 和端口）
//   2. 如果存在，更新心跳时间并返回
//   3. 如果不存在，创建新 DataNode 并初始化：
//      - 设置基本信息（IP、端口、公网地址等）
//      - 为每种磁盘类型创建 Disk 子节点
//      - 设置每个磁盘的最大 Volume 容量
//      - 链接到机架（触发统计向上传播）
//
// 线程安全：
//   - 使用写锁保护并发访问
//
// 使用场景：
//   - Volume Server 首次心跳时注册到拓扑
//   - Volume Server 重启后重新注册
func (r *Rack) GetOrCreateDataNode(ip string, port int, grpcPort int, publicUrl string, maxVolumeCounts map[string]uint32) *DataNode {
	r.Lock()         // 获取写锁，保护 children map
	defer r.Unlock() // 函数返回时释放锁

	// 【步骤 1：查找是否已存在】
	// 遍历所有子节点，检查是否已有匹配的 DataNode
	for _, c := range r.children {
		dn := c.(*DataNode) // 类型断言：子节点是 DataNode
		if dn.MatchLocation(ip, port) {
			// 找到现有 DataNode，更新最后心跳时间
			dn.LastSeen = time.Now().Unix()
			return dn // 返回现有节点
		}
	}

	// 【步骤 2：创建新 DataNode】
	// 使用 "ip:port" 格式作为 DataNode ID
	dn := NewDataNode(util.JoinHostPort(ip, port))
	dn.Ip = ip
	dn.Port = port
	dn.GrpcPort = grpcPort
	dn.PublicUrl = publicUrl
	dn.LastSeen = time.Now().Unix() // 设置首次心跳时间

	// 【步骤 3：链接到机架】
	// 这会触发：
	//   1. 将 dn 添加到 r.children
	//   2. 设置 dn.parent = r
	//   3. 向上传播磁盘使用统计到 Rack → DataCenter → Topology
	r.doLinkChildNode(dn)

	// 【步骤 4：为每种磁盘类型创建 Disk 子节点】
	// maxVolumeCounts 示例：{"hdd": 100, "ssd": 50, "nvme": 20}
	for diskType, maxVolumeCount := range maxVolumeCounts {
		// 创建 Disk 节点（如 "hdd", "ssd"）
		disk := NewDisk(diskType)
		// 设置该磁盘的最大 Volume 容量
		disk.diskUsages.getOrCreateDisk(types.ToDiskType(diskType)).maxVolumeCount = int64(maxVolumeCount)
		// 将 Disk 链接到 DataNode
		dn.LinkChildNode(disk)
	}

	return dn
}

// RackInfo 是机架的 JSON 序列化结构
// 用于 HTTP API 响应和集群状态展示
type RackInfo struct {
	Id        NodeId         `json:"Id"`        // 机架 ID
	DataNodes []DataNodeInfo `json:"DataNodes"` // 包含的所有 DataNode 信息列表
}

// ToInfo 将 Rack 转换为可序列化的 Info 结构
// 主要用于 HTTP API 的 /cluster/status 等端点
//
// 返回：
//   - RackInfo: 包含机架 ID 和所有 DataNode 信息的结构
//
// 特性：
//   - 按 DataNode URL 字典序排序，确保输出稳定
//   - 递归收集所有子 DataNode 的信息
func (r *Rack) ToInfo() (info RackInfo) {
	info.Id = r.Id()
	var dns []DataNodeInfo

	// 遍历所有子节点（DataNode），收集它们的信息
	for _, c := range r.Children() {
		dn := c.(*DataNode) // 类型断言：子节点是 DataNode
		dns = append(dns, dn.ToInfo())
	}

	// 按 DataNode URL 排序，确保输出顺序稳定
	// 这对于 UI 展示和测试很重要
	slices.SortFunc(dns, func(a, b DataNodeInfo) int {
		return strings.Compare(a.Url, b.Url)
	})

	info.DataNodes = dns
	return
}

// ToRackInfo 将 Rack 转换为 protobuf 消息格式
// 用于 gRPC 通信，包含更详细的磁盘使用统计
//
// 返回：
//   - *master_pb.RackInfo: protobuf 格式的机架信息，包含：
//     - Id: 机架 ID
//     - DiskInfos: 聚合的磁盘使用统计（按磁盘类型分组）
//     - DataNodeInfos: 所有 DataNode 的详细信息列表
//
// 用途：
//   - Master 节点间的拓扑同步
//   - Volume server 心跳响应
//   - 集群状态查询的 gRPC API
func (r *Rack) ToRackInfo() *master_pb.RackInfo {
	m := &master_pb.RackInfo{
		Id:        string(r.Id()),           // 机架 ID
		DiskInfos: r.diskUsages.ToDiskInfo(), // 聚合所有子 DataNode 的磁盘使用情况
	}

	// 遍历所有 DataNode，收集它们的 protobuf 信息
	for _, c := range r.Children() {
		dn := c.(*DataNode) // 类型断言：子节点是 DataNode
		m.DataNodeInfos = append(m.DataNodeInfos, dn.ToDataNodeInfo())
	}
	return m
}
