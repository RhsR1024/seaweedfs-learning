// Package topology 实现 SeaweedFS 的拓扑结构管理
// 拓扑层次：Topology → DataCenter → Rack → DataNode → Disk
package topology

import (
	"slices"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

// DataCenter 表示数据中心节点
// 在 SeaweedFS 拓扑结构中，DataCenter 是顶层容器，包含多个 Rack
// 层次关系：Topology → DataCenter → Rack → DataNode → Disk
//
// 用途：
//   - 实现跨数据中心的副本放置
//   - 隔离不同地理位置的存储资源
//   - 支持数据中心级别的故障隔离
type DataCenter struct {
	NodeImpl // 继承通用节点实现，提供拓扑树的基本操作
}

// NewDataCenter 创建新的数据中心节点
//
// 参数：
//   - id: 数据中心的唯一标识符，通常是地理位置标识（如 "dc1", "us-west"）
//
// 返回：
//   - *DataCenter: 初始化完成的数据中心实例
//
// 初始化内容：
//   - 设置节点 ID 和类型
//   - 初始化磁盘使用统计（聚合所有子节点）
//   - 初始化子节点映射（存储 Rack）
//   - 初始化容量预留管理器
func NewDataCenter(id string) *DataCenter {
	dc := &DataCenter{}
	dc.id = NodeId(id)                              // 设置数据中心 ID
	dc.nodeType = "DataCenter"                      // 标记节点类型为 DataCenter
	dc.diskUsages = newDiskUsages()                 // 初始化磁盘使用统计，聚合所有子节点的磁盘使用情况
	dc.children = make(map[NodeId]Node)             // 初始化子节点映射，key 是 Rack ID，value 是 Rack 节点
	dc.capacityReservations = newCapacityReservations() // 初始化容量预留，防止并发创建 volume 时的竞争条件
	dc.NodeImpl.value = dc                          // 保存对自身的引用，用于类型转换
	return dc
}

// GetOrCreateRack 获取或创建指定名称的 Rack
// 如果 Rack 已存在则返回现有实例，否则创建新 Rack 并链接到数据中心
//
// 参数：
//   - rackName: Rack 的名称标识符（如 "rack1", "row-A"）
//
// 返回：
//   - *Rack: 获取到或新创建的 Rack 实例
//
// 线程安全：
//   - 使用写锁保护并发访问
//   - 确保同时只有一个 goroutine 可以创建/查找 Rack
func (dc *DataCenter) GetOrCreateRack(rackName string) *Rack {
	dc.Lock()         // 获取写锁，保护 children map
	defer dc.Unlock() // 函数返回时释放锁

	// 遍历所有子节点，查找是否已存在该 Rack
	for _, c := range dc.children {
		rack := c.(*Rack) // 类型断言：子节点必定是 Rack 类型
		if string(rack.Id()) == rackName {
			return rack // 找到现有 Rack，直接返回
		}
	}

	// Rack 不存在，创建新的 Rack
	rack := NewRack(rackName)
	// 将新 Rack 链接到数据中心
	// doLinkChildNode 会：
	//   1. 将 rack 添加到 dc.children
	//   2. 设置 rack.parent = dc
	//   3. 向上传播磁盘使用统计
	dc.doLinkChildNode(rack)
	return rack
}

// DataCenterInfo 是数据中心的 JSON 序列化结构
// 用于 HTTP API 响应和集群状态展示
type DataCenterInfo struct {
	Id    NodeId     `json:"Id"`    // 数据中心 ID
	Racks []RackInfo `json:"Racks"` // 包含的所有 Rack 信息列表
}

// ToInfo 将 DataCenter 转换为可序列化的 Info 结构
// 主要用于 HTTP API 的 /cluster/status 等端点
//
// 返回：
//   - DataCenterInfo: 包含数据中心 ID 和所有 Rack 信息的结构
//
// 特性：
//   - 按 Rack ID 字典序排序，确保输出稳定
//   - 递归收集所有子 Rack 的信息
func (dc *DataCenter) ToInfo() (info DataCenterInfo) {
	info.Id = dc.Id()
	var racks []RackInfo

	// 遍历所有子节点（Rack），收集它们的信息
	for _, c := range dc.Children() {
		rack := c.(*Rack) // 类型断言：子节点是 Rack
		racks = append(racks, rack.ToInfo())
	}

	// 按 Rack ID 排序，确保输出顺序稳定
	// 这对于 UI 展示和测试很重要
	slices.SortFunc(racks, func(a, b RackInfo) int {
		return strings.Compare(string(a.Id), string(b.Id))
	})
	info.Racks = racks
	return
}

// ToDataCenterInfo 将 DataCenter 转换为 protobuf 消息格式
// 用于 gRPC 通信，包含更详细的磁盘使用统计
//
// 返回：
//   - *master_pb.DataCenterInfo: protobuf 格式的数据中心信息，包含：
//     - Id: 数据中心 ID
//     - DiskInfos: 聚合的磁盘使用统计（按磁盘类型分组）
//     - RackInfos: 所有 Rack 的详细信息列表
//
// 用途：
//   - Master 节点间的拓扑同步
//   - Volume server 心跳响应
//   - 集群状态查询的 gRPC API
func (dc *DataCenter) ToDataCenterInfo() *master_pb.DataCenterInfo {
	m := &master_pb.DataCenterInfo{
		Id:        string(dc.Id()),           // 数据中心 ID
		DiskInfos: dc.diskUsages.ToDiskInfo(), // 聚合所有子节点的磁盘使用情况
	}

	// 遍历所有 Rack，收集它们的 protobuf 信息
	for _, c := range dc.Children() {
		rack := c.(*Rack) // 类型断言：子节点是 Rack
		m.RackInfos = append(m.RackInfos, rack.ToRackInfo())
	}
	return m
}
