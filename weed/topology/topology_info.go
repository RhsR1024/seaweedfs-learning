// Package topology 实现了 SeaweedFS 的拓扑结构管理
// 本文件包含拓扑信息的序列化和展示功能
package topology

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"slices"
	"strings"
)

// TopologyInfo 是集群拓扑的 JSON 序列化结构
// 用于 HTTP API 响应,提供集群整体状态的快照视图
//
// 字段说明:
//   - Max: 集群总的最大 Volume 容量（所有节点的最大 Volume 数之和）
//   - Free: 集群剩余可用空间（字节数）
//   - DataCenters: 所有数据中心的详细信息列表
//   - Layouts: 所有 VolumeLayout 的配置信息列表
//
// 用途:
//   - /cluster/status API 响应
//   - 集群监控和仪表盘
//   - 容量规划和负载分析
type TopologyInfo struct {
	Max         int64              `json:"Max"`         // 集群最大 Volume 容量
	Free        int64              `json:"Free"`        // 集群剩余可用空间
	DataCenters []DataCenterInfo   `json:"DataCenters"` // 数据中心列表
	Layouts     []VolumeLayoutInfo `json:"Layouts"`     // Volume 布局配置列表
}

// VolumeLayoutCollection 是 Collection 和其 VolumeLayout 的关联结构
// 用于将 VolumeLayout 与其所属的 Collection 名称绑定
//
// 用途:
//   - 遍历所有 Collection 的 VolumeLayout
//   - Volume 增长策略的执行
//   - 批量 Volume 管理操作
type VolumeLayoutCollection struct {
	Collection   string        // Collection 名称
	VolumeLayout *VolumeLayout // 对应的 VolumeLayout 实例
}

// ToInfo 将 Topology 转换为可序列化的 TopologyInfo 结构
// 用于 HTTP API 响应,提供集群的完整状态快照
//
// 返回:
//   - TopologyInfo: 包含集群容量、数据中心列表、VolumeLayout 列表的完整信息
//
// 收集的信息包括:
//   1. 集群级别的容量统计（Max、Free）
//   2. 所有数据中心的详细信息（递归收集 DataCenter -> Rack -> DataNode）
//   3. 所有 VolumeLayout 的配置（副本策略、TTL、可写 Volume 列表等）
//
// 输出特性:
//   - 数据中心按 ID 字典序排序,确保输出稳定
//   - 包含所有 Collection 的 VolumeLayout 信息
func (t *Topology) ToInfo() (info TopologyInfo) {
	// 【1. 收集集群容量信息】
	// 获取集群总的最大 Volume 数量
	// 这是所有 DataNode 上报的 maxVolumeCount 之和
	info.Max = t.diskUsages.GetMaxVolumeCount()

	// 获取集群剩余可用空间（字节数）
	// 这是所有 DataNode 的剩余磁盘空间之和
	info.Free = t.diskUsages.FreeSpace()

	// 【2. 收集所有数据中心的信息】
	var dcs []DataCenterInfo
	for _, c := range t.Children() {
		dc := c.(*DataCenter)
		// 递归调用 dc.ToInfo()，收集该数据中心下所有 Rack 和 DataNode 的信息
		dcs = append(dcs, dc.ToInfo())
	}

	// 按数据中心 ID 排序，确保输出顺序稳定
	// 这对于 UI 展示和测试非常重要
	slices.SortFunc(dcs, func(a, b DataCenterInfo) int {
		return strings.Compare(string(a.Id), string(b.Id))
	})

	info.DataCenters = dcs

	// 【3. 收集所有 VolumeLayout 的配置信息】
	var layouts []VolumeLayoutInfo
	// 遍历所有 Collection
	for _, col := range t.collectionMap.Items() {
		c := col.(*Collection)
		// 遍历该 Collection 下的所有 VolumeLayout（按存储类型分组）
		for _, layout := range c.storageType2VolumeLayout.Items() {
			if layout != nil {
				// 转换 VolumeLayout 为可序列化的 Info 结构
				tmp := layout.(*VolumeLayout).ToInfo()
				// 关联 Collection 名称
				tmp.Collection = c.Name
				layouts = append(layouts, tmp)
			}
		}
	}
	info.Layouts = layouts
	return
}

// ListVolumeLayoutCollections 列出所有 Collection 的 VolumeLayout
// 返回 VolumeLayoutCollection 列表，包含 Collection 名称和对应的 VolumeLayout
//
// 返回:
//   - []*VolumeLayoutCollection: VolumeLayout 和 Collection 的关联列表
//
// 用途:
//   - Volume 自动增长策略：遍历所有 VolumeLayout，检查是否需要创建新 Volume
//   - 集群统计：收集所有 VolumeLayout 的统计信息
//   - Volume 重平衡：分析各个 VolumeLayout 的负载情况
//
// 注意:
//   - 一个 Collection 可能有多个 VolumeLayout（不同副本策略、TTL、磁盘类型）
//   - 返回的列表顺序不保证，如需排序请在调用方处理
func (t *Topology) ListVolumeLayoutCollections() (volumeLayouts []*VolumeLayoutCollection) {
	// 遍历所有 Collection
	for _, col := range t.collectionMap.Items() {
		// 遍历该 Collection 下的所有 VolumeLayout
		// storageType2VolumeLayout 按存储类型（副本策略+TTL+磁盘类型）分组
		for _, volumeLayout := range col.(*Collection).storageType2VolumeLayout.Items() {
			volumeLayouts = append(volumeLayouts,
				&VolumeLayoutCollection{col.(*Collection).Name, volumeLayout.(*VolumeLayout)},
			)
		}
	}
	return volumeLayouts
}

// ToVolumeMap 将拓扑结构转换为嵌套的 map 结构
// 用于展示完整的拓扑层次和每个节点上的 Volume 列表
//
// 返回:
//   - interface{}: 嵌套的 map 结构，层次为:
//     {
//       "Max": 集群最大容量,
//       "Free": 集群剩余空间,
//       "DataCenters": {
//         "dc1": {
//           "rack1": {
//             "datanode1": [volume1, volume2, ...],
//             "datanode2": [volume3, volume4, ...]
//           }
//         }
//       }
//     }
//
// 用途:
//   - 调试和诊断：查看每个节点上的具体 Volume 分布
//   - Volume 迁移：了解 Volume 的当前位置
//   - 负载分析：检查各节点的 Volume 数量
//
// 注意:
//   - 返回的是 interface{} 类型，需要在使用时进行类型断言
//   - 这个结构比 ToInfo() 更详细，包含每个 DataNode 上的具体 Volume 列表
func (t *Topology) ToVolumeMap() interface{} {
	m := make(map[string]interface{})
	// 集群容量信息
	m["Max"] = t.diskUsages.GetMaxVolumeCount()
	m["Free"] = t.diskUsages.FreeSpace()

	// 【构建三层嵌套的 map 结构：DataCenter -> Rack -> DataNode -> Volumes】
	dcs := make(map[NodeId]interface{})
	// 遍历所有 DataCenter
	for _, c := range t.Children() {
		dc := c.(*DataCenter)
		racks := make(map[NodeId]interface{})

		// 遍历该 DataCenter 下的所有 Rack
		for _, r := range dc.Children() {
			rack := r.(*Rack)
			dataNodes := make(map[NodeId]interface{})

			// 遍历该 Rack 下的所有 DataNode
			for _, d := range rack.Children() {
				dn := d.(*DataNode)
				var volumes []interface{}
				// 收集该 DataNode 上的所有 Volume
				for _, v := range dn.GetVolumes() {
					volumes = append(volumes, v)
				}
				// DataNode ID -> Volume 列表
				dataNodes[d.Id()] = volumes
			}
			// Rack ID -> DataNode map
			racks[r.Id()] = dataNodes
		}
		// DataCenter ID -> Rack map
		dcs[dc.Id()] = racks
	}
	m["DataCenters"] = dcs
	return m
}

// ToVolumeLocations 收集所有 DataNode 的位置信息和 Volume 列表
// 返回 protobuf 格式的 VolumeLocation 列表，用于 gRPC 通信
//
// 返回:
//   - []*master_pb.VolumeLocation: 所有 DataNode 的位置和 Volume 列表
//
// VolumeLocation 包含:
//   - Url: DataNode 的访问地址（用于数据传输）
//   - PublicUrl: DataNode 的公网地址（用于客户端访问）
//   - DataCenter: DataNode 所属的数据中心 ID
//   - GrpcPort: gRPC 端口
//   - NewVids: 该节点上所有 Volume 的 ID 列表（包括普通 Volume 和 EC Volume）
//
// 用途:
//   - Volume 查找：客户端通过 Volume ID 找到对应的 DataNode
//   - Volume 同步：Master 节点间同步 Volume 位置信息
//   - 负载均衡：选择合适的 DataNode 进行读写操作
//   - EC Volume 恢复：查找 EC Shard 所在的节点
//
// 注意:
//   - NewVids 同时包含普通 Volume 和 EC Volume 的 ID
//   - 返回的列表包含集群中所有 DataNode，即使某些节点没有 Volume
func (t *Topology) ToVolumeLocations() (volumeLocations []*master_pb.VolumeLocation) {
	// 遍历拓扑树：DataCenter -> Rack -> DataNode
	for _, c := range t.Children() {
		dc := c.(*DataCenter)
		for _, r := range dc.Children() {
			rack := r.(*Rack)
			for _, d := range rack.Children() {
				dn := d.(*DataNode)

				// 构建 VolumeLocation 消息
				volumeLocation := &master_pb.VolumeLocation{
					Url:        dn.Url(),             // DataNode 内网地址
					PublicUrl:  dn.PublicUrl,         // DataNode 公网地址
					DataCenter: dn.GetDataCenterId(), // 所属数据中心
					GrpcPort:   uint32(dn.GrpcPort),  // gRPC 端口
				}

				// 收集该 DataNode 上的所有普通 Volume ID
				for _, v := range dn.GetVolumes() {
					volumeLocation.NewVids = append(volumeLocation.NewVids, uint32(v.Id))
				}

				// 收集该 DataNode 上的所有 EC Shard 对应的 Volume ID
				for _, s := range dn.GetEcShards() {
					volumeLocation.NewVids = append(volumeLocation.NewVids, uint32(s.VolumeId))
				}

				volumeLocations = append(volumeLocations, volumeLocation)
			}
		}
	}
	return
}

// ToTopologyInfo 将 Topology 转换为 protobuf 格式的 TopologyInfo
// 用于 gRPC 通信，提供比 ToInfo() 更详细的磁盘使用统计
//
// 返回:
//   - *master_pb.TopologyInfo: protobuf 格式的拓扑信息，包含：
//     - Id: 拓扑 ID（通常是 "topo"）
//     - DiskInfos: 按磁盘类型分组的磁盘使用统计（HDD、SSD 等）
//     - DataCenterInfos: 所有数据中心的详细信息
//
// 与 ToInfo() 的区别:
//   - ToInfo() 返回 JSON 格式，用于 HTTP API
//   - ToTopologyInfo() 返回 protobuf 格式，用于 gRPC API
//   - ToTopologyInfo() 包含更详细的磁盘类型统计
//
// 用途:
//   - Master 节点间的拓扑同步（gRPC）
//   - Volume server 心跳响应
//   - 集群状态查询的 gRPC API
//   - 监控系统的数据采集
func (t *Topology) ToTopologyInfo() *master_pb.TopologyInfo {
	m := &master_pb.TopologyInfo{
		Id:        string(t.Id()),           // 拓扑 ID
		DiskInfos: t.diskUsages.ToDiskInfo(), // 磁盘使用统计（按磁盘类型分组）
	}

	// 收集所有数据中心的 protobuf 信息
	for _, c := range t.Children() {
		dc := c.(*DataCenter)
		// 递归收集数据中心的详细信息（包括 Rack、DataNode）
		m.DataCenterInfos = append(m.DataCenterInfos, dc.ToDataCenterInfo())
	}
	return m
}
