// Package weed_server 中的 master_grpc_server_cluster.go 实现 Master 的集群节点管理 gRPC 服务
//
// 核心功能：
//   管理和查询 SeaweedFS 集群中的各类节点（Filer、Volume、Master）
//
// 集群节点类型：
//   - FilerType: Filer 服务器节点，提供文件系统接口
//   - VolumeServerType: Volume 服务器节点，存储实际文件数据
//   - MasterType: Master 服务器节点，负责元数据管理和协调
//
// 节点注册和心跳：
//   - 各类型节点启动后会向 Master 注册自己的地址、版本、位置信息
//   - 节点定期发送心跳以保持活跃状态
//   - Master 维护集群节点列表，剔除长时间未心跳的节点
//
// Filer Group（Filer 分组）：
//   - 支持将 Filer 节点分组管理，实现多租户隔离
//   - 默认分组为空字符串（全局 Filer）
//   - 客户端可以指定连接特定 Filer 组
//
// 使用场景：
//   - 客户端获取可用 Filer 列表，实现负载均衡和故障转移
//   - 管理工具查询集群拓扑和节点状态
//   - Shell 工具随机选择 Filer 节点进行操作
package weed_server

import (
	"context"
	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"math/rand/v2"
)

// ListClusterNodes 列出集群中指定类型的节点
// 支持按 Filer 分组和节点类型过滤，并可限制返回数量
//
// 参数说明：
//   - ctx: 请求上下文
//   - req: 查询请求，包含以下字段：
//     * FilerGroup: Filer 分组名称（仅对 FilerType 有效，为空表示默认分组）
//     * ClientType: 节点类型过滤（"filer"、"volume"、"master"）
//     * Limit: 返回节点数量限制（0 或负数表示不限制）
//
// 返回值：
//   - ListClusterNodesResponse: 节点列表，每个节点包含：
//     * Address: 节点地址（IP:Port）
//     * Version: 节点版本号
//     * CreatedAtNs: 节点注册时间（纳秒时间戳）
//     * DataCenter: 数据中心 ID
//     * Rack: 机架 ID
//
// 工作流程：
//   1. 解析 FilerGroup 参数
//   2. 从集群管理器获取指定类型的节点列表
//   3. 如果指定了 Limit，随机选择指定数量的节点
//   4. 构建响应返回
//
// 使用场景：
//   - 客户端发现可用 Filer 节点，实现负载均衡
//   - 管理工具查询集群拓扑和节点分布
//   - 监控系统获取节点列表和状态
func (ms *MasterServer) ListClusterNodes(ctx context.Context, req *master_pb.ListClusterNodesRequest) (*master_pb.ListClusterNodesResponse, error) {
	resp := &master_pb.ListClusterNodesResponse{}

	// 解析 Filer 分组名称
	// FilerGroupName 是一个类型别名，用于区分不同的 Filer 组
	filerGroup := cluster.FilerGroupName(req.FilerGroup)

	// 从集群管理器获取节点列表
	// 参数：filerGroup（仅对 FilerType 有效）、ClientType（节点类型）
	clusterNodes := ms.Cluster.ListClusterNode(filerGroup, req.ClientType)

	// 如果请求指定了 Limit，随机选择指定数量的节点
	// 避免总是返回相同的节点，提高负载均衡效果
	clusterNodes = limitTo(clusterNodes, req.Limit)

	// 构建响应：将内部 ClusterNode 转换为 protobuf 消息
	for _, node := range clusterNodes {
		resp.ClusterNodes = append(resp.ClusterNodes, &master_pb.ListClusterNodesResponse_ClusterNode{
			Address:     string(node.Address),              // 节点地址
			Version:     node.Version,                      // 版本号（如 "3.50"）
			CreatedAtNs: node.CreatedTs.UnixNano(),         // 注册时间（纳秒）
			DataCenter:  string(node.DataCenter),           // 数据中心
			Rack:        string(node.Rack),                 // 机架
		})
	}
	return resp, nil
}

// GetOneFiler 从指定 Filer 组中随机选择一个 Filer 节点
// 用于负载均衡和故障转移
//
// 参数说明：
//   - filerGroup: Filer 分组名称（为空表示默认分组）
//
// 返回值：
//   - ServerAddress: Filer 节点地址（IP:Port）
//     如果没有可用 Filer，返回默认地址 "localhost:8888"
//
// 工作原理：
//   1. 获取指定分组的所有 Filer 节点
//   2. 随机选择一个节点（使用 rand.IntN 实现简单负载均衡）
//   3. 如果没有可用节点，返回本地默认地址
//
// 使用场景：
//   - Shell 工具需要连接 Filer 执行操作
//   - Volume Server 向 Filer 同步元数据
//   - 客户端自动发现 Filer 节点
//
// 注意事项：
//   - 随机选择可能导致负载不均（不考虑节点实际负载）
//   - 默认地址 "localhost:8888" 仅适用于本地开发环境
//   - 生产环境应确保至少有一个 Filer 节点注册
func (ms *MasterServer) GetOneFiler(filerGroup cluster.FilerGroupName) pb.ServerAddress {

	// 获取指定分组的所有 Filer 节点
	filers := ms.Cluster.ListClusterNode(filerGroup, cluster.FilerType)

	// 如果有可用 Filer，随机选择一个
	if len(filers) > 0 {
		// rand.IntN(n) 返回 [0, n) 范围内的随机整数
		return filers[rand.IntN(len(filers))].Address
	}

	// 如果没有 Filer 节点，返回本地默认地址
	// 适用于单机开发环境
	return "localhost:8888"
}

// limitTo 从节点列表中随机选择指定数量的节点
// 使用采样算法避免总是返回相同的节点子集
//
// 参数说明：
//   - nodes: 原始节点列表
//   - limit: 需要选择的节点数量（<=0 表示不限制）
//
// 返回值：
//   - selected: 随机选择的节点列表
//
// 算法实现：
//   1. 如果 limit <= 0 或节点数量不足，直接返回全部节点
//   2. 使用 map 去重，避免重复选择同一节点
//   3. 采样次数为 limit * 3，提高随机性和覆盖率
//   4. 每次随机选择一个节点，如果未被选中则加入结果集
//   5. 返回最终选中的节点列表
//
// 为什么采样 limit * 3 次：
//   - 提高随机性：避免前几个节点被频繁选中
//   - 保证数量：通常能选满 limit 个节点（除非节点总数不足）
//   - 性能平衡：采样次数不会太多，避免无意义的重复尝试
//
// 使用场景：
//   - 客户端请求部分 Filer 节点，避免返回完整列表
//   - 负载均衡：不同客户端可能获得不同的节点子集
//   - 减少网络传输：限制响应大小
func limitTo(nodes []*cluster.ClusterNode, limit int32) (selected []*cluster.ClusterNode) {
	// 如果不需要限制，或节点数量不足，直接返回全部节点
	if limit <= 0 || len(nodes) < int(limit) {
		return nodes
	}

	// 使用 map 存储已选择的节点，key 为节点地址，保证唯一性
	selectedSet := make(map[pb.ServerAddress]*cluster.ClusterNode)

	// 采样 limit * 3 次，提高随机性
	// 例如：limit=10 时，会尝试 30 次随机选择
	for i := 0; i < int(limit)*3; i++ {
		// 随机选择一个节点索引
		x := rand.IntN(len(nodes))

		// 检查该节点是否已被选中
		if _, found := selectedSet[nodes[x].Address]; found {
			// 已选中，跳过（避免重复）
			continue
		}

		// 将节点加入结果集
		selectedSet[nodes[x].Address] = nodes[x]
	}

	// 将 map 转换为切片返回
	// 注意：遍历 map 的顺序是随机的，进一步增加了随机性
	for _, node := range selectedSet {
		selected = append(selected, node)
	}

	return
}
