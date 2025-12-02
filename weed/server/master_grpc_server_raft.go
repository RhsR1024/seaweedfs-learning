// Package weed_server 中的 master_grpc_server_raft.go 实现 Master 的 Raft 集群管理 gRPC 服务
//
// 核心功能：
//   管理 SeaweedFS Master 的 Raft 集群，实现节点的查询、添加和删除
//
// Raft 共识协议：
//   - SeaweedFS 使用 Raft 协议实现 Master 的高可用和数据一致性
//   - Raft 集群由多个 Master 节点组成，通过选举产生 Leader
//   - 只有 Leader 可以处理写操作（如分配 fid、删除 Collection）
//   - Follower 将写请求重定向到 Leader，或返回 NotLeaderError
//
// Raft 节点类型（Suffrage）：
//   1. Voter（投票节点）：
//      - 参与 Leader 选举，拥有投票权
//      - 接收 Leader 的日志复制
//      - 可以成为 Leader
//      - 至少需要 3 个 Voter 节点才能容忍 1 个节点故障
//
//   2. Nonvoter（非投票节点）：
//      - 不参与 Leader 选举，无投票权
//      - 接收 Leader 的日志复制（只读副本）
//      - 不能成为 Leader
//      - 用于扩展读能力，不影响选举性能
//
// Raft 集群配置：
//   - 使用 Hashicorp Raft 实现（github.com/hashicorp/raft）
//   - 推荐配置：3 或 5 个 Voter 节点（奇数个，保证多数派）
//   - 可以添加多个 Nonvoter 节点用于读扩展
//   - 集群配置变更（添加/删除节点）必须在 Leader 上执行
//
// 高可用保证：
//   - 集群容忍 (N-1)/2 个节点故障（N 为 Voter 节点数）
//   - 3 节点集群：容忍 1 个节点故障
//   - 5 节点集群：容忍 2 个节点故障
//   - 节点故障后，剩余节点自动选举新 Leader
//
// 使用场景：
//   - 运维工具查询 Raft 集群状态和节点列表
//   - 动态添加新 Master 节点扩展集群
//   - 下线故障或过期的 Master 节点
//   - 监控 Leader 变化和集群健康状态
package weed_server

import (
	"context"
	"fmt"

	"github.com/hashicorp/raft"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

// RaftListClusterServers 列出 Raft 集群中的所有 Master 节点
// 包括节点 ID、地址、投票权、Leader 标识等信息
//
// 参数说明：
//   - ctx: 请求上下文
//   - req: 查询请求（当前无参数）
//
// 返回值：
//   - RaftListClusterServersResponse: 节点列表，每个节点包含：
//     * Id: 节点 ID（通常是启动时指定的名称，如 "master1"）
//     * Address: 节点地址（IP:Port，用于 Raft 通信）
//     * Suffrage: 投票权类型（"Voter" 或 "Nonvoter"）
//     * IsLeader: 是否为当前 Leader
//
// 工作流程：
//   1. 加读锁访问 Raft 实例
//   2. 检查 Raft 是否已初始化（单机模式下为 nil）
//   3. 获取集群配置和 Leader ID
//   4. 释放锁
//   5. 构建响应返回
//
// 使用场景：
//   - 运维工具查询 Raft 集群状态
//   - 监控系统检测 Leader 变化
//   - 客户端发现所有 Master 节点地址
//
// 注意事项：
//   - 任何节点都可以响应此请求（不需要是 Leader）
//   - 单机模式下 HashicorpRaft 为 nil，返回空列表
//   - 使用读锁保证并发安全
func (ms *MasterServer) RaftListClusterServers(ctx context.Context, req *master_pb.RaftListClusterServersRequest) (*master_pb.RaftListClusterServersResponse, error) {
	resp := &master_pb.RaftListClusterServersResponse{}

	// 【步骤 1：加读锁访问 Raft 实例】
	// RaftServerAccessLock 保护 HashicorpRaft 的并发访问
	ms.Topo.RaftServerAccessLock.RLock()

	// 【步骤 2：检查 Raft 是否已初始化】
	// 单机模式下 HashicorpRaft 为 nil，返回空列表
	if ms.Topo.HashicorpRaft == nil {
		ms.Topo.RaftServerAccessLock.RUnlock()
		return resp, nil
	}

	// 【步骤 3：获取集群配置和 Leader ID】
	// GetConfiguration() 返回当前 Raft 集群的配置
	// Servers 包含所有节点（Voter + Nonvoter）
	servers := ms.Topo.HashicorpRaft.GetConfiguration().Configuration().Servers

	// LeaderWithID() 返回当前 Leader 的地址和 ID
	// 如果没有 Leader（选举中），leaderId 为空字符串
	_, leaderId := ms.Topo.HashicorpRaft.LeaderWithID()

	// 【步骤 4：释放锁】
	// 已获取需要的数据，尽早释放锁
	ms.Topo.RaftServerAccessLock.RUnlock()

	// 【步骤 5：构建响应】
	// 遍历所有节点，构建 protobuf 消息
	for _, server := range servers {
		resp.ClusterServers = append(resp.ClusterServers, &master_pb.RaftListClusterServersResponse_ClusterServers{
			Id:       string(server.ID),               // 节点 ID
			Address:  string(server.Address),          // 节点地址
			Suffrage: server.Suffrage.String(),        // 投票权类型（"Voter" 或 "Nonvoter"）
			IsLeader: server.ID == leaderId,           // 是否为 Leader
		})
	}

	return resp, nil
}

// RaftAddServer 向 Raft 集群添加新的 Master 节点
// 可以添加 Voter（投票节点）或 Nonvoter（非投票节点）
//
// 参数说明：
//   - ctx: 请求上下文
//   - req: 添加请求，包含以下字段：
//     * Id: 新节点的 ID（如 "master2"）
//     * Address: 新节点的地址（IP:Port，用于 Raft 通信）
//     * Voter: 是否为投票节点（true=Voter, false=Nonvoter）
//
// 返回值：
//   - RaftAddServerResponse: 添加成功响应（空响应）
//   - error: 添加失败时返回错误
//
// 工作流程：
//   1. 加读锁访问 Raft 实例
//   2. 检查 Raft 是否已初始化
//   3. 验证当前节点是否为 Leader（只有 Leader 可以添加节点）
//   4. 根据 Voter 参数调用 AddVoter 或 AddNonvoter
//   5. 等待操作完成并检查错误
//
// Voter vs Nonvoter 选择：
//   - Voter（投票节点）：
//     * 参与 Leader 选举，增强高可用性
//     * 增加写操作延迟（需要同步到多数派）
//     * 推荐：3 或 5 个 Voter 节点
//
//   - Nonvoter（非投票节点）：
//     * 只接收日志，不参与选举
//     * 不影响写性能和选举速度
//     * 用于扩展读能力或跨地域部署
//
// 注意事项：
//   - 只有 Leader 可以添加节点
//   - 新节点必须先启动并配置好，然后才能加入集群
//   - 添加 Voter 节点时，集群可能短暂不可用（同步数据）
//   - 不要同时添加多个节点，可能导致集群不稳定
//
// 使用场景：
//   - 扩展 Master 集群，提高高可用性
//   - 添加跨地域 Nonvoter 节点用于读扩展
//   - 替换故障节点
func (ms *MasterServer) RaftAddServer(ctx context.Context, req *master_pb.RaftAddServerRequest) (*master_pb.RaftAddServerResponse, error) {
	resp := &master_pb.RaftAddServerResponse{}

	// 【步骤 1：加读锁访问 Raft 实例】
	ms.Topo.RaftServerAccessLock.RLock()
	defer ms.Topo.RaftServerAccessLock.RUnlock()

	// 【步骤 2：检查 Raft 是否已初始化】
	// 单机模式下 HashicorpRaft 为 nil，返回成功（无操作）
	if ms.Topo.HashicorpRaft == nil {
		return resp, nil
	}

	// 【步骤 3：验证 Leader 身份】
	// 只有 Leader 可以修改集群配置
	if ms.Topo.HashicorpRaft.State() != raft.Leader {
		return nil, fmt.Errorf("raft add server %s failed: %s is no current leader", req.Id, ms.Topo.HashicorpRaft.String())
	}

	// 【步骤 4：添加节点】
	var idxFuture raft.IndexFuture

	if req.Voter {
		// 添加 Voter 节点（投票节点）
		// 参数：
		//   - ServerID: 节点 ID
		//   - ServerAddress: 节点地址
		//   - prevIndex: 前一个日志索引（0 表示不检查）
		//   - timeout: 超时时间（0 表示使用默认超时）
		idxFuture = ms.Topo.HashicorpRaft.AddVoter(raft.ServerID(req.Id), raft.ServerAddress(req.Address), 0, 0)
	} else {
		// 添加 Nonvoter 节点（非投票节点）
		// 参数同上
		idxFuture = ms.Topo.HashicorpRaft.AddNonvoter(raft.ServerID(req.Id), raft.ServerAddress(req.Address), 0, 0)
	}

	// 【步骤 5：等待操作完成】
	// IndexFuture.Error() 会阻塞直到操作完成
	// 成功返回 nil，失败返回错误（如节点已存在、网络故障等）
	if err := idxFuture.Error(); err != nil {
		return nil, err
	}

	return resp, nil
}

// RaftRemoveServer 从 Raft 集群中移除 Master 节点
// 支持强制删除和安全检查模式
//
// 参数说明：
//   - ctx: 请求上下文
//   - req: 删除请求，包含以下字段：
//     * Id: 要删除的节点 ID
//     * Force: 是否强制删除（true=跳过安全检查，false=检查客户端连接）
//
// 返回值：
//   - RaftRemoveServerResponse: 删除成功响应（空响应）
//   - error: 删除失败时返回错误
//
// 工作流程：
//   1. 加读锁访问 Raft 实例
//   2. 检查 Raft 是否已初始化
//   3. 验证当前节点是否为 Leader
//   4. 如果非强制模式，检查是否有客户端连接到目标节点
//   5. 调用 RemoveServer 移除节点
//   6. 等待操作完成并检查错误
//
// Force 参数说明：
//   - Force=false（安全模式）：
//     * 检查是否有 Volume Server 连接到目标 Master
//     * 如果有连接，拒绝删除（防止 Volume Server 失联）
//     * 推荐在正常下线时使用
//
//   - Force=true（强制模式）：
//     * 跳过客户端连接检查
//     * 立即删除节点
//     * 用于节点永久故障或紧急情况
//
// 安全检查逻辑：
//   - 检查 ms.clientChans 中是否有目标节点的客户端连接
//   - clientChans 存储所有 Volume Server 到 Master 的长连接
//   - 键格式："{类型}@{节点ID}"，例如："master@master2"
//   - 如果有连接，说明 Volume Server 仍在与该 Master 通信
//
// 注意事项：
//   - 只有 Leader 可以删除节点
//   - 删除节点后，该节点的数据不会自动清理（需要手动停止进程）
//   - 删除 Voter 节点会降低集群的容错能力
//   - 不要删除太多节点导致失去多数派（集群不可用）
//   - 删除 Leader 会触发新一轮选举
//
// 使用场景：
//   - 缩减集群规模
//   - 替换故障节点（先添加新节点，再删除旧节点）
//   - 清理长期离线的节点
func (ms *MasterServer) RaftRemoveServer(ctx context.Context, req *master_pb.RaftRemoveServerRequest) (*master_pb.RaftRemoveServerResponse, error) {
	resp := &master_pb.RaftRemoveServerResponse{}

	// 【步骤 1：加读锁访问 Raft 实例】
	ms.Topo.RaftServerAccessLock.RLock()
	defer ms.Topo.RaftServerAccessLock.RUnlock()

	// 【步骤 2：检查 Raft 是否已初始化】
	// 单机模式下 HashicorpRaft 为 nil，返回成功（无操作）
	if ms.Topo.HashicorpRaft == nil {
		return resp, nil
	}

	// 【步骤 3：验证 Leader 身份】
	// 只有 Leader 可以修改集群配置
	if ms.Topo.HashicorpRaft.State() != raft.Leader {
		return nil, fmt.Errorf("raft remove server %s failed: %s is no current leader", req.Id, ms.Topo.HashicorpRaft.String())
	}

	// 【步骤 4：安全检查（非强制模式）】
	// 检查是否有 Volume Server 连接到目标 Master
	if !req.Force {
		ms.clientChansLock.RLock()

		// 检查 clientChans 中是否有目标节点的客户端连接
		// 键格式："{类型}@{节点ID}"，例如："master@master2"
		_, ok := ms.clientChans[fmt.Sprintf("%s@%s", cluster.MasterType, req.Id)]

		ms.clientChansLock.RUnlock()

		// 如果有连接，拒绝删除
		// 防止 Volume Server 失去与 Master 的连接
		if ok {
			return resp, fmt.Errorf("raft remove server %s failed: client connection to master exists", req.Id)
		}
	}

	// 【步骤 5：删除节点】
	// RemoveServer 将节点从 Raft 集群配置中移除
	// 参数：
	//   - ServerID: 节点 ID
	//   - prevIndex: 前一个日志索引（0 表示不检查）
	//   - timeout: 超时时间（0 表示使用默认超时）
	idxFuture := ms.Topo.HashicorpRaft.RemoveServer(raft.ServerID(req.Id), 0, 0)

	// 【步骤 6：等待操作完成】
	// IndexFuture.Error() 会阻塞直到操作完成
	// 成功返回 nil，失败返回错误（如节点不存在、网络故障等）
	if err := idxFuture.Error(); err != nil {
		return nil, err
	}

	return resp, nil
}
