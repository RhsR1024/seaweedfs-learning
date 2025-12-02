// Package weed_server 中的 master_grpc_server_collection.go 实现 Master 的 Collection 管理 gRPC 服务
//
// 核心功能：
//   管理 SeaweedFS 的 Collection（集合），实现集合的查询和删除操作
//
// Collection（集合）概念：
//   - Collection 是 Volume 的逻辑分组，用于隔离和组织文件
//   - 类似于数据库中的 database 或 namespace 概念
//   - 每个 Collection 可以有独立的副本策略、TTL、磁盘类型
//   - Collection 名称作为 Volume 的前缀，例如：photos_volume_1
//
// Collection 的两种类型：
//   1. Normal Volume（普通卷）：
//      - 基于副本复制的存储方式（如 001、010、100）
//      - 适合热数据和频繁访问的文件
//      - 每个 Volume 32GB，包含多个 Needle
//
//   2. EC Volume（纠删码卷）：
//      - 基于 Reed-Solomon 纠删码的存储方式
//      - 适合冷数据和归档文件
//      - 存储开销更低（例如 10+4 方案，14 个分片，允许最多 4 个分片丢失）
//
// 集合删除流程：
//   1. 验证 Leader 身份
//   2. 删除所有普通 Volume（向每个 Volume Server 发送删除请求）
//   3. 删除所有 EC Volume（向每个 Volume Server 发送删除请求）
//   4. 从拓扑中移除 Collection 元数据
//
// 使用场景：
//   - 管理工具查询所有 Collection 列表
//   - 清理不再使用的 Collection 及其所有数据
//   - 多租户环境中按 Collection 隔离不同业务的数据
package weed_server

import (
	"context"

	"github.com/seaweedfs/raft"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

// CollectionList 列出集群中所有的 Collection
// 支持分别查询普通 Volume 和 EC Volume 的 Collection
//
// 参数说明：
//   - ctx: 请求上下文
//   - req: 查询请求，包含以下字段：
//     * IncludeNormalVolumes: 是否包含普通 Volume 的 Collection
//     * IncludeEcVolumes: 是否包含 EC Volume 的 Collection
//
// 返回值：
//   - CollectionListResponse: Collection 列表
//     * Collections: Collection 数组，每个元素包含 Name 字段
//
// 工作流程：
//   1. 验证 Leader 身份（只有 Leader 可以查询）
//   2. 从拓扑中获取 Collection 列表
//   3. 构建响应返回
//
// 使用场景：
//   - 管理工具查询所有 Collection
//   - 监控系统统计 Collection 数量和分布
//   - 客户端选择 Collection 上传文件
//
// 注意事项：
//   - 只有 Raft Leader 可以响应此请求
//   - Collection 列表来自内存拓扑结构，性能很高
func (ms *MasterServer) CollectionList(ctx context.Context, req *master_pb.CollectionListRequest) (*master_pb.CollectionListResponse, error) {

	// 【步骤 1：Leader 检查】
	// 只有 Raft Leader 可以提供准确的 Collection 列表
	if !ms.Topo.IsLeader() {
		return nil, raft.NotLeaderError
	}

	resp := &master_pb.CollectionListResponse{}

	// 【步骤 2：获取 Collection 列表】
	// 参数：
	//   - IncludeNormalVolumes: 是否包含普通 Volume 的 Collection
	//   - IncludeEcVolumes: 是否包含 EC Volume 的 Collection
	// 返回去重后的 Collection 名称列表
	collections := ms.Topo.ListCollections(req.IncludeNormalVolumes, req.IncludeEcVolumes)

	// 【步骤 3：构建响应】
	// 将 Collection 名称转换为 protobuf 消息
	for _, c := range collections {
		resp.Collections = append(resp.Collections, &master_pb.Collection{
			Name: c,  // Collection 名称
		})
	}

	return resp, nil
}

// CollectionDelete 删除指定的 Collection 及其所有数据
// 包括普通 Volume 和 EC Volume
//
// 参数说明：
//   - ctx: 请求上下文
//   - req: 删除请求，包含以下字段：
//     * Name: 要删除的 Collection 名称
//
// 返回值：
//   - CollectionDeleteResponse: 删除响应（空响应，成功时无内容）
//   - error: 删除失败时返回错误
//
// 工作流程：
//   1. 验证 Leader 身份
//   2. 删除所有普通 Volume（调用 doDeleteNormalCollection）
//   3. 删除所有 EC Volume（调用 doDeleteEcCollection）
//   4. 返回成功响应
//
// 删除操作详解：
//   - 向每个 Volume Server 发送 DeleteCollection gRPC 请求
//   - Volume Server 会删除该 Collection 的所有 Volume 文件（.dat、.idx）
//   - Master 从拓扑结构中移除 Collection 元数据
//
// 注意事项：
//   - 此操作不可逆，会永久删除所有文件数据
//   - 只有 Raft Leader 可以执行删除操作
//   - 如果任何 Volume Server 删除失败，整个操作会失败并返回错误
//   - 删除过程中 Collection 可能处于不一致状态
//
// 使用场景：
//   - 清理不再使用的 Collection
//   - 多租户环境中删除某个租户的所有数据
//   - 测试环境清理数据
func (ms *MasterServer) CollectionDelete(ctx context.Context, req *master_pb.CollectionDeleteRequest) (*master_pb.CollectionDeleteResponse, error) {

	// 【步骤 1：Leader 检查】
	// 只有 Raft Leader 可以执行删除操作
	if !ms.Topo.IsLeader() {
		return nil, raft.NotLeaderError
	}

	resp := &master_pb.CollectionDeleteResponse{}

	// 【步骤 2：删除普通 Volume】
	// 向所有 Volume Server 发送删除请求，并从拓扑中移除
	err := ms.doDeleteNormalCollection(req.Name)
	if err != nil {
		return nil, err
	}

	// 【步骤 3：删除 EC Volume】
	// 向所有 EC Volume Server 发送删除请求，并从拓扑中移除
	err = ms.doDeleteEcCollection(req.Name)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

// doDeleteNormalCollection 删除指定 Collection 的所有普通 Volume
// 向每个 Volume Server 发送删除请求，然后从拓扑中移除
//
// 参数说明：
//   - collectionName: 要删除的 Collection 名称
//
// 返回值：
//   - error: 删除失败时返回错误，Collection 不存在时返回 nil（视为成功）
//
// 工作流程：
//   1. 查找 Collection（如果不存在则直接返回成功）
//   2. 获取该 Collection 的所有 Volume Server
//   3. 向每个 Volume Server 发送 DeleteCollection gRPC 请求
//   4. 从拓扑中移除 Collection 元数据
//
// Volume Server 删除操作：
//   - Volume Server 收到请求后会删除该 Collection 的所有 Volume 文件
//   - 包括 .dat（数据文件）和 .idx（索引文件）
//   - 删除失败会返回错误，导致整个删除操作失败
//
// 错误处理：
//   - 如果任何 Volume Server 删除失败，立即返回错误
//   - 此时可能部分 Volume Server 已删除成功，导致数据不一致
//   - 调用方需要处理这种部分失败的情况
func (ms *MasterServer) doDeleteNormalCollection(collectionName string) error {

	// 【步骤 1：查找 Collection】
	// 从拓扑中查找指定名称的 Collection
	collection, ok := ms.Topo.FindCollection(collectionName)
	if !ok {
		// Collection 不存在，视为已删除，返回成功
		return nil
	}

	// 【步骤 2：向每个 Volume Server 发送删除请求】
	// ListVolumeServers() 返回该 Collection 所在的所有 Volume Server
	for _, server := range collection.ListVolumeServers() {
		// 使用 WithVolumeServerClient 建立 gRPC 连接并执行删除操作
		err := operation.WithVolumeServerClient(false, server.ServerAddress(), ms.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
			// 调用 Volume Server 的 DeleteCollection 方法
			// 会删除该 Collection 的所有 Volume 文件（.dat、.idx）
			_, deleteErr := client.DeleteCollection(context.Background(), &volume_server_pb.DeleteCollectionRequest{
				Collection: collectionName,
			})
			return deleteErr
		})
		if err != nil {
			// 如果任何 Volume Server 删除失败，立即返回错误
			return err
		}
	}

	// 【步骤 3：从拓扑中移除 Collection】
	// 删除 Collection 的元数据，包括 VolumeLayout 和 Volume 映射
	ms.Topo.DeleteCollection(collectionName)

	return nil
}

// doDeleteEcCollection 删除指定 Collection 的所有 EC Volume
// 向每个 EC Volume Server 发送删除请求，然后从拓扑中移除
//
// 参数说明：
//   - collectionName: 要删除的 Collection 名称
//
// 返回值：
//   - error: 删除失败时返回错误
//
// 工作流程：
//   1. 获取该 Collection 的所有 EC Volume Server
//   2. 向每个 EC Volume Server 发送 DeleteCollection gRPC 请求
//   3. 从拓扑中移除 EC Collection 元数据
//
// EC Volume 删除操作：
//   - EC Volume 使用纠删码存储，数据被分成多个分片（shards）
//   - 删除操作会删除所有分片文件（.ecx、.ec00 ~ .ec13）
//   - 如果某个分片删除失败，整个删除操作失败
//
// EC Volume 分片说明：
//   - 以 10+4 配置为例：10 个数据分片 + 4 个校验分片
//   - 文件格式：volumeId.ec00 ~ volumeId.ec13（14 个分片）
//   - 索引文件：volumeId.ecx（EC 索引）
//   - 删除时需要清理所有分片和索引文件
//
// 错误处理：
//   - 如果任何 EC Volume Server 删除失败，立即返回错误
//   - 部分删除成功可能导致数据不一致
//   - 建议在删除前确保 Collection 不再被使用
func (ms *MasterServer) doDeleteEcCollection(collectionName string) error {

	// 【步骤 1：获取 EC Volume Server 列表】
	// ListEcServersByCollection 返回该 Collection 的所有 EC Volume Server 地址
	listOfEcServers := ms.Topo.ListEcServersByCollection(collectionName)

	// 【步骤 2：向每个 EC Volume Server 发送删除请求】
	for _, server := range listOfEcServers {
		// 使用 WithVolumeServerClient 建立 gRPC 连接并执行删除操作
		err := operation.WithVolumeServerClient(false, server, ms.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
			// 调用 Volume Server 的 DeleteCollection 方法
			// Volume Server 会删除该 Collection 的所有 EC Volume 分片文件
			_, deleteErr := client.DeleteCollection(context.Background(), &volume_server_pb.DeleteCollectionRequest{
				Collection: collectionName,
			})
			return deleteErr
		})
		if err != nil {
			// 如果任何 EC Volume Server 删除失败，立即返回错误
			return err
		}
	}

	// 【步骤 3：从拓扑中移除 EC Collection】
	// 删除 EC Collection 的元数据，包括 EcVolumeLayout 和分片映射
	ms.Topo.DeleteEcCollection(collectionName)

	return nil
}
