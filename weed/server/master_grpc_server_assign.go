// Package weed_server 中的 master_grpc_server_assign.go 实现 Master 的文件 ID 分配 gRPC 服务
//
// 核心功能：
//   为文件上传请求分配文件 ID（fid）和 Volume Server 地址
//
// Assign 流程（文件 ID 分配）：
//   1. 客户端请求：上传文件前，向 Master 请求分配 fid
//   2. Master 选择：根据副本策略、数据中心等参数选择合适的 Volume
//   3. 返回结果：fid + Volume Server 地址 + JWT token
//   4. 客户端上传：使用 fid 和地址上传文件到 Volume Server
//
// fid 格式：
//   volumeId,fileKey[_cookie]
//   - volumeId：32 位无符号整数，标识 Volume
//   - fileKey：64 位无符号整数（十六进制），Volume 内唯一
//   - cookie：32 位无符号整数（十六进制），防止 URL 猜测（可选）
//   - 示例：3,01e3b0756f 或 3,01e3b0756f_a1b2c3d4
//
// Volume 自动增长：
//   - 如果没有可写 Volume，Master 会触发 Volume 增长请求
//   - 通过 volumeGrowthRequestChan 发送增长请求到后台 goroutine
//   - 后台 goroutine 向 Volume Server 发送创建 Volume 的请求
//
// 重试机制：
//   - 如果暂时没有可用 Volume，会重试最多 10 秒
//   - 每次重试间隔 200 毫秒
//   - 重试期间可能触发 Volume 增长
package weed_server

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/stats"

	"github.com/seaweedfs/raft"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/topology"
)

// StreamAssign 实现流式文件 ID 分配服务
// 使用双向流 gRPC，支持批量分配文件 ID，减少网络往返次数
//
// 工作流程：
//   1. 客户端通过流发送多个 AssignRequest
//   2. Master 为每个请求调用 Assign() 分配 fid
//   3. Master 通过流返回对应的 AssignResponse
//   4. 重复上述过程，直到客户端关闭流或发生错误
//
// 使用场景：
//   - 批量上传文件：一次性请求多个 fid，减少 RPC 调用开销
//   - 流水线处理：客户端可以边接收 fid 边上传文件，提高并发度
//
// 错误处理：
//   - 接收错误：客户端断开连接或网络故障
//   - 分配错误：没有可用 Volume、参数错误等
//   - 发送错误：响应发送失败（通常是客户端断开）
func (ms *MasterServer) StreamAssign(server master_pb.Seaweed_StreamAssignServer) error {
	for {
		// 接收客户端的分配请求
		// 会阻塞等待，直到收到请求或流关闭
		req, err := server.Recv()
		if err != nil {
			glog.Errorf("StreamAssign failed to receive: %v", err)
			return err
		}

		// 调用 Assign 处理分配逻辑
		// 使用 Background context，因为流本身控制生命周期
		resp, err := ms.Assign(context.Background(), req)
		if err != nil {
			glog.Errorf("StreamAssign failed to assign: %v", err)
			return err
		}

		// 将响应发送回客户端
		// 如果客户端已断开，Send 会返回错误
		if err = server.Send(resp); err != nil {
			glog.Errorf("StreamAssign failed to send: %v", err)
			return err
		}
	}
}
// Assign 实现文件 ID 分配的核心逻辑
// 这是 SeaweedFS 上传流程的第一步，为文件分配唯一的 fid 和可写的 Volume Server
//
// 参数说明：
//   - ctx: 请求上下文，用于超时控制和取消传播
//   - req: 分配请求，包含以下字段：
//     * Count: 需要分配的 fid 数量（默认 1）
//     * Replication: 副本策略，如 "000"（无副本）、"001"（同机架 1 副本）
//     * Collection: 集合名称，用于逻辑分组和隔离
//     * Ttl: 文件生存时间，如 "3d"（3 天）、"1m"（1 个月）
//     * DiskType: 磁盘类型，如 "hdd"、"ssd"
//     * DataCenter: 指定数据中心，为空则自动选择
//     * Rack: 指定机架，为空则自动选择
//     * DataNode: 指定节点，为空则自动选择
//     * MemoryMapMaxSizeMb: Volume 的 mmap 最大大小
//     * WritableVolumeCount: 期望的可写 Volume 数量
//
// 返回值：
//   - AssignResponse: 分配成功时返回
//     * Fid: 文件 ID，格式 "volumeId,fileKey" 或 "volumeId,fileKey_cookie"
//     * Location: 主 Volume Server 位置（URL、公网 URL、gRPC 端口、数据中心）
//     * Count: 实际分配的数量（通常等于请求的 Count）
//     * Auth: JWT 令牌，用于向 Volume Server 上传文件时的身份验证
//     * Replicas: 副本 Volume Server 列表
//   - error: 分配失败时返回错误
//
// 工作流程：
//   1. Leader 检查：只有 Raft Leader 可以处理分配请求
//   2. 参数解析：解析副本策略、TTL、磁盘类型等
//   3. 拓扑验证：检查请求的数据中心是否存在
//   4. 获取 VolumeLayout：根据 Collection、副本策略、TTL、磁盘类型查找或创建
//   5. 循环重试（最多 10 秒）：
//      a. 调用 Topo.PickForWrite 选择可写 Volume
//      b. 如果需要增长且未禁用：发送 Volume 增长请求到后台 goroutine
//      c. 如果成功：返回 fid + Volume Server 地址 + JWT token
//      d. 如果失败：等待 200ms 后重试
//   6. 超时失败：返回最后一次的错误
//
// 重试机制：
//   - 最大重试时间：10 秒
//   - 重试间隔：200 毫秒
//   - 触发 Volume 增长：如果 shouldGrow 为 true 且未禁用增长
//   - 提前退出：如果指定了数据中心/机架但没有可写 Volume
//
// Volume 自动增长：
//   - 触发条件：PickForWrite 返回 shouldGrow=true
//   - 限制检查：避免重复发送增长请求（HasGrowRequest）
//   - 异步处理：通过 volumeGrowthRequestChan 发送到后台 goroutine
//   - 后台创建：后台 goroutine 向 Volume Server 发送创建 Volume 的 gRPC 请求
func (ms *MasterServer) Assign(ctx context.Context, req *master_pb.AssignRequest) (*master_pb.AssignResponse, error) {

	// 【步骤 1：Leader 检查】
	// 只有 Raft Leader 可以处理写操作（分配 fid）
	// Follower 会返回 NotLeaderError，客户端需要重定向到 Leader
	if !ms.Topo.IsLeader() {
		return nil, raft.NotLeaderError
	}

	// 【步骤 2：参数默认值处理】
	// Count 默认为 1（分配 1 个 fid）
	if req.Count == 0 {
		req.Count = 1
	}

	// Replication 默认使用 Master 配置的副本策略
	// 例如：-defaultReplication=001 表示同机架 1 个副本
	if req.Replication == "" {
		req.Replication = ms.option.DefaultReplicaPlacement
	}

	// 解析副本策略字符串为 ReplicaPlacement 对象
	// 格式：XYZ（X=不同数据中心副本数，Y=不同机架副本数，Z=不同服务器副本数）
	replicaPlacement, err := super_block.NewReplicaPlacementFromString(req.Replication)
	if err != nil {
		return nil, err
	}

	// 解析 TTL（Time To Live）字符串
	// 支持格式：3m（3 分钟）、2h（2 小时）、5d（5 天）
	ttl, err := needle.ReadTTL(req.Ttl)
	if err != nil {
		return nil, err
	}

	// 解析磁盘类型：hdd、ssd 等
	diskType := types.ToDiskType(req.DiskType)

	// 【步骤 3：构建 VolumeGrowOption】
	// 获取当前 Needle 格式版本（v1/v2/v3）
	ver := needle.GetCurrentVersion()

	// VolumeGrowOption 包含 Volume 创建和选择所需的所有参数
	option := &topology.VolumeGrowOption{
		Collection:         req.Collection,          // 集合名称
		ReplicaPlacement:   replicaPlacement,        // 副本策略
		Ttl:                ttl,                     // 文件 TTL
		DiskType:           diskType,                // 磁盘类型（hdd/ssd）
		Preallocate:        ms.preallocateSize,      // Volume 预分配大小
		DataCenter:         req.DataCenter,          // 指定数据中心
		Rack:               req.Rack,                // 指定机架
		DataNode:           req.DataNode,            // 指定节点
		MemoryMapMaxSizeMb: req.MemoryMapMaxSizeMb,  // mmap 最大大小
		Version:            uint32(ver),             // Needle 版本
	}

	// 【步骤 4：拓扑验证】
	// 如果请求指定了数据中心，检查该数据中心是否存在
	// 空字符串表示自动选择，不需要检查
	if !ms.Topo.DataCenterExists(option.DataCenter) {
		return nil, fmt.Errorf("data center %v not found in topology", option.DataCenter)
	}

	// 【步骤 5：获取 VolumeLayout】
	// VolumeLayout 管理具有相同特征的 Volume 集合
	// 根据 Collection、副本策略、TTL、磁盘类型进行分组
	vl := ms.Topo.GetVolumeLayout(option.Collection, option.ReplicaPlacement, option.Ttl, option.DiskType)

	// 设置上次 Volume 增长数量
	// 用于计算下次需要增长多少个 Volume（动态调整）
	vl.SetLastGrowCount(req.WritableVolumeCount)

	// 【步骤 6：重试循环，最多 10 秒】
	var (
		lastErr    error                 // 记录最后一次错误，超时后返回
		maxTimeout = time.Second * 10    // 最大重试时间 10 秒
		startTime  = time.Now()          // 记录开始时间
	)

	for time.Now().Sub(startTime) < maxTimeout {
		// 【步骤 6.1：调用 PickForWrite 选择可写 Volume】
		// 返回值：
		//   - fid: 分配的文件 ID
		//   - count: 实际分配的数量
		//   - dnList: Volume Server 列表（主节点 + 副本节点）
		//   - shouldGrow: 是否需要增长 Volume
		//   - err: 错误信息
		fid, count, dnList, shouldGrow, err := ms.Topo.PickForWrite(req.Count, option, vl)

		// 【步骤 6.2：触发 Volume 自动增长】
		// 触发条件：
		//   1. shouldGrow = true（当前可写 Volume 不足）
		//   2. !vl.HasGrowRequest()（避免重复发送增长请求）
		//   3. !ms.option.VolumeGrowthDisabled（未禁用自动增长）
		if shouldGrow && !vl.HasGrowRequest() && !ms.option.VolumeGrowthDisabled {
			// 如果出错且没有可用空间，提示更详细的错误信息
			if err != nil && ms.Topo.AvailableSpaceFor(option) <= 0 {
				err = fmt.Errorf("%s and no free volumes left for %s", err.Error(), option.String())
			}

			// 标记 VolumeLayout 已有增长请求，避免重复触发
			vl.AddGrowRequest()

			// 发送 Volume 增长请求到后台 goroutine
			// 后台 goroutine 会向 Volume Server 发送创建 Volume 的 gRPC 请求
			ms.volumeGrowthRequestChan <- &topology.VolumeGrowRequest{
				Option: option,                   // Volume 创建参数
				Count:  req.WritableVolumeCount,  // 期望的可写 Volume 数量
				Reason: "grpc assign",            // 增长原因（用于日志）
			}
		}

		// 【步骤 6.3：处理选择失败】
		if err != nil {
			// 记录日志（级别 1，默认不输出）
			glog.V(1).Infof("assign %v %v: %v", req, option.String(), err)

			// 统计指标：PickForWrite 失败次数
			stats.MasterPickForWriteErrorCounter.Inc()

			// 保存错误，用于最终返回
			lastErr = err

			// 提前退出条件：
			// 如果指定了数据中心或机架，但没有可写 Volume，不再重试
			// 因为等待也不会有新的 Volume 出现在指定位置
			if (req.DataCenter != "" || req.Rack != "") && strings.Contains(err.Error(), topology.NoWritableVolumes) {
				break
			}

			// 等待 200ms 后重试
			// 期间可能有 Volume 增长完成或其他客户端释放 Volume
			time.Sleep(200 * time.Millisecond)
			continue
		}

		// 【步骤 6.4：构建响应】
		// 获取主 Volume Server（dnList 的第一个节点）
		dn := dnList.Head()
		if dn == nil {
			// 理论上不应该发生（PickForWrite 成功但返回空列表）
			// 继续重试
			continue
		}

		// 【步骤 6.5：构建副本位置列表】
		// dnList.Rest() 返回除主节点外的所有副本节点
		var replicas []*master_pb.Location
		for _, r := range dnList.Rest() {
			replicas = append(replicas, &master_pb.Location{
				Url:        r.Url(),              // 内网地址（IP:Port）
				PublicUrl:  r.PublicUrl,          // 公网地址
				GrpcPort:   uint32(r.GrpcPort),   // gRPC 端口
				DataCenter: r.GetDataCenterId(),  // 数据中心 ID
			})
		}

		// 【步骤 6.6：返回分配结果】
		return &master_pb.AssignResponse{
			Fid: fid,  // 文件 ID，格式：volumeId,fileKey 或 volumeId,fileKey_cookie
			Location: &master_pb.Location{
				Url:        dn.Url(),              // 主 Volume Server 内网地址
				PublicUrl:  dn.PublicUrl,          // 主 Volume Server 公网地址
				GrpcPort:   uint32(dn.GrpcPort),   // 主 Volume Server gRPC 端口
				DataCenter: dn.GetDataCenterId(),  // 主 Volume Server 数据中心
			},
			Count: count,  // 实际分配的数量
			// 生成 JWT 令牌用于上传验证
			// 客户端上传文件到 Volume Server 时需要在 HTTP Header 中携带此令牌
			// Volume Server 会验证令牌的签名和有效期
			Auth: string(security.GenJwtForVolumeServer(ms.guard.SigningKey, ms.guard.ExpiresAfterSec, fid)),
			Replicas: replicas,  // 副本 Volume Server 列表
		}, nil
	}

	// 【步骤 7：超时失败】
	// 重试 10 秒后仍然失败，记录日志并返回错误
	if lastErr != nil {
		glog.V(0).Infof("assign %v %v: %v", req, option.String(), lastErr)
	}
	return nil, lastErr
}
