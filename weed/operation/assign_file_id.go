// Package operation 提供了文件ID分配和卷操作相关的功能
// 主要包含向Master服务器请求分配文件ID、查询JWT令牌等操作
package operation

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"google.golang.org/grpc"
	"sync"
)

// VolumeAssignRequest 表示向Master服务器请求分配卷的请求参数
// 用于指定文件存储的各种约束条件和要求
type VolumeAssignRequest struct {
	Count               uint64 // 请求分配的文件ID数量
	Replication         string // 副本策略，如 "000" 表示无副本，"001" 表示同机架1个副本
	Collection          string // 集合名称，用于逻辑分组不同类型的文件
	Ttl                 string // 生存时间(Time To Live)，如 "3d" 表示3天后过期
	DiskType            string // 磁盘类型，如 "hdd" 或 "ssd"
	DataCenter          string // 指定数据中心
	Rack                string // 指定机架
	DataNode            string // 指定具体的数据节点
	WritableVolumeCount uint32 // 可写卷的数量，用于卷的预分配
}

// AssignResult 表示文件ID分配请求的返回结果
// 包含了分配的文件ID、存储位置、副本信息等
type AssignResult struct {
	Fid       string              `json:"fid,omitempty"`       // 文件ID，格式如 "3,01637037d6"，逗号前是卷ID，后面是文件在卷中的偏移
	Url       string              `json:"url,omitempty"`       // 存储服务器的内部URL地址，如 "127.0.0.1:8080"
	PublicUrl string              `json:"publicUrl,omitempty"` // 存储服务器的公网URL地址，供外部访问使用
	GrpcPort  int                 `json:"grpcPort,omitempty"`  // gRPC服务端口号
	Count     uint64              `json:"count,omitempty"`     // 实际分配的文件ID数量
	Error     string              `json:"error,omitempty"`     // 错误信息，如果分配失败则包含错误描述
	Auth      security.EncodedJwt `json:"auth,omitempty"`      // JWT认证令牌，用于后续文件操作的安全验证
	Replicas  []Location          `json:"replicas,omitempty"`  // 副本存储位置列表，每个副本的URL和数据中心信息
}

// AssignProxy 是Master服务器的代理，专门用于分配卷ID
// 它通过gRPC以流式模式与Master服务器通信
// 只有在上次连接出错时才会重新建立与Master的连接
// 使用连接池模式，支持并发请求，每个连接独立维护自己的流
type AssignProxy struct {
	grpcConnection *grpc.ClientConn            // 与Master服务器的gRPC连接
	pool           chan *singleThreadAssignProxy // 单线程代理对象池，用于控制并发数
}

// NewAssignProxy 创建一个新的分配代理实例
// masterFn: 获取Master服务器地址的函数
// grpcDialOption: gRPC连接选项，如TLS配置等
// concurrency: 并发度，决定连接池大小，控制同时进行的分配请求数量
// 返回: 分配代理实例和可能的错误
func NewAssignProxy(masterFn GetMasterFn, grpcDialOption grpc.DialOption, concurrency int) (ap *AssignProxy, err error) {
	ap = &AssignProxy{
		pool: make(chan *singleThreadAssignProxy, concurrency),
	}
	// 建立与Master服务器的gRPC连接
	ap.grpcConnection, err = pb.GrpcDial(context.Background(), masterFn(context.Background()).ToGrpcAddress(), true, grpcDialOption)
	if err != nil {
		return nil, fmt.Errorf("fail to dial %s: %v", masterFn(context.Background()).ToGrpcAddress(), err)
	}
	// 初始化连接池，预创建指定数量的单线程代理对象
	for i := 0; i < concurrency; i++ {
		ap.pool <- &singleThreadAssignProxy{}
	}
	return ap, nil
}

// Assign 执行文件ID分配请求
// primaryRequest: 主要请求，包含首选的存储位置约束条件
// alternativeRequests: 备选请求列表，当主请求失败时依次尝试
// 返回: 分配结果和可能的错误
// 使用对象池模式，从池中获取一个代理对象执行请求，完成后归还到池中
func (ap *AssignProxy) Assign(primaryRequest *VolumeAssignRequest, alternativeRequests ...*VolumeAssignRequest) (ret *AssignResult, err error) {
	// 从池中获取一个单线程代理对象
	p := <-ap.pool
	defer func() {
		// 使用完毕后归还到池中，供其他请求使用
		ap.pool <- p
	}()

	// 执行实际的分配操作
	return p.doAssign(ap.grpcConnection, primaryRequest, alternativeRequests...)
}

// singleThreadAssignProxy 单线程分配代理
// 每个实例维护一个独立的流式客户端连接，用于串行处理分配请求
// 使用互斥锁确保线程安全
type singleThreadAssignProxy struct {
	assignClient master_pb.Seaweed_StreamAssignClient // 流式分配客户端，保持与Master的长连接
	sync.Mutex                                         // 互斥锁，保护assignClient的并发访问
}

// doAssign 执行实际的文件ID分配操作
// grpcConnection: 与Master的gRPC连接
// primaryRequest: 主请求
// alternativeRequests: 备选请求列表
// 返回: 分配结果和可能的错误
// 工作流程:
// 1. 如果流式客户端未初始化，则创建新的流式连接
// 2. 依次尝试主请求和备选请求，直到成功分配或全部失败
// 3. 使用双向流式通信，每个请求都是Send-Recv配对
func (ap *singleThreadAssignProxy) doAssign(grpcConnection *grpc.ClientConn, primaryRequest *VolumeAssignRequest, alternativeRequests ...*VolumeAssignRequest) (ret *AssignResult, err error) {
	ap.Lock()
	defer ap.Unlock()

	// 如果流式客户端未初始化，创建新的流式连接
	if ap.assignClient == nil {
		client := master_pb.NewSeaweedClient(grpcConnection)
		ap.assignClient, err = client.StreamAssign(context.Background())
		if err != nil {
			ap.assignClient = nil
			return nil, fmt.Errorf("fail to create stream assign client: %w", err)
		}
	}

	// 构建请求列表：主请求 + 所有备选请求
	var requests []*VolumeAssignRequest
	requests = append(requests, primaryRequest)
	requests = append(requests, alternativeRequests...)
	ret = &AssignResult{}

	// 依次尝试每个请求，直到成功分配
	for _, request := range requests {
		if request == nil {
			continue
		}
		// 构造gRPC请求对象
		req := &master_pb.AssignRequest{
			Count:               request.Count,
			Replication:         request.Replication,
			Collection:          request.Collection,
			Ttl:                 request.Ttl,
			DiskType:            request.DiskType,
			DataCenter:          request.DataCenter,
			Rack:                request.Rack,
			DataNode:            request.DataNode,
			WritableVolumeCount: request.WritableVolumeCount,
		}
		// 发送请求到Master
		if err = ap.assignClient.Send(req); err != nil {
			return nil, fmt.Errorf("StreamAssignSend: %w", err)
		}
		// 接收Master的响应
		resp, grpcErr := ap.assignClient.Recv()
		if grpcErr != nil {
			return nil, grpcErr
		}
		if resp.Error != "" {
			return nil, fmt.Errorf("StreamAssignRecv: %v", resp.Error)
		}

		// 填充返回结果
		ret.Count = resp.Count
		ret.Fid = resp.Fid
		ret.Url = resp.Location.Url
		ret.PublicUrl = resp.Location.PublicUrl
		ret.GrpcPort = int(resp.Location.GrpcPort)
		ret.Error = resp.Error
		ret.Auth = security.EncodedJwt(resp.Auth)
		// 填充副本位置信息
		for _, r := range resp.Replicas {
			ret.Replicas = append(ret.Replicas, Location{
				Url:        r.Url,
				PublicUrl:  r.PublicUrl,
				DataCenter: r.DataCenter,
			})
		}

		// 如果成功分配了文件ID（Count > 0），则返回成功
		if ret.Count <= 0 {
			continue
		}
		break
	}

	return
}

// Assign 向Master服务器请求分配文件ID（非流式模式）
// 这是一个更简单的版本，每次请求都会创建新的连接，不保持长连接
// ctx: 上下文，用于控制请求的生命周期
// masterFn: 获取Master服务器地址的函数
// grpcDialOption: gRPC连接选项
// primaryRequest: 主请求
// alternativeRequests: 备选请求列表
// 返回: 分配结果和最后一次的错误（如果全部失败）
//
// 与AssignProxy的区别:
// - Assign函数每次请求都建立新连接，适合低频调用
// - AssignProxy保持长连接，适合高频调用，性能更好
func Assign(ctx context.Context, masterFn GetMasterFn, grpcDialOption grpc.DialOption, primaryRequest *VolumeAssignRequest, alternativeRequests ...*VolumeAssignRequest) (*AssignResult, error) {

	// 构建完整的请求列表
	var requests []*VolumeAssignRequest
	requests = append(requests, primaryRequest)
	requests = append(requests, alternativeRequests...)

	var lastError error
	ret := &AssignResult{}

	// 依次尝试每个请求
	for i, request := range requests {
		if request == nil {
			continue
		}

		// 使用WithMasterServerClient帮助函数建立临时连接并执行请求
		lastError = WithMasterServerClient(false, masterFn(ctx), grpcDialOption, func(masterClient master_pb.SeaweedClient) error {
			// 构造gRPC请求
			req := &master_pb.AssignRequest{
				Count:               request.Count,
				Replication:         request.Replication,
				Collection:          request.Collection,
				Ttl:                 request.Ttl,
				DiskType:            request.DiskType,
				DataCenter:          request.DataCenter,
				Rack:                request.Rack,
				DataNode:            request.DataNode,
				WritableVolumeCount: request.WritableVolumeCount,
			}
			// 调用Master的Assign RPC方法（非流式）
			resp, grpcErr := masterClient.Assign(ctx, req)
			if grpcErr != nil {
				return grpcErr
			}

			if resp.Error != "" {
				return fmt.Errorf("assignRequest: %v", resp.Error)
			}

			// 填充返回结果
			ret.Count = resp.Count
			ret.Fid = resp.Fid
			ret.Url = resp.Location.Url
			ret.PublicUrl = resp.Location.PublicUrl
			ret.GrpcPort = int(resp.Location.GrpcPort)
			ret.Error = resp.Error
			ret.Auth = security.EncodedJwt(resp.Auth)
			for _, r := range resp.Replicas {
				ret.Replicas = append(ret.Replicas, Location{
					Url:        r.Url,
					PublicUrl:  r.PublicUrl,
					DataCenter: r.DataCenter,
				})
			}

			return nil

		})

		// 如果请求失败，记录统计信息并继续尝试下一个请求
		if lastError != nil {
			stats.FilerHandlerCounter.WithLabelValues(stats.ErrorChunkAssign).Inc()
			continue
		}

		// 如果分配的数量为0，表示没有可用的存储空间
		if ret.Count <= 0 {
			lastError = fmt.Errorf("assign failure %d: %v", i+1, ret.Error)
			continue
		}

		// 分配成功，跳出循环
		break
	}

	return ret, lastError
}

// LookupJwt 根据文件ID查询对应的JWT令牌
// master: Master服务器地址
// grpcDialOption: gRPC连接选项
// fileId: 文件ID，格式如 "3,01637037d6"
// 返回: JWT令牌，用于后续的文件访问验证
//
// 此函数通过Master服务器查询指定文件ID所在卷的JWT令牌
// 令牌用于在访问Volume服务器时进行身份验证
func LookupJwt(master pb.ServerAddress, grpcDialOption grpc.DialOption, fileId string) (token security.EncodedJwt) {

	WithMasterServerClient(false, master, grpcDialOption, func(masterClient master_pb.SeaweedClient) error {

		// 向Master查询文件ID对应的卷信息
		resp, grpcErr := masterClient.LookupVolume(context.Background(), &master_pb.LookupVolumeRequest{
			VolumeOrFileIds: []string{fileId},
		})
		if grpcErr != nil {
			return grpcErr
		}

		// 如果没有找到卷信息，返回空令牌
		if len(resp.VolumeIdLocations) == 0 {
			return nil
		}

		// 提取JWT令牌
		token = security.EncodedJwt(resp.VolumeIdLocations[0].Auth)

		return nil

	})

	return
}

// StorageOption 存储选项配置
// 定义了文件存储的各种策略和约束条件
type StorageOption struct {
	Replication       string // 副本策略，如 "000"=无副本, "001"=同机架1副本, "010"=不同机架1副本
	DiskType          string // 磁盘类型，如 "hdd", "ssd", "nvme"
	Collection        string // 集合名称，用于将相同类型的文件分组存储
	DataCenter        string // 指定数据中心
	Rack              string // 指定机架
	DataNode          string // 指定数据节点
	TtlSeconds        int32  // 生存时间（秒），文件过期后可被自动清理
	VolumeGrowthCount uint32 // 卷增长数量，预分配的可写卷数量
	MaxFileNameLength uint32 // 最大文件名长度限制
	Fsync             bool   // 是否在写入后立即执行fsync，确保数据持久化
	SaveInside        bool   // 是否将小文件直接保存在索引中（内嵌存储）
}

// TtlString 将TTL秒数转换为字符串格式
// 返回: TTL字符串，如 "3d" 表示3天，"2h" 表示2小时
func (so *StorageOption) TtlString() string {
	return needle.SecondsToTTL(so.TtlSeconds)
}

// ToAssignRequests 将StorageOption转换为VolumeAssignRequest
// count: 需要分配的文件ID数量
// 返回: 主请求和备选请求
//
// 工作原理:
// - 主请求（ar）包含所有指定的约束条件（DataCenter、Rack、DataNode）
// - 备选请求（altRequest）移除了位置约束，如果主请求失败时使用
// - 这种双重请求机制提高了分配成功率，在指定位置无可用空间时可以降级到其他位置
func (so *StorageOption) ToAssignRequests(count int) (ar *VolumeAssignRequest, altRequest *VolumeAssignRequest) {
	// 构造主请求，包含所有约束条件
	ar = &VolumeAssignRequest{
		Count:               uint64(count),
		Replication:         so.Replication,
		Collection:          so.Collection,
		Ttl:                 so.TtlString(),
		DiskType:            so.DiskType,
		DataCenter:          so.DataCenter,
		Rack:                so.Rack,
		DataNode:            so.DataNode,
		WritableVolumeCount: so.VolumeGrowthCount,
	}
	// 如果指定了位置约束（数据中心、机架或数据节点），创建备选请求
	// 备选请求移除位置约束，允许在任意位置分配
	if so.DataCenter != "" || so.Rack != "" || so.DataNode != "" {
		altRequest = &VolumeAssignRequest{
			Count:               uint64(count),
			Replication:         so.Replication,
			Collection:          so.Collection,
			Ttl:                 so.TtlString(),
			DiskType:            so.DiskType,
			DataCenter:          "", // 清空位置约束
			Rack:                "", // 清空位置约束
			DataNode:            "", // 清空位置约束
			WritableVolumeCount: so.VolumeGrowthCount,
		}
	}
	return
}
