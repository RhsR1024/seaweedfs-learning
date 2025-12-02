// Package weed_server 中的 filer_grpc_server_admin.go 提供 filer 侧管理类 gRPC 接口
// 主要用于统计查询、延迟测试以及配置获取。
package weed_server

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/util/version"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

// Statistics 调用 master 端的统计接口，返回容量/数量等指标
// 参数可指定副本策略、集合、TTL、磁盘类型以便过滤
func (fs *FilerServer) Statistics(ctx context.Context, req *filer_pb.StatisticsRequest) (resp *filer_pb.StatisticsResponse, err error) {

	// 用于保存 master 返回的统计数据
	var output *master_pb.StatisticsResponse

	// 通过 MasterClient 连接池执行统计查询
	// WithClient 会自动选择可用的 master 节点
	err = fs.filer.MasterClient.WithClient(false, func(masterClient master_pb.SeaweedClient) error {
		// 将 filer 层的请求参数转发给 master
		// master 会根据这些过滤条件统计符合要求的 volume 信息
		grpcResponse, grpcErr := masterClient.Statistics(context.Background(), &master_pb.StatisticsRequest{
			Replication: req.Replication, // 副本策略过滤,如 "001"
			Collection:  req.Collection,  // 集合名称过滤
			Ttl:         req.Ttl,         // TTL 过滤
			DiskType:    req.DiskType,    // 磁盘类型过滤(hdd/ssd)
		})
		if grpcErr != nil {
			return grpcErr
		}

		// 保存 master 返回的统计结果
		output = grpcResponse
		return nil
	})

	// 如果 master 连接或查询失败,直接返回错误
	if err != nil {
		return nil, err
	}

	// 将 master 返回的数据转换为 filer 层的响应格式
	return &filer_pb.StatisticsResponse{
		TotalSize: output.TotalSize, // 总容量(字节)
		UsedSize:  output.UsedSize,  // 已使用容量(字节)
		FileCount: output.FileCount, // 文件数量
	}, nil
}

// Ping 支持在 Filer、Volume、Master 之间执行延迟探测
// 根据 TargetType 自动选择客户端并记录本地/远端时间戳
func (fs *FilerServer) Ping(ctx context.Context, req *filer_pb.PingRequest) (resp *filer_pb.PingResponse, pingErr error) {
	// 初始化响应,记录本地开始时间戳(纳秒级)
	resp = &filer_pb.PingResponse{
		StartTimeNs: time.Now().UnixNano(),
	}

	// 【情况 1】目标是 Filer 类型
	// 通过 gRPC 连接到目标 Filer 并调用其 Ping 接口
	if req.TargetType == cluster.FilerType {
		pingErr = pb.WithFilerClient(false, 0, pb.ServerAddress(req.Target), fs.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
			// 调用远程 Filer 的 Ping 接口
			pingResp, err := client.Ping(ctx, &filer_pb.PingRequest{})
			if pingResp != nil {
				// 记录远程服务器的时间戳,用于计算时钟偏移
				resp.RemoteTimeNs = pingResp.StartTimeNs
			}
			return err
		})
	}

	// 【情况 2】目标是 Volume Server 类型
	// 通过 gRPC 连接到目标 Volume Server 并调用其 Ping 接口
	if req.TargetType == cluster.VolumeServerType {
		pingErr = pb.WithVolumeServerClient(false, pb.ServerAddress(req.Target), fs.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
			// 调用远程 Volume Server 的 Ping 接口
			pingResp, err := client.Ping(ctx, &volume_server_pb.PingRequest{})
			if pingResp != nil {
				// 记录远程服务器的时间戳
				resp.RemoteTimeNs = pingResp.StartTimeNs
			}
			return err
		})
	}

	// 【情况 3】目标是 Master 类型
	// 通过 gRPC 连接到目标 Master 并调用其 Ping 接口
	if req.TargetType == cluster.MasterType {
		pingErr = pb.WithMasterClient(false, pb.ServerAddress(req.Target), fs.grpcDialOption, false, func(client master_pb.SeaweedClient) error {
			// 调用远程 Master 的 Ping 接口
			pingResp, err := client.Ping(ctx, &master_pb.PingRequest{})
			if pingResp != nil {
				// 记录远程服务器的时间戳
				resp.RemoteTimeNs = pingResp.StartTimeNs
			}
			return err
		})
	}

	// 如果 ping 失败,包装错误信息
	if pingErr != nil {
		pingErr = fmt.Errorf("ping %s %s: %v", req.TargetType, req.Target, pingErr)
	}

	// 记录本地结束时间戳
	// 客户端可以通过 StopTimeNs - StartTimeNs 计算往返延迟
	resp.StopTimeNs = time.Now().UnixNano()
	return
}

// GetFilerConfiguration 返回当前 Filer 实例的主要配置项
// 包括 master 列表、默认副本策略、自定义 HTTP 行为等
func (fs *FilerServer) GetFilerConfiguration(ctx context.Context, req *filer_pb.GetFilerConfigurationRequest) (resp *filer_pb.GetFilerConfigurationResponse, err error) {

	// 构建配置响应,包含 Filer 的所有关键配置信息
	t := &filer_pb.GetFilerConfigurationResponse{
		// Master 服务器地址列表,格式: ["host1:port1", "host2:port2"]
		Masters:            fs.option.Masters.GetInstancesAsStrings(),
		// 默认集合名称,用于逻辑分组文件
		Collection:         fs.option.Collection,
		// 默认副本策略,如 "000"(无副本) 或 "001"(同机架 1 副本)
		Replication:        fs.option.DefaultReplication,
		// 单个文件最大大小(MB),超过此大小会被分块
		MaxMb:              uint32(fs.option.MaxMB),
		// 目录桶路径,用于分布式元数据存储
		DirBuckets:         fs.filer.DirBucketsPath,
		// 是否启用加密存储
		Cipher:             fs.filer.Cipher,
		// JWT 签名密钥,用于访问控制
		Signature:          fs.filer.Signature,
		// Prometheus metrics 暴露地址
		MetricsAddress:     fs.metricsAddress,
		// Metrics 采集间隔(秒)
		MetricsIntervalSec: int32(fs.metricsIntervalSec),
		// SeaweedFS 完整版本号,如 "3.70"
		Version:            version.Version(),
		// Filer 组名,用于集群内 Filer 分组
		FilerGroup:         fs.option.FilerGroup,
		// 主版本号
		MajorVersion:       version.MAJOR_VERSION,
		// 次版本号
		MinorVersion:       version.MINOR_VERSION,
	}

	// 记录配置查询日志(仅在 verbose level 4 时输出)
	glog.V(4).InfofCtx(ctx, "GetFilerConfiguration: %v", t)

	return t, nil
}
