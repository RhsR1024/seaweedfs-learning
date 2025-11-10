// Package pb 提供了 SeaweedFS 的 gRPC 客户端和服务器的核心功能
// 包括连接管理、连接池、地址转换、以及各种服务客户端的封装
package pb

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/seaweedfs/seaweedfs/weed/util/request_id"
	"google.golang.org/grpc/metadata"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
)

const (
	// Max_Message_Size gRPC 消息的最大大小限制
	// 设置为 1GB，允许传输大型文件块和元数据
	Max_Message_Size = 1 << 30 // 1 GB
)

var (
	// grpcClients gRPC 连接池，缓存已建立的连接以便重用
	// key 为服务器地址，value 为带版本的连接对象
	// 连接池可以减少频繁建立/关闭连接的开销，提高性能
	grpcClients     = make(map[string]*versionedGrpcClient)
	grpcClientsLock sync.Mutex // 保护 grpcClients 的并发访问
)

// versionedGrpcClient 带版本信息的 gRPC 客户端连接
// 版本号用于检测连接是否已被替换，避免关闭错误的连接
type versionedGrpcClient struct {
	*grpc.ClientConn        // 嵌入 gRPC 连接对象
	version          int    // 连接版本号，每次创建新连接时生成随机值
	errCount         int    // 错误计数器，用于追踪连接的错误次数
}

// init 初始化 HTTP 传输层的连接池配置
// 增加每个主机的最大空闲连接数和总的最大空闲连接数
// 这对于高并发场景下的 HTTP 请求性能很重要
func init() {
	// 设置每个主机的最大空闲连接数为 1024
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 1024
	// 设置总的最大空闲连接数为 1024
	http.DefaultTransport.(*http.Transport).MaxIdleConns = 1024
}

// NewGrpcServer 创建一个配置好的 gRPC 服务器
// opts: 额外的服务器选项，会追加到默认配置之后
// 返回: 配置完成的 gRPC 服务器实例
//
// 默认配置包括:
// - KeepAlive 参数: 保持连接活跃，检测死连接
// - 消息大小限制: 支持最大 1GB 的消息
// - 请求 ID 拦截器: 自动处理请求追踪
func NewGrpcServer(opts ...grpc.ServerOption) *grpc.Server {
	var options []grpc.ServerOption
	options = append(options,
		// 配置服务端的 KeepAlive 参数
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    10 * time.Second, // 如果无活动，10秒后发送 ping
			Timeout: 20 * time.Second, // ping 超时时间
			// MaxConnectionAge: 10 * time.Hour, // 可选：连接最大存活时间
		}),
		// 配置 KeepAlive 强制策略，防止客户端过于频繁地发送 ping
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             60 * time.Second, // 客户端两次 ping 之间的最小间隔
			PermitWithoutStream: true,             // 允许在没有活动流时发送 ping
		}),
		// 设置接收消息的最大大小
		grpc.MaxRecvMsgSize(Max_Message_Size),
		// 设置发送消息的最大大小
		grpc.MaxSendMsgSize(Max_Message_Size),
		// 添加请求 ID 拦截器，用于请求追踪和日志关联
		grpc.UnaryInterceptor(requestIDUnaryInterceptor()),
	)
	// 追加用户提供的额外选项
	for _, opt := range opts {
		if opt != nil {
			options = append(options, opt)
		}
	}
	return grpc.NewServer(options...)
}

// GrpcDial 创建到指定地址的 gRPC 客户端连接
// ctx: 上下文，用于控制拨号过程
// address: 目标服务器地址，格式如 "127.0.0.1:18000"
// waitForReady: 是否等待连接就绪
//   - true: RPC 调用会等待连接建立后才执行
//   - false: 如果连接未就绪，RPC 调用会立即失败
// opts: 额外的拨号选项，如 TLS 配置
// 返回: gRPC 客户端连接和可能的错误
func GrpcDial(ctx context.Context, address string, waitForReady bool, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	// opts = append(opts, grpc.WithBlock())  // 已注释：阻塞直到连接建立
	// opts = append(opts, grpc.WithTimeout(time.Duration(5*time.Second)))  // 已注释：设置超时
	var options []grpc.DialOption

	options = append(options,
		// grpc.WithTransportCredentials(insecure.NewCredentials()),  // 已注释：使用不安全的连接
		// 设置默认的调用选项
		grpc.WithDefaultCallOptions(
			grpc.MaxCallSendMsgSize(Max_Message_Size), // 发送消息的最大大小
			grpc.MaxCallRecvMsgSize(Max_Message_Size), // 接收消息的最大大小
			grpc.WaitForReady(waitForReady),           // 是否等待连接就绪
		),
		// 配置客户端的 KeepAlive 参数
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                30 * time.Second, // 如果无活动，30秒后发送 ping
			Timeout:             20 * time.Second, // ping 超时时间
			PermitWithoutStream: true,             // 允许在没有活动流时发送 ping
		}))
	// 追加用户提供的额外选项
	for _, opt := range opts {
		if opt != nil {
			options = append(options, opt)
		}
	}
	return grpc.DialContext(ctx, address, options...)
}

// getOrCreateConnection 获取或创建到指定地址的 gRPC 连接
// 这是连接池的核心函数，实现了连接复用
// address: 服务器地址
// waitForReady: 是否等待连接就绪
// opts: 拨号选项
// 返回: 带版本信息的连接和可能的错误
//
// 工作流程:
// 1. 检查连接池中是否已存在到该地址的连接
// 2. 如果存在则直接返回（连接复用）
// 3. 如果不存在则创建新连接并加入连接池
func getOrCreateConnection(address string, waitForReady bool, opts ...grpc.DialOption) (*versionedGrpcClient, error) {

	grpcClientsLock.Lock()
	defer grpcClientsLock.Unlock()

	// 检查连接池中是否已有该地址的连接
	existingConnection, found := grpcClients[address]
	if found {
		return existingConnection, nil
	}

	// 连接不存在，创建新连接
	ctx := context.Background()
	grpcConnection, err := GrpcDial(ctx, address, waitForReady, opts...)
	if err != nil {
		return nil, fmt.Errorf("fail to dial %s: %v", address, err)
	}

	// 创建带版本信息的连接对象
	vgc := &versionedGrpcClient{
		grpcConnection,
		rand.Int(), // 生成随机版本号
		0,          // 初始错误计数为 0
	}
	// 将新连接加入连接池
	grpcClients[address] = vgc

	return vgc, nil
}

// requestIDUnaryInterceptor 创建一个请求 ID 拦截器
// 用于在 gRPC 服务端处理请求时自动生成和传播请求 ID
// 请求 ID 用于分布式追踪和日志关联，方便调试和问题排查
//
// 工作流程:
// 1. 从传入的元数据中提取请求 ID（如果客户端提供）
// 2. 如果没有请求 ID，则生成一个新的 UUID
// 3. 将请求 ID 注入到上下文和响应元数据中
// 4. 继续处理请求
func requestIDUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		// 从传入的元数据中获取请求 ID
		incomingMd, _ := metadata.FromIncomingContext(ctx)
		idList := incomingMd.Get(request_id.AmzRequestIDHeader)
		var reqID string
		if len(idList) > 0 {
			reqID = idList[0]
		}
		// 如果没有请求 ID，生成一个新的 UUID
		if reqID == "" {
			reqID = uuid.New().String()
		}

		// 创建包含请求 ID 的传出元数据
		ctx = metadata.NewOutgoingContext(ctx,
			metadata.New(map[string]string{
				request_id.AmzRequestIDHeader: reqID,
			}))

		// 将请求 ID 设置到上下文中，供后续处理使用
		ctx = request_id.Set(ctx, reqID)

		// 将请求 ID 添加到响应的 trailer 中，客户端可以获取
		grpc.SetTrailer(ctx, metadata.Pairs(request_id.AmzRequestIDHeader, reqID))

		// 继续处理请求
		return handler(ctx, req)
	}
}

// WithGrpcClient 使用 gRPC 客户端连接执行指定的函数
// 这是 SeaweedFS 中最核心的 gRPC 客户端管理函数
//
// 参数:
//   streamingMode: 流式模式标志
//     - true: 总是创建新的连接，用于长时间的流式调用
//     - false: 尝试重用连接池中的现有连接，用于普通 RPC 调用
//   signature: 客户端签名 ID，用于标识客户端（流式模式下有效）
//   fn: 回调函数，接收连接并执行具体的 RPC 操作
//   address: 服务器地址
//   waitForReady: 是否等待连接就绪
//   opts: 额外的拨号选项
//
// 返回: 执行过程中的错误
//
// 连接管理策略:
// - 非流式模式: 使用连接池，如果连接出现传输错误则自动清理并重建
// - 流式模式: 每次创建新连接，使用完后立即关闭
func WithGrpcClient(streamingMode bool, signature int32, fn func(*grpc.ClientConn) error, address string, waitForReady bool, opts ...grpc.DialOption) error {

	if !streamingMode {
		// 非流式模式: 使用连接池中的连接
		vgc, err := getOrCreateConnection(address, waitForReady, opts...)
		if err != nil {
			return fmt.Errorf("getOrCreateConnection %s: %v", address, err)
		}
		// 执行用户提供的函数
		executionErr := fn(vgc.ClientConn)
		if executionErr != nil {
			// 检查是否是传输层或连接错误
			if strings.Contains(executionErr.Error(), "transport") ||
				strings.Contains(executionErr.Error(), "connection closed") {
				// 清理失效的连接
				grpcClientsLock.Lock()
				if t, ok := grpcClients[address]; ok {
					// 只有当版本号匹配时才关闭连接
					// 避免关闭已被其他协程替换的新连接
					if t.version == vgc.version {
						vgc.Close()
						delete(grpcClients, address)
					}
				}
				grpcClientsLock.Unlock()
			}
		}
		return executionErr
	} else {
		// 流式模式: 创建新的连接
		ctx := context.Background()
		// 如果提供了签名，将其添加到元数据中
		if signature != 0 {
			md := metadata.New(map[string]string{"sw-client-id": fmt.Sprintf("%d", signature)})
			ctx = metadata.NewOutgoingContext(ctx, md)
		}
		// 创建新的 gRPC 连接
		grpcConnection, err := GrpcDial(ctx, address, waitForReady, opts...)
		if err != nil {
			return fmt.Errorf("fail to dial %s: %v", address, err)
		}
		// 确保函数返回时关闭连接
		defer grpcConnection.Close()
		// 执行用户提供的函数
		executionErr := fn(grpcConnection)
		if executionErr != nil {
			return executionErr
		}
		return nil
	}

}

// ParseServerAddress 解析服务器地址并调整端口号
// server: 原始服务器地址，格式如 "127.0.0.1:8080"
// deltaPort: 端口号偏移量，会加到原端口上
// 返回: 新的服务器地址和可能的错误
//
// 使用场景: 根据基础端口计算其他服务的端口
// 例如: HTTP 端口是 8080，gRPC 端口可能是 8080 + 10000 = 18080
func ParseServerAddress(server string, deltaPort int) (newServerAddress string, err error) {

	host, port, parseErr := hostAndPort(server)
	if parseErr != nil {
		return "", fmt.Errorf("server port parse error: %w", parseErr)
	}

	newPort := int(port) + deltaPort

	return util.JoinHostPort(host, newPort), nil
}

// hostAndPort 从地址字符串中提取主机和端口
// address: 地址字符串，格式如 "127.0.0.1:8080" 或 "localhost:9333"
// 返回: 主机名、端口号和可能的错误
//
// 实现细节:
// - 从右向左查找最后一个冒号，支持 IPv6 地址格式
// - 端口号必须是有效的 uint64 数字
func hostAndPort(address string) (host string, port uint64, err error) {
	colonIndex := strings.LastIndex(address, ":")
	if colonIndex < 0 {
		return "", 0, fmt.Errorf("server should have hostname:port format: %v", address)
	}
	port, err = strconv.ParseUint(address[colonIndex+1:], 10, 64)
	if err != nil {
		return "", 0, fmt.Errorf("server port parse error: %w", err)
	}

	return address[:colonIndex], port, err
}

// ServerToGrpcAddress 将服务器地址转换为 gRPC 地址
// server: HTTP 服务器地址，格式如 "127.0.0.1:8080"
// 返回: gRPC 服务器地址
//
// SeaweedFS 的端口约定:
// - HTTP 端口: 用户指定的端口（如 8080）
// - gRPC 端口: HTTP 端口 + 10000（如 18080）
// 这种约定简化了配置，只需指定一个端口即可
func ServerToGrpcAddress(server string) (serverGrpcAddress string) {

	host, port, parseErr := hostAndPort(server)
	if parseErr != nil {
		glog.Fatalf("server address %s parse error: %v", server, parseErr)
	}

	grpcPort := int(port) + 10000

	return util.JoinHostPort(host, grpcPort)
}

// GrpcAddressToServerAddress 将 gRPC 地址转换为服务器地址
// grpcAddress: gRPC 服务器地址，格式如 "127.0.0.1:18080"
// 返回: HTTP 服务器地址
//
// 这是 ServerToGrpcAddress 的反向操作
// gRPC 端口 - 10000 = HTTP 端口
func GrpcAddressToServerAddress(grpcAddress string) (serverAddress string) {
	host, grpcPort, parseErr := hostAndPort(grpcAddress)
	if parseErr != nil {
		glog.Fatalf("server grpc address %s parse error: %v", grpcAddress, parseErr)
	}

	port := int(grpcPort) - 10000

	return util.JoinHostPort(host, port)
}

// WithMasterClient 与 Master 服务器建立连接并执行操作
// streamingMode: 是否使用流式模式
// master: Master 服务器地址
// grpcDialOption: gRPC 拨号选项
// waitForReady: 是否等待连接就绪
// fn: 回调函数，接收 Master 客户端并执行具体操作
// 返回: 执行过程中的错误
func WithMasterClient(streamingMode bool, master ServerAddress, grpcDialOption grpc.DialOption, waitForReady bool, fn func(client master_pb.SeaweedClient) error) error {
	return WithGrpcClient(streamingMode, 0, func(grpcConnection *grpc.ClientConn) error {
		client := master_pb.NewSeaweedClient(grpcConnection)
		return fn(client)
	}, master.ToGrpcAddress(), waitForReady, grpcDialOption)

}

// WithVolumeServerClient 与 Volume 服务器建立连接并执行操作
// streamingMode: 是否使用流式模式
// volumeServer: Volume 服务器地址
// grpcDialOption: gRPC 拨号选项
// fn: 回调函数，接收 Volume 客户端并执行具体操作
// 返回: 执行过程中的错误
func WithVolumeServerClient(streamingMode bool, volumeServer ServerAddress, grpcDialOption grpc.DialOption, fn func(client volume_server_pb.VolumeServerClient) error) error {
	return WithGrpcClient(streamingMode, 0, func(grpcConnection *grpc.ClientConn) error {
		client := volume_server_pb.NewVolumeServerClient(grpcConnection)
		return fn(client)
	}, volumeServer.ToGrpcAddress(), false, grpcDialOption)

}

// WithOneOfGrpcMasterClients 尝试连接多个 Master 服务器中的一个
// 依次尝试每个 Master 地址，直到有一个成功为止
// streamingMode: 是否使用流式模式
// masterGrpcAddresses: Master 服务器地址映射表
// grpcDialOption: gRPC 拨号选项
// fn: 回调函数，接收 Master 客户端并执行具体操作
// 返回: 最后一次的错误（如果全部失败）
//
// 使用场景: 高可用场景下，有多个 Master 服务器，尝试连接任意一个可用的
func WithOneOfGrpcMasterClients(streamingMode bool, masterGrpcAddresses map[string]ServerAddress, grpcDialOption grpc.DialOption, fn func(client master_pb.SeaweedClient) error) (err error) {

	for _, masterGrpcAddress := range masterGrpcAddresses {
		err = WithGrpcClient(streamingMode, 0, func(grpcConnection *grpc.ClientConn) error {
			client := master_pb.NewSeaweedClient(grpcConnection)
			return fn(client)
		}, masterGrpcAddress.ToGrpcAddress(), false, grpcDialOption)
		if err == nil {
			return nil
		}
	}

	return err
}

// WithBrokerGrpcClient 与消息队列 Broker 服务器建立连接并执行操作
// streamingMode: 是否使用流式模式
// brokerGrpcAddress: Broker 的 gRPC 地址
// grpcDialOption: gRPC 拨号选项
// fn: 回调函数，接收消息队列客户端并执行具体操作
// 返回: 执行过程中的错误
//
// 使用场景: 与 SeaweedFS 的消息队列功能交互
func WithBrokerGrpcClient(streamingMode bool, brokerGrpcAddress string, grpcDialOption grpc.DialOption, fn func(client mq_pb.SeaweedMessagingClient) error) error {

	return WithGrpcClient(streamingMode, 0, func(grpcConnection *grpc.ClientConn) error {
		client := mq_pb.NewSeaweedMessagingClient(grpcConnection)
		return fn(client)
	}, brokerGrpcAddress, false, grpcDialOption)

}

// WithFilerClient 与 Filer 服务器建立连接并执行操作
// streamingMode: 是否使用流式模式
// signature: 客户端签名 ID
// filer: Filer 服务器地址
// grpcDialOption: gRPC 拨号选项
// fn: 回调函数，接收 Filer 客户端并执行具体操作
// 返回: 执行过程中的错误
//
// 这是 WithGrpcFilerClient 的别名，保持向后兼容
func WithFilerClient(streamingMode bool, signature int32, filer ServerAddress, grpcDialOption grpc.DialOption, fn func(client filer_pb.SeaweedFilerClient) error) error {

	return WithGrpcFilerClient(streamingMode, signature, filer, grpcDialOption, fn)

}

// WithGrpcFilerClient 与 Filer 服务器建立连接并执行操作
// streamingMode: 是否使用流式模式
// signature: 客户端签名 ID，用于标识客户端
// filerAddress: Filer 服务器地址
// grpcDialOption: gRPC 拨号选项
// fn: 回调函数，接收 Filer 客户端并执行具体操作
// 返回: 执行过程中的错误
//
// Filer 是 SeaweedFS 的文件系统层，提供目录结构和元数据管理
func WithGrpcFilerClient(streamingMode bool, signature int32, filerAddress ServerAddress, grpcDialOption grpc.DialOption, fn func(client filer_pb.SeaweedFilerClient) error) error {

	return WithGrpcClient(streamingMode, signature, func(grpcConnection *grpc.ClientConn) error {
		client := filer_pb.NewSeaweedFilerClient(grpcConnection)
		return fn(client)
	}, filerAddress.ToGrpcAddress(), false, grpcDialOption)

}

// WithOneOfGrpcFilerClients 尝试连接多个 Filer 服务器中的一个
// 依次尝试每个 Filer 地址，直到有一个成功为止
// streamingMode: 是否使用流式模式
// filerAddresses: Filer 服务器地址列表
// grpcDialOption: gRPC 拨号选项
// fn: 回调函数，接收 Filer 客户端并执行具体操作
// 返回: 最后一次的错误（如果全部失败）
//
// 使用场景: 高可用场景下，有多个 Filer 服务器，尝试连接任意一个可用的
func WithOneOfGrpcFilerClients(streamingMode bool, filerAddresses []ServerAddress, grpcDialOption grpc.DialOption, fn func(client filer_pb.SeaweedFilerClient) error) (err error) {

	for _, filerAddress := range filerAddresses {
		err = WithGrpcClient(streamingMode, 0, func(grpcConnection *grpc.ClientConn) error {
			client := filer_pb.NewSeaweedFilerClient(grpcConnection)
			return fn(client)
		}, filerAddress.ToGrpcAddress(), false, grpcDialOption)
		if err == nil {
			return nil
		}
	}

	return err
}

// WithWorkerClient 与 Worker 服务器建立连接并执行操作
// streamingMode: 是否使用流式模式
// workerAddress: Worker 服务器地址
// grpcDialOption: gRPC 拨号选项
// fn: 回调函数，接收 Worker 客户端并执行具体操作
// 返回: 执行过程中的错误
//
// Worker 用于执行后台任务和分布式计算
func WithWorkerClient(streamingMode bool, workerAddress string, grpcDialOption grpc.DialOption, fn func(client worker_pb.WorkerServiceClient) error) error {
	return WithGrpcClient(streamingMode, 0, func(grpcConnection *grpc.ClientConn) error {
		client := worker_pb.NewWorkerServiceClient(grpcConnection)
		return fn(client)
	}, workerAddress, false, grpcDialOption)
}
