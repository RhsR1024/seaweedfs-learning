// Package operation 提供了与SeaweedFS各个服务通信的gRPC客户端封装
// 本文件定义了与Volume服务器和Master服务器交互的辅助函数
package operation

import (
	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

// WithVolumeServerClient 与Volume服务器建立gRPC连接并执行指定的操作
// 这是一个辅助函数，封装了连接建立、客户端创建、错误处理和连接关闭的逻辑
//
// 参数:
//   streamingMode: 是否使用流式模式
//     - true: 用于长时间运行的流式RPC调用（如StreamRead、StreamWrite）
//     - false: 用于普通的请求-响应模式
//   volumeServer: Volume服务器的地址，格式如 "127.0.0.1:8080"
//   grpcDialOption: gRPC拨号选项，包含TLS配置、超时设置等
//   fn: 回调函数，接收VolumeServerClient并执行具体的RPC操作
//
// 返回: 如果连接建立或RPC调用失败，返回错误；否则返回nil
//
// 使用示例:
//   err := WithVolumeServerClient(false, serverAddr, grpcOption, func(client volume_server_pb.VolumeServerClient) error {
//       resp, err := client.VacuumVolumeCheck(ctx, req)
//       return err
//   })
func WithVolumeServerClient(streamingMode bool, volumeServer pb.ServerAddress, grpcDialOption grpc.DialOption, fn func(volume_server_pb.VolumeServerClient) error) error {

	// 调用底层的WithGrpcClient函数来管理gRPC连接生命周期
	return pb.WithGrpcClient(streamingMode, 0, func(grpcConnection *grpc.ClientConn) error {
		// 使用建立的连接创建VolumeServer客户端
		client := volume_server_pb.NewVolumeServerClient(grpcConnection)
		// 执行用户提供的回调函数，传入客户端
		return fn(client)
	}, volumeServer.ToGrpcAddress(), false, grpcDialOption)

}

// WithMasterServerClient 与Master服务器建立gRPC连接并执行指定的操作
// 这是一个辅助函数，封装了与Master服务器交互的底层细节
//
// 参数:
//   streamingMode: 是否使用流式模式
//     - true: 用于长时间运行的流式RPC调用（如StreamAssign、KeepConnected）
//     - false: 用于普通的请求-响应模式（如Assign、LookupVolume、GetMasterConfiguration）
//   masterServer: Master服务器的地址，格式如 "127.0.0.1:9333"
//   grpcDialOption: gRPC拨号选项，包含TLS配置、超时设置等
//   fn: 回调函数，接收SeaweedClient（Master客户端）并执行具体的RPC操作
//
// 返回: 如果连接建立或RPC调用失败，返回错误；否则返回nil
//
// 使用示例:
//   err := WithMasterServerClient(false, masterAddr, grpcOption, func(client master_pb.SeaweedClient) error {
//       resp, err := client.Assign(ctx, &master_pb.AssignRequest{Count: 1})
//       return err
//   })
//
// 设计模式:
// - 资源管理模式（RAII）: 自动管理gRPC连接的打开和关闭
// - 回调模式: 通过函数参数传递具体的业务逻辑
// - 这种设计避免了重复的连接管理代码，使调用方可以专注于业务逻辑
func WithMasterServerClient(streamingMode bool, masterServer pb.ServerAddress, grpcDialOption grpc.DialOption, fn func(masterClient master_pb.SeaweedClient) error) error {

	// 调用底层的WithGrpcClient函数来管理gRPC连接生命周期
	// 参数说明:
	//   - streamingMode: 传递流式模式标志
	//   - 0: maxIdleTime，0表示使用默认值
	//   - 回调函数: 创建Master客户端并执行用户逻辑
	//   - masterServer.ToGrpcAddress(): 将服务器地址转换为gRPC格式
	//   - false: appendToClientName，是否在客户端名称后添加后缀
	//   - grpcDialOption: gRPC拨号选项
	return pb.WithGrpcClient(streamingMode, 0, func(grpcConnection *grpc.ClientConn) error {
		// 使用建立的连接创建Master（Seaweed）客户端
		client := master_pb.NewSeaweedClient(grpcConnection)
		// 执行用户提供的回调函数，传入客户端
		return fn(client)
	}, masterServer.ToGrpcAddress(), false, grpcDialOption)

}
