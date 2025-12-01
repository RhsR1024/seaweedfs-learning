// Package topology 实现了 SeaweedFS 的拓扑管理功能
// 本文件负责卷的分配和删除操作，通过 gRPC 与 Volume Server 通信
package topology

import (
	"context"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"google.golang.org/grpc"
)

// AllocateVolumeResult 卷分配结果
// 用于返回卷分配操作的执行状态
type AllocateVolumeResult struct {
	// Error 错误信息，为空表示成功
	Error string
}

// AllocateVolume 在指定的数据节点上分配（创建）一个新的卷
// 这是 Master 向 Volume Server 发送创建卷命令的核心函数
//
// 执行流程：
//   1. 建立与 Volume Server 的 gRPC 连接
//   2. 发送 AllocateVolume RPC 请求
//   3. Volume Server 执行实际的卷创建操作：
//      - 创建 .dat 文件（数据文件）
//      - 创建 .idx 文件（索引文件）
//      - 写入 SuperBlock（卷元数据）
//      - 预分配磁盘空间（如果配置了 Preallocate）
//
// 参数:
//   - dn: 目标数据节点
//   - grpcDialOption: gRPC 连接选项（TLS、超时等）
//   - vid: 要创建的 Volume ID
//   - option: 卷增长选项（副本策略、TTL、磁盘类型等）
// 返回:
//   - error: 创建失败时的错误信息
func AllocateVolume(dn *DataNode, grpcDialOption grpc.DialOption, vid needle.VolumeId, option *VolumeGrowOption) error {

	// 使用 gRPC 客户端连接到 Volume Server
	// operation.WithVolumeServerClient 是一个工具函数，封装了连接建立和关闭逻辑
	return operation.WithVolumeServerClient(false, dn.ServerAddress(), grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {

		// 发送 AllocateVolume RPC 请求
		// 请求参数包含卷的所有配置信息
		_, allocateErr := client.AllocateVolume(context.Background(), &volume_server_pb.AllocateVolumeRequest{
			// VolumeId: 卷的唯一标识符（32 位无符号整数）
			VolumeId:           uint32(vid),
			// Collection: 集合名称，用于逻辑分组
			Collection:         option.Collection,
			// Replication: 副本策略（如 "001"、"010"、"100"）
			Replication:        option.ReplicaPlacement.String(),
			// Ttl: 生存时间（如 "3d"、"1h"、"30m"）
			Ttl:                option.Ttl.String(),
			// Preallocate: 预分配磁盘空间大小（字节）
			// 预分配可以减少文件碎片，提升写入性能
			Preallocate:        option.Preallocate,
			// MemoryMapMaxSizeMb: 内存映射最大大小（MB）
			// 用于 mmap 文件读取优化
			MemoryMapMaxSizeMb: option.MemoryMapMaxSizeMb,
			// DiskType: 磁盘类型（"hdd"、"ssd"、"nvme"）
			DiskType:           string(option.DiskType),
			// Version: 卷的版本号（v1/v2/v3）
			Version:            option.Version,
		})
		return allocateErr
	})

}

// DeleteVolume 删除指定数据节点上的卷
// 用于清理失败创建的卷或执行卷回收操作
//
// 执行流程：
//   1. 建立与 Volume Server 的 gRPC 连接
//   2. 发送 VolumeDelete RPC 请求
//   3. Volume Server 执行实际的卷删除操作：
//      - 删除 .dat 文件
//      - 删除 .idx 文件
//      - 删除 .cpd/.cpx 文件（如果存在压缩）
//      - 删除 .ecx/.ec00~.ec13 文件（如果存在纠删码）
//
// 注意事项：
//   - 删除操作不可逆，数据将永久丢失
//   - 通常只在以下场景使用：
//     1. 卷创建失败需要回滚
//     2. 卷迁移后清理旧副本
//     3. 卷回收（vacuum）后删除空卷
//
// 参数:
//   - dn: 目标数据节点
//   - grpcDialOption: gRPC 连接选项
//   - vid: 要删除的 Volume ID
// 返回:
//   - error: 删除失败时的错误信息
func DeleteVolume(dn *DataNode, grpcDialOption grpc.DialOption, vid needle.VolumeId) error {

	// 使用 gRPC 客户端连接到 Volume Server
	return operation.WithVolumeServerClient(false, dn.ServerAddress(), grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {

		// 发送 VolumeDelete RPC 请求
		_, allocateErr := client.VolumeDelete(context.Background(), &volume_server_pb.VolumeDeleteRequest{
			// VolumeId: 要删除的卷 ID
			VolumeId: uint32(vid),
		})
		return allocateErr
	})

}
