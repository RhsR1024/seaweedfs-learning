// Package storage 实现 SeaweedFS 的 EC 删除操作
// 本文件包含 EC Volume 的 Needle 删除逻辑
//
// EC 删除策略:
//   与普通 Volume 不同，EC Volume 的删除需要：
//   1. 验证 Cookie（确保删除权限）
//   2. 删除至少一个分片中的 Needle（标记删除）
//   3. 尝试删除所有校验分片中的 Needle（提高删除可靠性）
//
// 删除可靠性:
//   - 数据分片：只需删除一个即可（读取时会检测到删除标记）
//   - 校验分片：尽可能全部删除（避免恢复时重现已删除数据）
//
// 工作原理:
//   EC Volume 的删除实际上是在 .ecx 索引中标记为已删除
//   原始数据仍在分片文件中，但读取时会返回 ErrorDeleted
package storage

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// DeleteEcShardNeedle 删除 EC Volume 中的 Needle
// 这是 EC 存储的核心删除函数，确保删除操作的安全性和可靠性
//
// 参数:
//   - ecVolume: EC Volume 对象
//   - n: Needle 对象（需要设置 Id 和 Cookie）
//   - cookie: 删除令牌（用于验证删除权限）
//
// 返回值:
//   - int64: 删除的字节数
//   - error: 删除错误
//
// 删除流程:
//  1. 【读取验证】先读取 Needle，确保其存在
//  2. 【权限验证】比对 Cookie，确保有删除权限
//  3. 【执行删除】删除至少一个数据分片中的 Needle
//  4. 【清理校验分片】尝试删除所有校验分片中的 Needle
//
// 权限验证:
//   Cookie 是上传时生成的随机令牌，只有持有正确 Cookie 的客户端才能删除
//   这防止了未授权的删除操作
//
// 删除可靠性策略:
//   - 至少一个成功：只要有一个分片删除成功，整个操作就成功
//   - 尽力而为：尝试删除所有校验分片，但部分失败不影响结果
//   - 防止恢复：删除校验分片避免 Reed-Solomon 恢复出已删除的数据
//
// 错误处理:
//   - Cookie 不匹配：返回权限错误
//   - Needle 不存在：返回 ErrorNotFound
//   - 所有分片删除失败：返回最后一个错误
//
// 注意:
//   删除操作是标记删除，不立即释放磁盘空间
//   空间回收需要等待 Volume 压缩（Compaction）
func (s *Store) DeleteEcShardNeedle(ecVolume *erasure_coding.EcVolume, n *needle.Needle, cookie types.Cookie) (int64, error) {

	count, err := s.ReadEcShardNeedle(ecVolume.VolumeId, n, nil)

	if err != nil {
		return 0, err
	}

	if cookie != n.Cookie {
		return 0, fmt.Errorf("unexpected cookie %x", cookie)
	}

	if err = s.doDeleteNeedleFromAtLeastOneRemoteEcShards(ecVolume, n.Id); err != nil {
		return 0, err
	}

	return int64(count), nil

}

// doDeleteNeedleFromAtLeastOneRemoteEcShards 从至少一个 EC 分片中删除 Needle
// 实现"至少一个成功"的删除策略，确保删除的可靠性
//
// 参数:
//   - ecVolume: EC Volume 对象
//   - needleId: 要删除的 Needle ID
//
// 返回值:
//   - error: 所有分片删除都失败时返回错误
//
// 删除策略:
//  1. 【数据分片】从 Needle 所在的第一个数据分片删除
//  2. 【校验分片】遍历所有校验分片（ID 10-13），尝试删除
//  3. 【成功判定】只要有一个分片删除成功，整个操作成功
//
// 为什么删除校验分片:
//   如果只删除数据分片，Reed-Solomon 恢复可能从校验分片恢复出已删除的数据
//   删除校验分片可以防止这种情况发生
//
// 容错机制:
//   - 部分分片删除失败不影响整体结果
//   - 至少一个成功即可保证读取时返回 ErrorDeleted
//   - 网络故障或节点宕机不会导致删除失败
//
// 注意:
//   此函数不删除所有数据分片，只删除第一个和所有校验分片
func (s *Store) doDeleteNeedleFromAtLeastOneRemoteEcShards(ecVolume *erasure_coding.EcVolume, needleId types.NeedleId) error {

	_, _, intervals, err := ecVolume.LocateEcShardNeedle(needleId, ecVolume.Version)
	if err != nil {
		return err
	}
	if len(intervals) == 0 {
		return erasure_coding.NotFoundError
	}

	shardId, _ := intervals[0].ToShardIdAndOffset(erasure_coding.ErasureCodingLargeBlockSize, erasure_coding.ErasureCodingSmallBlockSize)

	hasDeletionSuccess := false
	err = s.doDeleteNeedleFromRemoteEcShardServers(shardId, ecVolume, needleId)
	if err == nil {
		hasDeletionSuccess = true
	}

	for shardId = erasure_coding.DataShardsCount; shardId < erasure_coding.TotalShardsCount; shardId++ {
		if parityDeletionError := s.doDeleteNeedleFromRemoteEcShardServers(shardId, ecVolume, needleId); parityDeletionError == nil {
			hasDeletionSuccess = true
		}
	}

	if hasDeletionSuccess {
		return nil
	}

	return err

}

// doDeleteNeedleFromRemoteEcShardServers 从指定分片的所有副本节点删除 Needle
// 遍历分片的所有位置，逐个发送删除请求
//
// 参数:
//   - shardId: 要删除的分片 ID
//   - ecVolume: EC Volume 对象
//   - needleId: Needle ID
//
// 返回值:
//   - error: 分片位置未知或删除失败
//
// 工作流程:
//  1. 获取分片的所有副本位置（ShardLocations）
//  2. 遍历每个位置，发送 gRPC 删除请求
//  3. 任何一个失败都返回错误
//
// 注意:
//   - 同一分片可能有多个副本（分布在不同节点）
//   - 需要删除所有副本以确保彻底删除
func (s *Store) doDeleteNeedleFromRemoteEcShardServers(shardId erasure_coding.ShardId, ecVolume *erasure_coding.EcVolume, needleId types.NeedleId) error {

	ecVolume.ShardLocationsLock.RLock()
	sourceDataNodes, hasShardLocations := ecVolume.ShardLocations[shardId]
	ecVolume.ShardLocationsLock.RUnlock()

	if !hasShardLocations {
		return fmt.Errorf("ec shard %d.%d not located", ecVolume.VolumeId, shardId)
	}

	for _, sourceDataNode := range sourceDataNodes {
		glog.V(4).Infof("delete from remote ec shard %d.%d from %s", ecVolume.VolumeId, shardId, sourceDataNode)
		err := s.doDeleteNeedleFromRemoteEcShard(sourceDataNode, ecVolume.VolumeId, ecVolume.Collection, ecVolume.Version, needleId)
		if err != nil {
			return err
		}
		glog.V(1).Infof("delete from remote ec shard %d.%d from %s: %v", ecVolume.VolumeId, shardId, sourceDataNode, err)
	}

	return nil

}

// doDeleteNeedleFromRemoteEcShard 通过 gRPC 从远程 EC 分片删除 Needle
// 这是最底层的删除操作，直接调用远程 Volume Server 的 API
//
// 参数:
//   - sourceDataNode: 远程 Volume Server 地址
//   - vid: Volume ID
//   - collection: Collection 名称
//   - version: Needle 版本
//   - needleId: Needle ID
//
// 返回值:
//   - error: gRPC 调用失败或删除操作失败
//
// gRPC 协议:
//   调用 VolumeEcBlobDelete API:
//   - VolumeId: 目标 Volume
//   - Collection: Collection 名称
//   - FileKey: Needle ID（uint64）
//   - Version: 用于正确解析 Needle 格式
//
// 远程操作:
//   远程节点会在其本地 .ecx 索引中标记 Needle 为已删除
//   下次读取该 Needle 时将返回 ErrorDeleted
func (s *Store) doDeleteNeedleFromRemoteEcShard(sourceDataNode pb.ServerAddress, vid needle.VolumeId, collection string, version needle.Version, needleId types.NeedleId) error {

	return operation.WithVolumeServerClient(false, sourceDataNode, s.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {

		// copy data slice
		_, err := client.VolumeEcBlobDelete(context.Background(), &volume_server_pb.VolumeEcBlobDeleteRequest{
			VolumeId:   uint32(vid),
			Collection: collection,
			FileKey:    uint64(needleId),
			Version:    uint32(version),
		})
		if err != nil {
			return fmt.Errorf("failed to delete from ec shard %d on %s: %v", vid, sourceDataNode, err)
		}
		return nil
	})

}
