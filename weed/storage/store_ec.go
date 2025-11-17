// Package storage 实现 SeaweedFS 的 Erasure Coding (纠删码) 存储功能
// 本文件包含 Store 层的 EC 操作，支持分片管理、数据恢复和远程读取
//
// EC 架构说明:
//   SeaweedFS 使用 Reed-Solomon 纠删码算法实现数据冗余和恢复
//   - 默认配置：10 个数据分片 + 4 个校验分片 = 14 个总分片
//   - 数据保护：最多可丢失 4 个分片，仍能恢复完整数据
//   - 存储效率：相比 3 副本 (33% 利用率)，EC 可达 71% 利用率 (10/14)
//
// EC Volume 组成:
//   - .ecx 索引文件：记录每个 Needle 在哪些分片中的位置
//   - .ec00 ~ .ec13：14 个数据/校验分片文件
//   - 分片分布：可分散在多个 Volume Server 上，提高可用性
//
// 读取策略:
//  1. 优先读取本地分片（零网络开销）
//  2. 本地不存在时，从远程节点读取
//  3. 远程节点失败时，使用 Reed-Solomon 算法从其他分片恢复
//
// 性能优化:
//   - 分片位置缓存：减少 Master 查询
//   - 并发恢复：同时从多个节点读取分片进行恢复
//   - 间隔读取：对于跨多个分片的大文件，分段读取
package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/klauspost/reedsolomon"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// CollectErasureCodingHeartbeat 收集 EC 分片信息，用于心跳上报到 Master
// 这是 Volume Server 向 Master 定期报告自己持有的 EC 分片的关键函数
//
// 返回值:
//   - *master_pb.Heartbeat: 包含所有 EC 分片信息的心跳消息
//
// 工作流程:
//  1. 遍历所有磁盘位置 (Locations)
//  2. 收集每个位置的 EC 分片信息
//  3. 统计每个 Collection 的 EC 分片总大小
//  4. 更新 Prometheus 监控指标
//  5. 返回心跳消息给 Master
//
// 心跳消息包含:
//   - EcShards: EC 分片列表（VolumeId、ShardId、DiskType、ExpireAtSec）
//   - HasNoEcShards: 是否没有 EC 分片（用于优化 Master 处理）
//
// 用途:
//   - Master 了解整个集群的 EC 分片分布
//   - 用于客户端查询 EC Volume 的分片位置
//   - 监控和负载均衡的数据基础
func (s *Store) CollectErasureCodingHeartbeat() *master_pb.Heartbeat {
	var ecShardMessages []*master_pb.VolumeEcShardInformationMessage
	collectionEcShardSize := make(map[string]int64)
	for diskId, location := range s.Locations {
		location.ecVolumesLock.RLock()
		for _, ecShards := range location.ecVolumes {
			ecShardMessages = append(ecShardMessages, ecShards.ToVolumeEcShardInformationMessage(uint32(diskId))...)

			for _, ecShard := range ecShards.Shards {
				collectionEcShardSize[ecShards.Collection] += ecShard.Size()
			}
		}
		location.ecVolumesLock.RUnlock()
	}

	for col, size := range collectionEcShardSize {
		stats.VolumeServerDiskSizeGauge.WithLabelValues(col, "ec").Set(float64(size))
	}

	return &master_pb.Heartbeat{
		EcShards:      ecShardMessages,
		HasNoEcShards: len(ecShardMessages) == 0,
	}

}

// MountEcShards 挂载（加载）指定的 EC 分片到内存
// 当新的 EC 分片文件被复制到本地时，需要调用此函数将其加载到内存中
//
// 参数:
//   - collection: Collection 名称
//   - vid: Volume ID
//   - shardId: 分片 ID (0-13，其中 0-9 是数据分片，10-13 是校验分片)
//
// 返回值:
//   - error: 加载失败时的错误信息
//
// 工作流程:
//  1. 遍历所有磁盘位置，尝试加载分片文件
//  2. 找到分片后，加载到内存（LoadEcShard）
//  3. 通过 NewEcShardsChan 通知 Master 有新分片
//  4. Master 更新全局的 EC 分片位置映射
//
// 用途:
//   - EC Volume 迁移：从其他节点复制分片后挂载
//   - 启动时恢复：重新加载磁盘上的分片文件
//   - 动态添加：在运行时添加新的 EC 分片
//
// 注意:
//   挂载后需要通知 Master，以便客户端能查询到新的分片位置
func (s *Store) MountEcShards(collection string, vid needle.VolumeId, shardId erasure_coding.ShardId) error {
	for diskId, location := range s.Locations {
		if ecVolume, err := location.LoadEcShard(collection, vid, shardId); err == nil {
			glog.V(0).Infof("MountEcShards %d.%d on disk ID %d", vid, shardId, diskId)

			var shardBits erasure_coding.ShardBits

			s.NewEcShardsChan <- master_pb.VolumeEcShardInformationMessage{
				Id:          uint32(vid),
				Collection:  collection,
				EcIndexBits: uint32(shardBits.AddShardId(shardId)),
				DiskType:    string(location.DiskType),
				ExpireAtSec: ecVolume.ExpireAtSec,
				DiskId:      uint32(diskId),
			}
			return nil
		} else if err == os.ErrNotExist {
			continue
		} else {
			return fmt.Errorf("%s load ec shard %d.%d: %v", location.Directory, vid, shardId, err)
		}
	}

	return fmt.Errorf("MountEcShards %d.%d not found on disk", vid, shardId)
}

// UnmountEcShards 卸载指定的 EC 分片from内存
// 当需要删除或迁移 EC 分片时，先卸载内存中的数据结构
//
// 参数:
//   - vid: Volume ID
//   - shardId: 分片 ID
//
// 返回值:
//   - error: 卸载失败时的错误信息
//
// 工作流程:
//  1. 查找分片所在的磁盘位置 (findEcShard)
//  2. 从对应的 Location 卸载分片 (UnloadEcShard)
//  3. 通过 DeletedEcShardsChan 通知 Master 分片已删除
//  4. Master 更新全局映射，移除该分片位置
//
// 用途:
//   - EC Volume 迁移：迁出分片前卸载
//   - 空间清理：删除不再需要的分片
//   - 维护操作：临时卸载分片进行修复
//
// 注意:
//   卸载后需要通知 Master，以便更新全局的分片位置信息
func (s *Store) UnmountEcShards(vid needle.VolumeId, shardId erasure_coding.ShardId) error {

	diskId, ecShard, found := s.findEcShard(vid, shardId)
	if !found {
		return nil
	}

	var shardBits erasure_coding.ShardBits
	message := master_pb.VolumeEcShardInformationMessage{
		Id:          uint32(vid),
		Collection:  ecShard.Collection,
		EcIndexBits: uint32(shardBits.AddShardId(shardId)),
		DiskType:    string(ecShard.DiskType),
		DiskId:      diskId,
	}

	location := s.Locations[diskId]

	if deleted := location.UnloadEcShard(vid, shardId); deleted {
		glog.V(0).Infof("UnmountEcShards %d.%d", vid, shardId)
		s.DeletedEcShardsChan <- message
		return nil
	}

	return fmt.Errorf("UnmountEcShards %d.%d not found on disk", vid, shardId)
}

// findEcShard 在所有磁盘位置中查找指定的 EC 分片
// 这是一个内部辅助函数，用于定位分片所在的物理位置
//
// 参数:
//   - vid: Volume ID
//   - shardId: 分片 ID
//
// 返回值:
//   - diskId: 分片所在的磁盘 ID
//   - shard: EC 分片对象
//   - found: 是否找到分片
//
// 注意:
//   - 仅搜索本地磁盘，不查询远程节点
//   - O(n) 时间复杂度，n 为磁盘数量
func (s *Store) findEcShard(vid needle.VolumeId, shardId erasure_coding.ShardId) (diskId uint32, shard *erasure_coding.EcVolumeShard, found bool) {
	for diskId, location := range s.Locations {
		if v, found := location.FindEcShard(vid, shardId); found {
			return uint32(diskId), v, found
		}
	}
	return 0, nil, false
}

// FindEcVolume 在所有磁盘位置中查找指定的 EC Volume
// EC Volume 包含该卷的所有分片和索引文件
//
// 参数:
//   - vid: Volume ID
//
// 返回值:
//   - *erasure_coding.EcVolume: EC Volume 对象（包含分片集合）
//   - bool: 是否找到
//
// 注意:
//   即使只有部分分片在本地，也会返回 EC Volume 对象
func (s *Store) FindEcVolume(vid needle.VolumeId) (*erasure_coding.EcVolume, bool) {
	for _, location := range s.Locations {
		if s, found := location.FindEcVolume(vid); found {
			return s, true
		}
	}
	return nil, false
}

// CollectEcShards 收集指定 EC Volume 的所有分片文件名
// 用于获取 Volume 的完整分片列表，包括本地和远程分片
//
// 参数:
//   - vid: Volume ID
//   - shardFileNames: 分片文件名列表（用于返回分片位置）
//
// 返回值:
//   - ecVolume: EC Volume 对象
//   - found: 是否找到任何分片
//
// 用途:
//   - 数据恢复：查找分散在不同位置的分片
//   - 完整性检查：验证是否有足够的分片进行恢复
func (s *Store) CollectEcShards(vid needle.VolumeId, shardFileNames []string) (ecVolume *erasure_coding.EcVolume, found bool) {
	for _, location := range s.Locations {
		if s, foundShards := location.CollectEcShards(vid, shardFileNames); foundShards {
			ecVolume = s
			found = true
		}
	}
	return
}

// DestroyEcVolume 销毁（删除）指定的 EC Volume 及其所有分片
// 这是一个危险操作，会永久删除数据
//
// 参数:
//   - vid: Volume ID
//
// 工作流程:
//  1. 遍历所有磁盘位置
//  2. 删除该 Volume 的所有分片文件（.ec00 ~ .ec13）
//  3. 删除索引文件（.ecx）
//  4. 从内存中移除 EC Volume 对象
//
// 注意:
//   - 此操作不可恢复
//   - 调用前应确保已通知 Master 更新映射
//   - 通常在 Volume 完全废弃时使用
func (s *Store) DestroyEcVolume(vid needle.VolumeId) {
	for _, location := range s.Locations {
		location.DestroyEcVolume(vid)
	}
}

// ReadEcShardNeedle 读取 EC Volume 中的 Needle 数据
// 这是 EC 存储的核心读取函数，实现了智能的数据恢复机制
//
// 参数:
//   - vid: Volume ID
//   - n: Needle 对象（需要设置 Id 字段）
//   - onReadSizeFn: 读取大小回调函数（用于流量控制）
//
// 返回值:
//   - int: 实际读取的字节数
//   - error: 读取错误
//
// 读取策略（三级回退）:
//  1. 【本地读取】优先从本地分片读取（零网络开销）
//  2. 【远程读取】本地不存在时，从远程节点直接读取
//  3. 【数据恢复】远程失败时，使用 Reed-Solomon 算法从其他分片恢复
//
// 工作流程:
//  1. 从 .ecx 索引文件定位 Needle 位置（LocateEcShardNeedle）
//  2. 计算 Needle 跨越的分片和偏移（intervals）
//  3. 对每个 interval，按策略读取数据
//  4. 拼接所有 intervals 的数据
//  5. 解析为完整的 Needle 对象
//
// Intervals 说明:
//   大文件可能跨越多个分片，例如：
//   - Needle 从分片 3 的 offset 1000 开始
//   - 跨越分片 3、4、5
//   - intervals = [(shard:3, offset:1000, size:8MB), (shard:4, offset:0, size:8MB), ...]
//
// 错误处理:
//   - ErrorDeleted: Needle 已被删除
//   - ErrorNotFound: EC Volume 或 Needle 不存在
//   - 网络错误: 自动尝试从其他分片恢复
//
// 示例:
//   n := &needle.Needle{Id: needleId}
//   count, err := store.ReadEcShardNeedle(volumeId, n, nil)
//   if err == nil {
//       // n.Data 包含文件数据
//       // n.Mime 包含 MIME 类型
//   }
func (s *Store) ReadEcShardNeedle(vid needle.VolumeId, n *needle.Needle, onReadSizeFn func(size types.Size)) (int, error) {
	for _, location := range s.Locations {
		if localEcVolume, found := location.FindEcVolume(vid); found {

			offset, size, intervals, err := localEcVolume.LocateEcShardNeedle(n.Id, localEcVolume.Version)
			if err != nil {
				return 0, fmt.Errorf("locate in local ec volume: %w", err)
			}
			if size.IsDeleted() {
				return 0, ErrorDeleted
			}

			if onReadSizeFn != nil {
				onReadSizeFn(size)
			}

			glog.V(3).Infof("read ec volume %d offset %d size %d intervals:%+v", vid, offset.ToActualOffset(), size, intervals)

			if len(intervals) > 1 {
				glog.V(3).Infof("ReadEcShardNeedle needle id %s intervals:%+v", n.String(), intervals)
			}
			bytes, isDeleted, err := s.readEcShardIntervals(vid, n.Id, localEcVolume, intervals)
			if err != nil {
				return 0, fmt.Errorf("ReadEcShardIntervals: %w", err)
			}
			if isDeleted {
				return 0, ErrorDeleted
			}

			err = n.ReadBytes(bytes, offset.ToActualOffset(), size, localEcVolume.Version)
			if err != nil {
				return 0, fmt.Errorf("readbytes: %w", err)
			}

			return len(bytes), nil
		}
	}
	return 0, fmt.Errorf("ec shard %d not found", vid)
}

// readEcShardIntervals 读取 Needle 的多个 interval 数据并拼接
// 处理跨多个分片的大文件，依次读取每个 interval 并合并
//
// 参数:
//   - vid: Volume ID
//   - needleId: Needle ID
//   - ecVolume: EC Volume 对象
//   - intervals: Needle 数据的区间列表（可能跨多个分片）
//
// 返回值:
//   - data: 完整的 Needle 原始数据
//   - is_deleted: 是否已删除
//   - err: 读取错误
//
// 工作原理:
//   每个 interval 可能在不同的分片中，需要：
//   1. 缓存分片位置（避免重复查询 Master）
//   2. 依次读取每个 interval
//   3. 拼接为完整数据
//
// 注意:
//   如果任何一个 interval 读取失败，整个操作失败
func (s *Store) readEcShardIntervals(vid needle.VolumeId, needleId types.NeedleId, ecVolume *erasure_coding.EcVolume, intervals []erasure_coding.Interval) (data []byte, is_deleted bool, err error) {

	if err = s.cachedLookupEcShardLocations(ecVolume); err != nil {
		return nil, false, fmt.Errorf("failed to locate shard via master grpc %s: %v", s.MasterAddress, err)
	}

	for i, interval := range intervals {
		if d, isDeleted, e := s.readOneEcShardInterval(needleId, ecVolume, interval); e != nil {
			return nil, isDeleted, e
		} else {
			if isDeleted {
				is_deleted = true
			}
			if i == 0 {
				data = d
			} else {
				data = append(data, d...)
			}
		}
	}
	return
}

// readOneEcShardInterval 读取单个 interval 的数据（智能三级回退）
// 这是 EC 读取的核心算法，实现了本地 → 远程 → 恢复的智能策略
//
// 参数:
//   - needleId: Needle ID
//   - ecVolume: EC Volume 对象
//   - interval: 数据区间（shardId、offset、size）
//
// 返回值:
//   - data: 区间数据
//   - is_deleted: 是否已删除
//   - err: 读取错误
//
// 三级读取策略:
//
//	【级别 1】本地分片读取（最快，零网络开销）
//	  if 分片在本地:
//	      直接从本地文件读取
//	      return 数据
//
//	【级别 2】远程直接读取（中等速度，网络传输）
//	  if 分片位置已缓存:
//	      gRPC 调用远程节点读取分片数据
//	      if 成功: return 数据
//	      if 失败: 清除该分片位置缓存，进入级别 3
//
//	【级别 3】Reed-Solomon 恢复（最慢，但可靠）
//	  并发从其他分片读取数据
//	  使用 Reed-Solomon 算法恢复目标分片
//	  return 恢复的数据
//
// Reed-Solomon 恢复原理:
//   - 需要至少 DataShardsCount (10) 个分片
//   - 可以从任意 10 个分片恢复缺失的分片
//   - 例如：有分片 0,1,2,3,4,5,6,7,8,11 → 可恢复分片 9
//
// 性能特点:
//   - 本地读取：<1ms
//   - 远程读取：10-50ms（取决于网络）
//   - 数据恢复：100-500ms（需要读取多个分片并计算）
//
// 错误处理:
//   - 远程节点失败：自动切换到恢复模式
//   - 恢复失败：返回错误（分片数量不足）
func (s *Store) readOneEcShardInterval(needleId types.NeedleId, ecVolume *erasure_coding.EcVolume, interval erasure_coding.Interval) (data []byte, is_deleted bool, err error) {
	shardId, actualOffset := interval.ToShardIdAndOffset(erasure_coding.ErasureCodingLargeBlockSize, erasure_coding.ErasureCodingSmallBlockSize)
	data = make([]byte, interval.Size)
	if shard, found := ecVolume.FindEcVolumeShard(shardId); found {
		var readSize int
		if readSize, err = shard.ReadAt(data, actualOffset); err != nil {
			if readSize != int(interval.Size) {
				glog.V(0).Infof("read local ec shard %d.%d offset %d: %v", ecVolume.VolumeId, shardId, actualOffset, err)
				return
			}
		}
	} else {
		ecVolume.ShardLocationsLock.RLock()
		sourceDataNodes, hasShardIdLocation := ecVolume.ShardLocations[shardId]
		ecVolume.ShardLocationsLock.RUnlock()

		// try reading directly
		if hasShardIdLocation {
			_, is_deleted, err = s.readRemoteEcShardInterval(sourceDataNodes, needleId, ecVolume.VolumeId, shardId, data, actualOffset)
			if err == nil {
				return
			}
			glog.V(0).Infof("clearing ec shard %d.%d locations: %v", ecVolume.VolumeId, shardId, err)
		}

		// try reading by recovering from other shards
		_, is_deleted, err = s.recoverOneRemoteEcShardInterval(needleId, ecVolume, shardId, data, actualOffset)
		if err == nil {
			return
		}
		glog.V(0).Infof("recover ec shard %d.%d : %v", ecVolume.VolumeId, shardId, err)
	}
	return
}

// forgetShardId 清除 EC Volume 中特定分片的位置缓存
// 当远程访问分片失败时调用，强制下次重新查询 Master
//
// 参数:
//   - ecVolume: EC Volume 对象
//   - shardId: 要清除的分片 ID
//
// 用途:
//   - 处理节点故障：节点宕机后清除其分片位置
//   - 网络故障恢复：临时网络问题后重新定位
//   - 确保最终一致性：下次读取时获取最新位置
func forgetShardId(ecVolume *erasure_coding.EcVolume, shardId erasure_coding.ShardId) {
	// failed to access the source data nodes, clear it up
	ecVolume.ShardLocationsLock.Lock()
	delete(ecVolume.ShardLocations, shardId)
	ecVolume.ShardLocationsLock.Unlock()
}

// cachedLookupEcShardLocations 缓存查询 EC 分片位置（智能刷新策略）
// 从 Master 查询分片位置并缓存，采用差异化的缓存过期时间
//
// 参数:
//   - ecVolume: EC Volume 对象
//
// 返回值:
//   - error: 查询失败时的错误
//
// 智能缓存策略:
//
//	【策略 1】分片数量 < 10（数据不完整）
//	  缓存时间：11 秒（频繁刷新，快速发现新分片）
//	  场景：Volume 正在迁移，分片陆续到达
//
//	【策略 2】分片数量 == 14（所有分片齐全）
//	  缓存时间：37 分钟（长期缓存，减少 Master 负载）
//	  场景：稳定运行，分片位置很少变化
//
//	【策略 3】分片数量 >= 10（数据完整但有冗余）
//	  缓存时间：7 分钟（中等刷新，平衡性能和一致性）
//	  场景：部分分片丢失或正在恢复
//
// 工作流程:
//  1. 检查缓存是否过期（根据分片数量）
//  2. 如果未过期，直接返回（减少 Master 查询）
//  3. 如果过期，gRPC 调用 Master.LookupEcVolume
//  4. 更新 ecVolume.ShardLocations 映射
//  5. 记录刷新时间 (ShardLocationsRefreshTime)
//
// 最少分片要求:
//   - 至少需要 DataShardsCount (10) 个分片
//   - 少于 10 个分片无法恢复数据，返回错误
//
// 注意:
//   - 缓存时间的设计权衡了性能和数据一致性
//   - Master 查询失败不清除旧缓存，仍可使用旧数据
func (s *Store) cachedLookupEcShardLocations(ecVolume *erasure_coding.EcVolume) (err error) {

	shardCount := len(ecVolume.ShardLocations)
	if shardCount < erasure_coding.DataShardsCount &&
		ecVolume.ShardLocationsRefreshTime.Add(11*time.Second).After(time.Now()) ||
		shardCount == erasure_coding.TotalShardsCount &&
			ecVolume.ShardLocationsRefreshTime.Add(37*time.Minute).After(time.Now()) ||
		shardCount >= erasure_coding.DataShardsCount &&
			ecVolume.ShardLocationsRefreshTime.Add(7*time.Minute).After(time.Now()) {
		// still fresh
		return nil
	}

	glog.V(3).Infof("lookup and cache ec volume %d locations", ecVolume.VolumeId)

	err = operation.WithMasterServerClient(false, s.MasterAddress, s.grpcDialOption, func(masterClient master_pb.SeaweedClient) error {
		req := &master_pb.LookupEcVolumeRequest{
			VolumeId: uint32(ecVolume.VolumeId),
		}
		resp, err := masterClient.LookupEcVolume(context.Background(), req)
		if err != nil {
			return fmt.Errorf("lookup ec volume %d: %v", ecVolume.VolumeId, err)
		}
		if len(resp.ShardIdLocations) < erasure_coding.DataShardsCount {
			return fmt.Errorf("only %d shards found but %d required", len(resp.ShardIdLocations), erasure_coding.DataShardsCount)
		}

		ecVolume.ShardLocationsLock.Lock()
		for _, shardIdLocations := range resp.ShardIdLocations {
			shardId := erasure_coding.ShardId(shardIdLocations.ShardId)
			delete(ecVolume.ShardLocations, shardId)
			for _, loc := range shardIdLocations.Locations {
				ecVolume.ShardLocations[shardId] = append(ecVolume.ShardLocations[shardId], pb.NewServerAddressFromLocation(loc))
			}
		}
		ecVolume.ShardLocationsRefreshTime = time.Now()
		ecVolume.ShardLocationsLock.Unlock()

		return nil
	})
	return
}

func (s *Store) readRemoteEcShardInterval(sourceDataNodes []pb.ServerAddress, needleId types.NeedleId, vid needle.VolumeId, shardId erasure_coding.ShardId, buf []byte, offset int64) (n int, is_deleted bool, err error) {

	if len(sourceDataNodes) == 0 {
		return 0, false, fmt.Errorf("failed to find ec shard %d.%d", vid, shardId)
	}

	for _, sourceDataNode := range sourceDataNodes {
		glog.V(3).Infof("read remote ec shard %d.%d from %s", vid, shardId, sourceDataNode)
		n, is_deleted, err = s.doReadRemoteEcShardInterval(sourceDataNode, needleId, vid, shardId, buf, offset)
		if err == nil {
			return
		}
		glog.V(1).Infof("read remote ec shard %d.%d from %s: %v", vid, shardId, sourceDataNode, err)
	}

	return
}

func (s *Store) doReadRemoteEcShardInterval(sourceDataNode pb.ServerAddress, needleId types.NeedleId, vid needle.VolumeId, shardId erasure_coding.ShardId, buf []byte, offset int64) (n int, is_deleted bool, err error) {

	err = operation.WithVolumeServerClient(false, sourceDataNode, s.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {

		// copy data slice
		shardReadClient, err := client.VolumeEcShardRead(context.Background(), &volume_server_pb.VolumeEcShardReadRequest{
			VolumeId: uint32(vid),
			ShardId:  uint32(shardId),
			Offset:   offset,
			Size:     int64(len(buf)),
			FileKey:  uint64(needleId),
		})
		if err != nil {
			return fmt.Errorf("failed to start reading ec shard %d.%d from %s: %v", vid, shardId, sourceDataNode, err)
		}

		for {
			resp, receiveErr := shardReadClient.Recv()
			if receiveErr == io.EOF {
				break
			}
			if receiveErr != nil {
				return fmt.Errorf("receiving ec shard %d.%d from %s: %v", vid, shardId, sourceDataNode, receiveErr)
			}
			if resp.IsDeleted {
				is_deleted = true
			}
			copy(buf[n:n+len(resp.Data)], resp.Data)
			n += len(resp.Data)
		}

		return nil
	})
	if err != nil {
		return 0, is_deleted, fmt.Errorf("read ec shard %d.%d from %s: %v", vid, shardId, sourceDataNode, err)
	}

	return
}

// recoverOneRemoteEcShardInterval 使用 Reed-Solomon 算法恢复缺失的分片数据
// 这是 EC 存储的核心恢复算法，当分片丢失或无法访问时，从其他分片计算恢复
//
// 参数:
//   - needleId: Needle ID
//   - ecVolume: EC Volume 对象
//   - shardIdToRecover: 要恢复的分片 ID
//   - buf: 输出缓冲区（存放恢复的数据）
//   - offset: 分片内的偏移量
//
// 返回值:
//   - n: 恢复的字节数
//   - is_deleted: 数据是否已删除
//   - err: 恢复失败时的错误
//
// Reed-Solomon 算法原理:
//   SeaweedFS 使用 (10, 4) Reed-Solomon 编码:
//   - 10 个数据分片 + 4 个校验分片 = 14 个总分片
//   - 任意 10 个分片可以恢复全部 14 个分片
//   - 可以容忍最多 4 个分片丢失
//
// 恢复流程:
//  1. 【并发读取】同时从多个远程节点读取可用分片
//  2. 【数据收集】等待所有 goroutine 完成，收集分片数据
//  3. 【算法恢复】调用 Reed-Solomon 算法重建缺失分片
//  4. 【返回数据】从恢复的分片中提取目标 interval 数据
//
// 并发策略:
//   使用 sync.WaitGroup 并发读取多个分片:
//   - 每个分片一个 goroutine
//   - 最多可能启动 13 个 goroutine（除目标分片外的所有分片）
//   - 实际只需要 10 个成功即可恢复
//
// 性能优化:
//   - 并发读取：减少总延迟（100-500ms vs 1000-5000ms 串行）
//   - 提前分配：bufs 数组预分配 MaxShardCount (32) 容量
//   - 失败容忍：某些分片读取失败不影响恢复（只要有 10 个成功）
//
// 错误处理:
//   - 分片不足 10 个：无法恢复，返回错误
//   - 远程读取失败：自动清除失败节点的缓存
//   - Reed-Solomon 失败：数据损坏或分片不匹配
//
// 示例场景:
//   场景 1：节点故障
//     分片 3 所在节点宕机
//     → 从分片 0,1,2,4,5,6,7,8,9,10 恢复分片 3
//
//   场景 2：网络分区
//     分片 5,6,7 网络不可达
//     → 从分片 0,1,2,3,4,8,9,10,11,12 恢复分片 5,6,7
//
// 注意:
//   - 恢复操作消耗 CPU（Reed-Solomon 计算）
//   - 需要额外的网络带宽（读取多个分片）
//   - 应优先使用直接读取，恢复作为最后手段
func (s *Store) recoverOneRemoteEcShardInterval(needleId types.NeedleId, ecVolume *erasure_coding.EcVolume, shardIdToRecover erasure_coding.ShardId, buf []byte, offset int64) (n int, is_deleted bool, err error) {
	glog.V(3).Infof("recover ec shard %d.%d from other locations", ecVolume.VolumeId, shardIdToRecover)

	enc, err := reedsolomon.New(erasure_coding.DataShardsCount, erasure_coding.ParityShardsCount)
	if err != nil {
		return 0, false, fmt.Errorf("failed to create encoder: %w", err)
	}

	// Use MaxShardCount to support custom EC ratios up to 32 shards
	bufs := make([][]byte, erasure_coding.MaxShardCount)

	var wg sync.WaitGroup
	ecVolume.ShardLocationsLock.RLock()
	for shardId, locations := range ecVolume.ShardLocations {

		// skip current shard or empty shard
		if shardId == shardIdToRecover {
			continue
		}
		if len(locations) == 0 {
			glog.V(3).Infof("readRemoteEcShardInterval missing %d.%d from %+v", ecVolume.VolumeId, shardId, locations)
			continue
		}

		// read from remote locations
		wg.Add(1)
		go func(shardId erasure_coding.ShardId, locations []pb.ServerAddress) {
			defer wg.Done()
			data := make([]byte, len(buf))
			nRead, isDeleted, readErr := s.readRemoteEcShardInterval(locations, needleId, ecVolume.VolumeId, shardId, data, offset)
			if readErr != nil {
				glog.V(3).Infof("recover: readRemoteEcShardInterval %d.%d %d bytes from %+v: %v", ecVolume.VolumeId, shardId, nRead, locations, readErr)
				forgetShardId(ecVolume, shardId)
			}
			if isDeleted {
				is_deleted = true
			}
			if nRead == len(buf) {
				bufs[shardId] = data
			}
		}(shardId, locations)
	}
	ecVolume.ShardLocationsLock.RUnlock()

	wg.Wait()

	if err = enc.ReconstructData(bufs); err != nil {
		glog.V(3).Infof("recovered ec shard %d.%d failed: %v", ecVolume.VolumeId, shardIdToRecover, err)
		return 0, false, err
	}
	glog.V(4).Infof("recovered ec shard %d.%d from other locations", ecVolume.VolumeId, shardIdToRecover)

	copy(buf, bufs[shardIdToRecover])

	return len(buf), is_deleted, nil
}

// EcVolumes 返回所有 EC Volume 的列表（按 VolumeId 排序）
// 用于遍历和管理所有 EC Volume
//
// 返回值:
//   - ecVolumes: EC Volume 对象列表（已排序）
//
// 工作流程:
//  1. 遍历所有磁盘位置
//  2. 收集每个位置的 EC Volume
//  3. 按 VolumeId 排序（确保顺序一致）
//  4. 返回完整列表
//
// 用途:
//   - 状态查询：查看所有 EC Volume
//   - 批量操作：遍历所有 Volume 执行操作
//   - 监控统计：计算 EC 存储的总量和分布
func (s *Store) EcVolumes() (ecVolumes []*erasure_coding.EcVolume) {
	for _, location := range s.Locations {
		location.ecVolumesLock.RLock()
		for _, v := range location.ecVolumes {
			ecVolumes = append(ecVolumes, v)
		}
		location.ecVolumesLock.RUnlock()
	}
	slices.SortFunc(ecVolumes, func(a, b *erasure_coding.EcVolume) int {
		return int(a.VolumeId) - int(b.VolumeId)
	})
	return ecVolumes
}
