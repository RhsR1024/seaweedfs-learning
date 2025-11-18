package erasure_coding

import (
	"errors"
	"fmt"
	"math"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
)

var (
	// NotFoundError 表示在 EC Volume 中未找到指定的 Needle
	NotFoundError = errors.New("needle not found")
	// destroyDelaySeconds EC Volume 销毁延迟时间(秒)，用于给清理操作预留时间
	destroyDelaySeconds int64 = 0
)

// EcVolume 表示一个纠删码(Erasure Coding) Volume
// 纠删码技术将数据分片并添加冗余，即使部分分片丢失也能恢复数据
// 相比副本方式，纠删码可以用更少的存储空间实现相同的数据可靠性
type EcVolume struct {
	VolumeId                  needle.VolumeId              // Volume ID
	Collection                string                       // 所属集合
	dir                       string                       // 数据文件目录
	dirIdx                    string                       // 索引文件目录
	ecxFile                   *os.File                     // .ecx 索引文件句柄（存储 needle 索引）
	ecxFileSize               int64                        // .ecx 文件大小
	ecxCreatedAt              time.Time                    // .ecx 文件创建时间
	Shards                    []*EcVolumeShard             // EC 分片列表（本地存储的分片）
	ShardLocations            map[ShardId][]pb.ServerAddress // 分片位置映射（记录所有分片的服务器位置）
	ShardLocationsRefreshTime time.Time                    // 分片位置信息的刷新时间
	ShardLocationsLock        sync.RWMutex                 // 保护分片位置映射的读写锁
	Version                   needle.Version               // Volume 版本
	ecjFile                   *os.File                     // .ecj 日志文件句柄（存储增量更新）
	ecjFileAccessLock         sync.Mutex                   // 保护 .ecj 文件访问的互斥锁
	diskType                  types.DiskType               // 磁盘类型
	datFileSize               int64                        // 原始 .dat 文件大小
	ExpireAtSec               uint64                       // EC Volume 过期时间（Unix 时间戳），从创建时计算
	ECContext                 *ECContext                   // EC 编码参数（数据分片数、校验分片数等）
}

// NewEcVolume 创建一个新的 EC Volume 实例
// 参数:
//   - diskType: 磁盘类型
//   - dir: 数据文件目录
//   - dirIdx: 索引文件目录
//   - collection: 集合名称
//   - vid: Volume ID
// 返回值:
//   - ev: EC Volume 实例
//   - err: 错误信息
// 该函数会:
//   1. 打开 .ecx 索引文件
//   2. 打开 .ecj 日志文件
//   3. 读取 .vif Volume 信息文件，加载 EC 配置
func NewEcVolume(diskType types.DiskType, dir string, dirIdx string, collection string, vid needle.VolumeId) (ev *EcVolume, err error) {
	ev = &EcVolume{dir: dir, dirIdx: dirIdx, Collection: collection, VolumeId: vid, diskType: diskType}

	dataBaseFileName := EcShardFileName(collection, dir, int(vid))
	indexBaseFileName := EcShardFileName(collection, dirIdx, int(vid))

	// 打开 ecx 索引文件（只读+写入模式）
	if ev.ecxFile, err = os.OpenFile(indexBaseFileName+".ecx", os.O_RDWR, 0644); err != nil {
		return nil, fmt.Errorf("cannot open ec volume index %s.ecx: %v", indexBaseFileName, err)
	}
	ecxFi, statErr := ev.ecxFile.Stat()
	if statErr != nil {
		_ = ev.ecxFile.Close()
		return nil, fmt.Errorf("can not stat ec volume index %s.ecx: %v", indexBaseFileName, statErr)
	}
	ev.ecxFileSize = ecxFi.Size()
	ev.ecxCreatedAt = ecxFi.ModTime()

	// 打开 ecj 日志文件（读写+创建模式）
	if ev.ecjFile, err = os.OpenFile(indexBaseFileName+".ecj", os.O_RDWR|os.O_CREATE, 0644); err != nil {
		return nil, fmt.Errorf("cannot open ec volume journal %s.ecj: %v", indexBaseFileName, err)
	}

	// 读取 Volume 信息文件
	ev.Version = needle.Version3
	if volumeInfo, _, found, _ := volume_info.MaybeLoadVolumeInfo(dataBaseFileName + ".vif"); found {
		ev.Version = needle.Version(volumeInfo.Version)
		ev.datFileSize = volumeInfo.DatFileSize
		ev.ExpireAtSec = volumeInfo.ExpireAtSec

		// 从 .vif 文件初始化 EC 上下文，如果不存在则使用默认配置
		if volumeInfo.EcShardConfig != nil {
			ds := int(volumeInfo.EcShardConfig.DataShards)
			ps := int(volumeInfo.EcShardConfig.ParityShards)

			// 验证分片数量，防止零值或无效值
			if ds <= 0 || ps <= 0 || ds+ps > MaxShardCount {
				glog.Warningf("Invalid EC config in VolumeInfo for volume %d (data=%d, parity=%d), using defaults", vid, ds, ps)
				ev.ECContext = NewDefaultECContext(collection, vid)
			} else {
				ev.ECContext = &ECContext{
					Collection:   collection,
					VolumeId:     vid,
					DataShards:   ds,
					ParityShards: ps,
				}
				glog.V(1).Infof("Loaded EC config from VolumeInfo for volume %d: %s", vid, ev.ECContext.String())
			}
		} else {
			ev.ECContext = NewDefaultECContext(collection, vid)
		}
	} else {
		// .vif 文件不存在，创建默认配置
		glog.Warningf("vif file not found,volumeId:%d, filename:%s", vid, dataBaseFileName)
		volume_info.SaveVolumeInfo(dataBaseFileName+".vif", &volume_server_pb.VolumeInfo{Version: uint32(ev.Version)})
		ev.ECContext = NewDefaultECContext(collection, vid)
	}

	ev.ShardLocations = make(map[ShardId][]pb.ServerAddress)

	return
}

// AddEcVolumeShard 向 EC Volume 添加一个分片
// 参数:
//   - ecVolumeShard: 要添加的 EC Volume 分片
// 返回值:
//   - bool: true 表示添加成功，false 表示分片已存在
// 添加后会自动对分片列表进行排序（按 VolumeId 和 ShardId）
func (ev *EcVolume) AddEcVolumeShard(ecVolumeShard *EcVolumeShard) bool {
	// 检查分片是否已存在
	for _, s := range ev.Shards {
		if s.ShardId == ecVolumeShard.ShardId {
			return false
		}
	}
	ev.Shards = append(ev.Shards, ecVolumeShard)
	// 对分片列表排序，确保顺序一致性
	slices.SortFunc(ev.Shards, func(a, b *EcVolumeShard) int {
		if a.VolumeId != b.VolumeId {
			return int(a.VolumeId - b.VolumeId)
		}
		return int(a.ShardId - b.ShardId)
	})
	return true
}

// DeleteEcVolumeShard 从 EC Volume 删除指定的分片
// 参数:
//   - shardId: 要删除的分片 ID
// 返回值:
//   - ecVolumeShard: 被删除的分片实例
//   - deleted: 是否成功删除
func (ev *EcVolume) DeleteEcVolumeShard(shardId ShardId) (ecVolumeShard *EcVolumeShard, deleted bool) {
	foundPosition := -1
	for i, s := range ev.Shards {
		if s.ShardId == shardId {
			foundPosition = i
		}
	}
	if foundPosition < 0 {
		return nil, false
	}

	ecVolumeShard = ev.Shards[foundPosition]
	ecVolumeShard.Unmount() // 卸载分片
	// 从切片中移除该分片
	ev.Shards = append(ev.Shards[:foundPosition], ev.Shards[foundPosition+1:]...)
	return ecVolumeShard, true
}

// FindEcVolumeShard 查找指定的 EC Volume 分片
// 参数:
//   - shardId: 要查找的分片 ID
// 返回值:
//   - ecVolumeShard: 找到的分片实例
//   - found: 是否找到
func (ev *EcVolume) FindEcVolumeShard(shardId ShardId) (ecVolumeShard *EcVolumeShard, found bool) {
	for _, s := range ev.Shards {
		if s.ShardId == shardId {
			return s, true
		}
	}
	return nil, false
}

// Close 关闭 EC Volume，释放所有资源
// 关闭所有分片和文件句柄
func (ev *EcVolume) Close() {
	// 关闭所有分片
	for _, s := range ev.Shards {
		s.Close()
	}
	// 关闭日志文件
	if ev.ecjFile != nil {
		ev.ecjFileAccessLock.Lock()
		_ = ev.ecjFile.Close()
		ev.ecjFile = nil
		ev.ecjFileAccessLock.Unlock()
	}
	// 关闭并同步索引文件
	if ev.ecxFile != nil {
		_ = ev.ecxFile.Sync()
		_ = ev.ecxFile.Close()
		ev.ecxFile = nil
	}
}

// Destroy 销毁 EC Volume，删除所有相关文件
// 该操作会:
//   1. 关闭所有文件
//   2. 销毁所有分片
//   3. 删除 .ecx、.ecj、.vif 文件
func (ev *EcVolume) Destroy() {

	ev.Close()

	// 销毁所有分片文件
	for _, s := range ev.Shards {
		s.Destroy()
	}
	// 删除索引、日志和信息文件
	os.Remove(ev.FileName(".ecx"))
	os.Remove(ev.FileName(".ecj"))
	os.Remove(ev.FileName(".vif"))
}

// FileName 根据扩展名返回对应文件的完整路径
// 参数:
//   - ext: 文件扩展名（如 ".ecx", ".ecj", ".vif"）
// 返回值:
//   - string: 文件的完整路径
func (ev *EcVolume) FileName(ext string) string {
	switch ext {
	case ".ecx", ".ecj":
		// 索引和日志文件使用索引目录
		return ev.IndexBaseFileName() + ext
	}
	// .vif 文件使用数据目录
	return ev.DataBaseFileName() + ext
}

// DataBaseFileName 返回数据文件的基础文件名（不含扩展名）
func (ev *EcVolume) DataBaseFileName() string {
	return EcShardFileName(ev.Collection, ev.dir, int(ev.VolumeId))
}

// IndexBaseFileName 返回索引文件的基础文件名（不含扩展名）
func (ev *EcVolume) IndexBaseFileName() string {
	return EcShardFileName(ev.Collection, ev.dirIdx, int(ev.VolumeId))
}

// ShardSize 返回单个分片的大小
// 如果有分片，返回第一个分片的大小（所有分片大小应该相同）
func (ev *EcVolume) ShardSize() uint64 {
	if len(ev.Shards) > 0 {
		return uint64(ev.Shards[0].Size())
	}
	return 0
}

// Size 返回所有本地分片的总大小
func (ev *EcVolume) Size() (size uint64) {
	for _, shard := range ev.Shards {
		if shardSize := shard.Size(); shardSize > 0 {
			size += uint64(shardSize)
		}
	}
	return
}

// CreatedAt 返回 EC Volume 的创建时间
// 使用 .ecx 文件的修改时间作为创建时间
func (ev *EcVolume) CreatedAt() time.Time {
	return ev.ecxCreatedAt
}

// ShardIdList 返回所有本地分片的 ID 列表
func (ev *EcVolume) ShardIdList() (shardIds []ShardId) {
	for _, s := range ev.Shards {
		shardIds = append(shardIds, s.ShardId)
	}
	return
}

// ShardInfo 包含分片的 ID 和大小信息
type ShardInfo struct {
	ShardId ShardId // 分片 ID
	Size    uint64  // 分片大小
}

// ShardDetails 返回所有本地分片的详细信息
func (ev *EcVolume) ShardDetails() (shards []ShardInfo) {
	for _, s := range ev.Shards {
		shardSize := s.Size()
		if shardSize >= 0 {
			shards = append(shards, ShardInfo{
				ShardId: s.ShardId,
				Size:    uint64(shardSize),
			})
		}
	}
	return
}

// ToVolumeEcShardInformationMessage 将 EC Volume 信息转换为 protobuf 消息格式
// 用于向 Master 服务器报告 EC Volume 的分片信息
// 参数:
//   - diskId: 磁盘 ID
// 返回值:
//   - messages: EC Volume 分片信息消息列表
func (ev *EcVolume) ToVolumeEcShardInformationMessage(diskId uint32) (messages []*master_pb.VolumeEcShardInformationMessage) {
	prevVolumeId := needle.VolumeId(math.MaxUint32)
	var m *master_pb.VolumeEcShardInformationMessage
	for _, s := range ev.Shards {
		if s.VolumeId != prevVolumeId {
			m = &master_pb.VolumeEcShardInformationMessage{
				Id:          uint32(s.VolumeId),
				Collection:  s.Collection,
				DiskType:    string(ev.diskType),
				ExpireAtSec: ev.ExpireAtSec,
				DiskId:      diskId,
			}
			messages = append(messages, m)
		}
		prevVolumeId = s.VolumeId
		// 使用位图记录分片ID
		m.EcIndexBits = uint32(ShardBits(m.EcIndexBits).AddShardId(s.ShardId))

		// 使用优化格式添加分片大小信息
		SetShardSize(m, s.ShardId, s.Size())
	}
	return
}

// LocateEcShardNeedle 定位 EC 分片中的 Needle 位置
// 参数:
//   - needleId: Needle ID
//   - version: Needle 版本
// 返回值:
//   - offset: Needle 在原始数据中的偏移量
//   - size: Needle 的大小
//   - intervals: Needle 数据在各个分片中的位置区间
//   - err: 错误信息
func (ev *EcVolume) LocateEcShardNeedle(needleId types.NeedleId, version needle.Version) (offset types.Offset, size types.Size, intervals []Interval, err error) {

	// 从 ecx 文件中查找 needle
	offset, size, err = ev.FindNeedleFromEcx(needleId)
	if err != nil {
		return types.Offset{}, 0, nil, fmt.Errorf("FindNeedleFromEcx: %w", err)
	}

	// 计算 needle 数据在分片中的位置区间
	intervals = ev.LocateEcShardNeedleInterval(version, offset.ToActualOffset(), types.Size(needle.GetActualSize(size, version)))
	return
}

// LocateEcShardNeedleInterval 计算 Needle 数据在 EC 分片中的位置区间
// 参数:
//   - version: Needle 版本
//   - offset: Needle 在原始数据中的偏移量
//   - size: Needle 的实际大小
// 返回值:
//   - intervals: Needle 数据跨越的分片区间列表
// 说明:
//   EC 编码时，数据被分成大小两种块:
//   - 大块(LargeBlock): 用于数据主体
//   - 小块(SmallBlock): 用于不足一个大块的尾部数据
//   该方法计算指定数据段在各个分片中的具体位置
func (ev *EcVolume) LocateEcShardNeedleInterval(version needle.Version, offset int64, size types.Size) (intervals []Interval) {
	shard := ev.Shards[0]
	// 通常分片会填充到 ErasureCodingSmallBlockSize 的整数倍
	// 因此，如果 shardSize 等于 n * ErasureCodingLargeBlockSize，
	// 数据将使用小块存储
	shardSize := shard.ecdFileSize - 1
	if ev.datFileSize > 0 {
		// 使用 datFileSize 计算 shardSize 以匹配 EC 编码逻辑
		// 这样可以获得正确的 LargeBlockRowsCount
		shardSize = ev.datFileSize / int64(ev.ECContext.DataShards)
	}
	// 计算数据在 EC 分片中的位置
	intervals = LocateData(ErasureCodingLargeBlockSize, ErasureCodingSmallBlockSize, shardSize, offset, types.Size(needle.GetActualSize(size, version)))

	return
}

// FindNeedleFromEcx 从 .ecx 索引文件中查找 Needle
// 参数:
//   - needleId: Needle ID
// 返回值:
//   - offset: Needle 的偏移量
//   - size: Needle 的大小
//   - err: 错误信息（如果未找到返回 NotFoundError）
func (ev *EcVolume) FindNeedleFromEcx(needleId types.NeedleId) (offset types.Offset, size types.Size, err error) {
	return SearchNeedleFromSortedIndex(ev.ecxFile, ev.ecxFileSize, needleId, nil)
}

// SearchNeedleFromSortedIndex 在排序的索引文件中搜索 Needle
// 使用二分查找算法，时间复杂度 O(log n)
// 参数:
//   - ecxFile: .ecx 索引文件句柄
//   - ecxFileSize: 索引文件大小
//   - needleId: 要查找的 Needle ID
//   - processNeedleFn: 可选的处理函数，找到 Needle 时调用
// 返回值:
//   - offset: Needle 的偏移量
//   - size: Needle 的大小
//   - err: 错误信息
func SearchNeedleFromSortedIndex(ecxFile *os.File, ecxFileSize int64, needleId types.NeedleId, processNeedleFn func(file *os.File, offset int64) error) (offset types.Offset, size types.Size, err error) {
	var key types.NeedleId
	buf := make([]byte, types.NeedleMapEntrySize)
	l, h := int64(0), ecxFileSize/types.NeedleMapEntrySize
	// 二分查找
	for l < h {
		m := (l + h) / 2
		if n, err := ecxFile.ReadAt(buf, m*types.NeedleMapEntrySize); err != nil {
			if n != types.NeedleMapEntrySize {
				return types.Offset{}, types.TombstoneFileSize, fmt.Errorf("ecx file %d read at %d: %v", ecxFileSize, m*types.NeedleMapEntrySize, err)
			}
		}
		key, offset, size = idx.IdxFileEntry(buf)
		if key == needleId {
			// 找到目标 Needle
			if processNeedleFn != nil {
				err = processNeedleFn(ecxFile, m*types.NeedleMapEntrySize)
			}
			return
		}
		if key < needleId {
			l = m + 1
		} else {
			h = m
		}
	}

	err = NotFoundError
	return
}

// IsTimeToDestroy 判断 EC Volume 是否到了应该销毁的时间
// 返回值:
//   - bool: true 表示已过期且超过延迟时间，应该销毁
func (ev *EcVolume) IsTimeToDestroy() bool {
	return ev.ExpireAtSec > 0 && time.Now().Unix() > (int64(ev.ExpireAtSec)+destroyDelaySeconds)
}
