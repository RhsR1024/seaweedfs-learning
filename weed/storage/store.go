// Package storage 实现 SeaweedFS 的存储层
// 负责管理多个磁盘位置上的卷和 EC 分片
package storage

import (
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
	"github.com/seaweedfs/seaweedfs/weed/util"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
)

const (
	// MAX_TTL_VOLUME_REMOVAL_DELAY TTL 过期卷的最大删除延迟时间（分钟）
	// 防止在 TTL 刚过期时立即删除，给予一定的缓冲时间
	MAX_TTL_VOLUME_REMOVAL_DELAY = 10 // 10 minutes
)

// ReadOption 读取选项配置
// 用于控制从卷中读取 Needle 的行为和性能特性
type ReadOption struct {
	// === 请求参数 ===

	// ReadDeleted 是否读取已删除的文件
	// true: 即使文件被标记为删除也会返回数据
	// false: 已删除的文件返回错误
	ReadDeleted bool

	// AttemptMetaOnly 尝试仅读取元数据
	// 如果可能，只读取 Needle 头部信息，不读取实际数据
	AttemptMetaOnly bool

	// MustMetaOnly 强制仅读取元数据
	// 必须只读取元数据，不加载数据内容
	MustMetaOnly bool

	// === 响应状态 ===

	// IsMetaOnly 实际是否只读取了元数据
	// 标识本次读取操作实际上是否只返回了元数据
	IsMetaOnly bool

	// VolumeRevision 卷的修订版本号
	// 用于检测卷是否发生了变更（如压缩）
	VolumeRevision uint16

	// IsOutOfRange 是否读取超出范围
	// 标识请求的偏移量是否超过 MaxPossibleVolumeSize
	IsOutOfRange bool

	// === 性能控制参数 ===

	// HasSlowRead 是否启用慢读模式
	//
	// 设置为 true 时（慢读模式）:
	//  * 读请求和写请求会竞争锁
	//  * 大文件读取的 P99 延迟会增加（因为需要多次获取锁）
	//  * 写请求会获得更低的延迟
	//
	// 设置为 false 时（快读模式）:
	//  * 读请求会尽快完成，不阻塞其他请求
	//  * 下载大文件时，写请求可能会遇到高延迟
	//
	// 使用场景：
	//  - 写密集型场景：设置为 true
	//  - 读密集型场景：设置为 false
	HasSlowRead bool

	// ReadBufferSize 读取缓冲区大小
	// 增大 ReadBufferSize 可以减少获取锁的次数，缩短读取 P99 延迟
	// 但会稍微增加内存使用。通常与 HasSlowRead 一起使用
	//
	// 默认值：0（使用系统默认）
	// 建议值：64KB - 1MB（取决于文件大小和访问模式）
	ReadBufferSize int
}

/*
 * Store 存储服务器的核心数据结构
 * 一个 VolumeServer 包含一个 Store 实例
 * 负责管理多个磁盘位置上的所有卷和 EC 分片
 */
type Store struct {
	// === 网络配置 ===

	// MasterAddress Master 服务器地址
	// 用于与 Master 通信，上报心跳和接收指令
	MasterAddress pb.ServerAddress

	// grpcDialOption gRPC 连接选项
	// 用于建立到其他服务器的 gRPC 连接
	grpcDialOption grpc.DialOption

	// === 卷配置参数 ===

	// volumeSizeLimit 单个卷的大小限制（字节）
	// 从 Master 读取，使用原子操作保证并发安全
	volumeSizeLimit uint64

	// preallocate 是否预分配卷空间
	// 从 Master 读取，预分配可以减少文件碎片但会占用磁盘空间
	preallocate atomic.Bool

	// === 服务器标识信息 ===

	// Ip 本机 IP 地址
	Ip string

	// Port HTTP 服务端口
	Port int

	// GrpcPort gRPC 服务端口
	GrpcPort int

	// PublicUrl 公开访问 URL
	// 客户端通过此 URL 访问该存储服务器
	PublicUrl string

	// === 存储位置管理 ===

	// 每个卷是一个完整的存储单元，每个 DiskLocation 代表一个存储目录，可以包含多个卷，DiskLocation 管理该目录下的所有卷
	// Locations 所有磁盘位置列表
	Locations []*DiskLocation

	// === 拓扑信息 ===

	// dataCenter 数据中心标识（可选）
	// 如果设置，会覆盖 Master 的配置
	dataCenter string

	// rack 机架标识（可选）
	// 如果设置，会覆盖 Master 的配置
	rack string

	// === 连接状态 ===

	// connected 是否已连接到 Master
	connected bool

	// === 索引类型 ===

	// NeedleMapKind Needle 映射的类型
	// 支持：NeedleMapInMemory（内存）、NeedleMapLevelDb（LevelDB）等
	NeedleMapKind NeedleMapKind

	// === 卷变更通知通道 ===

	// NewVolumesChan 新增卷通知通道
	// 当创建新卷时，通过此通道通知 Master
	NewVolumesChan chan master_pb.VolumeShortInformationMessage

	// DeletedVolumesChan 删除卷通知通道
	// 当删除卷时，通过此通道通知 Master
	DeletedVolumesChan chan master_pb.VolumeShortInformationMessage

	// === EC 分片变更通知通道 ===

	// NewEcShardsChan 新增 EC 分片通知通道
	NewEcShardsChan chan master_pb.VolumeEcShardInformationMessage

	// DeletedEcShardsChan 删除 EC 分片通知通道
	DeletedEcShardsChan chan master_pb.VolumeEcShardInformationMessage

	// === 状态标识 ===

	// isStopping 是否正在停止
	// true: 服务器正在关闭，禁用某些操作（如 fsync）
	isStopping bool
}

// String 返回 Store 的字符串表示
// 用于日志输出和调试，包含 Store 的主要配置信息
func (s *Store) String() (str string) {
	str = fmt.Sprintf("Ip:%s, Port:%d, GrpcPort:%d PublicUrl:%s, dataCenter:%s, rack:%s, connected:%v, volumeSizeLimit:%d", s.Ip, s.Port, s.GrpcPort, s.PublicUrl, s.dataCenter, s.rack, s.connected, s.GetVolumeSizeLimit())
	return
}

// NewStore 创建新的 Store 实例
//
// 参数说明:
//   - grpcDialOption: gRPC 连接选项
//   - ip: 服务器 IP 地址
//   - port: HTTP 服务端口
//   - grpcPort: gRPC 服务端口
//   - publicUrl: 公开访问 URL
//   - dirnames: 存储目录列表
//   - maxVolumeCounts: 每个目录的最大卷数限制
//   - minFreeSpaces: 每个目录的最小空闲空间要求
//   - idxFolder: 索引文件目录（为空则与数据文件同目录）
//   - needleMapKind: Needle 映射类型（内存/LevelDB 等）
//   - diskTypes: 每个目录的磁盘类型（HDD/SSD）
//   - ldbTimeout: LevelDB 超时时间（毫秒）
//
// 返回值:
//   - s: 初始化完成的 Store 实例
//
// 工作流程:
//  1. 创建 Store 基本结构
//  2. 为每个存储目录创建 DiskLocation
//  3. 并发加载每个目录中已存在的卷
//  4. 初始化卷和 EC 分片的通知通道
//
// 注意:
//   - 会并发加载所有目录的卷以提高启动速度
//   - 通道缓冲区大小为 3，避免阻塞但不会缓存太多消息
func NewStore(grpcDialOption grpc.DialOption, ip string, port int, grpcPort int, publicUrl string, dirnames []string, maxVolumeCounts []int32,
	minFreeSpaces []util.MinFreeSpace, idxFolder string, needleMapKind NeedleMapKind, diskTypes []DiskType, ldbTimeout int64) (s *Store) {

	// 步骤 1: 创建 Store 基础结构
	s = &Store{grpcDialOption: grpcDialOption, Port: port, Ip: ip, GrpcPort: grpcPort, PublicUrl: publicUrl, NeedleMapKind: needleMapKind}
	s.Locations = make([]*DiskLocation, 0)

	// 步骤 2 & 3: 为每个目录创建 DiskLocation 并并发加载已存在的卷
	var wg sync.WaitGroup
	for i := 0; i < len(dirnames); i++ {
		// 创建 DiskLocation
		location := NewDiskLocation(dirnames[i], int32(maxVolumeCounts[i]), minFreeSpaces[i], idxFolder, diskTypes[i])
		s.Locations = append(s.Locations, location)

		// 更新 Prometheus 指标
		stats.VolumeServerMaxVolumeCounter.Add(float64(maxVolumeCounts[i]))

		// 并发加载该目录下已存在的卷
		diskId := uint32(i) // 跟踪磁盘 ID
		wg.Add(1)
		go func(id uint32, diskLoc *DiskLocation) {
			defer wg.Done()
			diskLoc.loadExistingVolumesWithId(needleMapKind, ldbTimeout, id)
		}(diskId, location)
	}
	// 等待所有卷加载完成
	wg.Wait()

	// 步骤 4: 初始化通知通道
	s.NewVolumesChan = make(chan master_pb.VolumeShortInformationMessage, 3)
	s.DeletedVolumesChan = make(chan master_pb.VolumeShortInformationMessage, 3)

	s.NewEcShardsChan = make(chan master_pb.VolumeEcShardInformationMessage, 3)
	s.DeletedEcShardsChan = make(chan master_pb.VolumeEcShardInformationMessage, 3)

	return
}

// AddVolume 添加新卷（解析字符串参数版本）
//
// 这是一个便捷方法，将字符串形式的副本放置策略和 TTL 转换为对象后调用 addVolume
//
// 参数:
//   - volumeId: 卷 ID
//   - collection: 所属集合名称
//   - needleMapKind: Needle 映射类型
//   - replicaPlacement: 副本放置策略字符串（如 "001" 表示 0 副本，0 机架，1 数据中心）
//   - ttlString: TTL 字符串（如 "3d" 表示 3 天）
//   - preallocate: 预分配大小（字节）
//   - ver: 卷版本号
//   - MemoryMapMaxSizeMb: 内存映射最大大小（MB）
//   - diskType: 磁盘类型（HDD/SSD）
//   - ldbTimeout: LevelDB 超时时间（毫秒）
//
// 返回值:
//   - error: 错误信息（成功返回 nil）
func (s *Store) AddVolume(volumeId needle.VolumeId, collection string, needleMapKind NeedleMapKind, replicaPlacement string, ttlString string, preallocate int64, ver needle.Version, MemoryMapMaxSizeMb uint32, diskType DiskType, ldbTimeout int64) error {
	// 解析副本放置策略字符串
	rt, e := super_block.NewReplicaPlacementFromString(replicaPlacement)
	if e != nil {
		return e
	}

	// 解析 TTL 字符串
	ttl, e := needle.ReadTTL(ttlString)
	if e != nil {
		return e
	}

	// 调用实际的添加方法
	e = s.addVolume(volumeId, collection, needleMapKind, rt, ttl, preallocate, ver, MemoryMapMaxSizeMb, diskType, ldbTimeout)
	return e
}

// DeleteCollection 删除指定集合的所有卷
//
// 从所有磁盘位置删除属于指定集合的卷
//
// 参数:
//   - collection: 要删除的集合名称
//
// 返回值:
//   - error: 错误信息（成功返回 nil）
//
// 工作流程:
//  1. 遍历所有磁盘位置
//  2. 从每个位置删除该集合的卷
//  3. 清除该集合的 Prometheus 指标
//
// 注意:
//   - 不会向 DeletedVolumesChan 发送消息，而是让心跳发送卷列表
//   - 这样可以避免发送大量删除消息
func (s *Store) DeleteCollection(collection string) (e error) {
	for _, location := range s.Locations {
		e = location.DeleteCollectionFromDiskLocation(collection)
		if e != nil {
			return
		}
		// 删除集合的指标数据
		stats.DeleteCollectionMetrics(collection)
		// 让心跳发送卷列表，而不是向 DeletedVolumesChan 发送已删除的卷 ID
	}
	return
}

// findVolume 在所有磁盘位置查找指定 ID 的卷
//
// 参数:
//   - vid: 要查找的卷 ID
//
// 返回值:
//   - *Volume: 找到的卷对象，未找到返回 nil
//
// 实现:
//   - 遍历所有磁盘位置，返回第一个匹配的卷
func (s *Store) findVolume(vid needle.VolumeId) *Volume {
	for _, location := range s.Locations {
		if v, found := location.FindVolume(vid); found {
			return v
		}
	}
	return nil
}

// FindFreeLocation 查找具有最多可用空间的磁盘位置
//
// 在创建新卷或 EC 分片时使用，选择空间最充足的位置
//
// 参数:
//   - filterFn: 过滤函数，返回 false 的位置会被排除
//
// 返回值:
//   - ret: 可用空间最多的磁盘位置，未找到返回 nil
//
// 工作流程:
//  1. 遍历所有磁盘位置
//  2. 应用过滤函数（如果提供）
//  3. 跳过磁盘空间不足的位置
//  4. 计算可用卷数（考虑 EC 分片占用）
//  5. 返回可用空间最多的位置
//
// EC 分片计算逻辑:
//   - 每个卷等价于 DataShardsCount (10) 个 EC 分片
//   - 计算公式: (MaxVolumeCount - CurrentVolumes) * 10 - EcShardCount) / 10
func (s *Store) FindFreeLocation(filterFn func(location *DiskLocation) bool) (ret *DiskLocation) {
	max := int32(0)
	for _, location := range s.Locations {
		// 应用过滤器
		if filterFn != nil && !filterFn(location) {
			continue
		}

		// 跳过磁盘空间不足的位置
		if location.isDiskSpaceLow {
			continue
		}

		// 计算可用卷数（考虑 EC 分片）
		currentFreeCount := location.MaxVolumeCount - int32(location.VolumesLen())
		currentFreeCount *= erasure_coding.DataShardsCount // 转换为 EC 分片单位
		currentFreeCount -= int32(location.EcShardCount()) // 减去已有 EC 分片
		currentFreeCount /= erasure_coding.DataShardsCount // 转换回卷单位

		// 找到可用空间最多的位置
		if currentFreeCount > max {
			max = currentFreeCount
			ret = location
		}
	}
	return ret
}

// addVolume 在合适的磁盘位置创建新卷（内部方法）
//
// 这是实际执行卷创建的方法，AddVolume 是其包装器
//
// 参数:
//   - vid: 卷 ID
//   - collection: 所属集合名称
//   - needleMapKind: Needle 映射类型
//   - replicaPlacement: 副本放置策略对象
//   - ttl: TTL 对象
//   - preallocate: 预分配大小（字节）
//   - ver: 卷版本号
//   - memoryMapMaxSizeMb: 内存映射最大大小（MB）
//   - diskType: 磁盘类型（HDD/SSD）
//   - ldbTimeout: LevelDB 超时时间（毫秒）
//
// 返回值:
//   - error: 错误信息（成功返回 nil）
//
// 工作流程:
//  1. 检查卷是否已存在
//  2. 查找具有最少卷数的匹配磁盘类型的位置（负载均衡）
//  3. 在该位置创建新卷
//  4. 设置卷的磁盘 ID
//  5. 通过 NewVolumesChan 通知 Master
//
// 负载均衡策略:
//   - 在相同磁盘类型的位置中选择卷数最少的
//   - 确保卷均匀分布在所有可用位置
func (s *Store) addVolume(vid needle.VolumeId, collection string, needleMapKind NeedleMapKind, replicaPlacement *super_block.ReplicaPlacement, ttl *needle.TTL, preallocate int64, ver needle.Version, memoryMapMaxSizeMb uint32, diskType DiskType, ldbTimeout int64) error {
	// 步骤 1: 检查卷是否已存在
	if s.findVolume(vid) != nil {
		return fmt.Errorf("Volume Id %d already exists!", vid)
	}

	// 步骤 2: 查找具有最少本地卷数的位置（负载均衡）
	var location *DiskLocation
	var diskId uint32
	var minVolCount int
	for i, loc := range s.Locations {
		// 只考虑匹配磁盘类型且有可用空间的位置
		if loc.DiskType == diskType && s.hasFreeDiskLocation(loc) {
			volCount := loc.LocalVolumesLen()
			if location == nil || volCount < minVolCount {
				location = loc
				diskId = uint32(i)
				minVolCount = volCount
			}
		}
	}

	// 步骤 3-5: 如果找到合适位置，创建卷
	if location != nil {
		glog.V(0).Infof("In dir %s (disk ID %d) adds volume:%v collection:%s replicaPlacement:%v ttl:%v",
			location.Directory, diskId, vid, collection, replicaPlacement, ttl)

		// 创建新卷
		if volume, err := NewVolume(location.Directory, location.IdxDirectory, collection, vid, needleMapKind, replicaPlacement, ttl, preallocate, ver, memoryMapMaxSizeMb, ldbTimeout); err == nil {
			volume.diskId = diskId // 设置磁盘 ID
			location.SetVolume(vid, volume)
			glog.V(0).Infof("add volume %d on disk ID %d", vid, diskId)

			// 通知 Master 新增了卷
			s.NewVolumesChan <- master_pb.VolumeShortInformationMessage{
				Id:               uint32(vid),
				Collection:       collection,
				ReplicaPlacement: uint32(replicaPlacement.Byte()),
				Version:          uint32(volume.Version()),
				Ttl:              ttl.ToUint32(),
				DiskType:         string(diskType),
				DiskId:           diskId,
			}
			return nil
		} else {
			return err
		}
	}

	// 没有找到可用空间
	return fmt.Errorf("No more free space left")
}

// hasFreeDiskLocation 检查磁盘位置是否有可用空间
//
// 参数:
//   - location: 要检查的磁盘位置
//
// 返回值:
//   - bool: 有可用空间返回 true，否则返回 false
//
// 检查逻辑:
//  1. 首先检查磁盘空间是否不足
//  2. 如果 MaxVolumeCount 为 0，表示无限制，返回 true
//  3. 否则检查当前卷数是否小于最大限制
func (s *Store) hasFreeDiskLocation(location *DiskLocation) bool {
	// 检查磁盘空间是否不足
	if location.isDiskSpaceLow {
		return false
	}

	// MaxVolumeCount 为 0 表示允许无限制的卷
	if location.MaxVolumeCount == 0 {
		return true
	}

	// 检查当前卷数是否低于最大值
	return int64(location.VolumesLen()) < int64(location.MaxVolumeCount)
}

// VolumeInfos 获取所有卷的信息列表
//
// 返回值:
//   - allStats: 所有卷的信息列表（已排序）
//
// 工作流程:
//  1. 从所有磁盘位置收集卷信息
//  2. 对结果进行排序
func (s *Store) VolumeInfos() (allStats []*VolumeInfo) {
	for _, location := range s.Locations {
		stats := collectStatsForOneLocation(location)
		allStats = append(allStats, stats...)
	}
	sortVolumeInfos(allStats)
	return allStats
}

// collectStatsForOneLocation 收集单个磁盘位置的所有卷信息
//
// 参数:
//   - location: 磁盘位置
//
// 返回值:
//   - stats: 该位置所有卷的信息列表
//
// 注意:
//   - 使用读锁保护，避免阻塞写操作
func collectStatsForOneLocation(location *DiskLocation) (stats []*VolumeInfo) {
	location.volumesLock.RLock()
	defer location.volumesLock.RUnlock()

	for k, v := range location.volumes {
		s := collectStatForOneVolume(k, v)
		stats = append(stats, s)
	}
	return stats
}

// collectStatForOneVolume 收集单个卷的统计信息
//
// 参数:
//   - vid: 卷 ID
//   - v: 卷对象
//
// 返回值:
//   - s: 卷的统计信息
//
// 统计信息包括:
//   - 基本信息: ID、集合、副本策略、版本、TTL 等
//   - 文件统计: 文件数、删除数、删除字节数、大小
//   - 远程存储: 远程存储名称和键
func collectStatForOneVolume(vid needle.VolumeId, v *Volume) (s *VolumeInfo) {

	s = &VolumeInfo{
		Id:               vid,
		Collection:       v.Collection,
		ReplicaPlacement: v.ReplicaPlacement,
		Version:          v.Version(),
		ReadOnly:         v.IsReadOnly(),
		Ttl:              v.Ttl,
		CompactRevision:  uint32(v.CompactionRevision),
		DiskType:         v.DiskType().String(),
		DiskId:           v.diskId,
	}
	s.RemoteStorageName, s.RemoteStorageKey = v.RemoteStorageNameKey()

	v.dataFileAccessLock.RLock()
	defer v.dataFileAccessLock.RUnlock()

	if v.nm == nil {
		return
	}

	s.FileCount = v.nm.FileCount()
	s.DeleteCount = v.nm.DeletedCount()
	s.DeletedByteCount = v.nm.DeletedSize()
	s.Size = v.nm.ContentSize()

	return
}

// SetDataCenter 设置数据中心标识
// 会覆盖 Master 配置的数据中心设置
func (s *Store) SetDataCenter(dataCenter string) {
	s.dataCenter = dataCenter
}

// SetRack 设置机架标识
// 会覆盖 Master 配置的机架设置
func (s *Store) SetRack(rack string) {
	s.rack = rack
}

// GetDataCenter 获取数据中心标识
func (s *Store) GetDataCenter() string {
	return s.dataCenter
}

// GetRack 获取机架标识
func (s *Store) GetRack() string {
	return s.rack
}

// CollectHeartbeat 收集心跳信息发送给 Master
//
// 这是一个核心方法，负责向 Master 报告当前 Store 的状态
//
// 返回值:
//   - *master_pb.Heartbeat: 包含完整状态信息的心跳消息
//
// 心跳信息包括:
//  1. 服务器基本信息（IP、端口、URL 等）
//  2. 所有卷的详细信息
//  3. EC 分片信息
//  4. 每个集合的统计数据（大小、删除字节数、只读卷数量）
//  5. 磁盘位置的 UUID 列表
//  6. 删除已过期和有 IO 错误的卷
//
// 工作流程:
//  1. 遍历所有磁盘位置收集卷信息
//  2. 检测并标记需要删除的卷（IO 错误、TTL 过期）
//  3. 收集集合级别的统计数据
//  4. 删除过期的 EC 卷
//  5. 更新 Prometheus 指标
//  6. 构建并返回心跳消息
func (s *Store) CollectHeartbeat() *master_pb.Heartbeat {
	var volumeMessages []*master_pb.VolumeInformationMessage
	maxVolumeCounts := make(map[string]uint32)
	var maxFileKey NeedleId
	collectionVolumeSize := make(map[string]int64)
	collectionVolumeDeletedBytes := make(map[string]int64)
	collectionVolumeReadOnlyCount := make(map[string]map[string]uint8)

	// 遍历所有磁盘位置
	for _, location := range s.Locations {
		var deleteVids []needle.VolumeId
		maxVolumeCounts[string(location.DiskType)] += uint32(location.MaxVolumeCount)

		location.volumesLock.RLock()
		for _, v := range location.volumes {
			curMaxFileKey, volumeMessage := v.ToVolumeInformationMessage()
			if volumeMessage == nil {
				continue
			}
			if maxFileKey < curMaxFileKey {
				maxFileKey = curMaxFileKey
			}
			shouldDeleteVolume := false

			// 检查 IO 错误
			if v.lastIoError != nil {
				deleteVids = append(deleteVids, v.Id)
				shouldDeleteVolume = true
				glog.Warningf("volume %d has IO error: %v", v.Id, v.lastIoError)
			} else {
				// 检查卷是否过期
				if !v.expired(volumeMessage.Size, s.GetVolumeSizeLimit()) {
					volumeMessages = append(volumeMessages, volumeMessage)
				} else {
					// 卷已过期，检查是否超过删除延迟时间
					if v.expiredLongEnough(MAX_TTL_VOLUME_REMOVAL_DELAY) {
						if !shouldDeleteVolume {
							deleteVids = append(deleteVids, v.Id)
							shouldDeleteVolume = true
						}
					} else {
						glog.V(0).Infof("volume %d is expired", v.Id)
					}
				}
			}

			// 更新集合统计信息
			if _, exist := collectionVolumeSize[v.Collection]; !exist {
				collectionVolumeSize[v.Collection] = 0
				collectionVolumeDeletedBytes[v.Collection] = 0
			}
			if !shouldDeleteVolume {
				collectionVolumeSize[v.Collection] += int64(volumeMessage.Size)
				collectionVolumeDeletedBytes[v.Collection] += int64(volumeMessage.DeletedByteCount)
			} else {
				collectionVolumeSize[v.Collection] -= int64(volumeMessage.Size)
				if collectionVolumeSize[v.Collection] <= 0 {
					delete(collectionVolumeSize, v.Collection)
				}
			}

			// 统计只读卷数量
			if _, exist := collectionVolumeReadOnlyCount[v.Collection]; !exist {
				collectionVolumeReadOnlyCount[v.Collection] = map[string]uint8{
					stats.IsReadOnly:       0,
					stats.NoWriteOrDelete:  0,
					stats.NoWriteCanDelete: 0,
					stats.IsDiskSpaceLow:   0,
				}
			}
			if !shouldDeleteVolume && v.IsReadOnly() {
				collectionVolumeReadOnlyCount[v.Collection][stats.IsReadOnly] += 1
				if v.noWriteOrDelete {
					collectionVolumeReadOnlyCount[v.Collection][stats.NoWriteOrDelete] += 1
				}
				if v.noWriteCanDelete {
					collectionVolumeReadOnlyCount[v.Collection][stats.NoWriteCanDelete] += 1
				}
				if v.location.isDiskSpaceLow {
					collectionVolumeReadOnlyCount[v.Collection][stats.IsDiskSpaceLow] += 1
				}
			}
		}
		location.volumesLock.RUnlock()

		// 删除过期卷
		if len(deleteVids) > 0 {
			location.volumesLock.Lock()
			for _, vid := range deleteVids {
				found, err := location.deleteVolumeById(vid, false)
				if err == nil {
					if found {
						glog.V(0).Infof("volume %d is deleted", vid)
					}
				} else {
					glog.Warningf("delete volume %d: %v", vid, err)
				}
			}
			location.volumesLock.Unlock()
		}
	}

	// 删除过期的 EC 卷
	ecVolumeMessages, deletedEcVolumes := s.deleteExpiredEcVolumes()

	// 收集所有磁盘位置的 UUID
	var uuidList []string
	for _, loc := range s.Locations {
		uuidList = append(uuidList, loc.DirectoryUuid)
	}

	// 更新 Prometheus 指标
	for col, size := range collectionVolumeSize {
		stats.VolumeServerDiskSizeGauge.WithLabelValues(col, "normal").Set(float64(size))
	}

	for col, deletedBytes := range collectionVolumeDeletedBytes {
		stats.VolumeServerDiskSizeGauge.WithLabelValues(col, "deleted_bytes").Set(float64(deletedBytes))
	}

	for col, types := range collectionVolumeReadOnlyCount {
		for t, count := range types {
			stats.VolumeServerReadOnlyVolumeGauge.WithLabelValues(col, t).Set(float64(count))
		}
	}

	// 构建心跳消息
	return &master_pb.Heartbeat{
		Ip:              s.Ip,
		Port:            uint32(s.Port),
		GrpcPort:        uint32(s.GrpcPort),
		PublicUrl:       s.PublicUrl,
		MaxVolumeCounts: maxVolumeCounts,
		MaxFileKey:      NeedleIdToUint64(maxFileKey),
		DataCenter:      s.dataCenter,
		Rack:            s.rack,
		Volumes:         volumeMessages,
		DeletedEcShards: deletedEcVolumes,
		HasNoVolumes:    len(volumeMessages) == 0,
		HasNoEcShards:   len(ecVolumeMessages) == 0,
		LocationUuids:   uuidList,
	}

}

// deleteExpiredEcVolumes 删除过期的 EC 卷并返回当前 EC 分片信息
//
// 返回值:
//   - ecShards: 当前所有 EC 分片的信息列表
//   - deleted: 被删除的 EC 分片信息列表
//
// 工作流程:
//  1. 遍历所有磁盘位置
//  2. 收集需要删除的 EC 卷（已标记为销毁）
//  3. 收集未过期的 EC 分片信息
//  4. 删除过期的 EC 卷
//  5. 返回当前和已删除的 EC 分片信息
func (s *Store) deleteExpiredEcVolumes() (ecShards, deleted []*master_pb.VolumeEcShardInformationMessage) {
	for diskId, location := range s.Locations {
		// 收集要删除的 EC 卷
		var toDeleteEvs []*erasure_coding.EcVolume
		location.ecVolumesLock.RLock()
		for _, ev := range location.ecVolumes {
			if ev.IsTimeToDestroy() {
				// 已到销毁时间
				toDeleteEvs = append(toDeleteEvs, ev)
			} else {
				// 收集未过期的 EC 分片信息
				messages := ev.ToVolumeEcShardInformationMessage(uint32(diskId))
				ecShards = append(ecShards, messages...)
			}
		}
		location.ecVolumesLock.RUnlock()

		// 删除过期的卷
		for _, ev := range toDeleteEvs {
			messages := ev.ToVolumeEcShardInformationMessage(uint32(diskId))
			// deleteEcVolumeById 有自己的锁
			err := location.deleteEcVolumeById(ev.VolumeId)
			if err != nil {
				// 删除失败，仍然保留在 ecShards 列表中
				ecShards = append(ecShards, messages...)
				glog.Errorf("delete EcVolume err %d: %v", ev.VolumeId, err)
				continue
			}
			// 成功删除，添加到已删除列表
			deleted = append(deleted, messages...)
		}
	}
	return
}

// SetStopping 设置停止标志
// 通知所有磁盘位置和卷，系统正在关闭
func (s *Store) SetStopping() {
	s.isStopping = true
	for _, location := range s.Locations {
		location.SetStopping()
	}
}

// LoadNewVolumes 重新加载所有磁盘位置的卷
// 用于在运行时发现新添加的卷文件
func (s *Store) LoadNewVolumes() {
	for _, location := range s.Locations {
		location.loadExistingVolumes(s.NeedleMapKind, 0)
	}
}

// Close 关闭 Store 并释放所有资源
// 关闭所有磁盘位置和卷
func (s *Store) Close() {
	for _, location := range s.Locations {
		location.Close()
	}
}

// WriteVolumeNeedle Store 层的写入 Needle 方法
// 这是从 topology.ReplicatedWrite 调用的入口点
//
// 参数:
//   - i: Volume ID
//   - n: 要写入的 Needle 对象
//   - checkCookie: 是否验证 Cookie（防止错误覆盖）
//   - fsync: 是否需要 fsync 刷盘
//
// 返回值:
//   - isUnchanged: 文件是否未改变（幂等写入）
//   - err: 错误信息
//
// 工作流程:
//  1. 查找指定的 Volume
//  2. 检查 Volume 是否只读
//  3. 调用 Volume.writeNeedle2 执行实际写入
//  4. 如果 Store 正在停止，禁用 fsync（避免阻塞关闭）
//
// 注意:
//   - 如果 Volume 不存在，返回错误
//   - 如果 Volume 只读，返回错误
//   - fsync && !s.isStopping: 确保关闭时不会因为 fsync 而长时间阻塞
func (s *Store) WriteVolumeNeedle(i needle.VolumeId, n *needle.Needle, checkCookie bool, fsync bool) (isUnchanged bool, err error) {
	// 步骤 1: 查找 Volume
	if v := s.findVolume(i); v != nil {
		// 步骤 2: 检查 Volume 是否只读
		if v.IsReadOnly() {
			err = fmt.Errorf("volume %d is read only", i)
			return
		}
		// 步骤 3: 调用 Volume 的写入方法
		// fsync && !s.isStopping: 如果 Store 正在停止，禁用 fsync 避免阻塞
		_, _, isUnchanged, err = v.writeNeedle2(n, checkCookie, fsync && !s.isStopping)
		return
	}
	// Volume 不存在
	glog.V(0).Infoln("volume", i, "not found!")
	err = fmt.Errorf("volume %d not found on %s:%d", i, s.Ip, s.Port)
	return
}

// DeleteVolumeNeedle Store 层的删除 Needle 方法
// 从指定 Volume 中删除 Needle
//
// 参数:
//   - i: Volume ID
//   - n: 要删除的 Needle 对象（包含 needleId）
//
// 返回值:
//   - Size: 删除的数据大小
//   - error: 错误信息
//
// 工作流程:
//  1. 查找指定的 Volume
//  2. 检查 Volume 是否允许删除（noWriteOrDelete）
//  3. 调用 Volume.deleteNeedle2 执行实际删除
//
// 注意:
//   - 删除是逻辑删除，数据仍在磁盘上，通过 Compaction 回收空间
//   - 如果 Volume 不存在，返回错误
func (s *Store) DeleteVolumeNeedle(i needle.VolumeId, n *needle.Needle) (Size, error) {
	if v := s.findVolume(i); v != nil {
		if v.noWriteOrDelete {
			return 0, fmt.Errorf("volume %d is read only", i)
		}
		return v.deleteNeedle2(n)
	}
	return 0, fmt.Errorf("volume %d not found on %s:%d", i, s.Ip, s.Port)
}

// ReadVolumeNeedle Store 层的读取 Needle 方法
// 从指定 Volume 中读取 Needle 数据
//
// 参数:
//   - i: Volume ID
//   - n: Needle 对象（输入 needleId，输出完整数据）
//   - readOption: 读取选项（可选，如是否读取数据）
//   - onReadSizeFn: 读取大小回调函数（可选，用于统计）
//
// 返回值:
//   - int: 读取的字节数
//   - error: 错误信息
func (s *Store) ReadVolumeNeedle(i needle.VolumeId, n *needle.Needle, readOption *ReadOption, onReadSizeFn func(size Size)) (int, error) {
	if v := s.findVolume(i); v != nil {
		return v.readNeedle(n, readOption, onReadSizeFn)
	}
	return 0, fmt.Errorf("volume %d not found", i)
}

// ReadVolumeNeedleMetaAt 从指定偏移量读取 Needle 元数据
//
// 这个方法直接从指定位置读取 Needle 的头部信息，不使用索引
//
// 参数:
//   - i: 卷 ID
//   - n: Needle 对象（用于接收读取的元数据）
//   - offset: 读取偏移量
//   - size: 读取大小
//
// 返回值:
//   - error: 错误信息（成功返回 nil）
//
// 使用场景:
//   - 当已知 Needle 的确切位置时，绕过索引直接读取
//   - 用于卷修复和验证
func (s *Store) ReadVolumeNeedleMetaAt(i needle.VolumeId, n *needle.Needle, offset int64, size int32) error {
	if v := s.findVolume(i); v != nil {
		return v.readNeedleMetaAt(n, offset, size)
	}
	return fmt.Errorf("volume %d not found", i)
}

// ReadVolumeNeedleDataInto 读取 Needle 数据并写入到 Writer
//
// 这个方法用于流式读取大文件，避免将整个文件加载到内存
//
// 参数:
//   - i: 卷 ID
//   - n: Needle 对象（包含 needleId 和元数据）
//   - readOption: 读取选项
//   - writer: 数据写入目标
//   - offset: 读取偏移量（相对于 Needle 数据部分）
//   - size: 读取大小
//
// 返回值:
//   - error: 错误信息（成功返回 nil）
//
// 使用场景:
//   - HTTP Range 请求
//   - 大文件下载（流式传输）
//   - 避免内存占用过大
func (s *Store) ReadVolumeNeedleDataInto(i needle.VolumeId, n *needle.Needle, readOption *ReadOption, writer io.Writer, offset int64, size int64) error {
	if v := s.findVolume(i); v != nil {
		return v.readNeedleDataInto(n, readOption, writer, offset, size)
	}
	return fmt.Errorf("volume %d not found", i)
}

// GetVolume 获取指定 ID 的卷对象
//
// 参数:
//   - i: 卷 ID
//
// 返回值:
//   - *Volume: 卷对象，未找到返回 nil
func (s *Store) GetVolume(i needle.VolumeId) *Volume {
	return s.findVolume(i)
}

// HasVolume 检查是否存在指定 ID 的卷
//
// 参数:
//   - i: 卷 ID
//
// 返回值:
//   - bool: 存在返回 true，否则返回 false
func (s *Store) HasVolume(i needle.VolumeId) bool {
	v := s.findVolume(i)
	return v != nil
}

// MarkVolumeReadonly 将卷标记为只读
//
// 参数:
//   - i: 卷 ID
//   - persist: 是否持久化只读状态到磁盘
//
// 返回值:
//   - error: 错误信息（成功返回 nil）
//
// 注意:
//   - 只读状态下，卷不允许写入和删除操作
//   - persist=true 时，状态会持久化，重启后仍然保持只读
func (s *Store) MarkVolumeReadonly(i needle.VolumeId, persist bool) error {
	v := s.findVolume(i)
	if v == nil {
		return fmt.Errorf("volume %d not found", i)
	}
	v.noWriteLock.Lock()
	v.noWriteOrDelete = true
	if persist {
		v.PersistReadOnly(true)
	}
	v.noWriteLock.Unlock()
	return nil
}

// MarkVolumeWritable 将卷标记为可写
//
// 参数:
//   - i: 卷 ID
//
// 返回值:
//   - error: 错误信息（成功返回 nil）
//
// 注意:
//   - 恢复卷的写入和删除能力
//   - 会持久化可写状态到磁盘
func (s *Store) MarkVolumeWritable(i needle.VolumeId) error {
	v := s.findVolume(i)
	if v == nil {
		return fmt.Errorf("volume %d not found", i)
	}
	v.noWriteLock.Lock()
	v.noWriteOrDelete = false
	v.PersistReadOnly(false)
	v.noWriteLock.Unlock()
	return nil
}

// MountVolume 挂载磁盘上已存在的卷
//
// 从磁盘加载卷文件并添加到 Store 管理
//
// 参数:
//   - i: 要挂载的卷 ID
//
// 返回值:
//   - error: 错误信息（成功返回 nil）
//
// 工作流程:
//  1. 在所有磁盘位置查找卷文件
//  2. 加载卷到内存
//  3. 设置磁盘 ID
//  4. 通过 NewVolumesChan 通知 Master
//
// 使用场景:
//   - 热插拔磁盘
//   - 恢复之前卸载的卷
func (s *Store) MountVolume(i needle.VolumeId) error {
	for diskId, location := range s.Locations {
		if found := location.LoadVolume(uint32(diskId), i, s.NeedleMapKind); found == true {
			glog.V(0).Infof("mount volume %d", i)
			v := s.findVolume(i)
			v.diskId = uint32(diskId) // 设置磁盘 ID
			s.NewVolumesChan <- master_pb.VolumeShortInformationMessage{
				Id:               uint32(v.Id),
				Collection:       v.Collection,
				ReplicaPlacement: uint32(v.ReplicaPlacement.Byte()),
				Version:          uint32(v.Version()),
				Ttl:              v.Ttl.ToUint32(),
				DiskType:         string(v.location.DiskType),
				DiskId:           uint32(diskId),
			}
			return nil
		}
	}

	return fmt.Errorf("volume %d not found on disk", i)
}

// UnmountVolume 卸载卷（从内存中移除但保留文件）
//
// 参数:
//   - i: 要卸载的卷 ID
//
// 返回值:
//   - error: 错误信息（成功返回 nil）
//
// 工作流程:
//  1. 查找卷
//  2. 从对应的磁盘位置卸载
//  3. 通过 DeletedVolumesChan 通知 Master
//
// 注意:
//   - 只是从内存中移除，不删除磁盘文件
//   - 可以通过 MountVolume 重新挂载
func (s *Store) UnmountVolume(i needle.VolumeId) error {
	v := s.findVolume(i)
	if v == nil {
		return nil
	}
	message := master_pb.VolumeShortInformationMessage{
		Id:               uint32(v.Id),
		Collection:       v.Collection,
		ReplicaPlacement: uint32(v.ReplicaPlacement.Byte()),
		Version:          uint32(v.Version()),
		Ttl:              v.Ttl.ToUint32(),
		DiskType:         string(v.location.DiskType),
		DiskId:           v.diskId,
	}

	for _, location := range s.Locations {
		err := location.UnloadVolume(i)
		if err == nil {
			glog.V(0).Infof("UnmountVolume %d", i)
			s.DeletedVolumesChan <- message
			return nil
		} else if err == ErrVolumeNotFound {
			continue
		}
	}

	return fmt.Errorf("volume %d not found on disk", i)
}

// DeleteVolume 删除卷（从磁盘删除文件）
//
// 参数:
//   - i: 要删除的卷 ID
//   - onlyEmpty: 是否只删除空卷（true: 只删除空卷，false: 删除任何卷）
//
// 返回值:
//   - error: 错误信息（成功返回 nil）
//
// 工作流程:
//  1. 查找卷
//  2. 从磁盘位置删除卷文件
//  3. 通过 DeletedVolumesChan 通知 Master
//
// 注意:
//   - 会永久删除磁盘文件，无法恢复
//   - onlyEmpty=true 时，非空卷会返回 ErrVolumeNotEmpty 错误
func (s *Store) DeleteVolume(i needle.VolumeId, onlyEmpty bool) error {
	v := s.findVolume(i)
	if v == nil {
		return fmt.Errorf("delete volume %d not found on disk", i)
	}
	message := master_pb.VolumeShortInformationMessage{
		Id:               uint32(v.Id),
		Collection:       v.Collection,
		ReplicaPlacement: uint32(v.ReplicaPlacement.Byte()),
		Version:          uint32(v.Version()),
		Ttl:              v.Ttl.ToUint32(),
		DiskType:         string(v.location.DiskType),
		DiskId:           v.diskId,
	}
	for _, location := range s.Locations {
		err := location.DeleteVolume(i, onlyEmpty)
		if err == nil {
			glog.V(0).Infof("DeleteVolume %d", i)
			s.DeletedVolumesChan <- message
			return nil
		} else if err == ErrVolumeNotFound {
			continue
		} else if err == ErrVolumeNotEmpty {
			return fmt.Errorf("DeleteVolume %d: %v", i, err)
		} else {
			glog.Errorf("DeleteVolume %d: %v", i, err)
		}
	}

	return fmt.Errorf("volume %d not found on disk", i)
}

// ConfigureVolume 配置卷的副本策略
//
// 修改卷的副本配置并持久化到 .vif 文件
//
// 参数:
//   - i: 卷 ID
//   - replication: 新的副本策略字符串（如 "001"）
//
// 返回值:
//   - error: 错误信息（成功返回 nil）
//
// 工作流程:
//  1. 在所有磁盘位置查找卷文件
//  2. 加载卷的 .vif 配置文件
//  3. 修改副本策略
//  4. 保存配置文件
//
// 注意:
//   - 只修改配置，不会影响已有的副本
//   - 需要重新挂载卷才能使配置生效
func (s *Store) ConfigureVolume(i needle.VolumeId, replication string) error {

	for _, location := range s.Locations {
		fileInfo, found := location.LocateVolume(i)
		if !found {
			continue
		}
		// 加载、修改、保存配置
		baseFileName := strings.TrimSuffix(fileInfo.Name(), filepath.Ext(fileInfo.Name()))
		vifFile := filepath.Join(location.Directory, baseFileName+".vif")
		volumeInfo, _, _, err := volume_info.MaybeLoadVolumeInfo(vifFile)
		if err != nil {
			return fmt.Errorf("volume %d failed to load vif: %v", i, err)
		}
		volumeInfo.Replication = replication
		err = volume_info.SaveVolumeInfo(vifFile, volumeInfo)
		if err != nil {
			return fmt.Errorf("volume %d failed to save vif: %v", i, err)
		}
		return nil
	}

	return fmt.Errorf("volume %d not found on disk", i)
}

// SetVolumeSizeLimit 设置卷大小限制
//
// 参数:
//   - x: 新的卷大小限制（字节）
//
// 注意:
//   - 使用原子操作，并发安全
//   - 限制从 Master 同步而来
func (s *Store) SetVolumeSizeLimit(x uint64) {
	atomic.StoreUint64(&s.volumeSizeLimit, x)
}

// GetVolumeSizeLimit 获取当前卷大小限制
//
// 返回值:
//   - uint64: 卷大小限制（字节）
//
// 注意:
//   - 使用原子操作，并发安全
func (s *Store) GetVolumeSizeLimit() uint64 {
	return atomic.LoadUint64(&s.volumeSizeLimit)
}

// SetPreallocate 设置是否预分配卷空间
//
// 参数:
//   - x: true 启用预分配，false 禁用预分配
//
// 预分配优缺点:
//   - 优点: 减少文件碎片，提高写入性能
//   - 缺点: 立即占用磁盘空间，即使未使用
func (s *Store) SetPreallocate(x bool) {
	s.preallocate.Store(x)
}

// GetPreallocate 获取当前预分配设置
//
// 返回值:
//   - bool: true 启用预分配，false 禁用预分配
func (s *Store) GetPreallocate() bool {
	return s.preallocate.Load()
}

// MaybeAdjustVolumeMax 根据可用磁盘空间动态调整最大卷数
//
// 返回值:
//   - hasChanges: 是否有磁盘位置的最大卷数发生变化
//
// 工作流程:
//  1. 检查是否设置了卷大小限制（未设置则不调整）
//  2. 遍历所有磁盘位置
//  3. 对于未固定最大卷数的位置（OriginalMaxVolumeCount == 0）:
//     a. 获取磁盘状态（可用空间）
//     b. 计算未使用的预分配空间
//     c. 计算当前卷数和 EC 分片占用
//     d. 根据可用空间计算新的最大卷数
//  4. 更新 Prometheus 指标
//
// 最大卷数计算公式:
//   - 当前卷数 + (EC分片数 / 10) + (可用空间 / 卷大小限制) - 1
//
// 使用场景:
//   - 定期调用（如心跳时）以适应磁盘空间变化
//   - 自动扩展或收缩卷容量
func (s *Store) MaybeAdjustVolumeMax() (hasChanges bool) {
	volumeSizeLimit := s.GetVolumeSizeLimit()
	if volumeSizeLimit == 0 {
		return
	}
	var newMaxVolumeCount int32
	for _, diskLocation := range s.Locations {
		// 只调整未固定最大卷数的位置
		if diskLocation.OriginalMaxVolumeCount == 0 {
			currentMaxVolumeCount := atomic.LoadInt32(&diskLocation.MaxVolumeCount)
			diskStatus := stats.NewDiskStatus(diskLocation.Directory)
			var unusedSpace uint64 = 0
			unclaimedSpaces := int64(diskStatus.Free)

			// 如果未启用预分配，计算已分配但未使用的空间
			if !s.GetPreallocate() {
				unusedSpace = diskLocation.UnUsedSpace(volumeSizeLimit)
				unclaimedSpaces -= int64(unusedSpace)
			}

			// 计算当前卷数和 EC 分片等效卷数
			volCount := diskLocation.VolumesLen()
			ecShardCount := diskLocation.EcShardCount()
			maxVolumeCount := int32(volCount) + int32((ecShardCount+erasure_coding.DataShardsCount)/erasure_coding.DataShardsCount)

			// 根据未使用空间增加可容纳卷数
			if unclaimedSpaces > int64(volumeSizeLimit) {
				maxVolumeCount += int32(uint64(unclaimedSpaces)/volumeSizeLimit) - 1
			}

			newMaxVolumeCount = newMaxVolumeCount + maxVolumeCount
			atomic.StoreInt32(&diskLocation.MaxVolumeCount, maxVolumeCount)
			glog.V(4).Infof("disk %s max %d unclaimedSpace:%dMB, unused:%dMB volumeSizeLimit:%dMB",
				diskLocation.Directory, maxVolumeCount, unclaimedSpaces/1024/1024, unusedSpace/1024/1024, volumeSizeLimit/1024/1024)
			hasChanges = hasChanges || currentMaxVolumeCount != atomic.LoadInt32(&diskLocation.MaxVolumeCount)
		} else {
			// 固定最大卷数的位置直接使用原始值
			newMaxVolumeCount = newMaxVolumeCount + diskLocation.OriginalMaxVolumeCount
		}
	}
	stats.VolumeServerMaxVolumeCounter.Set(float64(newMaxVolumeCount))
	return
}
