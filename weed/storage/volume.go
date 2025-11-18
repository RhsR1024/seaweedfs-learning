package storage

import (
	"fmt"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// Volume 表示一个数据卷，是 SeaweedFS 中存储数据的基本单元
// 每个 Volume 包含一个数据文件(.dat)和一个索引文件(.idx)
// 数据文件存储实际的文件内容，索引文件存储 needle 的元数据信息
type Volume struct {
	Id                 needle.VolumeId              // Volume 的唯一标识符
	dir                string                       // 数据文件(.dat)所在的目录路径
	dirIdx             string                       // 索引文件(.idx)所在的目录路径
	Collection         string                       // Volume 所属的集合名称，用于分组管理
	DataBackend        backend.BackendStorageFile   // 数据后端存储接口，用于读写数据文件
	nm                 NeedleMapper                 // Needle 映射器，维护 needle ID 到磁盘位置的索引
	tmpNm              TempNeedleMapper             // 临时 Needle 映射器，用于压缩等操作
	needleMapKind      NeedleMapKind                // Needle 映射器的类型(如内存映射、LevelDB等)
	noWriteOrDelete    bool                         // 只读标志:既不能写入也不能删除
	noWriteCanDelete   bool                         // 只读标志:不能写入但可以删除
	noWriteLock        sync.RWMutex                 // 保护只读状态的读写锁
	hasRemoteFile      bool                         // 是否有远程文件(用于云存储等场景)
	MemoryMapMaxSizeMb uint32                       // 内存映射的最大大小(MB)

	super_block.SuperBlock                          // 嵌入 SuperBlock，包含 Volume 的元信息

	dataFileAccessLock    sync.RWMutex              // 保护数据文件访问的读写锁
	superBlockAccessLock  sync.Mutex                // 保护 SuperBlock 访问的互斥锁
	asyncRequestsChan     chan *needle.AsyncRequest // 异步请求通道，用于批量处理写入等操作
	lastModifiedTsSeconds uint64                    // 最后修改时间戳(Unix秒)
	lastAppendAtNs        uint64                    // 最后追加操作时间(Unix纳秒)

	lastCompactIndexOffset uint64                   // 最后压缩的索引偏移量
	lastCompactRevision    uint16                   // 最后压缩的版本号
	ldbTimeout             int64                    // LevelDB 操作超时时间

	isCompacting       bool                         // 是否正在执行压缩操作
	isCommitCompacting bool                         // 是否正在提交压缩结果

	volumeInfoRWLock sync.RWMutex                   // 保护 volumeInfo 的读写锁
	volumeInfo       *volume_server_pb.VolumeInfo   // Volume 的详细信息(protobuf格式)
	location         *DiskLocation                  // Volume 所在的磁盘位置
	diskId           uint32                         // 该 Volume 所在磁盘在 Store.Locations 数组中的ID

	lastIoError error                               // 最后一次 I/O 错误
}

// NewVolume 创建一个新的 Volume 实例
// 参数说明:
//   - dirname: 数据文件所在目录
//   - dirIdx: 索引文件所在目录
//   - collection: 集合名称
//   - id: Volume ID
//   - needleMapKind: Needle 映射器类型
//   - replicaPlacement: 副本放置策略,如果为 nil 则从磁盘加载
//   - ttl: 生存时间
//   - preallocate: 预分配空间大小
//   - ver: Volume 版本
//   - memoryMapMaxSizeMb: 内存映射最大大小(MB)
//   - ldbTimeout: LevelDB 超时时间
func NewVolume(dirname string, dirIdx string, collection string, id needle.VolumeId, needleMapKind NeedleMapKind, replicaPlacement *super_block.ReplicaPlacement, ttl *needle.TTL, preallocate int64, ver needle.Version, memoryMapMaxSizeMb uint32, ldbTimeout int64) (v *Volume, e error) {
	// 如果 replicaPlacement 为 nil，将从磁盘加载 superblock
	v = &Volume{dir: dirname, dirIdx: dirIdx, Collection: collection, Id: id, MemoryMapMaxSizeMb: memoryMapMaxSizeMb,
		asyncRequestsChan: make(chan *needle.AsyncRequest, 128)} // 创建容量为 128 的异步请求通道
	v.SuperBlock = super_block.SuperBlock{ReplicaPlacement: replicaPlacement, Ttl: ttl}
	v.needleMapKind = needleMapKind
	v.ldbTimeout = ldbTimeout
	e = v.load(true, true, needleMapKind, preallocate, ver) // 加载 Volume 数据
	v.startWorker()                                          // 启动后台工作线程处理异步请求
	return
}

// String 返回 Volume 的字符串表示，用于调试和日志输出
func (v *Volume) String() string {
	v.noWriteLock.RLock()
	defer v.noWriteLock.RUnlock()
	return fmt.Sprintf("Id:%v dir:%s dirIdx:%s Collection:%s dataFile:%v nm:%v noWrite:%v canDelete:%v", v.Id, v.dir, v.dirIdx, v.Collection, v.DataBackend, v.nm, v.noWriteOrDelete || v.noWriteCanDelete, v.noWriteCanDelete)
}

// VolumeFileName 根据目录、集合名和 Volume ID 生成文件名
// 文件名格式:
//   - 无集合: dir/id
//   - 有集合: dir/collection_id
func VolumeFileName(dir string, collection string, id int) (fileName string) {
	idString := strconv.Itoa(id)
	if collection == "" {
		fileName = path.Join(dir, idString)
	} else {
		fileName = path.Join(dir, collection+"_"+idString)
	}
	return
}

// DataFileName 返回数据文件(.dat)的完整路径
func (v *Volume) DataFileName() (fileName string) {
	return VolumeFileName(v.dir, v.Collection, int(v.Id))
}

// IndexFileName 返回索引文件(.idx)的完整路径
func (v *Volume) IndexFileName() (fileName string) {
	return VolumeFileName(v.dirIdx, v.Collection, int(v.Id))
}

// FileName 根据扩展名返回相应文件的完整路径
// 索引相关文件(.idx, .cpx, .ldb, .cpldb)存储在 dirIdx 目录
// 数据相关文件(.dat, .cpd, .vif)存储在 dir 目录
func (v *Volume) FileName(ext string) (fileName string) {
	switch ext {
	case ".idx", ".cpx", ".ldb", ".cpldb":
		return VolumeFileName(v.dirIdx, v.Collection, int(v.Id)) + ext
	}
	// .dat, .cpd, .vif
	return VolumeFileName(v.dir, v.Collection, int(v.Id)) + ext
}

// Version 返回 Volume 的版本号
// 优先使用 volumeInfo 中的版本，否则使用 SuperBlock 中的版本
func (v *Volume) Version() needle.Version {
	v.superBlockAccessLock.Lock()
	defer v.superBlockAccessLock.Unlock()
	if v.volumeInfo.Version != 0 {
		v.SuperBlock.Version = needle.Version(v.volumeInfo.Version)
	}
	return v.SuperBlock.Version
}

// FileStat 返回 Volume 的文件统计信息
// 返回值:
//   - datSize: 数据文件大小
//   - idxSize: 索引文件大小
//   - modTime: 最后修改时间
func (v *Volume) FileStat() (datSize uint64, idxSize uint64, modTime time.Time) {
	v.dataFileAccessLock.RLock()
	defer v.dataFileAccessLock.RUnlock()

	if v.DataBackend == nil {
		return
	}

	datFileSize, modTime, e := v.DataBackend.GetStat()
	if e == nil {
		return uint64(datFileSize), v.nm.IndexFileSize(), modTime
	}
	glog.V(0).Infof("Failed to read file size %s %v", v.DataBackend.Name(), e)
	return // -1 会导致整数溢出使 Volume 不可写
}

// ContentSize 返回 Volume 中实际存储的内容大小(不包括已删除的数据)
func (v *Volume) ContentSize() uint64 {
	v.dataFileAccessLock.RLock()
	defer v.dataFileAccessLock.RUnlock()
	if v.nm == nil {
		return 0
	}
	return v.nm.ContentSize()
}

// doIsEmpty 检查 Volume 是否为空
// 检查两个条件:
//   1. 数据文件大小是否仅包含 SuperBlock
//   2. Needle 映射器中是否有内容
func (v *Volume) doIsEmpty() (bool, error) {
	// 检查数据文件大小
	if v.DataBackend == nil {
		return false, fmt.Errorf("v.DataBackend is nil")
	} else {
		datFileSize, _, e := v.DataBackend.GetStat()
		if e != nil {
			glog.V(0).Infof("Failed to read file size %s %v", v.DataBackend.Name(), e)
			return false, fmt.Errorf("v.DataBackend.GetStat(): %v", e)
		}
		if datFileSize > super_block.SuperBlockSize {
			return false, nil
		}
	}
	// 检查 Needle 映射器内容大小
	if v.nm != nil {
		if v.nm.ContentSize() > 0 {
			return false, nil
		}
	}
	return true, nil
}

// DeletedSize 返回已删除数据的总大小
func (v *Volume) DeletedSize() uint64 {
	v.dataFileAccessLock.RLock()
	defer v.dataFileAccessLock.RUnlock()
	if v.nm == nil {
		return 0
	}
	return v.nm.DeletedSize()
}

// FileCount 返回 Volume 中的文件数量
func (v *Volume) FileCount() uint64 {
	v.dataFileAccessLock.RLock()
	defer v.dataFileAccessLock.RUnlock()
	if v.nm == nil {
		return 0
	}
	return uint64(v.nm.FileCount())
}

// DeletedCount 返回已删除的文件数量
func (v *Volume) DeletedCount() uint64 {
	v.dataFileAccessLock.RLock()
	defer v.dataFileAccessLock.RUnlock()
	if v.nm == nil {
		return 0
	}
	return uint64(v.nm.DeletedCount())
}

// MaxFileKey 返回 Volume 中最大的 Needle ID
func (v *Volume) MaxFileKey() types.NeedleId {
	v.dataFileAccessLock.RLock()
	defer v.dataFileAccessLock.RUnlock()
	if v.nm == nil {
		return 0
	}
	return v.nm.MaxFileKey()
}

// IndexFileSize 返回索引文件的大小
func (v *Volume) IndexFileSize() uint64 {
	v.dataFileAccessLock.RLock()
	defer v.dataFileAccessLock.RUnlock()
	if v.nm == nil {
		return 0
	}
	return v.nm.IndexFileSize()
}

// DiskType 返回 Volume 所在磁盘的类型(如 HDD、SSD 等)
func (v *Volume) DiskType() types.DiskType {
	return v.location.DiskType
}

// SyncToDisk 将 Volume 的索引和数据强制同步到磁盘
// 确保所有待写入的数据都持久化到磁盘上
func (v *Volume) SyncToDisk() {
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()
	if v.nm != nil {
		if err := v.nm.Sync(); err != nil {
			glog.Warningf("Volume Close fail to sync volume idx %d", v.Id)
		}
	}
	if v.DataBackend != nil {
		if err := v.DataBackend.Sync(); err != nil {
			glog.Warningf("Volume Close fail to sync volume %d", v.Id)
		}
	}
}

// Close 优雅地关闭 Volume，释放所有资源
// 这是对外暴露的关闭方法，内部调用 doClose
func (v *Volume) Close() {
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()

	v.doClose()
}

// doClose 执行实际的关闭操作
// 1. 等待压缩操作完成
// 2. 同步并关闭 Needle 映射器
// 3. 关闭数据后端
// 4. 更新监控指标
func (v *Volume) doClose() {
	// 等待压缩提交完成，避免在压缩过程中关闭
	for v.isCommitCompacting {
		time.Sleep(521 * time.Millisecond)
		glog.Warningf("Volume Close wait for compaction %d", v.Id)
	}

	// 关闭并清理 Needle 映射器
	if v.nm != nil {
		if err := v.nm.Sync(); err != nil {
			glog.Warningf("Volume Close fail to sync volume idx %d", v.Id)
		}
		v.nm.Close()
		v.nm = nil
	}
	// 关闭数据后端并更新指标
	if v.DataBackend != nil {
		if err := v.DataBackend.Close(); err != nil {
			glog.Warningf("Volume Close fail to sync volume %d", v.Id)
		}
		v.DataBackend = nil
		stats.VolumeServerVolumeGauge.WithLabelValues(v.Collection, "volume").Dec()
	}
}

// NeedToReplicate 判断是否需要副本复制
// 当副本数大于 1 时需要复制
func (v *Volume) NeedToReplicate() bool {
	return v.ReplicaPlacement.GetCopyCount() > 1
}

// expired 判断 Volume 是否已过期
// Volume 过期的条件:
//   - volumeSizeLimit 不为 0(服务器已启动完成)
//   - contentSize 大于 SuperBlockSize(Volume 不为空)
//   - 设置了 TTL 且 TTL 不为 0
//   - 当前时间 - 最后修改时间 > TTL
func (v *Volume) expired(contentSize uint64, volumeSizeLimit uint64) bool {
	if volumeSizeLimit == 0 {
		// 跳过，因为还不知道大小限制(服务器刚启动)
		return false
	}
	if contentSize <= super_block.SuperBlockSize {
		return false
	}
	if v.Ttl == nil || v.Ttl.Minutes() == 0 {
		return false
	}
	glog.V(2).Infof("volume %d now:%v lastModified:%v", v.Id, time.Now().Unix(), v.lastModifiedTsSeconds)
	livedMinutes := (time.Now().Unix() - int64(v.lastModifiedTsSeconds)) / 60
	glog.V(2).Infof("volume %d ttl:%v lived:%v", v.Id, v.Ttl, livedMinutes)
	if int64(v.Ttl.Minutes()) < livedMinutes {
		return true
	}
	return false
}

// expiredLongEnough 判断 Volume 是否过期足够长时间
// 等待时间为 min(TTL的10%, maxDelayMinutes)
// 这个延迟可以避免过于频繁地删除刚过期的 Volume
func (v *Volume) expiredLongEnough(maxDelayMinutes uint32) bool {
	if v.Ttl == nil || v.Ttl.Minutes() == 0 {
		return false
	}
	removalDelay := v.Ttl.Minutes() / 10
	if removalDelay > maxDelayMinutes {
		removalDelay = maxDelayMinutes
	}

	if uint64(v.Ttl.Minutes()+removalDelay)*60+v.lastModifiedTsSeconds < uint64(time.Now().Unix()) {
		return true
	}
	return false
}

// collectStatus 收集 Volume 的状态信息
// 返回值:
//   - maxFileKey: 最大的文件键
//   - datFileSize: 数据文件大小
//   - modTime: 最后修改时间
//   - fileCount: 文件数量
//   - deletedCount: 已删除文件数量
//   - deletedSize: 已删除数据大小
//   - ok: 是否成功收集
func (v *Volume) collectStatus() (maxFileKey types.NeedleId, datFileSize int64, modTime time.Time, fileCount, deletedCount, deletedSize uint64, ok bool) {
	v.dataFileAccessLock.RLock()
	defer v.dataFileAccessLock.RUnlock()
	glog.V(4).Infof("collectStatus volume %d", v.Id)

	if v.nm == nil || v.DataBackend == nil {
		return
	}

	ok = true

	maxFileKey = v.nm.MaxFileKey()
	datFileSize, modTime, _ = v.DataBackend.GetStat()
	fileCount = uint64(v.nm.FileCount())
	deletedCount = uint64(v.nm.DeletedCount())
	deletedSize = v.nm.DeletedSize()

	return
}

// ToVolumeInformationMessage 将 Volume 信息转换为 protobuf 消息格式
// 用于与 Master 服务器通信，上报 Volume 的状态信息
func (v *Volume) ToVolumeInformationMessage() (types.NeedleId, *master_pb.VolumeInformationMessage) {

	maxFileKey, volumeSize, modTime, fileCount, deletedCount, deletedSize, ok := v.collectStatus()

	if !ok {
		return 0, nil
	}

	volumeInfo := &master_pb.VolumeInformationMessage{
		Id:               uint32(v.Id),
		Size:             uint64(volumeSize),
		Collection:       v.Collection,
		FileCount:        fileCount,
		DeleteCount:      deletedCount,
		DeletedByteCount: deletedSize,
		ReadOnly:         v.IsReadOnly(),
		ReplicaPlacement: uint32(v.ReplicaPlacement.Byte()),
		Version:          uint32(v.Version()),
		Ttl:              v.Ttl.ToUint32(),
		CompactRevision:  uint32(v.SuperBlock.CompactionRevision),
		ModifiedAtSecond: modTime.Unix(),
		DiskType:         string(v.location.DiskType),
		DiskId:           v.diskId,
	}

	volumeInfo.RemoteStorageName, volumeInfo.RemoteStorageKey = v.RemoteStorageNameKey()

	return maxFileKey, volumeInfo
}

// RemoteStorageNameKey 返回远程存储的名称和键
// 用于云存储场景，返回 Volume 在远程存储中的位置信息
func (v *Volume) RemoteStorageNameKey() (storageName, storageKey string) {
	if v.volumeInfo == nil {
		return
	}
	if len(v.volumeInfo.GetFiles()) == 0 {
		return
	}
	return v.volumeInfo.GetFiles()[0].BackendName(), v.volumeInfo.GetFiles()[0].GetKey()
}

// IsReadOnly 判断 Volume 是否为只读状态
// 只读条件:
//   - noWriteOrDelete 标志为 true(完全只读)
//   - noWriteCanDelete 标志为 true(只读但可删除)
//   - 磁盘空间不足
func (v *Volume) IsReadOnly() bool {
	v.noWriteLock.RLock()
	defer v.noWriteLock.RUnlock()
	return v.noWriteOrDelete || v.noWriteCanDelete || v.location.isDiskSpaceLow
}

// PersistReadOnly 持久化只读状态到 Volume 信息文件
func (v *Volume) PersistReadOnly(readOnly bool) {
	v.volumeInfoRWLock.RLock()
	defer v.volumeInfoRWLock.RUnlock()
	v.volumeInfo.ReadOnly = readOnly
	v.SaveVolumeInfo()
}
