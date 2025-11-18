package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// DiskLocation 表示一个磁盘存储位置
// 每个 DiskLocation 管理一个物理目录，包含多个 Volume 和 EC Volume
// 负责加载、创建、删除和管理该目录下的所有 Volume
type DiskLocation struct {
	Directory              string                                    // Volume 数据文件所在的目录路径
	DirectoryUuid          string                                    // 目录的唯一标识符，用于识别同一物理磁盘
	IdxDirectory           string                                    // 索引文件所在的目录路径
	DiskType               types.DiskType                            // 磁盘类型(如 HDD、SSD 等)
	MaxVolumeCount         int32                                     // 该位置最多可存储的 Volume 数量
	OriginalMaxVolumeCount int32                                     // 原始的最大 Volume 数量，用于恢复
	MinFreeSpace           util.MinFreeSpace                         // 最小空闲空间要求
	volumes                map[needle.VolumeId]*Volume               // Volume ID 到 Volume 实例的映射
	volumesLock            sync.RWMutex                              // 保护 volumes 映射的读写锁

	// erasure coding 纠删码相关
	ecVolumes     map[needle.VolumeId]*erasure_coding.EcVolume // EC Volume ID 到 EC Volume 实例的映射
	ecVolumesLock sync.RWMutex                                  // 保护 ecVolumes 映射的读写锁

	isDiskSpaceLow bool           // 磁盘空间是否不足的标志
	closeCh        chan struct{}  // 用于通知关闭的通道
}

// GenerateDirUuid 为指定目录生成或读取 UUID
// 如果目录下已存在 vol_dir.uuid 文件，则读取其中的 UUID
// 否则生成新的 UUID 并保存到文件中
// UUID 用于唯一标识一个物理存储位置，避免重复挂载
func GenerateDirUuid(dir string) (dirUuidString string, err error) {
	glog.V(1).Infof("Getting uuid of volume directory:%s", dir)
	fileName := dir + "/vol_dir.uuid"
	if !util.FileExists(fileName) {
		// UUID 文件不存在，创建新的
		dirUuidString, err = writeNewUuid(fileName)
	} else {
		// 读取已存在的 UUID
		uuidData, readErr := os.ReadFile(fileName)
		if readErr != nil {
			return "", fmt.Errorf("failed to read uuid from %s : %v", fileName, readErr)
		}
		if len(uuidData) > 0 {
			dirUuidString = string(uuidData)
		} else {
			// UUID 文件为空，生成新的
			dirUuidString, err = writeNewUuid(fileName)
		}
	}
	return dirUuidString, err
}

// writeNewUuid 生成新的 UUID 并写入文件
func writeNewUuid(fileName string) (string, error) {
	dirUuid, _ := uuid.NewRandom()
	dirUuidString := dirUuid.String()
	if err := util.WriteFile(fileName, []byte(dirUuidString), 0644); err != nil {
		return "", fmt.Errorf("failed to write uuid to %s : %v", fileName, err)
	}
	return dirUuidString, nil
}

// NewDiskLocation 创建一个新的 DiskLocation 实例
// 参数说明:
//   - dir: Volume 数据文件目录
//   - maxVolumeCount: 最大 Volume 数量
//   - minFreeSpace: 最小空闲空间要求
//   - idxDir: 索引文件目录，如果为空则使用 dir
//   - diskType: 磁盘类型
// 该函数会启动一个后台协程定期检查磁盘空间
func NewDiskLocation(dir string, maxVolumeCount int32, minFreeSpace util.MinFreeSpace, idxDir string, diskType types.DiskType) *DiskLocation {
	glog.V(4).Infof("Added new Disk %s: maxVolumes=%d", dir, maxVolumeCount)
	dir = util.ResolvePath(dir)
	if idxDir == "" {
		idxDir = dir
	} else {
		idxDir = util.ResolvePath(idxDir)
	}
	// 生成或读取目录 UUID
	dirUuid, err := GenerateDirUuid(dir)
	if err != nil {
		glog.Fatalf("cannot generate uuid of dir %s: %v", dir, err)
	}
	location := &DiskLocation{
		Directory:              dir,
		DirectoryUuid:          dirUuid,
		IdxDirectory:           idxDir,
		DiskType:               diskType,
		MaxVolumeCount:         maxVolumeCount,
		OriginalMaxVolumeCount: maxVolumeCount,
		MinFreeSpace:           minFreeSpace,
	}
	location.volumes = make(map[needle.VolumeId]*Volume)
	location.ecVolumes = make(map[needle.VolumeId]*erasure_coding.EcVolume)
	location.closeCh = make(chan struct{})

	// 启动后台协程定期检查磁盘空间
	go func() {
		location.CheckDiskSpace() // 立即执行一次检查
		for {
			select {
			case <-location.closeCh:
				return
			case <-time.After(time.Minute): // 每分钟检查一次
				location.CheckDiskSpace()
			}
		}
	}()
	return location
}

// volumeIdFromFileName 从文件名中解析 Volume ID 和集合名称
// 文件名格式: collection_id.ext 或 id.ext
// 返回值:
//   - VolumeId: Volume 的 ID
//   - string: 集合名称(如果有)
//   - error: 解析错误
func volumeIdFromFileName(filename string) (needle.VolumeId, string, error) {
	if isValidVolume(filename) {
		base := filename[:len(filename)-4] // 移除扩展名(.idx 或 .vif)
		collection, volumeId, err := parseCollectionVolumeId(base)
		return volumeId, collection, err
	}

	return 0, "", fmt.Errorf("file is not a volume: %s", filename)
}

// parseCollectionVolumeId 解析集合名称和 Volume ID
// 格式: collection_id 或 id
func parseCollectionVolumeId(base string) (collection string, vid needle.VolumeId, err error) {
	i := strings.LastIndex(base, "_")
	if i > 0 {
		// 包含集合名称
		collection, base = base[0:i], base[i+1:]
	}
	vol, err := needle.NewVolumeId(base)
	return collection, vol, err
}

// isValidVolume 判断文件名是否是有效的 Volume 文件
// 有效的 Volume 文件以 .idx 或 .vif 结尾
func isValidVolume(basename string) bool {
	return strings.HasSuffix(basename, ".idx") || strings.HasSuffix(basename, ".vif")
}

// getValidVolumeName 获取有效的 Volume 名称(去除扩展名)
// 如果不是有效的 Volume 文件，返回空字符串
func getValidVolumeName(basename string) string {
	if isValidVolume(basename) {
		return basename[:len(basename)-4]
	}
	return ""
}

// loadExistingVolume 加载一个已存在的 Volume
// 参数说明:
//   - dirEntry: 目录项
//   - needleMapKind: Needle 映射器类型
//   - skipIfEcVolumesExists: 如果存在有效的 EC Volume 则跳过
//   - ldbTimeout: LevelDB 超时时间
//   - diskId: 磁盘 ID
// 返回值: 是否成功加载
func (l *DiskLocation) loadExistingVolume(dirEntry os.DirEntry, needleMapKind NeedleMapKind, skipIfEcVolumesExists bool, ldbTimeout int64, diskId uint32) bool {
	basename := dirEntry.Name()
	if dirEntry.IsDir() {
		return false
	}
	volumeName := getValidVolumeName(basename)
	if volumeName == "" {
		return false
	}

	// 解析集合名称和 Volume ID
	vid, collection, err := volumeIdFromFileName(basename)
	if err != nil {
		glog.Warningf("get volume id failed, %s, err : %s", volumeName, err)
		return false
	}

	// 如果需要跳过且存在 EC Volume，则先验证 EC 文件的有效性
	if skipIfEcVolumesExists {
		ecxFilePath := filepath.Join(l.IdxDirectory, volumeName+".ecx")
		if util.FileExists(ecxFilePath) {
			// 验证 EC Volume: 分片数量、大小一致性等
			if !l.validateEcVolume(collection, vid) {
				glog.Warningf("EC volume %d validation failed, removing incomplete EC files to allow .dat file loading", vid)
				l.removeEcVolumeFiles(collection, vid)
				// 继续加载 .dat 文件
			} else {
				// EC Volume 有效，跳过 .dat 文件
				return false
			}
		}
	}

	// 检查是否存在未完成的 Volume
	noteFile := l.Directory + "/" + volumeName + ".note"
	if util.FileExists(noteFile) {
		note, _ := os.ReadFile(noteFile)
		glog.Warningf("volume %s was not completed: %s", volumeName, string(note))
		removeVolumeFiles(l.Directory + "/" + volumeName)
		removeVolumeFiles(l.IdxDirectory + "/" + volumeName)
		return false
	}

	// 避免重复加载同一个 Volume
	l.volumesLock.RLock()
	_, found := l.volumes[vid]
	l.volumesLock.RUnlock()
	if found {
		glog.V(1).Infof("loaded volume, %v", vid)
		return true
	}

	// 加载 Volume
	v, e := NewVolume(l.Directory, l.IdxDirectory, collection, vid, needleMapKind, nil, nil, 0, needle.GetCurrentVersion(), 0, ldbTimeout)
	if e != nil {
		glog.V(0).Infof("new volume %s error %s", volumeName, e)
		return false
	}

	v.diskId = diskId // 设置磁盘 ID
	l.SetVolume(vid, v)

	size, _, _ := v.FileStat()
	glog.V(0).Infof("data file %s, replication=%s v=%d size=%d ttl=%s disk_id=%d",
		l.Directory+"/"+volumeName+".dat", v.ReplicaPlacement, v.Version(), size, v.Ttl.String(), diskId)
	return true
}

// concurrentLoadingVolumes 并发加载多个 Volume
// 使用多个工作协程并发加载，提高启动速度
func (l *DiskLocation) concurrentLoadingVolumes(needleMapKind NeedleMapKind, concurrency int, ldbTimeout int64, diskId uint32) {

	task_queue := make(chan os.DirEntry, 10*concurrency) // 任务队列，容量为并发数的10倍
	// 生产者协程：扫描目录并将任务放入队列
	go func() {
		foundVolumeNames := make(map[string]bool)
		if dirEntries, err := os.ReadDir(l.Directory); err == nil {
			for _, entry := range dirEntries {
				volumeName := getValidVolumeName(entry.Name())
				if volumeName == "" {
					continue
				}
				// 去重，避免重复加载
				if _, found := foundVolumeNames[volumeName]; !found {
					foundVolumeNames[volumeName] = true
					task_queue <- entry
				}
			}
		}
		close(task_queue)
	}()

	// 消费者协程：从队列中取任务并加载 Volume
	var wg sync.WaitGroup
	for workerNum := 0; workerNum < concurrency; workerNum++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for fi := range task_queue {
				_ = l.loadExistingVolume(fi, needleMapKind, true, ldbTimeout, diskId)
			}
		}()
	}
	wg.Wait()

}

// loadExistingVolumes 加载所有已存在的 Volume
// 使用默认的磁盘 ID 0，用于向后兼容
func (l *DiskLocation) loadExistingVolumes(needleMapKind NeedleMapKind, ldbTimeout int64) {
	l.loadExistingVolumesWithId(needleMapKind, ldbTimeout, 0) // 默认磁盘 ID 为 0
}

// loadExistingVolumesWithId 使用指定的磁盘 ID 加载所有已存在的 Volume
// 该方法会:
//   1. 根据 CPU 核心数确定并发加载的工作协程数
//   2. 并发加载所有普通 Volume
//   3. 加载所有 EC Volume 分片
func (l *DiskLocation) loadExistingVolumesWithId(needleMapKind NeedleMapKind, ldbTimeout int64, diskId uint32) {

	// 确定工作协程数量
	workerNum := runtime.NumCPU()
	val, ok := os.LookupEnv("GOMAXPROCS")
	if ok {
		num, err := strconv.Atoi(val)
		if err != nil || num < 1 {
			num = 10
			glog.Warningf("failed to set worker number from GOMAXPROCS , set to default:10")
		}
		workerNum = num
	} else {
		if workerNum <= 10 {
			workerNum = 10
		}
	}
	// 并发加载普通 Volume
	l.concurrentLoadingVolumes(needleMapKind, workerNum, ldbTimeout, diskId)
	glog.V(0).Infof("Store started on dir: %s with %d volumes max %d (disk ID: %d)", l.Directory, len(l.volumes), l.MaxVolumeCount, diskId)

	// 加载所有 EC Volume 分片
	l.loadAllEcShards()
	glog.V(0).Infof("Store started on dir: %s with %d ec shards (disk ID: %d)", l.Directory, len(l.ecVolumes), diskId)

}

// DeleteCollectionFromDiskLocation 从磁盘位置删除指定集合的所有 Volume
// 该方法会:
//   1. 卸载并删除所有属于该集合的普通 Volume
//   2. 卸载并删除所有属于该集合的 EC Volume
//   3. 并发执行删除操作以提高效率
func (l *DiskLocation) DeleteCollectionFromDiskLocation(collection string) (e error) {

	l.volumesLock.Lock()
	delVolsMap := l.unmountVolumeByCollection(collection) // 卸载普通 Volume
	l.volumesLock.Unlock()

	l.ecVolumesLock.Lock()
	delEcVolsMap := l.unmountEcVolumeByCollection(collection) // 卸载 EC Volume
	l.ecVolumesLock.Unlock()

	// 使用错误通道收集删除过程中的错误
	errChain := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)

	// 并发删除普通 Volume
	go func() {
		for _, v := range delVolsMap {
			if err := v.Destroy(false); err != nil {
				errChain <- err
			}
		}
		wg.Done()
	}()

	// 并发删除 EC Volume
	go func() {
		for _, v := range delEcVolsMap {
			v.Destroy()
		}
		wg.Done()
	}()

	// 等待所有删除操作完成后关闭错误通道
	go func() {
		wg.Wait()
		close(errChain)
	}()

	// 收集所有错误并拼接成一个错误信息
	errBuilder := strings.Builder{}
	for err := range errChain {
		errBuilder.WriteString(err.Error())
		errBuilder.WriteString("; ")
	}
	if errBuilder.Len() > 0 {
		e = fmt.Errorf("%s", errBuilder.String())
	}

	return
}

// deleteVolumeById 根据 Volume ID 删除 Volume
// onlyEmpty 为 true 时只删除空的 Volume
func (l *DiskLocation) deleteVolumeById(vid needle.VolumeId, onlyEmpty bool) (found bool, e error) {
	v, ok := l.volumes[vid]
	if !ok {
		return
	}
	e = v.Destroy(onlyEmpty)
	if e != nil {
		return
	}
	found = true
	delete(l.volumes, vid)
	return
}

// LoadVolume 加载指定的 Volume
// 如果 Volume 文件存在，则加载它
func (l *DiskLocation) LoadVolume(diskId uint32, vid needle.VolumeId, needleMapKind NeedleMapKind) bool {
	if fileInfo, found := l.LocateVolume(vid); found {
		return l.loadExistingVolume(fileInfo, needleMapKind, false, 0, diskId)
	}
	return false
}

// ErrVolumeNotFound Volume 未找到错误
var ErrVolumeNotFound = fmt.Errorf("volume not found")

// DeleteVolume 删除指定的 Volume
// onlyEmpty 为 true 时只删除空的 Volume
func (l *DiskLocation) DeleteVolume(vid needle.VolumeId, onlyEmpty bool) error {
	l.volumesLock.Lock()
	defer l.volumesLock.Unlock()

	_, ok := l.volumes[vid]
	if !ok {
		return ErrVolumeNotFound
	}
	_, err := l.deleteVolumeById(vid, onlyEmpty)
	return err
}

// UnloadVolume 卸载指定的 Volume(不删除文件)
// 关闭 Volume 并从内存中移除
func (l *DiskLocation) UnloadVolume(vid needle.VolumeId) error {
	l.volumesLock.Lock()
	defer l.volumesLock.Unlock()

	v, ok := l.volumes[vid]
	if !ok {
		return ErrVolumeNotFound
	}
	v.Close()
	delete(l.volumes, vid)
	return nil
}

// unmountVolumeByCollection 卸载指定集合的所有 Volume
// 返回被卸载的 Volume 映射
// 注意：不会卸载正在压缩的 Volume
func (l *DiskLocation) unmountVolumeByCollection(collectionName string) map[needle.VolumeId]*Volume {
	deltaVols := make(map[needle.VolumeId]*Volume, 0)
	for k, v := range l.volumes {
		if v.Collection == collectionName && !v.isCompacting && !v.isCommitCompacting {
			deltaVols[k] = v
		}
	}

	for k := range deltaVols {
		delete(l.volumes, k)
	}
	return deltaVols
}

// SetVolume 设置(添加或更新) Volume
// 将 Volume 添加到映射中并设置其所属的 DiskLocation
func (l *DiskLocation) SetVolume(vid needle.VolumeId, volume *Volume) {
	l.volumesLock.Lock()
	defer l.volumesLock.Unlock()

	l.volumes[vid] = volume
	volume.location = l
}

// FindVolume 查找指定的 Volume
// 返回 Volume 实例和是否找到的标志
func (l *DiskLocation) FindVolume(vid needle.VolumeId) (*Volume, bool) {
	l.volumesLock.RLock()
	defer l.volumesLock.RUnlock()

	v, ok := l.volumes[vid]
	return v, ok
}

// VolumesLen 返回 Volume 的数量
func (l *DiskLocation) VolumesLen() int {
	l.volumesLock.RLock()
	defer l.volumesLock.RUnlock()

	return len(l.volumes)
}

// LocalVolumesLen 返回本地 Volume 的数量(不包括远程 Volume)
func (l *DiskLocation) LocalVolumesLen() int {
	l.volumesLock.RLock()
	defer l.volumesLock.RUnlock()

	count := 0
	for _, v := range l.volumes {
		if !v.HasRemoteFile() {
			count++
		}
	}
	return count
}

// SetStopping 准备停止，将所有 Volume 同步到磁盘
func (l *DiskLocation) SetStopping() {
	l.volumesLock.Lock()
	for _, v := range l.volumes {
		v.SyncToDisk()
	}
	l.volumesLock.Unlock()

	return
}

// Close 关闭 DiskLocation，释放所有资源
// 关闭所有 Volume 和 EC Volume，并停止后台协程
func (l *DiskLocation) Close() {
	l.volumesLock.Lock()
	for _, v := range l.volumes {
		v.Close()
	}
	l.volumesLock.Unlock()

	l.ecVolumesLock.Lock()
	for _, ecVolume := range l.ecVolumes {
		ecVolume.Close()
	}
	l.ecVolumesLock.Unlock()

	close(l.closeCh) // 通知后台协程停止
	return
}

// LocateVolume 在目录中定位指定的 Volume 文件
// 返回文件的目录项和是否找到的标志
func (l *DiskLocation) LocateVolume(vid needle.VolumeId) (os.DirEntry, bool) {
	// println("LocateVolume", vid, "on", l.Directory)
	if dirEntries, err := os.ReadDir(l.Directory); err == nil {
		for _, entry := range dirEntries {
			// println("checking", entry.Name(), "...")
			volId, _, err := volumeIdFromFileName(entry.Name())
			// println("volId", volId, "err", err)
			if vid == volId && err == nil {
				return entry, true
			}
		}
	}

	return nil, false
}

// UnUsedSpace 计算未使用的空间
// 返回所有非只读 Volume 的剩余可用空间总和
func (l *DiskLocation) UnUsedSpace(volumeSizeLimit uint64) (unUsedSpace uint64) {
	l.volumesLock.RLock()
	defer l.volumesLock.RUnlock()

	for _, vol := range l.volumes {
		if vol.IsReadOnly() {
			continue
		}
		datSize, idxSize, _ := vol.FileStat()
		unUsedSpaceVolume := int64(volumeSizeLimit) - int64(datSize+idxSize)
		glog.V(4).Infof("Volume stats for %d: volumeSizeLimit=%d, datSize=%d idxSize=%d unused=%d", vol.Id, volumeSizeLimit, datSize, idxSize, unUsedSpaceVolume)
		if unUsedSpaceVolume >= 0 {
			unUsedSpace += uint64(unUsedSpaceVolume)
		}
	}

	return
}

// CheckDiskSpace 检查磁盘空间状态
// 定期调用以监控磁盘空间使用情况
// 更新 isDiskSpaceLow 标志，影响 Volume 的只读状态
func (l *DiskLocation) CheckDiskSpace() {
	if dir, e := filepath.Abs(l.Directory); e == nil {
		s := stats.NewDiskStatus(dir)
		// 更新 Prometheus 监控指标
		stats.VolumeServerResourceGauge.WithLabelValues(l.Directory, "all").Set(float64(s.All))
		stats.VolumeServerResourceGauge.WithLabelValues(l.Directory, "used").Set(float64(s.Used))
		stats.VolumeServerResourceGauge.WithLabelValues(l.Directory, "free").Set(float64(s.Free))

		// 检查是否低于最小空闲空间要求
		isLow, desc := l.MinFreeSpace.IsLow(s.Free, s.PercentFree)
		if isLow != l.isDiskSpaceLow {
			l.isDiskSpaceLow = !l.isDiskSpaceLow
		}

		// 根据空间状态调整日志级别
		logLevel := glog.Level(4)
		if l.isDiskSpaceLow {
			logLevel = glog.Level(0) // 空间不足时使用更高级别的日志
		}

		glog.V(logLevel).Infof("dir %s %s", dir, desc)
	}
}
