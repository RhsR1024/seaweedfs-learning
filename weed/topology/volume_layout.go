// Package topology 实现了 SeaweedFS 的拓扑管理
// 本文件定义 VolumeLayout，管理单个副本配置的所有卷
package topology

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

// copyState 副本状态枚举
// 用于判断卷的副本数量是否满足要求
type copyState int

const (
	// noCopies 没有副本（卷不存在或所有副本都下线）
	noCopies copyState = 0 + iota
	// insufficientCopies 副本数不足（少于 ReplicaPlacement 要求）
	insufficientCopies
	// enoughCopies 副本数充足（满足或超过 ReplicaPlacement 要求）
	enoughCopies
)

// volumeState 卷状态字符串
// 用于标识卷的特殊状态
type volumeState string

const (
	// readOnlyState 只读状态（卷被标记为只读）
	readOnlyState volumeState = "ReadOnly"
	// oversizedState 过大状态（卷大小超过限制）
	oversizedState = "Oversized"
	// crowdedState 拥挤状态（卷使用率超过阈值，应创建新卷）
	crowdedState = "Crowded"
	// NoWritableVolumes 错误消息：没有可写卷
	NoWritableVolumes = "No writable volumes"
)

// stateIndicator 状态指示器函数类型
// 根据 copyState 判断卷是否应该标记为某种状态
type stateIndicator func(copyState) bool

// ExistCopies 创建"存在副本"状态指示器
// 当至少有一个副本时返回 true
// 用于 readOnlyState 和 oversizedState
func ExistCopies() stateIndicator {
	return func(state copyState) bool { return state != noCopies }
}

// NoCopies 创建"没有副本"状态指示器
// 当没有任何副本时返回 true
// 用于反向逻辑（例如标记应该清理的卷）
func NoCopies() stateIndicator {
	return func(state copyState) bool { return state == noCopies }
}

// volumesBinaryState 卷的二进制状态跟踪器
// 用于跟踪卷的某种二进制状态（是/否），例如：
//   - 只读状态：卷是否为只读
//   - 过大状态：卷是否超过大小限制
//
// 工作原理：
//   1. 维护每个卷的副本位置列表
//   2. 根据副本数量（copyState）和状态指示器（indicator）判断状态
//   3. 提供 Add/Remove 方法动态更新副本列表
type volumesBinaryState struct {
	rp        *super_block.ReplicaPlacement // 副本放置策略
	name      volumeState                    // 状态名称（如 "ReadOnly", "Oversized"）
	indicator stateIndicator                 // 状态指示器：判断卷是否应该标记为此状态
	copyMap   map[needle.VolumeId]*VolumeLocationList // 卷 ID → 副本位置列表
}

// NewVolumesBinaryState 创建新的卷二进制状态跟踪器
func NewVolumesBinaryState(name volumeState, rp *super_block.ReplicaPlacement, indicator stateIndicator) *volumesBinaryState {
	return &volumesBinaryState{
		rp:        rp,
		name:      name,
		indicator: indicator,
		copyMap:   make(map[needle.VolumeId]*VolumeLocationList),
	}
}

// Dump 返回所有标记为此状态的卷 ID 列表
// 遍历所有卷，根据状态指示器判断是否应该标记
func (v *volumesBinaryState) Dump() (res []uint32) {
	for vid, list := range v.copyMap {
		if v.indicator(v.copyState(list)) {
			res = append(res, uint32(vid))
		}
	}
	return
}

// IsTrue 判断指定卷是否标记为此状态
// 使用状态指示器和副本状态进行判断
func (v *volumesBinaryState) IsTrue(vid needle.VolumeId) bool {
	list, _ := v.copyMap[vid]
	return v.indicator(v.copyState(list))
}

// Add 添加一个副本位置到卷的副本列表
// 如果卷不存在，创建新的副本列表
func (v *volumesBinaryState) Add(vid needle.VolumeId, dn *DataNode) {
	list, _ := v.copyMap[vid]
	if list != nil {
		// 卷已存在，添加或更新副本位置
		list.Set(dn)
		return
	}
	// 卷不存在，创建新的副本列表
	list = NewVolumeLocationList()
	list.Set(dn)
	v.copyMap[vid] = list
}

// Remove 从卷的副本列表中移除一个副本位置
// 如果副本列表变为空，删除整个卷记录
func (v *volumesBinaryState) Remove(vid needle.VolumeId, dn *DataNode) {
	list, _ := v.copyMap[vid]
	if list != nil {
		list.Remove(dn)
		// 如果没有副本了，删除卷记录
		if list.Length() == 0 {
			delete(v.copyMap, vid)
		}
	}
}

// copyState 计算卷的副本状态
// 根据副本数量和副本策略要求判断
func (v *volumesBinaryState) copyState(list *VolumeLocationList) copyState {
	if list == nil {
		return noCopies
	}
	if list.Length() < v.rp.GetCopyCount() {
		return insufficientCopies
	}
	return enoughCopies
}

// VolumeLayout 管理具有相同配置的所有卷
// 核心职责：
//   1. 维护卷 ID 到副本位置的映射（vid2location）
//   2. 管理可写卷列表（writables）
//   3. 跟踪只读卷、过大卷、拥挤卷
//   4. 提供卷选择逻辑（PickForWrite）
//
// 分组维度：
//   - 同一个 VolumeLayout 的所有卷具有相同的：
//     * 副本策略（ReplicaPlacement）
//     * TTL（生存时间）
//     * 磁盘类型（DiskType）
//
// 与 Server-to-Volume 的反向映射：
//   - 在 DataNode 中存储：Server → Volume 列表
//   - 在 VolumeLayout 中存储：Volume → Server 列表（副本位置）
type VolumeLayout struct {
	growRequest      atomic.Bool   // 是否有待处理的卷增长请求
	lastGrowCount    atomic.Uint32 // 上次增长创建的卷数量
	rp               *super_block.ReplicaPlacement // 副本放置策略
	ttl              *needle.TTL                   // 生存时间
	diskType         types.DiskType                // 磁盘类型（HDD/SSD）
	vid2location     map[needle.VolumeId]*VolumeLocationList // 卷 ID → 副本位置列表
	writables        []needle.VolumeId                       // 可写卷 ID 列表（动态变化）
	crowded          map[needle.VolumeId]struct{}            // 拥挤卷集合（接近容量限制）
	readonlyVolumes  *volumesBinaryState                     // 只读卷跟踪器
	oversizedVolumes *volumesBinaryState                     // 过大卷跟踪器
	vacuumedVolumes  map[needle.VolumeId]time.Time           // 已清理卷及清理时间
	volumeSizeLimit  uint64                                  // 卷大小限制（字节）
	replicationAsMin bool                                    // 是否将副本数作为最小值（允许超额副本）
	accessLock       sync.RWMutex                            // 读写锁保护并发访问
}

// VolumeLayoutStats 卷布局的统计信息
type VolumeLayoutStats struct {
	TotalSize uint64 // 总容量（包含未使用空间）
	UsedSize  uint64 // 已使用空间
	FileCount uint64 // 文件数量
}

// NewVolumeLayout 创建新的卷布局
// 参数：
//   - rp: 副本放置策略
//   - ttl: 生存时间
//   - diskType: 磁盘类型
//   - volumeSizeLimit: 单个卷的大小限制
//   - replicationAsMin: 是否允许超额副本
func NewVolumeLayout(rp *super_block.ReplicaPlacement, ttl *needle.TTL, diskType types.DiskType, volumeSizeLimit uint64, replicationAsMin bool) *VolumeLayout {
	return &VolumeLayout{
		rp:               rp,
		ttl:              ttl,
		diskType:         diskType,
		vid2location:     make(map[needle.VolumeId]*VolumeLocationList),
		writables:        *new([]needle.VolumeId),
		crowded:          make(map[needle.VolumeId]struct{}),
		readonlyVolumes:  NewVolumesBinaryState(readOnlyState, rp, ExistCopies()),
		oversizedVolumes: NewVolumesBinaryState(oversizedState, rp, ExistCopies()),
		vacuumedVolumes:  make(map[needle.VolumeId]time.Time),
		volumeSizeLimit:  volumeSizeLimit,
		replicationAsMin: replicationAsMin,
	}
}

// String 返回卷布局的字符串表示
// 用于日志输出和调试
func (vl *VolumeLayout) String() string {
	return fmt.Sprintf("rp:%v, ttl:%v, writables:%v, volumeSizeLimit:%v", vl.rp, vl.ttl, vl.writables, vl.volumeSizeLimit)
}

func (vl *VolumeLayout) RegisterVolume(v *storage.VolumeInfo, dn *DataNode) {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()

	defer vl.rememberOversizedVolume(v, dn)

	if _, ok := vl.vid2location[v.Id]; !ok {
		vl.vid2location[v.Id] = NewVolumeLocationList()
	}
	vl.vid2location[v.Id].Set(dn)
	// glog.V(4).Infof("volume %d added to %s len %d copy %d", v.Id, dn.Id(), vl.vid2location[v.Id].Length(), v.ReplicaPlacement.GetCopyCount())
	for _, dn := range vl.vid2location[v.Id].list {
		if vInfo, err := dn.GetVolumesById(v.Id); err == nil {
			if vInfo.ReadOnly {
				glog.V(1).Infof("vid %d removed from writable", v.Id)
				vl.removeFromWritable(v.Id)
				vl.readonlyVolumes.Add(v.Id, dn)
				return
			} else {
				vl.readonlyVolumes.Remove(v.Id, dn)
			}
		} else {
			glog.V(1).Infof("vid %d removed from writable", v.Id)
			vl.removeFromWritable(v.Id)
			vl.readonlyVolumes.Remove(v.Id, dn)
			return
		}
	}

}

func (vl *VolumeLayout) rememberOversizedVolume(v *storage.VolumeInfo, dn *DataNode) {
	if vl.isOversized(v) {
		vl.oversizedVolumes.Add(v.Id, dn)
	} else {
		vl.oversizedVolumes.Remove(v.Id, dn)
	}
}

func (vl *VolumeLayout) UnRegisterVolume(v *storage.VolumeInfo, dn *DataNode) {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()

	// remove from vid2location map
	location, ok := vl.vid2location[v.Id]
	if !ok {
		return
	}

	if location.Remove(dn) {

		vl.readonlyVolumes.Remove(v.Id, dn)
		vl.oversizedVolumes.Remove(v.Id, dn)
		vl.ensureCorrectWritables(v.Id)

		if location.Length() == 0 {
			delete(vl.vid2location, v.Id)
		}

	}
}

func (vl *VolumeLayout) EnsureCorrectWritables(v *storage.VolumeInfo) {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()

	vl.ensureCorrectWritables(v.Id)
}

func (vl *VolumeLayout) ensureCorrectWritables(vid needle.VolumeId) {
	isEnoughCopies := vl.enoughCopies(vid)
	isAllWritable := vl.isAllWritable(vid)
	isOversizedVolume := vl.oversizedVolumes.IsTrue(vid)
	if isEnoughCopies && isAllWritable && !isOversizedVolume {
		vl.setVolumeWritable(vid)
	} else {
		if !isEnoughCopies {
			glog.V(0).Infof("volume %d does not have enough copies", vid)
		}
		if !isAllWritable {
			glog.V(0).Infof("volume %d are not all writable", vid)
		}
		if isOversizedVolume {
			glog.V(1).Infof("volume %d are oversized", vid)
		}
		glog.V(0).Infof("volume %d remove from writable", vid)
		vl.removeFromWritable(vid)
	}
}

func (vl *VolumeLayout) isAllWritable(vid needle.VolumeId) bool {
	if location, ok := vl.vid2location[vid]; ok {
		for _, dn := range location.list {
			if v, getError := dn.GetVolumesById(vid); getError == nil {
				if v.ReadOnly {
					return false
				}
			}
		}
	} else {
		return false
	}

	return true
}

func (vl *VolumeLayout) isOversized(v *storage.VolumeInfo) bool {
	return uint64(v.Size) >= vl.volumeSizeLimit
}

func (vl *VolumeLayout) isCrowdedVolume(v *storage.VolumeInfo) bool {
	return float64(v.Size) > float64(vl.volumeSizeLimit)*VolumeGrowStrategy.Threshold
}

func (vl *VolumeLayout) isWritable(v *storage.VolumeInfo) bool {
	return !vl.isOversized(v) &&
		v.Version == needle.GetCurrentVersion() &&
		!v.ReadOnly
}

func (vl *VolumeLayout) isEmpty() bool {
	vl.accessLock.RLock()
	defer vl.accessLock.RUnlock()

	return len(vl.vid2location) == 0
}

func (vl *VolumeLayout) Lookup(vid needle.VolumeId) []*DataNode {
	vl.accessLock.RLock()
	defer vl.accessLock.RUnlock()

	if location := vl.vid2location[vid]; location != nil {
		return location.list
	}
	return nil
}

func (vl *VolumeLayout) ListVolumeServers() (nodes []*DataNode) {
	vl.accessLock.RLock()
	defer vl.accessLock.RUnlock()

	for _, location := range vl.vid2location {
		nodes = append(nodes, location.list...)
	}
	return
}

func (vl *VolumeLayout) PickForWrite(count uint64, option *VolumeGrowOption) (vid needle.VolumeId, counter uint64, locationList *VolumeLocationList, shouldGrow bool, err error) {
	vl.accessLock.RLock()
	defer vl.accessLock.RUnlock()

	lenWriters := len(vl.writables)
	if lenWriters <= 0 {
		return 0, 0, nil, true, fmt.Errorf("%s", NoWritableVolumes)
	}
	if option.DataCenter == "" && option.Rack == "" && option.DataNode == "" {
		vid := vl.writables[rand.IntN(lenWriters)]
		locationList = vl.vid2location[vid]
		if locationList == nil || len(locationList.list) == 0 {
			return 0, 0, nil, false, fmt.Errorf("Strangely vid %s is on no machine!", vid.String())
		}
		return vid, count, locationList.Copy(), false, nil
	}

	// clone vl.writables
	writables := make([]needle.VolumeId, len(vl.writables))
	copy(writables, vl.writables)
	// randomize the writables
	rand.Shuffle(len(writables), func(i, j int) {
		writables[i], writables[j] = writables[j], writables[i]
	})

	for _, writableVolumeId := range writables {
		volumeLocationList := vl.vid2location[writableVolumeId]
		for _, dn := range volumeLocationList.list {
			if option.DataCenter != "" && dn.GetDataCenter().Id() != NodeId(option.DataCenter) {
				continue
			}
			if option.Rack != "" && dn.GetRack().Id() != NodeId(option.Rack) {
				continue
			}
			if option.DataNode != "" && dn.Id() != NodeId(option.DataNode) {
				continue
			}
			vid, locationList, counter = writableVolumeId, volumeLocationList.Copy(), count
			return
		}
	}
	return vid, count, locationList, true, fmt.Errorf("%s in DataCenter:%v Rack:%v DataNode:%v", NoWritableVolumes, option.DataCenter, option.Rack, option.DataNode)
}

func (vl *VolumeLayout) HasGrowRequest() bool {
	return vl.growRequest.Load()
}
func (vl *VolumeLayout) AddGrowRequest() {
	vl.growRequest.Store(true)
}
func (vl *VolumeLayout) DoneGrowRequest() {
	vl.growRequest.Store(false)
}

func (vl *VolumeLayout) SetLastGrowCount(count uint32) {
	if vl.lastGrowCount.Load() != count && count != 0 {
		vl.lastGrowCount.Store(count)
	}
}

func (vl *VolumeLayout) GetLastGrowCount() uint32 {
	return vl.lastGrowCount.Load()
}

func (vl *VolumeLayout) ShouldGrowVolumes() bool {
	writable, crowded := vl.GetWritableVolumeCount()
	return writable <= crowded
}

func (vl *VolumeLayout) ShouldGrowVolumesByDcAndRack(writables *[]needle.VolumeId, dcId NodeId, rackId NodeId) bool {
	for _, v := range *writables {
		for _, dn := range vl.Lookup(v) {
			if dn.GetDataCenter().Id() == dcId && dn.GetRack().Id() == rackId {
				if info, err := dn.GetVolumesById(v); err == nil && !vl.isCrowdedVolume(&info) {
					return false
				}
			}
		}
	}
	return true
}

func (vl *VolumeLayout) GetWritableVolumeCount() (active, crowded int) {
	vl.accessLock.RLock()
	defer vl.accessLock.RUnlock()
	return len(vl.writables), len(vl.crowded)
}

func (vl *VolumeLayout) CloneWritableVolumes() (writables []needle.VolumeId) {
	vl.accessLock.RLock()
	writables = make([]needle.VolumeId, len(vl.writables))
	copy(writables, vl.writables)
	vl.accessLock.RUnlock()
	return writables
}

func (vl *VolumeLayout) removeFromWritable(vid needle.VolumeId) bool {
	toDeleteIndex := -1
	for k, id := range vl.writables {
		if id == vid {
			toDeleteIndex = k
			break
		}
	}
	vl.removeFromCrowded(vid)
	if toDeleteIndex >= 0 {
		glog.V(0).Infoln("Volume", vid, "becomes unwritable")
		vl.writables = append(vl.writables[0:toDeleteIndex], vl.writables[toDeleteIndex+1:]...)
		return true
	}
	return false
}
func (vl *VolumeLayout) setVolumeWritable(vid needle.VolumeId) bool {
	for _, v := range vl.writables {
		if v == vid {
			return false
		}
	}
	glog.V(0).Infoln("Volume", vid, "becomes writable")
	vl.writables = append(vl.writables, vid)
	return true
}

func (vl *VolumeLayout) SetVolumeReadOnly(dn *DataNode, vid needle.VolumeId) bool {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()

	if _, ok := vl.vid2location[vid]; ok {
		vl.readonlyVolumes.Add(vid, dn)
		return vl.removeFromWritable(vid)
	}
	return true
}

func (vl *VolumeLayout) SetVolumeWritable(dn *DataNode, vid needle.VolumeId) bool {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()

	if _, ok := vl.vid2location[vid]; ok {
		vl.readonlyVolumes.Remove(vid, dn)
	}

	if vl.enoughCopies(vid) {
		return vl.setVolumeWritable(vid)
	}
	return false
}

func (vl *VolumeLayout) SetVolumeUnavailable(dn *DataNode, vid needle.VolumeId) bool {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()

	if location, ok := vl.vid2location[vid]; ok {
		if location.Remove(dn) {
			vl.readonlyVolumes.Remove(vid, dn)
			vl.oversizedVolumes.Remove(vid, dn)
			if location.Length() < vl.rp.GetCopyCount() {
				glog.V(0).Infoln("Volume", vid, "has", location.Length(), "replica, less than required", vl.rp.GetCopyCount())
				return vl.removeFromWritable(vid)
			}
		}
	}
	return false
}
func (vl *VolumeLayout) SetVolumeAvailable(dn *DataNode, vid needle.VolumeId, isReadOnly, isFullCapacity bool) bool {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()

	vInfo, err := dn.GetVolumesById(vid)
	if err != nil {
		return false
	}

	vl.vid2location[vid].Set(dn)

	if vInfo.ReadOnly || isReadOnly || isFullCapacity {
		return false
	}

	if vl.enoughCopies(vid) {
		return vl.setVolumeWritable(vid)
	}
	return false
}

func (vl *VolumeLayout) enoughCopies(vid needle.VolumeId) bool {
	locations := vl.vid2location[vid].Length()
	desired := vl.rp.GetCopyCount()
	return locations == desired || (vl.replicationAsMin && locations > desired)
}

func (vl *VolumeLayout) SetVolumeCapacityFull(vid needle.VolumeId) bool {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()

	wasWritable := vl.removeFromWritable(vid)
	if wasWritable {
		glog.V(0).Infof("Volume %d reaches full capacity.", vid)
	}
	return wasWritable
}

func (vl *VolumeLayout) removeFromCrowded(vid needle.VolumeId) {
	if _, ok := vl.crowded[vid]; ok {
		glog.V(0).Infoln("Volume", vid, "becomes uncrowded")
		delete(vl.crowded, vid)
	}
}

func (vl *VolumeLayout) setVolumeCrowded(vid needle.VolumeId) {
	if _, ok := vl.crowded[vid]; !ok {
		vl.crowded[vid] = struct{}{}
		glog.V(0).Infoln("Volume", vid, "becomes crowded")
	}
}

func (vl *VolumeLayout) SetVolumeCrowded(vid needle.VolumeId) {
	// since delete is guarded by accessLock.Lock(),
	// and is always called in sequential order,
	// RLock() should be safe enough
	vl.accessLock.RLock()
	defer vl.accessLock.RUnlock()

	vl.setVolumeCrowded(vid)
}

type VolumeLayoutInfo struct {
	Replication string            `json:"replication"`
	TTL         string            `json:"ttl"`
	Writables   []needle.VolumeId `json:"writables"`
	Collection  string            `json:"collection"`
	DiskType    string            `json:"diskType"`
}

func (vl *VolumeLayout) ToInfo() (info VolumeLayoutInfo) {
	info.Replication = vl.rp.String()
	info.TTL = vl.ttl.String()
	info.Writables = vl.writables
	info.DiskType = vl.diskType.ReadableString()
	//m["locations"] = vl.vid2location
	return
}

func (vlc *VolumeLayoutCollection) ToVolumeGrowRequest() *master_pb.VolumeGrowRequest {
	return &master_pb.VolumeGrowRequest{
		Collection:  vlc.Collection,
		Replication: vlc.VolumeLayout.rp.String(),
		Ttl:         vlc.VolumeLayout.ttl.String(),
		DiskType:    vlc.VolumeLayout.diskType.String(),
	}
}

func (vl *VolumeLayout) Stats() *VolumeLayoutStats {
	vl.accessLock.RLock()
	defer vl.accessLock.RUnlock()

	ret := &VolumeLayoutStats{}

	freshThreshold := time.Now().Unix() - 60

	for vid, vll := range vl.vid2location {
		size, fileCount := vll.Stats(vid, freshThreshold)
		ret.FileCount += uint64(fileCount)
		ret.UsedSize += size * uint64(vll.Length())
		if vl.readonlyVolumes.IsTrue(vid) {
			ret.TotalSize += size * uint64(vll.Length())
		} else {
			ret.TotalSize += vl.volumeSizeLimit * uint64(vll.Length())
		}
	}

	return ret
}
