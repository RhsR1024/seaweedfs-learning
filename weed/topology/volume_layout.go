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

// RegisterVolume 注册卷到 VolumeLayout
// 使用场景：
//   1. Volume Server 启动时注册现有卷
//   2. Master 通过心跳发现新创建的卷
//   3. 卷副本状态更新时重新注册
//
// 执行流程：
//   1. 将卷添加到 vid2location 映射
//   2. 检查卷是否为只读状态
//   3. 检查卷是否超过大小限制
//   4. 更新可写卷列表
//
// 参数:
//   - v: 卷信息
//   - dn: 卷所在的数据节点
func (vl *VolumeLayout) RegisterVolume(v *storage.VolumeInfo, dn *DataNode) {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()

	// 延迟执行：记录卷是否超过大小限制
	defer vl.rememberOversizedVolume(v, dn)

	// 【步骤 1：添加卷到位置映射】
	// 如果卷不存在，创建新的位置列表
	if _, ok := vl.vid2location[v.Id]; !ok {
		vl.vid2location[v.Id] = NewVolumeLocationList()
	}
	// 添加或更新节点位置
	vl.vid2location[v.Id].Set(dn)

	// 【步骤 2：检查所有副本的状态】
	// 遍历卷的所有副本，检查是否都是可写的
	for _, dn := range vl.vid2location[v.Id].list {
		if vInfo, err := dn.GetVolumesById(v.Id); err == nil {
			if vInfo.ReadOnly {
				// 发现只读副本：从可写列表移除，标记为只读
				glog.V(1).Infof("vid %d removed from writable", v.Id)
				vl.removeFromWritable(v.Id)
				vl.readonlyVolumes.Add(v.Id, dn)
				return
			} else {
				// 副本可写：从只读列表移除
				vl.readonlyVolumes.Remove(v.Id, dn)
			}
		} else {
			// 无法获取副本信息（节点可能离线）：从可写列表移除
			glog.V(1).Infof("vid %d removed from writable", v.Id)
			vl.removeFromWritable(v.Id)
			vl.readonlyVolumes.Remove(v.Id, dn)
			return
		}
	}
}

// rememberOversizedVolume 记录卷是否超过大小限制
// 超过限制的卷会被标记为 oversized，不再接受新的写入
//
// 判断逻辑：
//   - 如果 v.Size >= volumeSizeLimit，标记为 oversized
//   - 否则从 oversized 列表移除
func (vl *VolumeLayout) rememberOversizedVolume(v *storage.VolumeInfo, dn *DataNode) {
	if vl.isOversized(v) {
		vl.oversizedVolumes.Add(v.Id, dn)
	} else {
		vl.oversizedVolumes.Remove(v.Id, dn)
	}
}

// UnRegisterVolume 从 VolumeLayout 注销卷
// 使用场景：
//   1. Volume Server 下线
//   2. 卷被删除
//   3. 卷迁移到其他节点
//
// 执行流程：
//   1. 从 vid2location 映射中移除节点
//   2. 从只读卷、过大卷列表中移除
//   3. 确保可写卷列表正确
//   4. 如果没有副本了，删除整个卷记录
//
// 参数:
//   - v: 卷信息
//   - dn: 要移除的数据节点
func (vl *VolumeLayout) UnRegisterVolume(v *storage.VolumeInfo, dn *DataNode) {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()

	// 【步骤 1：查找卷的位置列表】
	location, ok := vl.vid2location[v.Id]
	if !ok {
		// 卷不存在，直接返回
		return
	}

	// 【步骤 2：从位置列表移除节点】
	if location.Remove(dn) {
		// 【步骤 3：清理状态跟踪】
		vl.readonlyVolumes.Remove(v.Id, dn)
		vl.oversizedVolumes.Remove(v.Id, dn)

		// 【步骤 4：更新可写卷列表】
		// 检查剩余副本是否仍然满足可写条件
		vl.ensureCorrectWritables(v.Id)

		// 【步骤 5：清理空的位置列表】
		// 如果没有副本了，删除卷记录
		if location.Length() == 0 {
			delete(vl.vid2location, v.Id)
		}
	}
}

// EnsureCorrectWritables 确保卷在可写列表中的状态正确
// 使用场景：
//   1. 卷副本状态更新后重新验证可写性
//   2. 定期检查卷状态是否符合可写条件
//
// 执行流程：
//   1. 加锁保护并发访问
//   2. 调用内部 ensureCorrectWritables 进行检查
//
// 参数:
//   - v: 卷信息
func (vl *VolumeLayout) EnsureCorrectWritables(v *storage.VolumeInfo) {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()

	vl.ensureCorrectWritables(v.Id)
}

// ensureCorrectWritables 内部方法：确保卷在可写列表中的状态正确
// 使用场景：
//   - 内部调用，不加锁（调用方已加锁）
//
// 可写条件（必须同时满足三个条件）：
//   1. 副本数充足（enoughCopies）：等于或超过副本策略要求
//   2. 所有副本可写（isAllWritable）：无只读副本
//   3. 未超过大小限制（!isOversized）：卷大小未达到上限
//
// 执行逻辑：
//   - 满足所有条件：添加到可写列表
//   - 不满足任一条件：从可写列表移除，并记录原因
//
// 参数:
//   - vid: 卷 ID
func (vl *VolumeLayout) ensureCorrectWritables(vid needle.VolumeId) {
	// 【检查三个可写条件】
	isEnoughCopies := vl.enoughCopies(vid)
	isAllWritable := vl.isAllWritable(vid)
	isOversizedVolume := vl.oversizedVolumes.IsTrue(vid)

	if isEnoughCopies && isAllWritable && !isOversizedVolume {
		// 满足所有条件：设置为可写
		vl.setVolumeWritable(vid)
	} else {
		// 不满足条件：记录原因并移除可写状态
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

// isAllWritable 检查卷的所有副本是否都可写
// 使用场景：
//   - 判断卷是否应该加入可写列表
//   - 任一副本只读都会导致整个卷被标记为只读
//
// 判断逻辑：
//   1. 遍历卷的所有副本节点
//   2. 检查每个副本的 ReadOnly 状态
//   3. 任一副本只读返回 false
//   4. 所有副本可写返回 true
//
// 参数:
//   - vid: 卷 ID
// 返回:
//   - bool: true 表示所有副本可写，false 表示至少一个副本只读
func (vl *VolumeLayout) isAllWritable(vid needle.VolumeId) bool {
	if location, ok := vl.vid2location[vid]; ok {
		// 遍历所有副本节点
		for _, dn := range location.list {
			if v, getError := dn.GetVolumesById(vid); getError == nil {
				if v.ReadOnly {
					// 发现只读副本，整个卷不可写
					return false
				}
			}
		}
	} else {
		// 卷不存在，不可写
		return false
	}

	// 所有副本都可写
	return true
}

// isOversized 判断卷是否超过大小限制
// 使用场景：
//   - 决定卷是否应该继续接受新写入
//   - 超过限制的卷会被标记为 oversized
//
// 判断逻辑：
//   - v.Size >= volumeSizeLimit 时返回 true
//
// 参数:
//   - v: 卷信息
// 返回:
//   - bool: true 表示超过限制，false 表示未超过
func (vl *VolumeLayout) isOversized(v *storage.VolumeInfo) bool {
	return uint64(v.Size) >= vl.volumeSizeLimit
}

// isCrowdedVolume 判断卷是否拥挤（接近容量限制）
// 使用场景：
//   - 决定是否应该创建新卷
//   - 拥挤的卷会触发 Volume Growth 策略
//
// 判断逻辑：
//   - 卷大小超过 volumeSizeLimit * Threshold 时返回 true
//   - Threshold 通常设置为 0.9（90%）
//
// 示例：
//   - volumeSizeLimit = 1GB, Threshold = 0.9
//   - 卷大小 > 900MB 时标记为 crowded
//
// 参数:
//   - v: 卷信息
// 返回:
//   - bool: true 表示拥挤，false 表示不拥挤
func (vl *VolumeLayout) isCrowdedVolume(v *storage.VolumeInfo) bool {
	return float64(v.Size) > float64(vl.volumeSizeLimit)*VolumeGrowStrategy.Threshold
}

// isWritable 综合判断卷是否可写
// 使用场景：
//   - 一次性检查卷的所有可写条件
//
// 可写条件（必须同时满足）：
//   1. 未超过大小限制（!isOversized）
//   2. 版本匹配当前版本（Version == CurrentVersion）
//   3. 非只读状态（!ReadOnly）
//
// 参数:
//   - v: 卷信息
// 返回:
//   - bool: true 表示可写，false 表示不可写
func (vl *VolumeLayout) isWritable(v *storage.VolumeInfo) bool {
	return !vl.isOversized(v) &&
		v.Version == needle.GetCurrentVersion() &&
		!v.ReadOnly
}

// isEmpty 判断 VolumeLayout 是否为空（没有任何卷）
// 使用场景：
//   - 清理空的 VolumeLayout
//   - 检查是否需要初始化卷
//
// 判断逻辑：
//   - vid2location 映射为空时返回 true
//
// 返回:
//   - bool: true 表示空，false 表示有卷
func (vl *VolumeLayout) isEmpty() bool {
	vl.accessLock.RLock()
	defer vl.accessLock.RUnlock()

	return len(vl.vid2location) == 0
}

// Lookup 查找卷的所有副本节点
// 使用场景：
//   - 客户端查询卷所在的服务器位置
//   - Master 转发读/写请求到正确的 Volume Server
//   - 副本同步和数据迁移
//
// 返回结果：
//   - 返回存储该卷所有副本的 DataNode 列表
//   - 列表中的第一个节点通常是主副本（Master Volume）
//
// 参数:
//   - vid: 卷 ID
// 返回:
//   - []*DataNode: 副本节点列表，卷不存在时返回 nil
func (vl *VolumeLayout) Lookup(vid needle.VolumeId) []*DataNode {
	vl.accessLock.RLock()
	defer vl.accessLock.RUnlock()

	if location := vl.vid2location[vid]; location != nil {
		return location.list
	}
	return nil
}

// ListVolumeServers 列出所有拥有卷的服务器节点
// 使用场景：
//   - 获取 VolumeLayout 管理的所有 Volume Server
//   - 集群健康检查和拓扑管理
//   - 容量统计和负载均衡
//
// 返回结果：
//   - 返回所有拥有该配置卷的 DataNode 列表
//   - 同一节点可能出现多次（如果它有多个卷）
//
// 返回:
//   - []*DataNode: 所有卷服务器节点列表
func (vl *VolumeLayout) ListVolumeServers() (nodes []*DataNode) {
	vl.accessLock.RLock()
	defer vl.accessLock.RUnlock()

	for _, location := range vl.vid2location {
		nodes = append(nodes, location.list...)
	}
	return
}

// PickForWrite 为写入请求选择合适的卷
// 使用场景：
//   - 文件上传时选择目标卷
//   - 支持按数据中心、机架、节点进行选择
//
// 选择策略：
//   1. 无位置限制：随机选择一个可写卷
//   2. 有位置限制：按条件筛选后随机选择
//
// 参数:
//   - count: 预期写入的 Needle 数量
//   - option: 卷选择选项（数据中心、机架、节点限制）
//
// 返回值:
//   - vid: 选中的卷 ID
//   - counter: 返回 count（预留用于容量预留机制）
//   - locationList: 卷的副本位置列表
//   - shouldGrow: 是否需要创建新卷（无可写卷时为 true）
//   - err: 错误信息
func (vl *VolumeLayout) PickForWrite(count uint64, option *VolumeGrowOption) (vid needle.VolumeId, counter uint64, locationList *VolumeLocationList, shouldGrow bool, err error) {
	vl.accessLock.RLock()
	defer vl.accessLock.RUnlock()

	// 【检查是否有可写卷】
	lenWriters := len(vl.writables)
	if lenWriters <= 0 {
		// 没有可写卷，需要触发卷增长
		return 0, 0, nil, true, fmt.Errorf("%s", NoWritableVolumes)
	}

	// 【场景 1：无位置限制，随机选择】
	if option.DataCenter == "" && option.Rack == "" && option.DataNode == "" {
		vid := vl.writables[rand.IntN(lenWriters)]
		locationList = vl.vid2location[vid]
		if locationList == nil || len(locationList.list) == 0 {
			// 异常情况：卷在可写列表但没有副本位置
			return 0, 0, nil, false, fmt.Errorf("Strangely vid %s is on no machine!", vid.String())
		}
		return vid, count, locationList.Copy(), false, nil
	}

	// 【场景 2：有位置限制，按条件筛选】
	// 克隆可写卷列表（避免修改原列表）
	writables := make([]needle.VolumeId, len(vl.writables))
	copy(writables, vl.writables)

	// 随机打乱顺序（负载均衡）
	rand.Shuffle(len(writables), func(i, j int) {
		writables[i], writables[j] = writables[j], writables[i]
	})

	// 遍历可写卷，查找符合位置要求的卷
	for _, writableVolumeId := range writables {
		volumeLocationList := vl.vid2location[writableVolumeId]
		for _, dn := range volumeLocationList.list {
			// 检查数据中心限制
			if option.DataCenter != "" && dn.GetDataCenter().Id() != NodeId(option.DataCenter) {
				continue
			}
			// 检查机架限制
			if option.Rack != "" && dn.GetRack().Id() != NodeId(option.Rack) {
				continue
			}
			// 检查节点限制
			if option.DataNode != "" && dn.Id() != NodeId(option.DataNode) {
				continue
			}
			// 找到符合条件的卷
			vid, locationList, counter = writableVolumeId, volumeLocationList.Copy(), count
			return
		}
	}

	// 没有找到符合位置限制的可写卷，需要创建新卷
	return vid, count, locationList, true, fmt.Errorf("%s in DataCenter:%v Rack:%v DataNode:%v", NoWritableVolumes, option.DataCenter, option.Rack, option.DataNode)
}

// HasGrowRequest 检查是否有待处理的卷增长请求
// 使用场景：
//   - 避免并发创建多个卷（防止过度分配）
//   - 检查卷增长任务是否正在进行
//
// 返回:
//   - bool: true 表示有待处理的增长请求
func (vl *VolumeLayout) HasGrowRequest() bool {
	return vl.growRequest.Load()
}

// AddGrowRequest 标记卷增长请求开始
// 使用场景：
//   - 卷增长任务开始前调用
//   - 防止并发创建多个卷
func (vl *VolumeLayout) AddGrowRequest() {
	vl.growRequest.Store(true)
}

// DoneGrowRequest 标记卷增长请求完成
// 使用场景：
//   - 卷增长任务完成后调用
//   - 允许后续的卷增长请求
func (vl *VolumeLayout) DoneGrowRequest() {
	vl.growRequest.Store(false)
}

// SetLastGrowCount 设置上次增长创建的卷数量
// 使用场景：
//   - 记录卷增长历史，用于后续增长决策
//   - Volume Growth 策略参考
//
// 参数:
//   - count: 上次创建的卷数量
func (vl *VolumeLayout) SetLastGrowCount(count uint32) {
	if vl.lastGrowCount.Load() != count && count != 0 {
		vl.lastGrowCount.Store(count)
	}
}

// GetLastGrowCount 获取上次增长创建的卷数量
// 使用场景：
//   - Volume Growth 策略决策
//   - 统计和监控
//
// 返回:
//   - uint32: 上次创建的卷数量
func (vl *VolumeLayout) GetLastGrowCount() uint32 {
	return vl.lastGrowCount.Load()
}

// ShouldGrowVolumes 判断是否应该创建新卷
// 使用场景：
//   - 定期检查卷容量
//   - 决定是否触发 Volume Growth
//
// 判断逻辑：
//   - 可写卷数量 <= 拥挤卷数量时返回 true
//   - 意味着大部分可写卷都接近容量限制，应该创建新卷
//
// 返回:
//   - bool: true 表示应该创建新卷
func (vl *VolumeLayout) ShouldGrowVolumes() bool {
	writable, crowded := vl.GetWritableVolumeCount()
	return writable <= crowded
}

// ShouldGrowVolumesByDcAndRack 判断指定数据中心和机架是否应该创建新卷
// 使用场景：
//   - 按数据中心、机架维度决策卷增长
//   - 确保每个位置都有足够的可写卷
//
// 判断逻辑：
//   - 遍历指定位置的所有可写卷
//   - 如果找到至少一个不拥挤的卷，返回 false（不需要创建新卷）
//   - 如果所有卷都拥挤或不在该位置，返回 true（需要创建新卷）
//
// 参数:
//   - writables: 可写卷 ID 列表
//   - dcId: 数据中心 ID
//   - rackId: 机架 ID
//
// 返回:
//   - bool: true 表示应该在该位置创建新卷
func (vl *VolumeLayout) ShouldGrowVolumesByDcAndRack(writables *[]needle.VolumeId, dcId NodeId, rackId NodeId) bool {
	for _, v := range *writables {
		for _, dn := range vl.Lookup(v) {
			// 检查节点是否在指定的数据中心和机架
			if dn.GetDataCenter().Id() == dcId && dn.GetRack().Id() == rackId {
				// 检查卷是否拥挤
				if info, err := dn.GetVolumesById(v); err == nil && !vl.isCrowdedVolume(&info) {
					// 找到不拥挤的卷，不需要创建新卷
					return false
				}
			}
		}
	}
	// 没有找到不拥挤的卷，需要创建新卷
	return true
}

// GetWritableVolumeCount 获取可写卷和拥挤卷的数量
// 使用场景：
//   - 统计卷状态
//   - 决策是否需要创建新卷
//
// 返回值:
//   - active: 可写卷数量
//   - crowded: 拥挤卷数量（接近容量限制）
func (vl *VolumeLayout) GetWritableVolumeCount() (active, crowded int) {
	vl.accessLock.RLock()
	defer vl.accessLock.RUnlock()
	return len(vl.writables), len(vl.crowded)
}

// CloneWritableVolumes 克隆可写卷列表
// 使用场景：
//   - 在不持有锁的情况下遍历可写卷
//   - 避免长时间持有锁影响性能
//
// 返回:
//   - writables: 可写卷 ID 列表的副本
func (vl *VolumeLayout) CloneWritableVolumes() (writables []needle.VolumeId) {
	vl.accessLock.RLock()
	writables = make([]needle.VolumeId, len(vl.writables))
	copy(writables, vl.writables)
	vl.accessLock.RUnlock()
	return writables
}

// removeFromWritable 从可写卷列表移除指定卷
// 使用场景：
//   - 卷变为只读
//   - 卷超过大小限制
//   - 卷副本数不足
//   - 卷节点下线
//
// 执行流程：
//   1. 在可写列表中查找卷
//   2. 从拥挤列表移除（如果存在）
//   3. 从可写列表移除
//
// 参数:
//   - vid: 卷 ID
// 返回:
//   - bool: true 表示成功移除，false 表示卷不在可写列表
func (vl *VolumeLayout) removeFromWritable(vid needle.VolumeId) bool {
	// 查找卷在列表中的位置
	toDeleteIndex := -1
	for k, id := range vl.writables {
		if id == vid {
			toDeleteIndex = k
			break
		}
	}

	// 从拥挤列表移除
	vl.removeFromCrowded(vid)

	// 从可写列表移除
	if toDeleteIndex >= 0 {
		glog.V(0).Infoln("Volume", vid, "becomes unwritable")
		vl.writables = append(vl.writables[0:toDeleteIndex], vl.writables[toDeleteIndex+1:]...)
		return true
	}
	return false
}

// setVolumeWritable 将卷添加到可写列表
// 使用场景：
//   - 卷注册时满足可写条件
//   - 卷从只读恢复为可写
//   - 卷副本数达到要求
//
// 执行流程：
//   1. 检查卷是否已在可写列表（避免重复）
//   2. 添加到可写列表
//
// 参数:
//   - vid: 卷 ID
// 返回:
//   - bool: true 表示成功添加，false 表示卷已在列表
func (vl *VolumeLayout) setVolumeWritable(vid needle.VolumeId) bool {
	// 检查卷是否已在可写列表
	for _, v := range vl.writables {
		if v == vid {
			return false
		}
	}
	// 添加到可写列表
	glog.V(0).Infoln("Volume", vid, "becomes writable")
	vl.writables = append(vl.writables, vid)
	return true
}

// SetVolumeReadOnly 将卷设置为只读
// 使用场景：
//   - Volume Server 主动标记卷为只读
//   - 磁盘空间不足或错误
//   - 管理员手动设置只读
//
// 执行流程：
//   1. 将卷添加到只读卷跟踪器
//   2. 从可写列表移除
//
// 参数:
//   - dn: 数据节点
//   - vid: 卷 ID
// 返回:
//   - bool: true 表示成功移除可写状态
func (vl *VolumeLayout) SetVolumeReadOnly(dn *DataNode, vid needle.VolumeId) bool {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()

	if _, ok := vl.vid2location[vid]; ok {
		// 添加到只读卷列表
		vl.readonlyVolumes.Add(vid, dn)
		// 从可写列表移除
		return vl.removeFromWritable(vid)
	}
	return true
}

// SetVolumeWritable 将卷设置为可写
// 使用场景：
//   - 卷从只读恢复
//   - 磁盘空间问题解决
//   - 管理员手动设置可写
//
// 执行流程：
//   1. 从只读卷列表移除
//   2. 检查副本数是否充足
//   3. 满足条件时添加到可写列表
//
// 参数:
//   - dn: 数据节点
//   - vid: 卷 ID
// 返回:
//   - bool: true 表示成功设置为可写
func (vl *VolumeLayout) SetVolumeWritable(dn *DataNode, vid needle.VolumeId) bool {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()

	if _, ok := vl.vid2location[vid]; ok {
		// 从只读卷列表移除
		vl.readonlyVolumes.Remove(vid, dn)
	}

	// 检查副本数是否充足
	if vl.enoughCopies(vid) {
		return vl.setVolumeWritable(vid)
	}
	return false
}

// SetVolumeUnavailable 标记卷在指定节点上不可用
// 使用场景：
//   - Volume Server 下线
//   - 卷数据损坏
//   - 网络分区导致节点不可达
//
// 执行流程：
//   1. 从位置映射移除该节点
//   2. 从只读卷、过大卷列表移除
//   3. 检查副本数是否仍然充足
//   4. 副本数不足时从可写列表移除
//
// 参数:
//   - dn: 数据节点
//   - vid: 卷 ID
// 返回:
//   - bool: true 表示移除了可写状态
func (vl *VolumeLayout) SetVolumeUnavailable(dn *DataNode, vid needle.VolumeId) bool {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()

	if location, ok := vl.vid2location[vid]; ok {
		// 从位置列表移除节点
		if location.Remove(dn) {
			// 从状态跟踪列表移除
			vl.readonlyVolumes.Remove(vid, dn)
			vl.oversizedVolumes.Remove(vid, dn)

			// 检查副本数是否仍然充足
			if location.Length() < vl.rp.GetCopyCount() {
				glog.V(0).Infoln("Volume", vid, "has", location.Length(), "replica, less than required", vl.rp.GetCopyCount())
				return vl.removeFromWritable(vid)
			}
		}
	}
	return false
}

// SetVolumeAvailable 标记卷在指定节点上可用
// 使用场景：
//   - Volume Server 重新上线
//   - 卷数据恢复
//   - 网络问题解决
//
// 执行流程：
//   1. 将节点添加到位置映射
//   2. 检查卷是否只读或满容
//   3. 检查副本数是否充足
//   4. 满足条件时添加到可写列表
//
// 参数:
//   - dn: 数据节点
//   - vid: 卷 ID
//   - isReadOnly: 卷是否只读
//   - isFullCapacity: 卷是否满容
// 返回:
//   - bool: true 表示成功设置为可写
func (vl *VolumeLayout) SetVolumeAvailable(dn *DataNode, vid needle.VolumeId, isReadOnly, isFullCapacity bool) bool {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()

	// 获取卷信息
	vInfo, err := dn.GetVolumesById(vid)
	if err != nil {
		return false
	}

	// 添加节点到位置映射
	vl.vid2location[vid].Set(dn)

	// 检查卷是否可写
	if vInfo.ReadOnly || isReadOnly || isFullCapacity {
		return false
	}

	// 检查副本数是否充足
	if vl.enoughCopies(vid) {
		return vl.setVolumeWritable(vid)
	}
	return false
}

// enoughCopies 检查卷的副本数是否充足
// 使用场景：
//   - 决定卷是否应该加入可写列表
//   - 验证副本策略是否满足
//
// 判断逻辑：
//   1. replicationAsMin=false: 副本数必须等于 ReplicaPlacement 要求
//   2. replicationAsMin=true: 副本数可以大于或等于要求（允许超额副本）
//
// 参数:
//   - vid: 卷 ID
// 返回:
//   - bool: true 表示副本数充足
func (vl *VolumeLayout) enoughCopies(vid needle.VolumeId) bool {
	locations := vl.vid2location[vid].Length()
	desired := vl.rp.GetCopyCount()
	return locations == desired || (vl.replicationAsMin && locations > desired)
}

// SetVolumeCapacityFull 标记卷容量已满
// 使用场景：
//   - Volume Server 报告卷已满
//   - 卷大小达到 volumeSizeLimit
//   - 防止继续向满容卷写入
//
// 执行流程：
//   1. 从可写列表移除卷
//   2. 记录日志
//
// 参数:
//   - vid: 卷 ID
// 返回:
//   - bool: true 表示卷之前是可写的，false 表示之前已不可写
func (vl *VolumeLayout) SetVolumeCapacityFull(vid needle.VolumeId) bool {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()

	wasWritable := vl.removeFromWritable(vid)
	if wasWritable {
		glog.V(0).Infof("Volume %d reaches full capacity.", vid)
	}
	return wasWritable
}

// removeFromCrowded 从拥挤卷列表移除指定卷
// 使用场景：
//   - 卷被删除或下线
//   - 卷被清理（Vacuum）后容量降低
//   - 内部方法，调用方已加锁
//
// 参数:
//   - vid: 卷 ID
func (vl *VolumeLayout) removeFromCrowded(vid needle.VolumeId) {
	if _, ok := vl.crowded[vid]; ok {
		glog.V(0).Infoln("Volume", vid, "becomes uncrowded")
		delete(vl.crowded, vid)
	}
}

// setVolumeCrowded 将卷添加到拥挤列表
// 使用场景：
//   - 卷使用率超过阈值（如 90%）
//   - 内部方法，调用方已加锁
//
// 参数:
//   - vid: 卷 ID
func (vl *VolumeLayout) setVolumeCrowded(vid needle.VolumeId) {
	if _, ok := vl.crowded[vid]; !ok {
		vl.crowded[vid] = struct{}{}
		glog.V(0).Infoln("Volume", vid, "becomes crowded")
	}
}

// SetVolumeCrowded 将卷标记为拥挤
// 使用场景：
//   - Volume Server 报告卷接近容量限制
//   - 定期检查卷使用率
//
// 注意事项：
//   - 使用 RLock 而非 Lock，因为 delete 操作由 Lock 保护
//   - 按顺序调用，RLock 已足够安全
//
// 参数:
//   - vid: 卷 ID
func (vl *VolumeLayout) SetVolumeCrowded(vid needle.VolumeId) {
	// 使用读锁而非写锁：
	// 因为删除操作由 accessLock.Lock() 保护
	// 且总是按顺序调用，RLock() 已足够安全
	vl.accessLock.RLock()
	defer vl.accessLock.RUnlock()

	vl.setVolumeCrowded(vid)
}

// VolumeLayoutInfo VolumeLayout 的 JSON 表示
// 用于 API 输出和监控展示
type VolumeLayoutInfo struct {
	Replication string            `json:"replication"` // 副本策略（如 "000", "001"）
	TTL         string            `json:"ttl"`         // 生存时间
	Writables   []needle.VolumeId `json:"writables"`   // 可写卷 ID 列表
	Collection  string            `json:"collection"`  // 集合名称
	DiskType    string            `json:"diskType"`    // 磁盘类型（HDD/SSD）
}

// ToInfo 将 VolumeLayout 转换为 JSON 信息
// 使用场景：
//   - API 响应（/dir/status, /cluster/status）
//   - 监控和日志输出
//   - 调试和诊断
//
// 返回:
//   - info: VolumeLayout 的 JSON 表示
func (vl *VolumeLayout) ToInfo() (info VolumeLayoutInfo) {
	info.Replication = vl.rp.String()
	info.TTL = vl.ttl.String()
	info.Writables = vl.writables
	info.DiskType = vl.diskType.ReadableString()
	// 注：locations 信息未包含（避免输出过大）
	// 如需查询特定卷位置，使用 Lookup 方法
	return
}

// ToVolumeGrowRequest 将 VolumeLayoutCollection 转换为卷增长请求
// 使用场景：
//   - 触发卷增长时创建请求
//   - 发送到 Master 请求创建新卷
//
// 返回:
//   - *master_pb.VolumeGrowRequest: gRPC 卷增长请求
func (vlc *VolumeLayoutCollection) ToVolumeGrowRequest() *master_pb.VolumeGrowRequest {
	return &master_pb.VolumeGrowRequest{
		Collection:  vlc.Collection,
		Replication: vlc.VolumeLayout.rp.String(),
		Ttl:         vlc.VolumeLayout.ttl.String(),
		DiskType:    vlc.VolumeLayout.diskType.String(),
	}
}

// Stats 计算 VolumeLayout 的统计信息
// 使用场景：
//   - 容量统计和监控
//   - 集群资源规划
//   - 告警和健康检查
//
// 统计内容：
//   - TotalSize: 总容量（只读卷按实际大小，可写卷按限制大小）
//   - UsedSize: 已使用容量（所有副本的实际大小总和）
//   - FileCount: 文件总数
//
// 注意事项：
//   - 只统计最近 60 秒内有心跳的卷（freshThreshold）
//   - 副本会被重复计数（UsedSize 包含所有副本）
//
// 返回:
//   - *VolumeLayoutStats: 统计结果
func (vl *VolumeLayout) Stats() *VolumeLayoutStats {
	vl.accessLock.RLock()
	defer vl.accessLock.RUnlock()

	ret := &VolumeLayoutStats{}

	// freshThreshold: 60 秒内有心跳的卷才计入统计
	// 避免离线卷影响统计结果
	freshThreshold := time.Now().Unix() - 60

	// 遍历所有卷的位置列表
	for vid, vll := range vl.vid2location {
		// 获取卷的大小和文件数量
		size, fileCount := vll.Stats(vid, freshThreshold)
		ret.FileCount += uint64(fileCount)

		// 已使用大小 = 实际大小 * 副本数
		ret.UsedSize += size * uint64(vll.Length())

		// 总容量计算：
		// - 只读卷：按实际大小计算（不会再增长）
		// - 可写卷：按限制大小计算（预留空间）
		if vl.readonlyVolumes.IsTrue(vid) {
			ret.TotalSize += size * uint64(vll.Length())
		} else {
			ret.TotalSize += vl.volumeSizeLimit * uint64(vll.Length())
		}
	}

	return ret
}
