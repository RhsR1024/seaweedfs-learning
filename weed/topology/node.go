package topology

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// NodeId 是拓扑节点的唯一标识符
// 不同类型节点的 ID 格式：
//   - Topology: "topo" 或自定义名称
//   - DataCenter: 数据中心名称（如 "dc1", "us-west"）
//   - Rack: 机架名称（如 "rack1", "row-A"）
//   - DataNode: "ip:port" 格式（如 "192.168.1.10:8080"）
//   - Disk: 磁盘类型（如 "hdd", "ssd", "nvme"）
type NodeId string

// CapacityReservation 表示临时的容量预留
// 用于解决并发创建 Volume 时的竞争条件
//
// 问题场景：
//   多个客户端同时请求创建 Volume，Master 可能会将它们分配到同一个 DataNode，
//   导致该节点实际容量不足。使用容量预留机制可以在分配前先"锁定"容量。
//
// 工作流程：
//   1. 查询可用容量时，扣除已预留的容量
//   2. 分配 Volume 前，创建预留记录
//   3. Volume 创建成功后，释放预留
//   4. 预留会在一定时间后自动过期（默认 5 分钟）
type CapacityReservation struct {
	reservationId string         // 预留 ID，全局唯一
	diskType      types.DiskType // 磁盘类型（如 "hdd", "ssd"）
	count         int64          // 预留的 Volume 数量
	createdAt     time.Time      // 创建时间，用于过期清理
}

// CapacityReservations 管理节点的容量预留
// 每个节点（DataNode/Rack/DataCenter/Topology）都有一个 CapacityReservations
// 用于在分配 Volume 前临时"锁定"容量，防止并发分配导致的超额问题
//
// 数据结构：
//   - reservations: 所有预留记录（key 是预留 ID）
//   - reservedCounts: 按磁盘类型聚合的已预留数量，用于快速查询
//
// 线程安全：使用读写锁保护并发访问
type CapacityReservations struct {
	sync.RWMutex
	reservations   map[string]*CapacityReservation  // 所有预留记录
	reservedCounts map[types.DiskType]int64         // 按磁盘类型聚合的已预留数量
}

// newCapacityReservations 创建新的容量预留管理器
func newCapacityReservations() *CapacityReservations {
	return &CapacityReservations{
		reservations:   make(map[string]*CapacityReservation),
		reservedCounts: make(map[types.DiskType]int64),
	}
}

// addReservation 添加一个容量预留记录
// 使用写锁保护，确保线程安全
//
// 参数：
//   - diskType: 磁盘类型
//   - count: 预留的 Volume 数量
//
// 返回：
//   - string: 预留 ID，格式为 "diskType-count-timestamp-random"
func (cr *CapacityReservations) addReservation(diskType types.DiskType, count int64) string {
	cr.Lock()
	defer cr.Unlock()

	return cr.doAddReservation(diskType, count)
}

// removeReservation 移除一个容量预留记录
// 使用写锁保护，确保线程安全
//
// 参数：
//   - reservationId: 预留 ID
//
// 返回：
//   - bool: 是否成功移除（不存在返回 false）
//
// 副作用：
//   - 从 reservations map 中删除记录
//   - 更新 reservedCounts（减少已预留数量）
func (cr *CapacityReservations) removeReservation(reservationId string) bool {
	cr.Lock()
	defer cr.Unlock()

	if reservation, exists := cr.reservations[reservationId]; exists {
		delete(cr.reservations, reservationId)
		cr.decrementCount(reservation.diskType, reservation.count)
		return true
	}
	return false
}

// getReservedCount 获取指定磁盘类型的已预留数量
// 使用读锁保护，允许并发读取
//
// 参数：
//   - diskType: 磁盘类型
//
// 返回：
//   - int64: 已预留的 Volume 数量
func (cr *CapacityReservations) getReservedCount(diskType types.DiskType) int64 {
	cr.RLock()
	defer cr.RUnlock()

	return cr.reservedCounts[diskType]
}

// decrementCount 减少已预留计数的辅助函数
// 假设调用者已持有锁
//
// 参数：
//   - diskType: 磁盘类型
//   - count: 要减少的数量
//
// 副作用：
//   - 更新 reservedCounts
//   - 如果计数降为 0 或负数，从 map 中删除该项（防止 map 无限增长）
func (cr *CapacityReservations) decrementCount(diskType types.DiskType, count int64) {
	cr.reservedCounts[diskType] -= count
	// 清理零值或负值，防止 map 无限增长
	if cr.reservedCounts[diskType] <= 0 {
		delete(cr.reservedCounts, diskType)
	}
}

// doAddReservation 添加预留记录的内部实现
// 假设调用者已持有锁
//
// 参数：
//   - diskType: 磁盘类型
//   - count: 预留数量
//
// 返回：
//   - string: 生成的预留 ID
//
// 预留 ID 格式：
//   "diskType-count-timestamp-random"
//   例如："hdd-5-1234567890123456789-9876543210"
//
// 设计说明：
//   - 使用时间戳和随机数确保全局唯一性
//   - 包含磁盘类型和数量，便于调试
func (cr *CapacityReservations) doAddReservation(diskType types.DiskType, count int64) string {
	now := time.Now()
	// 生成唯一的预留 ID
	reservationId := fmt.Sprintf("%s-%d-%d-%d", diskType, count, now.UnixNano(), rand.Int64())

	// 创建预留记录
	cr.reservations[reservationId] = &CapacityReservation{
		reservationId: reservationId,
		diskType:      diskType,
		count:         count,
		createdAt:     now,
	}

	// 更新聚合计数
	cr.reservedCounts[diskType] += count

	return reservationId
}

// tryReserveAtomic 原子地检查可用空间并预留
// 在持有锁的情况下，原子地完成"检查空间→预留"操作，避免竞态条件
//
// 参数：
//   - diskType: 磁盘类型
//   - count: 需要预留的数量
//   - availableSpaceFunc: 获取可用空间的函数（在锁内调用）
//
// 返回：
//   - reservationId: 预留 ID（失败时返回空字符串）
//   - success: 是否预留成功
//
// 工作原理：
//   1. 获取写锁
//   2. 调用 availableSpaceFunc() 获取当前可用空间
//   3. 扣除已预留的空间
//   4. 检查剩余空间是否足够
//   5. 如果足够，创建预留记录并返回 true
//   6. 如果不足，返回 false
//
// 关键点：
//   - 整个过程在锁保护下进行，保证原子性
//   - 避免了"检查后使用"（TOCTOU）问题
func (cr *CapacityReservations) tryReserveAtomic(diskType types.DiskType, count int64, availableSpaceFunc func() int64) (reservationId string, success bool) {
	cr.Lock()
	defer cr.Unlock()

	// 在锁内检查可用空间
	currentReserved := cr.reservedCounts[diskType]
	availableSpace := availableSpaceFunc() - currentReserved

	if availableSpace >= count {
		// 原子地创建并添加预留
		return cr.doAddReservation(diskType, count), true
	}

	return "", false
}

// cleanExpiredReservations 清理过期的预留记录
// 遍历所有预留记录，删除超过过期时间的记录
//
// 参数：
//   - expirationDuration: 过期时间间隔（如 5 分钟）
//
// 工作原理：
//   1. 获取写锁
//   2. 遍历所有预留记录
//   3. 检查创建时间是否超过过期时间
//   4. 删除过期记录并更新计数
//
// 调用时机：
//   - 每次调用 TryReserveCapacity 时
//   - 定期后台任务
//
// 为什么需要过期清理：
//   - 防止预留泄漏（客户端崩溃、网络故障等导致未释放）
//   - 确保系统最终一致性
func (cr *CapacityReservations) cleanExpiredReservations(expirationDuration time.Duration) {
	cr.Lock()
	defer cr.Unlock()

	now := time.Now()
	for id, reservation := range cr.reservations {
		// 检查是否过期
		if now.Sub(reservation.createdAt) > expirationDuration {
			// 删除预留记录
			delete(cr.reservations, id)
			// 更新聚合计数
			cr.decrementCount(reservation.diskType, reservation.count)
			// 记录日志
			glog.V(1).Infof("Cleaned up expired capacity reservation: %s", id)
		}
	}
}

// Node 是拓扑树中所有节点的通用接口
// 实现类型：Topology, DataCenter, Rack, DataNode, Disk
//
// 拓扑层次结构：
//   Topology (顶层，全局视图)
//     └─ DataCenter (数据中心，地理位置)
//          └─ Rack (机架，物理位置)
//               └─ DataNode (Volume Server，物理机器)
//                    └─ Disk (磁盘，存储介质)
//
// 设计模式：
//   - 组合模式（Composite Pattern）：统一处理单个节点和节点树
//   - 树形结构：支持递归操作和统计聚合
//
// 核心功能：
//   1. 容量管理：查询可用空间、预留容量
//   2. Volume 分配：递归查找合适的 DataNode
//   3. 统计聚合：向上传播磁盘使用情况
//   4. 拓扑维护：添加/删除子节点、死节点检测
type Node interface {
	// ===== 基础信息 =====

	// Id 返回节点的唯一标识符
	Id() NodeId

	// String 返回节点的可读字符串表示（包含完整拓扑路径）
	String() string

	// GetValue 返回节点的实际值（类型断言用）
	// 返回 *Topology, *DataCenter, *Rack, *DataNode, 或 *Disk
	GetValue() interface{}

	// ===== 容量查询 =====

	// AvailableSpaceFor 计算可用的 Volume 槽位数（不考虑预留）
	// 参数：
	//   - option: Volume 增长选项（包含磁盘类型、副本策略等）
	// 返回：
	//   - int64: 可用的 Volume 槽位数
	AvailableSpaceFor(option *VolumeGrowOption) int64

	// AvailableSpaceForReservation 计算可用空间（扣除已预留的容量）
	// 参数：
	//   - option: Volume 增长选项
	// 返回：
	//   - int64: 扣除预留后的可用槽位数
	AvailableSpaceForReservation(option *VolumeGrowOption) int64

	// GetDiskUsages 获取磁盘使用统计
	GetDiskUsages() *DiskUsages

	// GetMaxVolumeId 获取该节点及其子树中的最大 VolumeId
	GetMaxVolumeId() needle.VolumeId

	// ===== Volume 预留与分配 =====

	// ReserveOneVolume 在子树中选择一个 DataNode 用于创建 Volume
	// 使用加权随机算法，空间越多的节点被选中概率越大
	// 参数：
	//   - r: 随机偏移量，用于加权选择
	//   - option: Volume 增长选项
	// 返回：
	//   - *DataNode: 被选中的数据节点
	//   - error: 没有可用节点时返回错误
	ReserveOneVolume(r int64, option *VolumeGrowOption) (*DataNode, error)

	// ReserveOneVolumeForReservation 同 ReserveOneVolume，但使用预留感知的容量检查
	// 这个方法会考虑已预留的容量，避免超额分配
	ReserveOneVolumeForReservation(r int64, option *VolumeGrowOption) (*DataNode, error)

	// ===== 容量预留管理（防止并发竞争）=====

	// TryReserveCapacity 尝试原子地预留指定数量的容量
	// 参数：
	//   - diskType: 磁盘类型
	//   - count: 预留的 Volume 数量
	// 返回：
	//   - reservationId: 预留 ID（用于后续释放）
	//   - success: 是否预留成功
	TryReserveCapacity(diskType types.DiskType, count int64) (reservationId string, success bool)

	// ReleaseReservedCapacity 释放已预留的容量
	// 参数：
	//   - reservationId: 预留 ID
	ReleaseReservedCapacity(reservationId string)

	// ===== 统计更新（向上传播）=====

	// UpAdjustDiskUsageDelta 向上调整磁盘使用统计
	// 当子节点的统计发生变化时，需要向上传播到所有祖先节点
	// 参数：
	//   - diskType: 磁盘类型
	//   - diskUsage: 统计增量（可以是负值）
	UpAdjustDiskUsageDelta(diskType types.DiskType, diskUsage *DiskUsageCounts)

	// UpAdjustMaxVolumeId 向上更新最大 VolumeId
	// 参数：
	//   - vid: 新的 VolumeId
	UpAdjustMaxVolumeId(vid needle.VolumeId)

	// ===== 拓扑树管理 =====

	// SetParent 设置父节点
	SetParent(Node)

	// Parent 获取父节点
	Parent() Node

	// Children 获取所有子节点
	Children() []Node

	// LinkChildNode 添加子节点（会触发统计向上传播）
	LinkChildNode(node Node)

	// UnlinkChildNode 移除子节点（会触发负统计向上传播）
	UnlinkChildNode(nodeId NodeId)

	// ===== 节点类型判断 =====

	// IsDataNode 是否是 DataNode 类型
	IsDataNode() bool

	// IsRack 是否是 Rack 类型
	IsRack() bool

	// IsDataCenter 是否是 DataCenter 类型
	IsDataCenter() bool

	// IsLocked 检查节点是否被锁定（用于并发控制）
	IsLocked() bool

	// ===== 集群维护 =====

	// CollectDeadNodeAndFullVolumes 收集死节点和已满的 Volume
	// 递归遍历子树，检查：
	//   1. Volume 是否超过大小限制（需要标记为只读）
	//   2. Volume 是否接近满（需要预创建新 Volume）
	//   3. 节点是否长时间未心跳（死节点）
	// 参数：
	//   - freshThreshHold: 心跳新鲜度阈值（Unix 时间戳）
	//   - volumeSizeLimit: Volume 大小限制（字节）
	//   - growThreshold: Volume 增长阈值（0.0-1.0，如 0.9 表示 90% 满时预创建）
	CollectDeadNodeAndFullVolumes(freshThreshHold int64, volumeSizeLimit uint64, growThreshold float64)
}

// NodeImpl 是 Node 接口的通用实现
// 被 Topology, DataCenter, Rack, DataNode, Disk 嵌入使用
//
// 设计思想：
//   - 提供拓扑树的基本操作（父子关系、统计聚合、加权选择等）
//   - 使用组合模式，让不同类型的节点共享相同的树操作逻辑
//   - 支持并发访问（使用读写锁保护共享状态）
//
// 关键特性：
//   1. 统计聚合：子节点的统计会自动向上传播
//   2. 加权随机选择：根据可用空间选择 DataNode
//   3. 容量预留：防止并发分配时的竞争条件
type NodeImpl struct {
	// 磁盘使用统计（聚合所有子节点）
	// 按磁盘类型分组（hdd, ssd, nvme 等）
	diskUsages *DiskUsages

	// 节点 ID（不同类型节点的 ID 格式不同）
	id NodeId

	// 父节点（Topology 的 parent 为 nil）
	parent Node

	// 读写锁：保护 children map 和其他共享状态
	sync.RWMutex

	// 子节点映射
	// key: 子节点的 ID
	// value: 子节点（可能是 DataCenter, Rack, DataNode, Disk）
	children map[NodeId]Node

	// 该节点及其子树中的最大 VolumeId
	// 用于快速分配新的 VolumeId（避免冲突）
	maxVolumeId needle.VolumeId

	// ===== 节点类型标识 =====

	// 节点类型字符串："Topology", "DataCenter", "Rack", "DataNode", "Disk"
	nodeType string

	// 节点的实际值（指向自身的引用）
	// 用于类型断言：node.GetValue().(*DataNode)
	value interface{}

	// ===== 容量预留（防止并发分配时的竞争条件）=====

	// 容量预留管理器
	// 在分配 Volume 前先"锁定"容量，防止多个请求同时分配到同一节点导致超额
	capacityReservations *CapacityReservations
}

// GetDiskUsages 获取节点的磁盘使用统计
//
// 返回：
//   - *DiskUsages: 磁盘使用统计管理器
func (n *NodeImpl) GetDiskUsages() *DiskUsages {
	return n.diskUsages
}

// PickNodesByWeight 使用加权随机算法选择指定数量的节点
// 空间越多的节点被选中的概率越大，确保负载均衡
//
// 参数：
//   - numberOfNodes: 需要选择的节点数量
//   - option: Volume 增长选项（包含磁盘类型等）
//   - filterFirstNodeFn: 第一个节点的过滤函数（通常用于检查特定条件）
//
// 返回：
//   - firstNode: 第一个被选中的节点（满足 filterFirstNodeFn）
//   - restNodes: 其余被选中的节点列表
//   - err: 错误信息
//
// 算法说明：
//   1. 收集所有有空闲空间的候选节点
//   2. 使用节点的空闲空间作为权重
//   3. 使用加权随机算法选择节点（空间越多，被选中概率越大）
//   4. 第一个节点必须满足 filterFirstNodeFn 的条件
//   5. 其余节点只需要有空闲空间即可
//
// 使用场景：
//   - 选择多个 DataNode 用于副本放置
//   - 在多个 Rack 中分散 Volume
//
// 约束条件：
//   - 第一个节点必须满足 filterFirstNodeFn()
//   - 其余节点必须至少有一个空闲槽位
func (n *NodeImpl) PickNodesByWeight(numberOfNodes int, option *VolumeGrowOption, filterFirstNodeFn func(dn Node) error) (firstNode Node, restNodes []Node, err error) {
	var totalWeights int64
	var errs []string
	n.RLock()
	candidates := make([]Node, 0, len(n.children))
	candidatesWeights := make([]int64, 0, len(n.children))
	//pick nodes which has enough free volumes as candidates, and use free volumes number as node weight.
	for _, node := range n.children {
		if node.AvailableSpaceFor(option) <= 0 {
			continue
		}
		totalWeights += node.AvailableSpaceFor(option)
		candidates = append(candidates, node)
		candidatesWeights = append(candidatesWeights, node.AvailableSpaceFor(option))
	}
	n.RUnlock()
	if len(candidates) < numberOfNodes {
		glog.V(0).Infoln(n.Id(), "failed to pick", numberOfNodes, "from ", len(candidates), "node candidates")
		return nil, nil, errors.New("Not enough data nodes found!")
	}

	//pick nodes randomly by weights, the node picked earlier has higher final weights
	sortedCandidates := make([]Node, 0, len(candidates))
	for i := 0; i < len(candidates); i++ {
		// Break if no more weights available to prevent panic in rand.Int64N
		if totalWeights <= 0 {
			break
		}
		weightsInterval := rand.Int64N(totalWeights)
		lastWeights := int64(0)
		for k, weights := range candidatesWeights {
			if (weightsInterval >= lastWeights) && (weightsInterval < lastWeights+weights) {
				sortedCandidates = append(sortedCandidates, candidates[k])
				candidatesWeights[k] = 0
				totalWeights -= weights
				break
			}
			lastWeights += weights
		}
	}

	restNodes = make([]Node, 0, numberOfNodes-1)
	ret := false
	n.RLock()
	for k, node := range sortedCandidates {
		if err := filterFirstNodeFn(node); err == nil {
			firstNode = node
			if k >= numberOfNodes-1 {
				restNodes = sortedCandidates[:numberOfNodes-1]
			} else {
				restNodes = append(restNodes, sortedCandidates[:k]...)
				restNodes = append(restNodes, sortedCandidates[k+1:numberOfNodes]...)
			}
			ret = true
			break
		} else {
			errs = append(errs, string(node.Id())+":"+err.Error())
		}
	}
	n.RUnlock()
	if !ret {
		return nil, nil, errors.New("No matching data node found! \n" + strings.Join(errs, "\n"))
	}
	return
}

// IsDataNode 检查节点是否是 DataNode 类型
//
// 返回：
//   - bool: 是否是 DataNode
func (n *NodeImpl) IsDataNode() bool {
	return n.nodeType == "DataNode"
}

// IsRack 检查节点是否是 Rack 类型
//
// 返回：
//   - bool: 是否是 Rack
func (n *NodeImpl) IsRack() bool {
	return n.nodeType == "Rack"
}

// IsDataCenter 检查节点是否是 DataCenter 类型
//
// 返回：
//   - bool: 是否是 DataCenter
func (n *NodeImpl) IsDataCenter() bool {
	return n.nodeType == "DataCenter"
}

// IsLocked 检查节点是否被锁定
// 尝试获取读锁来判断节点是否被其他操作锁定
//
// 返回：
//   - bool: true 表示节点被锁定，false 表示未被锁定
//
// 说明：
//   - 如果能立即获取读锁，说明节点未被锁定
//   - 如果无法获取读锁，说明节点被写锁锁定
func (n *NodeImpl) IsLocked() (isTryLock bool) {
	if isTryLock = n.TryRLock(); isTryLock {
		n.RUnlock() // 获取成功，立即释放
	}
	return !isTryLock // 取反：true 表示被锁定
}

// String 返回节点的完整拓扑路径字符串
// 格式：parent:parent:...:id
//
// 返回：
//   - string: 节点的完整路径
//
// 示例：
//   - Topology: "topo"
//   - DataCenter: "topo:dc1"
//   - Rack: "topo:dc1:rack1"
//   - DataNode: "topo:dc1:rack1:192.168.1.10:8080"
//   - Disk: "topo:dc1:rack1:192.168.1.10:8080:hdd"
func (n *NodeImpl) String() string {
	if n.parent != nil {
		return n.parent.String() + ":" + string(n.id) // 递归拼接父节点路径
	}
	return string(n.id) // 根节点（Topology）
}

// Id 返回节点的唯一标识符
//
// 返回：
//   - NodeId: 节点 ID
func (n *NodeImpl) Id() NodeId {
	return n.id
}

// getOrCreateDisk 获取或创建指定磁盘类型的统计对象
//
// 参数：
//   - diskType: 磁盘类型
//
// 返回：
//   - *DiskUsageCounts: 磁盘使用统计对象
func (n *NodeImpl) getOrCreateDisk(diskType types.DiskType) *DiskUsageCounts {
	return n.diskUsages.getOrCreateDisk(diskType)
}

// AvailableSpaceFor 计算节点的可用 Volume 槽位数（不考虑预留）
// 考虑了远程 Volume 和 EC 分片的影响
//
// 参数：
//   - option: Volume 增长选项（包含磁盘类型）
//
// 返回：
//   - int64: 可用的 Volume 槽位数
//
// 计算公式：
//   freeSpace = maxVolumeCount + remoteVolumeCount - volumeCount - ecVolumes
//   其中：ecVolumes = ecShardCount / 10 + 1（保守估计）
//
// 说明：
//   - 使用原子操作读取统计，保证并发安全
//   - 远程 Volume 不占用本地空间，所以加回来
//   - EC 分片每 10 个占 1 个 Volume 槽位
func (n *NodeImpl) AvailableSpaceFor(option *VolumeGrowOption) int64 {
	t := n.getOrCreateDisk(option.DiskType)
	// 原子读取统计数据
	freeVolumeSlotCount := atomic.LoadInt64(&t.maxVolumeCount) + atomic.LoadInt64(&t.remoteVolumeCount) - atomic.LoadInt64(&t.volumeCount)
	ecShardCount := atomic.LoadInt64(&t.ecShardCount)
	if ecShardCount > 0 {
		// 减去 EC 分片占用的空间
		// 每 10 个 EC 分片相当于 1 个 Volume，额外 -1 是为了保守估计
		freeVolumeSlotCount = freeVolumeSlotCount - ecShardCount/erasure_coding.DataShardsCount - 1
	}
	return freeVolumeSlotCount
}

// AvailableSpaceForReservation 计算可用空间（扣除已预留的容量）
// 在 AvailableSpaceFor 的基础上，进一步扣除已预留但尚未实际使用的容量
//
// 参数：
//   - option: Volume 增长选项
//
// 返回：
//   - int64: 扣除预留后的可用槽位数
//
// 计算公式：
//   availableForReservation = AvailableSpaceFor(option) - reservedCount
//
// 使用场景：
//   - 新的预留请求到来时，检查是否还有足够空间
//   - 避免超额分配（考虑了正在进行中的预留）
func (n *NodeImpl) AvailableSpaceForReservation(option *VolumeGrowOption) int64 {
	baseAvailable := n.AvailableSpaceFor(option)
	reservedCount := n.capacityReservations.getReservedCount(option.DiskType)
	return baseAvailable - reservedCount
}

// TryReserveCapacity 尝试原子地预留容量用于 Volume 创建
// 在分配 Volume 前调用，确保有足够的容量
//
// 参数：
//   - diskType: 磁盘类型
//   - count: 需要预留的 Volume 数量
//
// 返回：
//   - reservationId: 预留 ID（失败时返回空字符串）
//   - success: 是否预留成功
//
// 工作流程：
//   1. 清理过期的预留记录（默认 5 分钟过期）
//   2. 原子地检查可用空间并预留
//   3. 记录预留成功的日志
//
// 线程安全：
//   - 使用 tryReserveAtomic 保证原子性
//   - 避免"检查后使用"（TOCTOU）竞态条件
func (n *NodeImpl) TryReserveCapacity(diskType types.DiskType, count int64) (reservationId string, success bool) {
	const reservationTimeout = 5 * time.Minute // TODO: make this configurable

	// 【步骤 1：清理过期的预留】
	n.capacityReservations.cleanExpiredReservations(reservationTimeout)

	// 【步骤 2：原子地检查并预留空间】
	option := &VolumeGrowOption{DiskType: diskType}
	reservationId, success = n.capacityReservations.tryReserveAtomic(diskType, count, func() int64 {
		return n.AvailableSpaceFor(option)
	})

	if success {
		glog.V(1).Infof("Reserved %d capacity for diskType %s on node %s: %s", count, diskType, n.Id(), reservationId)
	}

	return reservationId, success
}

// ReleaseReservedCapacity 释放之前预留的容量
// Volume 创建成功或失败后调用，释放预留的容量
//
// 参数：
//   - reservationId: 预留 ID（由 TryReserveCapacity 返回）
//
// 副作用：
//   - 从预留记录中删除该预留
//   - 更新已预留计数
//   - 使该容量重新可用于其他请求
//
// 注意：
//   - 重复释放同一个预留 ID 是安全的（会记录日志但不会出错）
//   - 预留会自动过期，即使未显式释放也不会永久占用容量
func (n *NodeImpl) ReleaseReservedCapacity(reservationId string) {
	if n.capacityReservations.removeReservation(reservationId) {
		glog.V(1).Infof("Released capacity reservation on node %s: %s", n.Id(), reservationId)
	} else {
		glog.V(1).Infof("Attempted to release non-existent reservation on node %s: %s", n.Id(), reservationId)
	}
}

// SetParent 设置节点的父节点
//
// 参数：
//   - node: 父节点
func (n *NodeImpl) SetParent(node Node) {
	n.parent = node
}

// Children 获取节点的所有子节点
// 使用读锁保护，确保线程安全
//
// 返回：
//   - []Node: 子节点列表
func (n *NodeImpl) Children() (ret []Node) {
	n.RLock()
	defer n.RUnlock()
	for _, c := range n.children {
		ret = append(ret, c)
	}
	return ret
}

// Parent 获取节点的父节点
//
// 返回：
//   - Node: 父节点（Topology 的 parent 为 nil）
func (n *NodeImpl) Parent() Node {
	return n.parent
}

// GetValue 获取节点的实际值
// 用于类型断言，将 Node 接口转换为具体类型
//
// 返回：
//   - interface{}: 节点的实际值（*Topology, *DataCenter, *Rack, *DataNode, 或 *Disk）
//
// 使用示例：
//   if dn, ok := node.GetValue().(*DataNode); ok {
//       // 使用 DataNode 特有的方法
//   }
func (n *NodeImpl) GetValue() interface{} {
	return n.value
}

// ReserveOneVolume 在子树中选择一个 DataNode 用于创建 Volume
// 不考虑容量预留，直接基于当前可用空间选择
//
// 参数：
//   - r: 随机偏移量，用于加权随机选择
//   - option: Volume 增长选项
//
// 返回：
//   - assignedNode: 被选中的 DataNode
//   - err: 错误信息
//
// 注意：
//   - 这是旧版本的方法，不使用容量预留机制
//   - 新代码应使用 ReserveOneVolumeForReservation
func (n *NodeImpl) ReserveOneVolume(r int64, option *VolumeGrowOption) (assignedNode *DataNode, err error) {
	return n.reserveOneVolumeInternal(r, option, false)
}

// ReserveOneVolumeForReservation 使用预留感知的容量检查选择节点
// 考虑已预留的容量，避免超额分配
//
// 参数：
//   - r: 随机偏移量，用于加权随机选择
//   - option: Volume 增长选项
//
// 返回：
//   - assignedNode: 被选中的 DataNode
//   - err: 错误信息
//
// 与 ReserveOneVolume 的区别：
//   - 使用 AvailableSpaceForReservation 而不是 AvailableSpaceFor
//   - 考虑了已预留但尚未使用的容量
//   - 更适合高并发场景
func (n *NodeImpl) ReserveOneVolumeForReservation(r int64, option *VolumeGrowOption) (assignedNode *DataNode, err error) {
	return n.reserveOneVolumeInternal(r, option, true)
}

// reserveOneVolumeInternal 选择一个 DataNode 的内部实现
// 使用加权随机算法，根据可用空间选择合适的 DataNode
//
// 参数：
//   - r: 随机偏移量（0 到总可用空间之间的随机数）
//   - option: Volume 增长选项
//   - useReservations: 是否使用预留感知的容量检查
//
// 返回：
//   - assignedNode: 被选中的 DataNode
//   - err: 错误信息
//
// 算法说明：
//   1. 遍历所有子节点
//   2. 累加每个节点的可用空间
//   3. 当累加值超过随机偏移量 r 时，选中该节点
//   4. 如果该节点是 DataNode，直接返回
//   5. 如果是中间节点，递归调用其 ReserveOneVolume
//
// 加权随机原理：
//   - 可用空间越大的节点，被选中的概率越大
//   - 例如：节点 A 有 100 个空槽，节点 B 有 50 个空槽
//   - A 被选中的概率是 100/150 = 66.7%
//   - B 被选中的概率是 50/150 = 33.3%
//
// 线程安全：
//   - 使用读锁保护子节点列表
func (n *NodeImpl) reserveOneVolumeInternal(r int64, option *VolumeGrowOption, useReservations bool) (assignedNode *DataNode, err error) {
	n.RLock()
	defer n.RUnlock()

	// 遍历所有子节点
	for _, node := range n.children {
		// 【步骤 1：获取节点的可用空间】
		var freeSpace int64
		if useReservations {
			freeSpace = node.AvailableSpaceForReservation(option)
		} else {
			freeSpace = node.AvailableSpaceFor(option)
		}

		// 跳过没有空间的节点
		if freeSpace <= 0 {
			continue
		}

		// 【步骤 2：累加可用空间，判断是否选中该节点】
		if r >= freeSpace {
			// r 还大于当前节点的可用空间，继续累加
			r -= freeSpace
		} else {
			// r 落在当前节点的空间范围内，选中该节点

			// 【步骤 3：检查是否是 DataNode】
			var hasSpace bool
			if useReservations {
				hasSpace = node.IsDataNode() && node.AvailableSpaceForReservation(option) > 0
			} else {
				hasSpace = node.IsDataNode() && node.AvailableSpaceFor(option) > 0
			}

			if hasSpace {
				// 是 DataNode 且有空间，直接返回
				dn := node.(*DataNode)
				// 检查节点是否正在终止（下线中）
				if dn.IsTerminating {
					continue
				}
				return dn, nil
			}

			// 【步骤 4：是中间节点，递归调用】
			if useReservations {
				assignedNode, err = node.ReserveOneVolumeForReservation(r, option)
			} else {
				assignedNode, err = node.ReserveOneVolume(r, option)
			}
			if err == nil {
				return
			}
		}
	}

	// 没有找到合适的节点
	return nil, errors.New("No free volume slot found!")
}

// UpAdjustDiskUsageDelta 向上调整磁盘使用统计（可以是负值）
// 当子节点的统计发生变化时，需要向上传播到所有祖先节点
//
// 参数：
//   - diskType: 磁盘类型
//   - diskUsage: 统计增量（可以是负值，表示减少）
//
// 工作原理：
//   1. 更新当前节点的统计
//   2. 递归向上传播到父节点
//   3. 一直传播到 Topology 根节点
//
// 使用场景：
//   - 新增 Volume：传播正增量
//   - 删除 Volume：传播负增量
//   - 节点下线：传播负增量（抵消之前的统计）
func (n *NodeImpl) UpAdjustDiskUsageDelta(diskType types.DiskType, diskUsage *DiskUsageCounts) {
	// 更新当前节点的统计
	existingDisk := n.getOrCreateDisk(diskType)
	existingDisk.addDiskUsageCounts(diskUsage)

	// 递归向上传播
	if n.parent != nil {
		n.parent.UpAdjustDiskUsageDelta(diskType, diskUsage)
	}
}

// UpAdjustMaxVolumeId 向上更新最大 VolumeId
// 当创建新 Volume 时，需要更新拓扑树中所有祖先节点的 maxVolumeId
//
// 参数：
//   - vid: 新的 VolumeId
//
// 工作原理：
//   1. 如果 vid 大于当前节点的 maxVolumeId，更新它
//   2. 递归向上传播到父节点
//   3. 一直传播到 Topology 根节点
//
// 用途：
//   - 快速分配新的 VolumeId（使用 maxVolumeId + 1）
//   - 避免 VolumeId 冲突
func (n *NodeImpl) UpAdjustMaxVolumeId(vid needle.VolumeId) {
	if n.maxVolumeId < vid {
		n.maxVolumeId = vid
		// 递归向上传播
		if n.parent != nil {
			n.parent.UpAdjustMaxVolumeId(vid)
		}
	}
}

// GetMaxVolumeId 获取该节点及其子树中的最大 VolumeId
//
// 返回：
//   - needle.VolumeId: 最大的 VolumeId
func (n *NodeImpl) GetMaxVolumeId() needle.VolumeId {
	return n.maxVolumeId
}

// LinkChildNode 将子节点链接到当前节点
// 使用写锁保护，确保线程安全
//
// 参数：
//   - node: 要链接的子节点
//
// 副作用：
//   - 将子节点添加到 children map
//   - 向上传播子节点的统计信息
//   - 设置子节点的 parent 指针
func (n *NodeImpl) LinkChildNode(node Node) {
	n.Lock()
	defer n.Unlock()
	n.doLinkChildNode(node)
}

// doLinkChildNode 链接子节点的内部实现
// 假设调用者已持有锁
//
// 参数：
//   - node: 要链接的子节点
//
// 工作流程：
//   1. 检查子节点是否已存在（避免重复添加）
//   2. 将子节点添加到 children map
//   3. 向上传播子节点的磁盘使用统计
//   4. 向上更新最大 VolumeId
//   5. 设置子节点的 parent 指针
//   6. 记录日志
func (n *NodeImpl) doLinkChildNode(node Node) {
	// 检查是否已存在
	if n.children[node.Id()] == nil {
		// 【步骤 1：添加到 children map】
		n.children[node.Id()] = node

		// 【步骤 2：向上传播磁盘使用统计】
		for dt, du := range node.GetDiskUsages().usages {
			n.UpAdjustDiskUsageDelta(dt, du)
		}

		// 【步骤 3：向上更新最大 VolumeId】
		n.UpAdjustMaxVolumeId(node.GetMaxVolumeId())

		// 【步骤 4：设置子节点的 parent 指针】
		node.SetParent(n)

		// 【步骤 5：记录日志】
		glog.V(0).Infoln(n, "adds child", node.Id())
	}
}

// UnlinkChildNode 从当前节点移除子节点
// 使用写锁保护，确保线程安全
//
// 参数：
//   - nodeId: 要移除的子节点 ID
//
// 副作用：
//   - 从 children map 中删除子节点
//   - 向上传播负的统计信息（抵消之前的统计）
//   - 清除子节点的 parent 指针
//
// 使用场景：
//   - DataNode 下线
//   - Rack 或 DataCenter 移除
func (n *NodeImpl) UnlinkChildNode(nodeId NodeId) {
	n.Lock()
	defer n.Unlock()

	// 查找子节点
	node := n.children[nodeId]
	if node != nil {
		// 【步骤 1：清除子节点的 parent 指针】
		node.SetParent(nil)

		// 【步骤 2：从 children map 中删除】
		delete(n.children, node.Id())

		// 【步骤 3：向上传播负的统计信息】
		// 使用 negative() 获取负值统计，抵消之前的正值统计
		for dt, du := range node.GetDiskUsages().negative().usages {
			n.UpAdjustDiskUsageDelta(dt, du)
		}

		// 【步骤 4：记录日志】
		glog.V(0).Infoln(n, "removes", node.Id())
	}
}

// CollectDeadNodeAndFullVolumes 递归收集已满和拥挤的 Volume，并检查副本放置是否正确
// 此方法由 Master Server 的后台任务定期调用（每隔 pulse 间隔）
//
// 参数：
//   - freshThreshHoldUnixTime: 节点"新鲜度"阈值的 Unix 时间戳
//     节点的 LastSeen 时间早于此值时，视为死节点（当前未使用，但保留接口以便扩展）
//   - volumeSizeLimit: Volume 的大小限制（字节），超过此值标记为已满（Full）
//   - growThreshold: Volume 增长阈值（0.0-1.0），如 0.9 表示达到 90% 时标记为拥挤（Crowded）
//
// 工作原理：
//   1. 如果当前节点是 Rack，则遍历其所有 DataNode
//   2. 对每个 DataNode 的所有 Volume 进行检查：
//      a. 检查是否已满（Size >= volumeSizeLimit）
//      b. 检查是否拥挤（Size > volumeSizeLimit * growThreshold）
//      c. 检查副本数是否满足副本策略要求
//   3. 如果当前节点不是 Rack（如 DataCenter、Topology），则递归调用子节点
//
// Volume 状态判断：
//   - 已满（Full）：v.Size >= volumeSizeLimit
//     * 但如果 Volume 刚完成 Vacuum（20 秒内），跳过检查
//     * 原因：Vacuum 后 Volume 大小会减小，但心跳可能还未更新
//     * 等待 20 秒（gRPC 超时时间）确保所有心跳都已到达 Master
//   - 拥挤（Crowded）：v.Size > volumeSizeLimit * growThreshold
//     * 触发预创建新 Volume，避免 Volume 满了才创建导致写入延迟
//
// 副本放置检查：
//   - 如果副本策略要求多副本（copyCount > 1）
//   - 检查实际副本数是否满足要求
//   - 通过 Prometheus 指标上报副本不匹配情况
//
// 通知机制：
//   - 通过 channel 发送已满/拥挤的 Volume 通知
//   - topo.chanFullVolumes <- v：已满 Volume
//   - topo.chanCrowdedVolumes <- v：拥挤 Volume
//   - 接收端在 topology_event_handling.go 中处理这些通知
//
// 递归策略：
//   - 采用深度优先遍历
//   - 只在 Rack 级别检查 Volume（因为 Volume 存储在 DataNode 上）
//   - 避免在 DataCenter/Topology 级别重复检查
//
// 性能考虑：
//   - 此方法只由 Leader Master 执行，避免重复操作
//   - 使用 channel 异步通知，不阻塞检查流程
//   - 读锁保护 vacuumedVolumes 映射，允许并发读取
//
// 使用示例：
//   // 在 Master Server 的后台任务中
//   freshThreshold := time.Now().Unix() - 3*t.pulse  // 3 倍心跳间隔
//   t.CollectDeadNodeAndFullVolumes(freshThreshold, t.volumeSizeLimit, 0.9)
func (n *NodeImpl) CollectDeadNodeAndFullVolumes(freshThreshHoldUnixTime int64, volumeSizeLimit uint64, growThreshold float64) {
	// 【分支 1：当前节点是 Rack，直接检查其所有 DataNode】
	if n.IsRack() {
		// 遍历 Rack 下的所有子节点（DataNode）
		for _, c := range n.Children() {
			dn := c.(*DataNode) // 类型断言：Rack 的子节点必定是 DataNode

			// 遍历 DataNode 上的所有 Volume
			for _, v := range dn.GetVolumes() {
				// 获取 Topology 根节点，以便访问 VolumeLayout
				topo := n.GetTopology()
				diskType := types.ToDiskType(v.DiskType)
				vl := topo.GetVolumeLayout(v.Collection, v.ReplicaPlacement, v.Ttl, diskType)

				// 【检查 1：Volume 是否已满】
				if v.Size >= volumeSizeLimit {
					// 检查 Volume 是否刚完成 Vacuum
					// 需要使用读锁保护 vacuumedVolumes 映射
					vl.accessLock.RLock()
					vacuumTime, ok := vl.vacuumedVolumes[v.Id]
					vl.accessLock.RUnlock()

					// 如果 Volume 在 20 秒前完成 Vacuum，或者从未 Vacuum，则标记为已满
					// 20 秒是 gRPC 超时时间，确保所有 Volume Server 的心跳都已到达
					// 原因：Vacuum 后 Volume 大小会减小，但心跳可能还未更新到 Master
					if !ok || time.Now().Add(-20*time.Second).After(vacuumTime) {
						// 发送已满 Volume 通知到 channel
						// 接收端会调用 SetVolumeCapacityFull 标记为只读
						topo.chanFullVolumes <- v
					}
				} else if float64(v.Size) > float64(volumeSizeLimit)*growThreshold {
					// 【检查 2：Volume 是否拥挤】
					// 如果 Volume 大小超过阈值（如 90%），标记为拥挤
					// 触发 VolumeGrowth 逻辑，预创建新 Volume
					// 避免 Volume 满了之后才创建新 Volume，导致写入延迟
					topo.chanCrowdedVolumes <- v
				}

				// 【检查 3：副本数是否满足副本策略】
				copyCount := v.ReplicaPlacement.GetCopyCount()
				if copyCount > 1 {
					// 查询该 Volume 的实际副本位置
					actualLocations := topo.Lookup(v.Collection, v.Id)

					// 比较实际副本数和期望副本数
					if copyCount > len(actualLocations) {
						// 副本数不足，上报 Prometheus 指标
						stats.MasterReplicaPlacementMismatch.WithLabelValues(v.Collection, v.Id.String()).Set(1)
					} else {
						// 副本数正常，清除告警
						stats.MasterReplicaPlacementMismatch.WithLabelValues(v.Collection, v.Id.String()).Set(0)
					}
				}
			}
		}
	} else {
		// 【分支 2：当前节点不是 Rack（如 DataCenter、Topology），递归检查子节点】
		for _, c := range n.Children() {
			// 递归调用子节点的 CollectDeadNodeAndFullVolumes
			// 深度优先遍历整个拓扑树，直到到达 Rack 级别
			c.CollectDeadNodeAndFullVolumes(freshThreshHoldUnixTime, volumeSizeLimit, growThreshold)
		}
	}
}

// GetTopology 向上遍历拓扑树，获取根节点（Topology）
// 此方法从当前节点开始，沿着父节点链向上查找，直到找到根节点
//
// 返回：
//   - *Topology: 拓扑树的根节点
//
// 工作原理：
//   拓扑树的层次结构：
//     Topology (root, parent == nil)
//       └─ DataCenter
//            └─ Rack
//                 └─ DataNode
//                      └─ Disk
//
//   从任意节点开始，不断访问 Parent()，直到 Parent() == nil，即到达根节点
//
// 算法步骤：
//   1. 初始化 p = 当前节点
//   2. 循环：如果 p.Parent() != nil，则 p = p.Parent()
//   3. 循环结束时，p 就是根节点（Topology）
//   4. 通过 GetValue() 获取实际的 Topology 对象
//
// 类型转换：
//   - GetValue() 返回 interface{}
//   - 需要类型断言转换为 *Topology
//   - 根节点的 value 字段必定是 *Topology 类型
//
// 使用场景：
//   1. 在子节点中需要访问全局拓扑信息时
//   2. 查找 VolumeLayout（需要通过 Topology 访问）
//   3. 发送 Volume 状态变更通知（chanFullVolumes、chanCrowdedVolumes）
//   4. 查询 Volume 的所有副本位置
//
// 性能考虑：
//   - 时间复杂度：O(h)，h 是树的高度（最多 5 层）
//   - 无锁操作，因为父节点链是只读的（节点创建后不会改变父节点）
//   - 可以考虑缓存根节点引用以优化性能（但当前实现简单且足够快）
//
// 使用示例：
//   // 在 DataNode 中获取 Topology
//   topo := dataNode.GetTopology()
//   vl := topo.GetVolumeLayout(collection, replicaPlacement, ttl, diskType)
//
//   // 在 Rack 中发送 Volume 状态通知
//   topo := rack.GetTopology()
//   topo.chanFullVolumes <- volumeInfo
func (n *NodeImpl) GetTopology() *Topology {
	var p Node
	p = n

	// 向上遍历父节点链，直到到达根节点（Parent() == nil）
	for p.Parent() != nil {
		p = p.Parent()
	}

	// 此时 p 是根节点，其 value 字段存储的是 *Topology 实例
	// 通过类型断言将 interface{} 转换为 *Topology
	return p.GetValue().(*Topology)
}
