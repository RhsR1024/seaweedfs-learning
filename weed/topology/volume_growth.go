// Package topology 实现了 SeaweedFS 的拓扑管理和卷增长策略
// 本文件专注于 Volume 的自动增长和分配逻辑
package topology

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"reflect"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/server/constants"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

/*
本模块解决以下副本放置和容量增长问题：

1. 不同副本级别的增长因子
   - 例如：1 副本时创建 10 个卷，2 副本时创建 20 个卷，3 副本时创建 30 个卷
   - 通过 VolumeGrowStrategy 配置不同副本级别的批量创建数量

2. 存储空间紧张时如何降低副本级别
   - 在空间不足时自动调整副本策略
   - 支持动态副本降级以适应容量约束

3. 热数据和冷数据的存储优化
   - 热数据放在快速磁盘（SSD）
   - 冷数据放在廉价存储（HDD）
   - 通过 DiskType 参数实现分层存储

4. 为每个 bucket/collection 分配卷
   - 支持按集合（Collection）隔离存储
   - 每个集合可以有独立的副本策略和存储类型
*/

// VolumeGrowRequest 表示一个卷增长请求
// 用于向 Master 请求创建新的 Volume
type VolumeGrowRequest struct {
	// Option 卷的配置选项（副本策略、TTL、磁盘类型等）
	Option *VolumeGrowOption
	// Count 需要创建的逻辑卷数量
	// 注意：实际创建的物理卷数量 = Count × ReplicaPlacement.GetCopyCount()
	Count uint32
	// Force 是否强制创建，即使空间不足也尝试创建
	Force bool
	// Reason 创建原因，用于日志记录和调试
	Reason string
}

// Equals 比较两个 VolumeGrowRequest 是否相等
// 用于去重和避免重复请求
func (vg *VolumeGrowRequest) Equals(req *VolumeGrowRequest) bool {
	return reflect.DeepEqual(vg.Option, req.Option) && vg.Count == req.Count && vg.Force == req.Force
}

// volumeGrowthStrategy 定义了不同副本级别的卷增长策略
// 根据副本数量（CopyCount）决定一次性创建多少个逻辑卷
type volumeGrowthStrategy struct {
	// Copy1Count 单副本时一次创建的卷数量（默认 7）
	// 单副本意味着只有一个物理卷，所以可以多创建一些
	Copy1Count uint32
	// Copy2Count 双副本时一次创建的卷数量（默认 6）
	// 双副本会创建 6×2=12 个物理卷
	Copy2Count uint32
	// Copy3Count 三副本时一次创建的卷数量（默认 3）
	// 三副本会创建 3×3=9 个物理卷
	Copy3Count uint32
	// CopyOtherCount 其他副本级别时一次创建的卷数量（默认 1）
	// 用于 4 副本及以上的情况
	CopyOtherCount uint32
	// Threshold 卷空间使用阈值（默认 0.9 = 90%）
	// 当卷使用率超过此阈值时，会触发新卷的创建
	Threshold float64
}

var (
	// VolumeGrowStrategy 全局默认的卷增长策略
	// 这些值可以在运行时通过配置文件修改
	VolumeGrowStrategy = volumeGrowthStrategy{
		Copy1Count:     7, // 单副本：创建 7 个逻辑卷 = 7 个物理卷
		Copy2Count:     6, // 双副本：创建 6 个逻辑卷 = 12 个物理卷
		Copy3Count:     3, // 三副本：创建 3 个逻辑卷 = 9 个物理卷
		CopyOtherCount: 1, // 其他副本：保守策略，只创建 1 个逻辑卷
		Threshold:      0.9, // 触发阈值：90% 满时创建新卷
	}
)

// VolumeGrowOption 卷增长选项，定义创建新卷时的所有配置参数
type VolumeGrowOption struct {
	// Collection 集合名称，用于逻辑分组和隔离不同类型的文件
	// 例如：photos、videos、documents
	Collection string `json:"collection,omitempty"`

	// ReplicaPlacement 副本放置策略
	// 格式：XYZ（三位数字）
	//   X: 不同数据中心的副本数
	//   Y: 不同机架的副本数（同数据中心）
	//   Z: 不同服务器的副本数（同机架）
	// 例如："001" = 同机架不同服务器 1 个副本
	ReplicaPlacement *super_block.ReplicaPlacement `json:"replication,omitempty"`

	// Ttl 生存时间（Time To Live）
	// 文件到期后可以被自动删除，用于临时文件场景
	// 例如：验证码图片、临时下载链接
	Ttl *needle.TTL `json:"ttl,omitempty"`

	// DiskType 磁盘类型，用于分层存储
	// 可选值：
	//   - "hdd": 机械硬盘（冷数据、大容量）
	//   - "ssd": 固态硬盘（热数据、高性能）
	//   - "nvme": NVMe 固态硬盘（极热数据、最高性能）
	DiskType types.DiskType `json:"disk,omitempty"`

	// Preallocate 预分配空间大小（字节）
	// 在创建卷时预先分配磁盘空间，避免文件碎片化
	// 0 表示不预分配
	Preallocate int64 `json:"preallocate,omitempty"`

	// DataCenter 指定数据中心
	// 为空时自动选择，非空时必须在指定数据中心创建
	DataCenter string `json:"dataCenter,omitempty"`

	// Rack 指定机架
	// 为空时自动选择，非空时必须在指定机架创建
	Rack string `json:"rack,omitempty"`

	// DataNode 指定数据节点
	// 为空时自动选择，非空时必须在指定节点创建
	DataNode string `json:"dataNode,omitempty"`

	// MemoryMapMaxSizeMb 内存映射最大大小（MB）
	// 用于 mmap 文件读取优化，设置为 0 禁用 mmap
	MemoryMapMaxSizeMb uint32 `json:"memoryMapMaxSizeMb,omitempty"`

	// Version 卷的版本号
	// 不同版本的卷使用不同的数据格式（v1/v2/v3）
	Version uint32 `json:"version,omitempty"`
}

// VolumeGrowth 卷增长管理器，负责协调卷的创建和分配
type VolumeGrowth struct {
	// accessLock 保护并发创建卷的互斥锁
	// 避免同时创建多个卷导致资源竞争
	accessLock sync.Mutex
}

// VolumeGrowReservation 卷创建容量预留记录
// 在创建卷之前，先在目标服务器上预留空间，确保创建过程的原子性
// 如果创建失败，预留的空间会被释放
type VolumeGrowReservation struct {
	// servers 预留了空间的服务器列表
	servers []*DataNode
	// reservationIds 每个服务器上的预留 ID，用于释放预留
	reservationIds []string
	// diskType 预留空间的磁盘类型（HDD/SSD）
	diskType types.DiskType
}

// releaseAllReservations 释放所有预留的容量
// 在以下情况调用：
//   1. 卷创建失败，需要回滚预留
//   2. 卷创建成功后，转为实际占用
func (vgr *VolumeGrowReservation) releaseAllReservations() {
	for i, server := range vgr.servers {
		// 检查索引和预留 ID 是否有效
		if i < len(vgr.reservationIds) && vgr.reservationIds[i] != "" {
			// 调用服务器的释放预留容量方法
			server.ReleaseReservedCapacity(vgr.reservationIds[i])
		}
	}
}

// String 返回 VolumeGrowOption 的 JSON 字符串表示
// 用于日志记录和调试
func (o *VolumeGrowOption) String() string {
	blob, _ := json.Marshal(o)
	return string(blob)
}

// NewDefaultVolumeGrowth 创建默认的卷增长管理器
func NewDefaultVolumeGrowth() *VolumeGrowth {
	return &VolumeGrowth{}
}

// findVolumeCount 根据副本数量查找应该创建的逻辑卷数量
//
// 核心概念：
//   - 逻辑卷：从客户端角度看到的卷（一个 Volume ID）
//   - 物理卷：实际存储的卷文件（副本数 × 逻辑卷数）
//
// 示例：
//   - 副本数 = 1，创建 7 个逻辑卷 → 7 个物理卷
//   - 副本数 = 2，创建 6 个逻辑卷 → 12 个物理卷
//   - 副本数 = 3，创建 3 个逻辑卷 → 9 个物理卷
//
// 参数:
//   - copyCount: 副本数量（即 rp.GetCopyCount()）
// 返回:
//   - count: 应该创建的逻辑卷数量
func (vg *VolumeGrowth) findVolumeCount(copyCount int) (count uint32) {
	switch copyCount {
	case 1:
		// 单副本：创建 7 个逻辑卷
		count = VolumeGrowStrategy.Copy1Count
	case 2:
		// 双副本：创建 6 个逻辑卷（实际 12 个物理卷）
		count = VolumeGrowStrategy.Copy2Count
	case 3:
		// 三副本：创建 3 个逻辑卷（实际 9 个物理卷）
		count = VolumeGrowStrategy.Copy3Count
	default:
		// 其他副本数：保守策略，只创建 1 个逻辑卷
		count = VolumeGrowStrategy.CopyOtherCount
	}
	return
}

// AutomaticGrowByType 根据副本策略自动增长卷
// 如果 targetCount 为 0，则根据副本数量自动决定创建数量
//
// 执行流程：
//   1. 如果 targetCount=0，根据副本数查找默认创建数量
//   2. 调用 GrowByCountAndType 创建指定数量的卷
//   3. 验证创建结果是否符合副本要求
//
// 参数:
//   - option: 卷增长选项（副本策略、集合、TTL 等）
//   - grpcDialOption: gRPC 连接选项
//   - topo: 拓扑结构
//   - targetCount: 目标创建数量，0 表示使用默认策略
// 返回:
//   - result: 创建的卷位置列表
//   - err: 错误信息
func (vg *VolumeGrowth) AutomaticGrowByType(option *VolumeGrowOption, grpcDialOption grpc.DialOption, topo *Topology, targetCount uint32) (result []*master_pb.VolumeLocation, err error) {
	// 如果未指定目标数量，根据副本数自动确定
	if targetCount == 0 {
		targetCount = vg.findVolumeCount(option.ReplicaPlacement.GetCopyCount())
	}

	// 执行实际的卷创建
	result, err = vg.GrowByCountAndType(grpcDialOption, targetCount, option, topo)

	// 验证结果：确保创建的卷数量是副本数的整数倍
	// 例如：副本数=2，则结果数量应该是 2, 4, 6, 8...
	if len(result) > 0 && len(result)%option.ReplicaPlacement.GetCopyCount() == 0 {
		return result, nil
	}
	return result, err
}

// GrowByCountAndType 创建指定数量的卷
// 这是卷创建的核心循环函数
//
// 执行流程：
//   1. 加锁保护并发创建
//   2. 循环 targetCount 次，每次创建一个逻辑卷
//   3. 每个逻辑卷根据副本策略创建多个物理卷
//   4. 任意一次创建失败，立即返回错误
//
// 参数:
//   - grpcDialOption: gRPC 连接选项
//   - targetCount: 要创建的逻辑卷数量
//   - option: 卷增长选项
//   - topo: 拓扑结构
// 返回:
//   - result: 创建的卷位置列表（包含所有副本）
//   - err: 错误信息
func (vg *VolumeGrowth) GrowByCountAndType(grpcDialOption grpc.DialOption, targetCount uint32, option *VolumeGrowOption, topo *Topology) (result []*master_pb.VolumeLocation, err error) {
	// 加锁：避免并发创建卷导致资源冲突
	vg.accessLock.Lock()
	defer vg.accessLock.Unlock()

	// 循环创建指定数量的卷
	for i := uint32(0); i < targetCount; i++ {
		// 查找合适的服务器并创建一个逻辑卷（包含所有副本）
		if res, e := vg.findAndGrow(grpcDialOption, topo, option); e == nil {
			// 成功：将所有副本的位置添加到结果中
			result = append(result, res...)
		} else {
			// 失败：记录日志并返回已创建的部分结果
			glog.V(0).Infof("create %d volume, created %d: %v", targetCount, len(result), e)
			return result, e
		}
	}
	return
}

// findAndGrow 查找合适的服务器并创建一个逻辑卷（包含所有副本）
// 这是单个卷创建的完整流程
//
// 执行流程：
//   1. 查找空闲槽位并预留容量（原子操作）
//   2. 等待集群稳定（避免在 Leader 切换期间创建）
//   3. 分配新的 Volume ID
//   4. 在所有目标服务器上创建卷
//   5. 构建返回结果
//
// 参数:
//   - grpcDialOption: gRPC 连接选项
//   - topo: 拓扑结构
//   - option: 卷增长选项
// 返回:
//   - result: 卷位置列表（包含所有副本的位置）
//   - err: 错误信息
func (vg *VolumeGrowth) findAndGrow(grpcDialOption grpc.DialOption, topo *Topology, option *VolumeGrowOption) (result []*master_pb.VolumeLocation, err error) {
	// 【步骤 1：查找空闲槽位并预留容量】
	// useReservations=true 表示使用容量预留机制，确保原子性
	servers, reservation, e := vg.findEmptySlotsForOneVolume(topo, option, true)
	if e != nil {
		return nil, e
	}

	// 确保在出错时释放预留的容量
	// 这是一个错误处理的 defer，只在 err != nil 时执行
	defer func() {
		if err != nil && reservation != nil {
			reservation.releaseAllReservations()
		}
	}()

	// 【步骤 2：等待集群稳定】
	// 在 Leader 切换后，需要等待 Volume Server 重新加入集群
	// 避免在不稳定期间创建卷导致数据不一致
	// 等待条件：距离上次 Leader 切换至少 2 个心跳周期
	for !topo.LastLeaderChangeTime.Add(constants.VolumePulseSeconds * 2).Before(time.Now()) {
		glog.V(0).Infof("wait for volume servers to join back")
		// 每半个心跳周期检查一次
		time.Sleep(constants.VolumePulseSeconds / 2)
	}

	// 【步骤 3：分配新的 Volume ID】
	// 通过 Raft 共识算法分配全局唯一的 Volume ID
	vid, raftErr := topo.NextVolumeId()
	if raftErr != nil {
		return nil, raftErr
	}

	// 【步骤 4：在所有目标服务器上创建卷】
	// 这会在每个服务器上创建一个副本
	if err = vg.grow(grpcDialOption, topo, vid, option, reservation, servers...); err == nil {
		// 【步骤 5：构建返回结果】
		// 为每个副本创建 VolumeLocation 信息
		for _, server := range servers {
			result = append(result, &master_pb.VolumeLocation{
				Url:        server.Url(),         // HTTP URL
				PublicUrl:  server.PublicUrl,     // 公网 URL
				DataCenter: server.GetDataCenterId(), // 数据中心 ID
				GrpcPort:   uint32(server.GrpcPort),  // gRPC 端口
				NewVids:    []uint32{uint32(vid)},    // 新创建的 Volume ID
			})
		}
	}
	return
}

// findEmptySlotsForOneVolume 为一个逻辑卷查找所有副本的存储位置
// 这是 SeaweedFS 副本放置策略的核心实现
//
// 副本放置策略（ReplicaPlacement）示例：
//   - "000": 无副本，只有一个主副本
//   - "001": 同机架不同服务器 1 个副本（主副本 + 1 个副本 = 2 个物理卷）
//   - "010": 同数据中心不同机架 1 个副本（主副本 + 1 个副本 = 2 个物理卷）
//   - "100": 不同数据中心 1 个副本（主副本 + 1 个副本 = 2 个物理卷）
//   - "200": 不同数据中心 2 个副本（主副本 + 2 个副本 = 3 个物理卷）
//
// 执行流程（层级选择）：
//   1. 选择数据中心（DataCenter）
//      - 选择 1 个主数据中心
//      - 选择 rp.DiffDataCenterCount 个其他数据中心
//   2. 在主数据中心内选择机架（Rack）
//      - 选择 1 个主机架
//      - 选择 rp.DiffRackCount 个其他机架
//   3. 在主机架内选择服务器（DataNode）
//      - 选择 1 个主服务器
//      - 选择 rp.SameRackCount 个其他服务器
//   4. 在其他机架/数据中心中各选择 1 个服务器
//
// 容量预留机制（useReservations=true）：
//   - 在选择服务器的同时预留容量，确保创建过程的原子性
//   - 如果创建失败，预留的容量会被自动释放
//
// 参数:
//   - topo: 拓扑结构
//   - option: 卷增长选项（包含副本策略等）
//   - useReservations: 是否使用容量预留机制
// 返回:
//   - servers: 选中的服务器列表
//   - reservation: 容量预留信息（仅当 useReservations=true）
//   - err: 错误信息
func (vg *VolumeGrowth) findEmptySlotsForOneVolume(topo *Topology, option *VolumeGrowOption, useReservations bool) (servers []*DataNode, reservation *VolumeGrowReservation, err error) {
	// 获取副本放置策略
	rp := option.ReplicaPlacement

	// 临时预留记录：用于跟踪选择过程中的容量预留
	// 如果选择过程失败，会自动释放所有预留
	var tentativeReservation *VolumeGrowReservation

	// 根据 useReservations 标志选择合适的函数
	// 这是一个策略模式：使用或不使用容量预留
	var availableSpaceFunc func(Node, *VolumeGrowOption) int64
	var reserveOneVolumeFunc func(Node, int64, *VolumeGrowOption) (*DataNode, error)

	if useReservations {
		// 【预留模式】：在选择服务器时同时预留容量
		// 初始化临时预留记录
		tentativeReservation = &VolumeGrowReservation{
			servers:        make([]*DataNode, 0),
			reservationIds: make([]string, 0),
			diskType:       option.DiskType,
		}

		// 使用支持预留的可用空间查询函数
		availableSpaceFunc = func(node Node, option *VolumeGrowOption) int64 {
			return node.AvailableSpaceForReservation(option)
		}
		// 使用支持预留的卷预留函数
		reserveOneVolumeFunc = func(node Node, r int64, option *VolumeGrowOption) (*DataNode, error) {
			return node.ReserveOneVolumeForReservation(r, option)
		}
	} else {
		// 【非预留模式】：只查询可用空间，不实际预留
		availableSpaceFunc = func(node Node, option *VolumeGrowOption) int64 {
			return node.AvailableSpaceFor(option)
		}
		reserveOneVolumeFunc = func(node Node, r int64, option *VolumeGrowOption) (*DataNode, error) {
			return node.ReserveOneVolume(r, option)
		}
	}

	// 确保在出错时清理部分预留的容量
	// 这是一个错误恢复机制，保证操作的原子性
	defer func() {
		if err != nil && tentativeReservation != nil {
			tentativeReservation.releaseAllReservations()
		}
	}()
	// 【第一层：选择数据中心】
	// 需要选择 (rp.DiffDataCenterCount + 1) 个数据中心
	// 其中 1 个是主数据中心，其余是副本数据中心
	//
	// 示例：rp="100" 表示 1 个跨数据中心副本
	//   - 需要选择 1+1=2 个数据中心
	//   - 第一个是主数据中心，第二个存放副本
	mainDataCenter, otherDataCenters, dc_err := topo.PickNodesByWeight(rp.DiffDataCenterCount+1, option, func(node Node) error {
		// 验证条件 1：如果指定了数据中心，必须匹配
		if option.DataCenter != "" && node.IsDataCenter() && node.Id() != NodeId(option.DataCenter) {
			return fmt.Errorf("Not matching preferred data center:%s", option.DataCenter)
		}

		// 验证条件 2：数据中心内的机架数量必须足够
		// 需要至少 (rp.DiffRackCount + 1) 个机架
		if len(node.Children()) < rp.DiffRackCount+1 {
			return fmt.Errorf("Only has %d racks, not enough for %d.", len(node.Children()), rp.DiffRackCount+1)
		}

		// 验证条件 3：数据中心的总可用空间必须足够
		// 需要至少能容纳：
		//   - rp.DiffRackCount 个跨机架副本
		//   - rp.SameRackCount 个同机架副本
		//   - 1 个主副本
		if availableSpaceFunc(node, option) < int64(rp.DiffRackCount+rp.SameRackCount+1) {
			return fmt.Errorf("Free:%d < Expected:%d", availableSpaceFunc(node, option), rp.DiffRackCount+rp.SameRackCount+1)
		}

		// 验证条件 4：数据中心内必须有足够的"合格机架"
		// 合格机架 = 至少有 (rp.SameRackCount + 1) 个空闲服务器的机架
		possibleRacksCount := 0
		for _, rack := range node.Children() {
			// 统计该机架内有多少空闲服务器
			possibleDataNodesCount := 0
			for _, n := range rack.Children() {
				if availableSpaceFunc(n, option) >= 1 {
					possibleDataNodesCount++
				}
			}
			// 如果该机架的空闲服务器足够，计入合格机架
			if possibleDataNodesCount >= rp.SameRackCount+1 {
				possibleRacksCount++
			}
		}
		// 验证合格机架数量是否满足要求
		if possibleRacksCount < rp.DiffRackCount+1 {
			return fmt.Errorf("Only has %d racks with more than %d free data nodes, not enough for %d.", possibleRacksCount, rp.SameRackCount+1, rp.DiffRackCount+1)
		}
		return nil
	})
	if dc_err != nil {
		return nil, nil, dc_err
	}

	// 【第二层：在主数据中心内选择机架】
	// 需要选择 (rp.DiffRackCount + 1) 个机架
	// 其中 1 个是主机架，其余是副本机架
	//
	// 示例：rp="010" 表示 1 个跨机架副本
	//   - 需要选择 1+1=2 个机架
	//   - 第一个是主机架，第二个存放副本
	mainRack, otherRacks, rackErr := mainDataCenter.(*DataCenter).PickNodesByWeight(rp.DiffRackCount+1, option, func(node Node) error {
		// 验证条件 1：如果指定了机架，必须匹配
		if option.Rack != "" && node.IsRack() && node.Id() != NodeId(option.Rack) {
			return fmt.Errorf("Not matching preferred rack:%s", option.Rack)
		}

		// 验证条件 2：机架的可用空间必须足够
		// 需要至少能容纳 (rp.SameRackCount + 1) 个卷
		// 即 1 个主副本 + rp.SameRackCount 个同机架副本
		if availableSpaceFunc(node, option) < int64(rp.SameRackCount+1) {
			return fmt.Errorf("Free:%d < Expected:%d", availableSpaceFunc(node, option), rp.SameRackCount+1)
		}

		// 验证条件 3：机架内的服务器数量必须足够（快速检查）
		// 这是一个快速路径，避免详细遍历
		if len(node.Children()) < rp.SameRackCount+1 {
			return fmt.Errorf("Only has %d data nodes, not enough for %d.", len(node.Children()), rp.SameRackCount+1)
		}

		// 验证条件 4：机架内必须有足够的空闲服务器
		// 统计有空闲槽位的服务器数量
		possibleDataNodesCount := 0
		for _, n := range node.Children() {
			if availableSpaceFunc(n, option) >= 1 {
				possibleDataNodesCount++
			}
		}
		// 验证空闲服务器数量是否满足要求
		if possibleDataNodesCount < rp.SameRackCount+1 {
			return fmt.Errorf("Only has %d data nodes with a slot, not enough for %d.", possibleDataNodesCount, rp.SameRackCount+1)
		}
		return nil
	})
	if rackErr != nil {
		return nil, nil, rackErr
	}

	// 【第三层：在主机架内选择服务器】
	// 需要选择 (rp.SameRackCount + 1) 个服务器
	// 其中 1 个是主服务器，其余是同机架副本服务器
	//
	// 示例：rp="001" 表示 1 个同机架副本
	//   - 需要选择 1+1=2 个服务器
	//   - 第一个是主服务器，第二个存放副本
	mainServer, otherServers, serverErr := mainRack.(*Rack).PickNodesByWeight(rp.SameRackCount+1, option, func(node Node) error {
		// 验证条件 1：如果指定了数据节点，必须匹配
		if option.DataNode != "" && node.IsDataNode() && node.Id() != NodeId(option.DataNode) {
			return fmt.Errorf("Not matching preferred data node:%s", option.DataNode)
		}

		if useReservations {
			// 【预留模式】：原子性地检查并预留容量
			if node.IsDataNode() {
				// 尝试在该节点上预留 1 个卷的容量
				reservationId, success := node.TryReserveCapacity(option.DiskType, 1)
				if !success {
					return fmt.Errorf("Cannot reserve capacity on node %s", node.Id())
				}
				// 记录预留信息，用于后续清理
				tentativeReservation.servers = append(tentativeReservation.servers, node.(*DataNode))
				tentativeReservation.reservationIds = append(tentativeReservation.reservationIds, reservationId)
			} else if availableSpaceFunc(node, option) < 1 {
				// 非数据节点，只检查可用空间
				return fmt.Errorf("Free:%d < Expected:%d", availableSpaceFunc(node, option), 1)
			}
		} else {
			// 【非预留模式】：只检查可用空间
			if availableSpaceFunc(node, option) < 1 {
				return fmt.Errorf("Free:%d < Expected:%d", availableSpaceFunc(node, option), 1)
			}
		}
		return nil
	})
	if serverErr != nil {
		return nil, nil, serverErr
	}

	// 【组装服务器列表：第 1 部分 - 主机架服务器】
	// 添加主服务器
	servers = append(servers, mainServer.(*DataNode))
	// 添加同机架的其他服务器
	for _, server := range otherServers {
		servers = append(servers, server.(*DataNode))
	}

	// 【组装服务器列表：第 2 部分 - 其他机架服务器】
	// 在主数据中心的其他机架中，每个机架选择 1 个服务器
	for _, rack := range otherRacks {
		// 使用加权随机选择：根据可用空间大小进行随机选择
		// 可用空间越大，被选中的概率越高
		r := rand.Int64N(availableSpaceFunc(rack, option))
		if server, e := reserveOneVolumeFunc(rack, r, option); e == nil {
			servers = append(servers, server)

			// 如果使用预留模式，在选中的服务器上预留容量
			if useReservations {
				reservationId, success := server.TryReserveCapacity(option.DiskType, 1)
				if !success {
					return servers, nil, fmt.Errorf("failed to reserve capacity on server %s from other rack", server.Id())
				}
				tentativeReservation.servers = append(tentativeReservation.servers, server)
				tentativeReservation.reservationIds = append(tentativeReservation.reservationIds, reservationId)
			}
		} else {
			return servers, nil, e
		}
	}

	// 【组装服务器列表：第 3 部分 - 其他数据中心服务器】
	// 在其他数据中心中，每个数据中心选择 1 个服务器
	for _, datacenter := range otherDataCenters {
		// 使用加权随机选择
		r := rand.Int64N(availableSpaceFunc(datacenter, option))
		if server, e := reserveOneVolumeFunc(datacenter, r, option); e == nil {
			servers = append(servers, server)

			// 如果使用预留模式，在选中的服务器上预留容量
			if useReservations {
				reservationId, success := server.TryReserveCapacity(option.DiskType, 1)
				if !success {
					return servers, nil, fmt.Errorf("failed to reserve capacity on server %s from other datacenter", server.Id())
				}
				tentativeReservation.servers = append(tentativeReservation.servers, server)
				tentativeReservation.reservationIds = append(tentativeReservation.reservationIds, reservationId)
			}
		} else {
			return servers, nil, e
		}
	}

	// 【返回结果】
	// 如果使用了预留模式，返回预留信息
	if useReservations && tentativeReservation != nil {
		reservation = tentativeReservation
		glog.V(1).Infof("Successfully reserved capacity on %d servers for volume creation", len(servers))
	}

	return servers, reservation, nil
}

// grow 在指定的服务器上创建卷，并管理容量预留
// 这是卷创建的最终执行函数，包含完整的事务语义
//
// 执行流程（两阶段提交）：
//   【阶段 1：创建阶段】
//     1. 依次在每个服务器上调用 AllocateVolume 创建卷
//     2. 记录每个成功创建的卷信息
//     3. 任意一个服务器创建失败，立即中断
//
//   【阶段 2：提交或回滚阶段】
//     - 如果全部成功：
//       a. 在每个服务器上注册卷信息（AddOrUpdateVolume）
//       b. 在拓扑中注册卷布局（RegisterVolumeLayout）
//       c. 释放预留的容量（转为实际占用）
//     - 如果任意失败：
//       a. 删除已创建的卷（DeleteVolume）
//       b. 预留的容量由调用方释放（defer 机制）
//
// 参数:
//   - grpcDialOption: gRPC 连接选项
//   - topo: 拓扑结构
//   - vid: 要创建的 Volume ID
//   - option: 卷增长选项
//   - reservation: 容量预留信息（可为 nil）
//   - servers: 目标服务器列表
// 返回:
//   - growErr: 创建过程中的错误
func (vg *VolumeGrowth) grow(grpcDialOption grpc.DialOption, topo *Topology, vid needle.VolumeId, option *VolumeGrowOption, reservation *VolumeGrowReservation, servers ...*DataNode) (growErr error) {
	// 记录成功创建的卷信息
	var createdVolumes []storage.VolumeInfo

	// 【阶段 1：创建阶段】
	// 依次在每个服务器上创建卷
	for _, server := range servers {
		// 通过 gRPC 调用 Volume Server 的 AllocateVolume 接口
		if err := AllocateVolume(server, grpcDialOption, vid, option); err == nil {
			// 成功：记录卷信息
			createdVolumes = append(createdVolumes, storage.VolumeInfo{
				Id:               vid,                           // Volume ID
				Size:             0,                             // 初始大小为 0
				Collection:       option.Collection,             // 集合名称
				ReplicaPlacement: option.ReplicaPlacement,       // 副本策略
				Ttl:              option.Ttl,                    // 生存时间
				Version:          needle.Version(option.Version),// 卷版本
				DiskType:         option.DiskType.String(),      // 磁盘类型
				ModifiedAtSecond: time.Now().Unix(),             // 创建时间
			})
			glog.V(0).Infof("Created Volume %d on %s", vid, server.NodeImpl.String())
		} else {
			// 失败：记录错误并中断创建流程
			glog.Warningf("Failed to assign volume %d on %s: %v", vid, server.NodeImpl.String(), err)
			growErr = fmt.Errorf("failed to assign volume %d on %s: %v", vid, server.NodeImpl.String(), err)
			break
		}
	}

	// 【阶段 2：提交或回滚】
	if growErr == nil {
		// 【提交路径】：所有卷创建成功
		for i, vi := range createdVolumes {
			server := servers[i]
			// 在服务器的内存结构中注册卷信息
			server.AddOrUpdateVolume(vi)
			// 在拓扑的 VolumeLayout 中注册卷的位置信息
			topo.RegisterVolumeLayout(vi, server)
			glog.V(0).Infof("Registered Volume %d on %s", vid, server.NodeImpl.String())
		}
		// 释放预留的容量（转为实际占用）
		if reservation != nil {
			reservation.releaseAllReservations()
		}
	} else {
		// 【回滚路径】：部分卷创建失败，清理已创建的卷
		for i, vi := range createdVolumes {
			server := servers[i]
			// 通过 gRPC 调用删除已创建的卷
			if err := DeleteVolume(server, grpcDialOption, vi.Id); err != nil {
				glog.Warningf("Failed to clean up volume %d on %s", vid, server.NodeImpl.String())
			}
		}
		// 预留的容量会由调用方的 defer 释放
	}

	return growErr
}
