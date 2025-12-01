// Package weed_server 中的 master_grpc_server_volume.go 实现 Master 与 Volume/客户端之间的 gRPC 管理接口
// 主要职责包括自动扩容、卷位置查询、统计信息、Vacuum 控制等，帮助学习者理解 Master 的调度策略。
package weed_server

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/stats"

	"github.com/seaweedfs/seaweedfs/weed/topology"

	"github.com/seaweedfs/raft"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

const (
	// volumeGrowStepCount 是自动扩容时每次尝试新增的最小卷数
	// 设计成 2 是为了避免频繁地一卷一卷扩容，保证扩容效率且便于学习触发条件
	volumeGrowStepCount = 2
)

// DoAutomaticVolumeGrow 触发一次自动扩容流程
// 参数:
//   - req: 封装目标集合、副本、TTL、磁盘类型等条件的扩容请求
// 核心流程:
//   1. 检查是否允许自动扩容
//   2. 调用 VolumeGrowth 模块在指定拓扑上分配新卷
//   3. 向所有 KeepConnected 客户端广播新卷位置，保证缓存及时刷新
func (ms *MasterServer) DoAutomaticVolumeGrow(req *topology.VolumeGrowRequest) {
	if ms.option.VolumeGrowthDisabled {
		glog.V(1).Infof("automatic volume grow disabled")
		return
	}
	glog.V(1).Infoln("starting automatic volume grow")
	start := time.Now()
	// AutomaticGrowByType 内部会根据 ReplicaPlacement、DataCenter 等条件挑选节点
	newVidLocations, err := ms.vg.AutomaticGrowByType(req.Option, ms.grpcDialOption, ms.Topo, req.Count)
	glog.V(1).Infoln("finished automatic volume grow, cost ", time.Now().Sub(start))
	if err != nil {
		glog.V(1).Infof("automatic volume grow failed: %+v", err)
		return
	}
	for _, newVidLocation := range newVidLocations {
		// 广播给所有通过 streaming API 监听 Master 事件的客户端
		ms.broadcastToClients(&master_pb.KeepConnectedResponse{VolumeLocation: newVidLocation})
	}
}

// ProcessGrowRequest 启动两个后台协程:
//   1. 定期扫描拓扑、在需要时主动扩容
//   2. 处理来自 VolumeLayout 的即时扩容请求（volumeGrowthRequestChan）
// 参数:
//   - 无，依赖 MasterServer 内部的 Topology 与 vg（volume grower）
// 返回:
//   - 无，本函数仅注册协程并长时间运行
// 该函数通常在 Master 启动时调用一次，之后持续运行整个生命周期。
func (ms *MasterServer) ProcessGrowRequest() {
	go func() {
		ctx := context.Background()
		firstRun := true
		for {
			// 首次运行立即执行，之后每 5 分钟左右检查一次，避免频繁震荡
			if firstRun {
				firstRun = false
			} else {
				time.Sleep(5*time.Minute + time.Duration(30*rand.Float32())*time.Second)
			}
			if !ms.Topo.IsLeader() {
				continue
			}
			dcs := ms.Topo.ListDCAndRacks()
			var err error
			for _, vlc := range ms.Topo.ListVolumeLayoutCollections() {
				vl := vlc.VolumeLayout
				lastGrowCount := vl.GetLastGrowCount()
				if vl.HasGrowRequest() {
					// 当前 VolumeLayout 已有扩容任务在执行，跳过以免重复触发
					continue
				}
				writable, crowded := vl.GetWritableVolumeCount()
				mustGrow := int(lastGrowCount) - writable
				vgr := vlc.ToVolumeGrowRequest()
				stats.MasterVolumeLayoutWritable.WithLabelValues(vlc.Collection, vgr.DiskType, vgr.Replication, vgr.Ttl).Set(float64(writable))
				stats.MasterVolumeLayoutCrowded.WithLabelValues(vlc.Collection, vgr.DiskType, vgr.Replication, vgr.Ttl).Set(float64(crowded))

				switch {
				case mustGrow > 0:
					// Writable 数低于上次扩容目标值，优先补齐
					vgr.WritableVolumeCount = uint32(mustGrow)
					_, err = ms.VolumeGrow(ctx, vgr)
				case lastGrowCount > 0 && writable < int(lastGrowCount*2) && float64(crowded+volumeGrowStepCount) > float64(writable)*topology.VolumeGrowStrategy.Threshold:
					// 写入卷数量偏少且拥挤度过高（crowded/writable 超过阈值），按步长进行扩容
					vgr.WritableVolumeCount = volumeGrowStepCount
					_, err = ms.VolumeGrow(ctx, vgr)
				}
				if err != nil {
					glog.V(0).Infof("volume grow request failed: %+v", err)
				}
				writableVolumes := vl.CloneWritableVolumes()
				for dcId, racks := range dcs {
					for _, rackId := range racks {
						if vl.ShouldGrowVolumesByDcAndRack(&writableVolumes, dcId, rackId) {
							// 在数据中心+机架维度检查是否需要局部扩容
							vgr.DataCenter = string(dcId)
							vgr.Rack = string(rackId)
							if lastGrowCount > 0 {
								vgr.WritableVolumeCount = uint32(math.Ceil(float64(lastGrowCount) / float64(len(dcs)*len(racks))))
							} else {
								vgr.WritableVolumeCount = volumeGrowStepCount
							}

							if _, err = ms.VolumeGrow(ctx, vgr); err != nil {
								glog.V(0).Infof("volume grow request for dc:%s rack:%s failed: %+v", dcId, rackId, err)
							}
						}
					}
				}
			}
		}
	}()
	go func() {
		// filter 记录当前正在处理的 GrowRequest，避免相同参数重复扩容
		filter := sync.Map{}
		for {
			req, ok := <-ms.volumeGrowthRequestChan
			if !ok {
				break
			}

			option := req.Option
			vl := ms.Topo.GetVolumeLayout(option.Collection, option.ReplicaPlacement, option.Ttl, option.DiskType)

			if !ms.Topo.IsLeader() {
				//discard buffered requests
				time.Sleep(time.Second * 1)
				vl.DoneGrowRequest()
				continue
			}

			// filter out identical requests being processed
			found := false
			filter.Range(func(k, v interface{}) bool {
				existingReq := k.(*topology.VolumeGrowRequest)
				if existingReq.Equals(req) {
					found = true
				}
				return !found
			})

			// not atomic but it's okay
			if found || (!req.Force && !vl.ShouldGrowVolumes()) {
				// 如果已有相同任务或 VolumeLayout 判断无需扩容，则轻量 sleep 后丢弃
				glog.V(4).Infoln("discard volume grow request")
				time.Sleep(time.Millisecond * 211)
				vl.DoneGrowRequest()
				continue
			}

			filter.Store(req, nil)
			// we have lock called inside vg
			glog.V(0).Infof("volume grow %+v", req)
			go func(req *topology.VolumeGrowRequest, vl *topology.VolumeLayout) {
				ms.DoAutomaticVolumeGrow(req)
				vl.DoneGrowRequest()
				filter.Delete(req)
			}(req, vl)
		}
	}()
}

// LookupVolume 根据 VolumeId 或 FileId 查询位置
// 参数:
//   - ctx: 上下文，携带链路信息与超时控制
//   - req: gRPC 请求，包含 VolumeOrFileIds 与可选 collection 过滤
// 返回:
//   - LookupVolumeResponse: 每个请求 ID 在拓扑中的位置与 JWT
//   - error: 非 leader 或拓扑查询失败时返回
// 支持请求同时携带多个 ID，Master 会批量查找并回填 JWT 以供 Volume 访问
func (ms *MasterServer) LookupVolume(ctx context.Context, req *master_pb.LookupVolumeRequest) (*master_pb.LookupVolumeResponse, error) {

	resp := &master_pb.LookupVolumeResponse{}
	// 先批量查询，避免在循环内多次访问拓扑结构
	volumeLocations := ms.lookupVolumeId(req.VolumeOrFileIds, req.Collection)

	for _, volumeOrFileId := range req.VolumeOrFileIds {
		vid := volumeOrFileId
		commaSep := strings.Index(vid, ",")
		if commaSep > 0 {
			vid = vid[0:commaSep]
		}
		if result, found := volumeLocations[vid]; found {
			var locations []*master_pb.Location
			for _, loc := range result.Locations {
				locations = append(locations, &master_pb.Location{
					Url:        loc.Url,
					PublicUrl:  loc.PublicUrl,
					DataCenter: loc.DataCenter,
					GrpcPort:   uint32(loc.GrpcPort),
				})
			}
			var auth string
			if commaSep > 0 { // this is a file id
				// 对于具体文件，生成短期 JWT，Volume 会在读写接口验证
				auth = string(security.GenJwtForVolumeServer(ms.guard.SigningKey, ms.guard.ExpiresAfterSec, result.VolumeOrFileId))
			}
			resp.VolumeIdLocations = append(resp.VolumeIdLocations, &master_pb.LookupVolumeResponse_VolumeIdLocation{
				VolumeOrFileId: result.VolumeOrFileId,
				Locations:      locations,
				Error:          result.Error,
				Auth:           auth,
			})
		}
	}

	return resp, nil
}

// Statistics 返回指定集合/副本/磁盘类型下的磁盘统计信息
// 参数:
//   - ctx: gRPC 上下文
//   - req: 包含 Collection、Replication、Ttl、DiskType 等过滤条件
// 返回:
//   - StatisticsResponse: 包含总容量估算、已用容量以及文件数量
//   - error: 不是 leader、参数无法解析时返回
// 用于 CLI 或 UI 观察空间总量、已用量以及文件数
func (ms *MasterServer) Statistics(ctx context.Context, req *master_pb.StatisticsRequest) (*master_pb.StatisticsResponse, error) {

	if !ms.Topo.IsLeader() {
		return nil, raft.NotLeaderError
	}

	if req.Replication == "" {
		req.Replication = ms.option.DefaultReplicaPlacement
	}
	// ReplicaPlacement 字符串形如 "000"，解析后用于定位副本层级
	replicaPlacement, err := super_block.NewReplicaPlacementFromString(req.Replication)
	if err != nil {
		return nil, err
	}
	// TTL 为可选字段，若为空表示永久
	ttl, err := needle.ReadTTL(req.Ttl)
	if err != nil {
		return nil, err
	}

	volumeLayout := ms.Topo.GetVolumeLayout(req.Collection, replicaPlacement, ttl, types.ToDiskType(req.DiskType))
	stats := volumeLayout.Stats()
	// totalSize 以节点允许的最大卷数 × 单卷大小估算
	totalSize := ms.Topo.GetDiskUsages().GetMaxVolumeCount() * int64(ms.option.VolumeSizeLimitMB) * 1024 * 1024
	resp := &master_pb.StatisticsResponse{
		TotalSize: uint64(totalSize),
		UsedSize:  stats.UsedSize,
		FileCount: stats.FileCount,
	}

	return resp, nil
}

// VolumeList 将整个拓扑结构以及卷容量限制打包返回
// 参数:
//   - ctx: gRPC 上下文
//   - req: 目前未携带字段，占位
// 返回:
//   - VolumeListResponse: 包含拓扑快照和单卷大小限制
//   - error: 非 leader 时返回 NotLeader
// 主要供 UI 展示或外部工具调试使用
func (ms *MasterServer) VolumeList(ctx context.Context, req *master_pb.VolumeListRequest) (*master_pb.VolumeListResponse, error) {

	if !ms.Topo.IsLeader() {
		return nil, raft.NotLeaderError
	}

	resp := &master_pb.VolumeListResponse{
		TopologyInfo:      ms.Topo.ToTopologyInfo(),
		VolumeSizeLimitMb: uint64(ms.option.VolumeSizeLimitMB),
	}

	return resp, nil
}

// LookupEcVolume 查询纠删码卷对应 shard 的位置信息
// 参数:
//   - ctx: gRPC 上下文
//   - req: 包含 VolumeId 的请求
// 返回:
//   - LookupEcVolumeResponse: 每个 shard 的地址列表
//   - error: 非 leader 或未找到对应卷时返回
// 每个 shard 返回多个副本地址，供客户端汇聚下载或恢复
func (ms *MasterServer) LookupEcVolume(ctx context.Context, req *master_pb.LookupEcVolumeRequest) (*master_pb.LookupEcVolumeResponse, error) {

	if !ms.Topo.IsLeader() {
		return nil, raft.NotLeaderError
	}

	resp := &master_pb.LookupEcVolumeResponse{}

	ecLocations, found := ms.Topo.LookupEcShards(needle.VolumeId(req.VolumeId))

	if !found {
		return resp, fmt.Errorf("ec volume %d not found", req.VolumeId)
	}

	resp.VolumeId = req.VolumeId

	for shardId, shardLocations := range ecLocations.Locations {
		var locations []*master_pb.Location
		for _, dn := range shardLocations {
			locations = append(locations, &master_pb.Location{
				Url:        string(dn.Id()),
				PublicUrl:  dn.PublicUrl,
				DataCenter: dn.GetDataCenterId(),
			})
		}
		resp.ShardIdLocations = append(resp.ShardIdLocations, &master_pb.LookupEcVolumeResponse_EcShardIdLocation{
			ShardId:   uint32(shardId),
			Locations: locations,
		})
	}

	return resp, nil
}

// VacuumVolume 根据请求触发一次卷压缩操作
// 参数:
//   - ctx: gRPC 上下文
//   - req: 垃圾阈值、最大并发、目标卷等条件
// 返回:
//   - VacuumVolumeResponse: 当前没有实际字段，表示已接受
//   - error: 非 leader 时返回
// GarbageThreshold 控制允许的垃圾比例，VolumeId/Collection 可指定范围
func (ms *MasterServer) VacuumVolume(ctx context.Context, req *master_pb.VacuumVolumeRequest) (*master_pb.VacuumVolumeResponse, error) {

	if !ms.Topo.IsLeader() {
		return nil, raft.NotLeaderError
	}

	resp := &master_pb.VacuumVolumeResponse{}

	// Topology.Vacuum 会在 Volume Server 上逐个触发 vacuum 协程，参数 false 表示不强制 compact index
	ms.Topo.Vacuum(ms.grpcDialOption, float64(req.GarbageThreshold), ms.option.MaxParallelVacuumPerServer, req.VolumeId, req.Collection, ms.preallocateSize, false)

	return resp, nil
}

// DisableVacuum 全局关闭 Vacuum 任务调度
// 常用于维护窗口或磁盘压力过大时
func (ms *MasterServer) DisableVacuum(ctx context.Context, req *master_pb.DisableVacuumRequest) (*master_pb.DisableVacuumResponse, error) {

	ms.Topo.DisableVacuum()
	resp := &master_pb.DisableVacuumResponse{}
	return resp, nil
}

// EnableVacuum 重新开启 Vacuum 任务调度
// 与 DisableVacuum 成对出现
func (ms *MasterServer) EnableVacuum(ctx context.Context, req *master_pb.EnableVacuumRequest) (*master_pb.EnableVacuumResponse, error) {

	ms.Topo.EnableVacuum()
	resp := &master_pb.EnableVacuumResponse{}
	return resp, nil
}

// VolumeMarkReadonly 将指定数据节点上的卷标记为只读/可写
// 参数:
//   - ctx: gRPC 上下文
//   - req: 包含卷号、所在节点 IP/Port、Replica/Ttl/DiskType 等信息
// 返回:
//   - VolumeMarkReadonlyResponse: 空结构，仅表示操作已执行
// 场景: 升级、维护或灰度下线某台机器
func (ms *MasterServer) VolumeMarkReadonly(ctx context.Context, req *master_pb.VolumeMarkReadonlyRequest) (*master_pb.VolumeMarkReadonlyResponse, error) {

	if !ms.Topo.IsLeader() {
		return nil, raft.NotLeaderError
	}

	resp := &master_pb.VolumeMarkReadonlyResponse{}

	replicaPlacement, _ := super_block.NewReplicaPlacementFromByte(byte(req.ReplicaPlacement))
	vl := ms.Topo.GetVolumeLayout(req.Collection, replicaPlacement, needle.LoadTTLFromUint32(req.Ttl), types.ToDiskType(req.DiskType))
	// Lookup 会返回所有持有该卷的数据节点，遍历以找到目标节点
	dataNodes := ms.Topo.Lookup(req.Collection, needle.VolumeId(req.VolumeId))

	for _, dn := range dataNodes {
		if dn.Ip == req.Ip && dn.Port == int(req.Port) {
			if req.IsReadonly {
				// 同步更新 VolumeLayout 中的状态，使调度器不再分配写入
				vl.SetVolumeReadOnly(dn, needle.VolumeId(req.VolumeId))
			} else {
				vl.SetVolumeWritable(dn, needle.VolumeId(req.VolumeId))
			}
		}
	}

	return resp, nil
}

// VolumeGrow 暴露给客户端的显式扩容接口
// 参数:
//   - ctx: gRPC 上下文
//   - req: 包含 collection、replication、ttl、磁盘类型和目标数据中心等条件
// 返回:
//   - VolumeGrowResponse: 暂无字段，表示请求已受理
//   - error: 非 leader、空间不足或参数错误时返回
// 该接口会校验 leader、解析 Replica/Ttl 等信息后复用自动扩容逻辑
func (ms *MasterServer) VolumeGrow(ctx context.Context, req *master_pb.VolumeGrowRequest) (*master_pb.VolumeGrowResponse, error) {
	if !ms.Topo.IsLeader() {
		return nil, raft.NotLeaderError
	}
	if req.Replication == "" {
		req.Replication = ms.option.DefaultReplicaPlacement
	}
	replicaPlacement, err := super_block.NewReplicaPlacementFromString(req.Replication)
	if err != nil {
		return nil, err
	}
	ttl, err := needle.ReadTTL(req.Ttl)
	if err != nil {
		return nil, err
	}
	if req.DataCenter != "" && !ms.Topo.DataCenterExists(req.DataCenter) {
		return nil, fmt.Errorf("data center not exists")
	}

	ver := needle.GetCurrentVersion()
	volumeGrowOption := topology.VolumeGrowOption{
		Collection:         req.Collection,
		ReplicaPlacement:   replicaPlacement,
		Ttl:                ttl,
		DiskType:           types.ToDiskType(req.DiskType),
		Preallocate:        ms.preallocateSize,
		DataCenter:         req.DataCenter,
		Rack:               req.Rack,
		DataNode:           req.DataNode,
		MemoryMapMaxSizeMb: req.MemoryMapMaxSizeMb,
		Version:            uint32(ver),
	}
	volumeGrowRequest := topology.VolumeGrowRequest{
		Option: &volumeGrowOption,
		Count:  req.WritableVolumeCount,
		Force:  true,
		Reason: "grpc volume grow",
	}
	// 计算实际需要的卷数量 = 请求卷数 × 副本数量
	replicaCount := int64(req.WritableVolumeCount * uint32(replicaPlacement.GetCopyCount()))

	if ms.Topo.AvailableSpaceFor(&volumeGrowOption) < replicaCount {
		// 乘以副本数量确保所有 copy 都能分配到位
		return nil, fmt.Errorf("only %d volumes left, not enough for %d", ms.Topo.AvailableSpaceFor(&volumeGrowOption), replicaCount)
	}

	if !ms.Topo.DataCenterExists(volumeGrowOption.DataCenter) {
		err = fmt.Errorf("data center %v not found in topology", volumeGrowOption.DataCenter)
	}

	// 最终仍然调用自动扩容逻辑，保持统一入口（会广播新卷位置）
	ms.DoAutomaticVolumeGrow(&volumeGrowRequest)

	return &master_pb.VolumeGrowResponse{}, nil
}
