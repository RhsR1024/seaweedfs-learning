// Package weed_server 实现了 SeaweedFS Master Server 的 gRPC 服务
//
// 功能说明：
// 本文件实现了 Master Server 的核心 gRPC 服务接口，包括：
//   1. Volume Server 心跳处理 (SendHeartbeat)
//   2. 客户端长连接管理 (KeepConnected)
//   3. Volume 位置信息广播
//   4. 集群拓扑管理
//
// 关键概念：
//   - Heartbeat：Volume Server 定期发送心跳，报告其存储的 Volume 信息
//   - KeepConnected：Filer 等客户端与 Master 保持长连接，实时获取 Volume 位置变更
//   - UUID 管理：防止同一磁盘目录被多个 Volume Server 同时加载
//   - 增量同步：支持增量心跳，仅报告新增/删除的 Volume，提高效率
package weed_server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/seaweedfs/seaweedfs/weed/cluster"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/util"

	"github.com/seaweedfs/raft"
	"google.golang.org/grpc/peer"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/topology"
)

// RegisterUuids 注册 Volume Server 的存储目录 UUID
//
// 功能说明：
// 每个 Volume Server 的存储目录都有唯一的 UUID，此函数检查并注册这些 UUID，
// 防止同一磁盘目录被多个 Volume Server 同时加载（会导致数据损坏）
//
// 工作原理：
// 1. 检查心跳中的 UUID 是否已被其他 Volume Server 注册
// 2. 如果发现重复的 UUID，返回错误，拒绝该 Volume Server 的注册
// 3. 如果 UUID 都是新的，将其注册到 UuidMap 中
//
// 参数:
//   - heartbeat: Volume Server 发送的心跳消息，包含 LocationUuids 字段
//
// 返回:
//   - duplicated_uuids: 发现的重复 UUID 列表
//   - err: 如果有重复 UUID，返回错误
//
// 应用场景：
// 场景 1：Volume Server 重启 - 使用相同的 UUID，注册成功
// 场景 2：误启动多个 Volume Server 指向同一目录 - 检测到重复 UUID，拒绝注册
// 场景 3：添加新的存储目录 - UUID 是新的，注册成功
func (ms *MasterServer) RegisterUuids(heartbeat *master_pb.Heartbeat) (duplicated_uuids []string, err error) {
	// 加锁保护 UuidMap，防止并发访问导致数据竞争
	ms.Topo.UuidAccessLock.Lock()
	defer ms.Topo.UuidAccessLock.Unlock()

	// 构造 Volume Server 的唯一标识：IP:Port
	// 例如：192.168.1.100:8080
	key := fmt.Sprintf("%s:%d", heartbeat.Ip, heartbeat.Port)

	// 初始化 UuidMap（如果尚未初始化）
	// UuidMap 结构：map[volumeServer_address][]directory_uuid
	if ms.Topo.UuidMap == nil {
		ms.Topo.UuidMap = make(map[string][]string)
	}

	// 【步骤 1：检查 UUID 冲突】
	// 遍历所有已注册的 Volume Server，查找是否有重复的 UUID
	for k, v := range ms.Topo.UuidMap {
		// 对已注册的 UUID 列表排序，用于二分查找
		sort.Strings(v)

		// 检查心跳中的每个 UUID 是否已被其他 Volume Server 使用
		for _, id := range heartbeat.LocationUuids {
			// 使用二分查找，时间复杂度 O(log n)
			index := sort.SearchStrings(v, id)

			// 找到重复的 UUID
			if index < len(v) && v[index] == id {
				duplicated_uuids = append(duplicated_uuids, id)
				// 记录错误日志：哪个 Volume Server 已经加载了这个目录
				glog.Errorf("directory of %s on %s has been loaded", id, k)
			}
		}
	}

	// 【步骤 2：处理 UUID 冲突】
	// 如果发现重复的 UUID，拒绝注册
	if len(duplicated_uuids) > 0 {
		return duplicated_uuids, errors.New("volume: Duplicated volume directories were loaded")
	}

	// 【步骤 3：注册新的 UUID】
	// 将 Volume Server 的 UUID 列表注册到 UuidMap 中
	ms.Topo.UuidMap[key] = heartbeat.LocationUuids
	glog.V(0).Infof("found new uuid:%v %v , %v", key, heartbeat.LocationUuids, ms.Topo.UuidMap)

	return nil, nil
}

// UnRegisterUuids 注销 Volume Server 的存储目录 UUID
//
// 功能说明：
// 当 Volume Server 断开连接时，从 UuidMap 中删除其注册的 UUID，
// 使得这些存储目录可以被重新加载（如重启后）
//
// 参数:
//   - ip: Volume Server 的 IP 地址
//   - port: Volume Server 的端口号
//
// 调用时机：
// 1. Volume Server 断开心跳连接时
// 2. Volume Server 正常关闭时
// 3. 检测到 Volume Server 故障时
func (ms *MasterServer) UnRegisterUuids(ip string, port int) {
	// 加锁保护 UuidMap
	ms.Topo.UuidAccessLock.Lock()
	defer ms.Topo.UuidAccessLock.Unlock()

	// 构造 Volume Server 的唯一标识
	key := fmt.Sprintf("%s:%d", ip, port)

	// 从 UuidMap 中删除该 Volume Server 的 UUID 注册
	delete(ms.Topo.UuidMap, key)

	glog.V(0).Infof("remove volume server %v, online volume server: %v", key, ms.Topo.UuidMap)
}

// SendHeartbeat 处理 Volume Server 的心跳流
//
// 功能说明：
// 这是 Master Server 最核心的 gRPC 流式接口，Volume Server 通过此接口：
// 1. 首次连接时注册自己和所有 Volume 信息（完整心跳）
// 2. 之后定期发送增量心跳，仅报告变更的 Volume（新增/删除）
// 3. 接收 Master 的响应，获取配置信息和 Leader 地址
//
// 心跳类型：
//   - 完整心跳（Full Heartbeat）：包含所有 Volume 列表（Volumes 字段非空）
//   - 增量心跳（Delta Heartbeat）：仅包含变更（NewVolumes/DeletedVolumes 字段）
//   - 空心跳（Empty Heartbeat）：仅保持连接，无 Volume 变更（IP 为空）
//
// 工作流程：
// 1. 接收 Volume Server 的心跳消息
// 2. 如果不是 Leader，告知 Volume Server 新的 Leader 地址
// 3. 如果是首次连接，注册 DataNode 和 UUID
// 4. 处理 Volume 信息变更（完整或增量）
// 5. 处理 EC Shard 信息变更
// 6. 广播 Volume 位置变更给所有客户端
//
// 参数:
//   - stream: gRPC 双向流，用于接收心跳和发送响应
//
// 返回:
//   - error: 流处理错误（连接断开、解析错误等）
//
// 注意事项：
// 1. 使用 defer 处理连接断开时的清理工作
// 2. 支持 "phantom" Volume Server（多个连接共享同一个 DataNode）
// 3. 广播机制确保所有客户端实时获取 Volume 位置变更
func (ms *MasterServer) SendHeartbeat(stream master_pb.Seaweed_SendHeartbeatServer) error {
	// dn 指向当前 Volume Server 对应的 DataNode 对象
	// DataNode 代表拓扑树中的一个物理节点（Volume Server）
	var dn *topology.DataNode

	// 【defer：处理连接断开时的清理工作】
	// 当心跳流结束时（正常关闭或异常断开），执行清理逻辑
	defer func() {
		if dn != nil {
			// Counter 用于处理 "phantom" Volume Server
			// 即同一个 Volume Server 可能建立多个心跳连接（网络波动、重连等）
			dn.Counter--

			if dn.Counter > 0 {
				// 还有其他心跳连接存在，仅减少计数器，不注销节点
				glog.V(0).Infof("disconnect phantom volume server %s:%d remaining %d", dn.Ip, dn.Port, dn.Counter)
				return
			}

			// 【步骤 1：构造 Volume 位置删除消息】
			// 准备通知所有客户端：此 Volume Server 上的所有 Volume 已不可用
			message := &master_pb.VolumeLocation{
				DataCenter: dn.GetDataCenterId(),
				Url:        dn.Url(),
				PublicUrl:  dn.PublicUrl,
				GrpcPort:   uint32(dn.GrpcPort),
			}

			// 收集所有普通 Volume 的 ID
			for _, v := range dn.GetVolumes() {
				message.DeletedVids = append(message.DeletedVids, uint32(v.Id))
			}

			// 收集所有 EC Shard 的 Volume ID
			for _, s := range dn.GetEcShards() {
				message.DeletedEcVids = append(message.DeletedEcVids, uint32(s.VolumeId))
			}

			// 【步骤 2：注销 DataNode】
			// 注意：这里处理注册和注销的竞态条件
			// 如果 Volume Server 断开后快速重连，可能出现注销和注册交错执行
			ms.Topo.UnRegisterDataNode(dn)
			glog.V(0).Infof("unregister disconnected volume server %s:%d", dn.Ip, dn.Port)

			// 注销 UUID，允许该存储目录被重新加载
			ms.UnRegisterUuids(dn.Ip, dn.Port)

			// 【步骤 3：广播删除消息】
			// 如果是 Leader 且有 Volume 被删除，通知所有客户端
			if ms.Topo.IsLeader() && (len(message.DeletedVids) > 0 || len(message.DeletedEcVids) > 0) {
				ms.broadcastToClients(&master_pb.KeepConnectedResponse{VolumeLocation: message})
			}
		}
	}()

	// 【主循环：持续接收心跳消息】
	for {
		// 接收一条心跳消息
		// 心跳是 gRPC 流式消息，Volume Server 会周期性发送
		heartbeat, err := stream.Recv()
		if err != nil {
			// 接收错误：连接断开、网络错误等
			if dn != nil {
				glog.Warningf("SendHeartbeat.Recv server %s:%d : %v", dn.Ip, dn.Port, err)
			} else {
				glog.Warningf("SendHeartbeat.Recv: %v", err)
			}
			stats.MasterReceivedHeartbeatCounter.WithLabelValues("error").Inc()
			return err
		}

		// 【步骤 4：检查 Leader 状态】
		// 如果当前 Master 不是 Leader，告知 Volume Server 新的 Leader 地址
		if !ms.Topo.IsLeader() {
			// 查询当前的 Leader
			newLeader, err := ms.Topo.Leader()
			if err != nil {
				glog.Warningf("SendHeartbeat find leader: %v", err)
				return err
			}

			// 发送响应，告知 Volume Server 新的 Leader 地址
			// Volume Server 收到后会重新连接到 Leader
			if err := stream.Send(&master_pb.HeartbeatResponse{
				Leader: string(newLeader),
			}); err != nil {
				if dn != nil {
					glog.Warningf("SendHeartbeat.Send response to %s:%d %v", dn.Ip, dn.Port, err)
				} else {
					glog.Warningf("SendHeartbeat.Send response %v", err)
				}
				return err
			}
			// 继续接收下一条心跳（Volume Server 可能还在切换到新 Leader）
			continue
		}

		// 【步骤 5：更新全局 Sequence】
		// MaxFileKey 是 Volume Server 当前已使用的最大文件 Key
		// Master 维护全局 Sequence，确保生成的文件 ID 不会冲突
		ms.Topo.Sequence.SetMax(heartbeat.MaxFileKey)

		// 【步骤 6：处理首次连接（DataNode 注册）】
		if dn == nil {
			// 【优化：跳过空心跳】
			// SeaweedFS 3.28+ 版本支持增量心跳，IP 为空表示仅保持连接
			// 这种心跳不需要处理，直接跳过
			if heartbeat.Ip == "" {
				continue
			} // ToDo must be removed after update major version

			// 【步骤 6.1：定位数据中心和机架】
			// 根据 IP 和心跳中的提示，确定 Volume Server 所属的数据中心和机架
			// ms.Topo.Configuration.Locate 会根据配置文件进行映射
			dcName, rackName := ms.Topo.Configuration.Locate(heartbeat.Ip, heartbeat.DataCenter, heartbeat.Rack)

			// 【步骤 6.2：创建或获取拓扑节点】
			// 拓扑结构：DataCenter -> Rack -> DataNode
			dc := ms.Topo.GetOrCreateDataCenter(dcName)
			rack := dc.GetOrCreateRack(rackName)
			dn = rack.GetOrCreateDataNode(heartbeat.Ip, int(heartbeat.Port), int(heartbeat.GrpcPort), heartbeat.PublicUrl, heartbeat.MaxVolumeCounts)

			glog.V(0).Infof("added volume server %d: %v:%d %v", dn.Counter, heartbeat.GetIp(), heartbeat.GetPort(), heartbeat.LocationUuids)

			// 【步骤 6.3：注册 UUID】
			// 检查存储目录 UUID 是否已被其他 Volume Server 使用
			uuidlist, err := ms.RegisterUuids(heartbeat)
			if err != nil {
				// UUID 冲突：发送错误响应，包含重复的 UUID 列表
				if stream_err := stream.Send(&master_pb.HeartbeatResponse{
					DuplicatedUuids: uuidlist,
				}); stream_err != nil {
					glog.Warningf("SendHeartbeat.Send DuplicatedDirectory response to %s:%d %v", dn.Ip, dn.Port, stream_err)
					return stream_err
				}
				return err
			}

			// 【步骤 6.4：发送初始配置】
			// 首次连接成功，发送 Master 的配置信息
			if err := stream.Send(&master_pb.HeartbeatResponse{
				VolumeSizeLimit: uint64(ms.option.VolumeSizeLimitMB) * 1024 * 1024, // 单个 Volume 的大小限制
				Preallocate:     ms.preallocateSize > 0,                             // 是否预分配磁盘空间
			}); err != nil {
				glog.Warningf("SendHeartbeat.Send volume size to %s:%d %v", dn.Ip, dn.Port, err)
				return err
			}

			// 更新统计计数器
			stats.MasterReceivedHeartbeatCounter.WithLabelValues("dataNode").Inc()

			// Counter++ 记录连接数（处理 phantom Volume Server）
			dn.Counter++
		}

		// 【步骤 7：调整 Volume 容量】
		// Volume Server 可能动态调整容量（添加磁盘、修改配置等）
		dn.AdjustMaxVolumeCounts(heartbeat.MaxVolumeCounts)

		glog.V(4).Infof("master received heartbeat %s", heartbeat.String())
		stats.MasterReceivedHeartbeatCounter.WithLabelValues("total").Inc()

		// 【步骤 8：准备广播消息】
		// 如果有 Volume 变更，需要广播给所有客户端
		message := &master_pb.VolumeLocation{
			Url:        dn.Url(),
			PublicUrl:  dn.PublicUrl,
			DataCenter: dn.GetDataCenterId(),
			GrpcPort:   uint32(dn.GrpcPort),
		}

		// 【步骤 9：处理增量 Volume 变更】
		// SeaweedFS 3.28+ 支持增量心跳，仅报告新增/删除的 Volume
		if len(heartbeat.NewVolumes) > 0 {
			stats.MasterReceivedHeartbeatCounter.WithLabelValues("newVolumes").Inc()
		}
		if len(heartbeat.DeletedVolumes) > 0 {
			stats.MasterReceivedHeartbeatCounter.WithLabelValues("deletedVolumes").Inc()
		}
		if len(heartbeat.NewVolumes) > 0 || len(heartbeat.DeletedVolumes) > 0 {
			// 收集新增和删除的 Volume ID
			for _, volInfo := range heartbeat.NewVolumes {
				message.NewVids = append(message.NewVids, volInfo.Id)
			}
			for _, volInfo := range heartbeat.DeletedVolumes {
				message.DeletedVids = append(message.DeletedVids, volInfo.Id)
			}

			// 增量同步：更新 Master 的拓扑信息
			// 这比完整同步更高效，减少了 CPU 和内存开销
			ms.Topo.IncrementalSyncDataNodeRegistration(heartbeat.NewVolumes, heartbeat.DeletedVolumes, dn)
		}

		// 【步骤 10：处理完整 Volume 列表】
		// 如果心跳包含完整的 Volume 列表，执行完整同步
		if len(heartbeat.Volumes) > 0 || heartbeat.HasNoVolumes {
			if heartbeat.Ip != "" {
				// 重新定位 DataNode（可能配置变更）
				dcName, rackName := ms.Topo.Configuration.Locate(heartbeat.Ip, heartbeat.DataCenter, heartbeat.Rack)
				ms.Topo.DataNodeRegistration(dcName, rackName, dn)
			}

			// 完整同步：对比新旧 Volume 列表，找出变更
			stats.MasterReceivedHeartbeatCounter.WithLabelValues("Volumes").Inc()
			newVolumes, deletedVolumes := ms.Topo.SyncDataNodeRegistration(heartbeat.Volumes, dn)

			// 记录新增的 Volume
			for _, v := range newVolumes {
				glog.V(0).Infof("master see new volume %d from %s", uint32(v.Id), dn.Url())
				message.NewVids = append(message.NewVids, uint32(v.Id))
			}

			// 记录删除的 Volume
			for _, v := range deletedVolumes {
				glog.V(0).Infof("master see deleted volume %d from %s", uint32(v.Id), dn.Url())
				message.DeletedVids = append(message.DeletedVids, uint32(v.Id))
			}
		}

		// 【步骤 11：处理增量 EC Shard 变更】
		// EC (Erasure Coding) Shard 是纠删码的分片
		if len(heartbeat.NewEcShards) > 0 || len(heartbeat.DeletedEcShards) > 0 {
			stats.MasterReceivedHeartbeatCounter.WithLabelValues("newEcShards").Inc()

			// 增量同步 EC Shard 信息
			ms.Topo.IncrementalSyncDataNodeEcShards(heartbeat.NewEcShards, heartbeat.DeletedEcShards, dn)

			// 收集新增的 EC Shard Volume ID
			for _, s := range heartbeat.NewEcShards {
				message.NewEcVids = append(message.NewEcVids, s.Id)
			}

			// 收集删除的 EC Shard Volume ID
			// 注意：如果 DataNode 还有该 Volume 的其他 Shard，不报告删除
			for _, s := range heartbeat.DeletedEcShards {
				if dn.HasEcShards(needle.VolumeId(s.Id)) {
					continue
				}
				message.DeletedEcVids = append(message.DeletedEcVids, s.Id)
			}

		}

		// 【步骤 12：处理完整 EC Shard 列表】
		if len(heartbeat.EcShards) > 0 || heartbeat.HasNoEcShards {
			stats.MasterReceivedHeartbeatCounter.WithLabelValues("ecShards").Inc()
			glog.V(4).Infof("master received ec shards from %s: %+v", dn.Url(), heartbeat.EcShards)

			// 完整同步 EC Shard 信息
			newShards, deletedShards := ms.Topo.SyncDataNodeEcShards(heartbeat.EcShards, dn)

			// 收集新增的 EC Shard Volume ID
			for _, s := range newShards {
				message.NewEcVids = append(message.NewEcVids, uint32(s.VolumeId))
			}

			// 收集删除的 EC Shard Volume ID
			// 注意：如果 DataNode 还有该 Volume 的普通副本，不报告删除
			for _, s := range deletedShards {
				if dn.HasVolumesById(s.VolumeId) {
					continue
				}
				message.DeletedEcVids = append(message.DeletedEcVids, uint32(s.VolumeId))
			}

		}

		// 【步骤 13：广播 Volume 位置变更】
		// 如果有任何 Volume 或 EC Shard 变更，通知所有连接的客户端
		// 客户端（Filer、应用程序）会更新本地的 Volume 位置缓存
		if len(message.NewVids) > 0 || len(message.DeletedVids) > 0 || len(message.NewEcVids) > 0 || len(message.DeletedEcVids) > 0 {
			ms.broadcastToClients(&master_pb.KeepConnectedResponse{VolumeLocation: message})
		}
	}
}

// KeepConnected 维持客户端与 Master 的长连接
//
// 功能说明：
// 这是 Master Server 的客户端连接管理接口，Filer、应用程序等客户端通过此接口：
// 1. 与 Master 建立长连接，实时获取 Volume 位置信息
// 2. 接收 Volume 位置变更通知（新增、删除）
// 3. 监控 Master 的 Leader 状态变化
// 4. 注册自己到集群管理中（用于集群拓扑展示）
//
// 工作流程：
// 1. 客户端发送连接请求，包含自身信息（类型、数据中心、机架等）
// 2. Master 返回当前所有 Volume 的位置信息（初始同步）
// 3. 客户端保持连接，持续接收 Volume 位置变更通知
// 4. Master 定期检查 Leader 状态，如果切换则通知客户端
// 5. 连接断开时，从集群管理中移除客户端
//
// 参数:
//   - stream: gRPC 双向流，用于接收客户端请求和发送 Volume 位置更新
//
// 返回:
//   - error: 流处理错误（连接断开、发送失败等）
//
// 客户端类型：
//   - "filer": Filer Server，提供文件系统接口
//   - "mount": FUSE 挂载客户端
//   - "s3": S3 API 服务
//   - "webdav": WebDAV 服务
//
// 注意事项：
// 1. 使用 buffered channel 避免广播阻塞
// 2. 定期检查 Leader 状态（5 秒间隔）
// 3. 客户端断开时自动清理资源
func (ms *MasterServer) KeepConnected(stream master_pb.Seaweed_KeepConnectedServer) error {

	// 【步骤 1：接收客户端连接请求】
	req, recvErr := stream.Recv()
	if recvErr != nil {
		return recvErr
	}

	// 【步骤 2：检查 Leader 状态】
	// 如果当前 Master 不是 Leader，通知客户端连接到 Leader
	if !ms.Topo.IsLeader() {
		return ms.informNewLeader(stream)
	}

	// 【步骤 3：生成客户端唯一标识】
	clientAddress := req.ClientAddress
	// 确保客户端地址唯一，如果客户端未提供，使用 UUID
	if clientAddress == "" {
		clientAddress = uuid.New().String()
	}
	peerAddress := pb.ServerAddress(clientAddress)

	// 【步骤 4：创建停止信号通道】
	// buffer by 1 避免写入 stopChan 时永久阻塞
	stopChan := make(chan bool, 1)

	// 【步骤 5：注册客户端】
	// 将客户端添加到广播列表，之后所有 Volume 变更都会发送给此客户端
	clientName, messageChan := ms.addClient(req.FilerGroup, req.ClientType, peerAddress)

	// 【步骤 6：注册到集群管理】
	// 将客户端信息添加到集群拓扑中，用于集群状态展示
	// 返回的 update 消息会广播给其他客户端（通知有新节点加入）
	for _, update := range ms.Cluster.AddClusterNode(req.FilerGroup, req.ClientType, cluster.DataCenter(req.DataCenter), cluster.Rack(req.Rack), peerAddress, req.Version) {
		ms.broadcastToClients(update)
	}

	// 【defer：处理连接断开时的清理工作】
	defer func() {
		// 从集群管理中移除客户端
		// 返回的 update 消息会广播给其他客户端（通知有节点离开）
		for _, update := range ms.Cluster.RemoveClusterNode(req.FilerGroup, req.ClientType, peerAddress) {
			ms.broadcastToClients(update)
		}
		// 从广播列表中移除客户端
		ms.deleteClient(clientName)
	}()

	// 【步骤 7：发送初始 Volume 位置信息】
	// 将当前所有 Volume 的位置信息发送给客户端（完整同步）
	for i, message := range ms.Topo.ToVolumeLocations() {
		// 第一条消息附带 Leader 信息
		if i == 0 {
			if leader, err := ms.Topo.Leader(); err == nil {
				message.Leader = string(leader)
			}
		}

		// 发送 Volume 位置消息
		if sendErr := stream.Send(&master_pb.KeepConnectedResponse{VolumeLocation: message}); sendErr != nil {
			return sendErr
		}
	}

	// 【步骤 8：启动客户端心跳接收协程】
	// 持续接收客户端的心跳消息（保持连接活跃）
	go func() {
		for {
			_, err := stream.Recv()
			if err != nil {
				// 客户端断开连接
				glog.V(2).Infof("- client %v: %v", clientName, err)

				// 启动消息通道消费协程，避免死锁
				// 当连接断开后，messageChan 可能还有待发送的消息
				// 需要消费掉这些消息，否则发送方会阻塞
				go func() {
					// consume message chan to avoid deadlock, go routine exit when message chan is closed
					for range messageChan {
						// no op - 仅消费消息，不做处理
					}
				}()

				// 通知主循环停止
				close(stopChan)
				return
			}
		}
	}()

	// 【步骤 9：主循环 - 发送 Volume 位置更新】
	// 定期检查 Leader 状态，并转发 Volume 位置变更消息
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case message := <-messageChan:
			// 【情况 1：收到 Volume 位置变更消息】
			// 从广播通道接收到消息，转发给客户端
			if err := stream.Send(message); err != nil {
				glog.V(0).Infof("=> client %v: %+v", clientName, message)
				return err
			}

		case <-ticker.C:
			// 【情况 2：定期检查 Leader 状态】
			// 每 5 秒检查一次当前 Master 是否还是 Leader
			if !ms.Topo.IsLeader() {
				// Leader 发生变化，更新监控指标
				stats.MasterRaftIsleader.Set(0)
				stats.MasterAdminLock.Reset()
				stats.MasterReplicaPlacementMismatch.Reset()

				// 通知客户端新的 Leader 地址
				return ms.informNewLeader(stream)
			} else {
				// 仍然是 Leader，更新监控指标
				stats.MasterRaftIsleader.Set(1)
			}

		case <-stopChan:
			// 【情况 3：收到停止信号】
			// 客户端断开连接，退出主循环
			return nil
		}
	}

}

// broadcastToClients 向所有连接的客户端广播消息
//
// 功能说明：
// 当 Volume 位置发生变化时（新增、删除），此函数将变更消息广播给所有客户端
// 客户端收到消息后会更新本地的 Volume 位置缓存
//
// 工作原理：
// 1. 遍历所有客户端的消息通道
// 2. 尝试非阻塞地发送消息到每个通道
// 3. 如果通道已满，记录错误但不阻塞（避免影响其他客户端）
//
// 参数:
//   - message: 要广播的 Volume 位置变更消息
//
// 注意事项：
// 1. 使用读锁，允许并发广播
// 2. 使用非阻塞 select，避免慢客户端影响整体性能
// 3. 如果客户端通道已满，记录错误并增加监控计数器
func (ms *MasterServer) broadcastToClients(message *master_pb.KeepConnectedResponse) {
	// 使用读锁，允许多个广播同时进行
	ms.clientChansLock.RLock()

	// 遍历所有客户端的消息通道
	for client, ch := range ms.clientChans {
		select {
		case ch <- message:
			// 成功发送消息到客户端通道
			glog.V(4).Infof("send message to %s", client)
		default:
			// 客户端通道已满（客户端消费速度慢或网络阻塞）
			// 不阻塞等待，直接跳过此客户端，避免影响其他客户端
			stats.MasterBroadcastToFullErrorCounter.Inc()
			glog.Errorf("broadcastToClients %s message full", client)
		}
	}

	ms.clientChansLock.RUnlock()
}

// informNewLeader 通知客户端新的 Leader 地址
//
// 功能说明：
// 当 Master Leader 发生切换时，通过此函数告知客户端新的 Leader 地址
// 客户端收到后会断开当前连接，重新连接到新的 Leader
//
// 参数:
//   - stream: 客户端连接的 gRPC 流
//
// 返回:
//   - error: 发送失败时返回错误，否则返回 raft.NotLeaderError
//
// 调用时机：
// 1. 客户端首次连接时，发现当前 Master 不是 Leader
// 2. 连接期间，Master 失去 Leader 身份（Raft 选举）
// 3. 定期检查发现 Leader 状态变化
func (ms *MasterServer) informNewLeader(stream master_pb.Seaweed_KeepConnectedServer) error {
	// 查询当前的 Leader 地址
	leader, err := ms.Topo.Leader()
	if err != nil {
		glog.Errorf("topo leader: %v", err)
		return raft.NotLeaderError
	}

	// 发送响应，告知客户端新的 Leader 地址
	if err := stream.Send(&master_pb.KeepConnectedResponse{
		VolumeLocation: &master_pb.VolumeLocation{
			Leader: string(leader),
		},
	}); err != nil {
		return err
	}

	return nil
}

// addClient 添加客户端到广播列表
//
// 功能说明：
// 为新连接的客户端创建消息通道，并注册到广播列表中
// 之后所有 Volume 位置变更都会通过此通道发送给客户端
//
// 参数:
//   - filerGroup: Filer 组名（用于 Filer 集群管理）
//   - clientType: 客户端类型（filer、mount、s3、webdav 等）
//   - clientAddress: 客户端地址
//
// 返回:
//   - clientName: 客户端的唯一标识，格式：{filerGroup}.{clientType}@{clientAddress}
//   - messageChan: 消息通道，用于接收 Volume 位置变更
//
// 通道缓冲区大小：
//   - 10000：足够大的缓冲区，避免快速变更时阻塞
//   - 如果客户端消费速度慢，缓冲区满后会丢弃消息（broadcastToClients 的非阻塞发送）
func (ms *MasterServer) addClient(filerGroup, clientType string, clientAddress pb.ServerAddress) (clientName string, messageChan chan *master_pb.KeepConnectedResponse) {
	// 构造客户端唯一标识
	// 例如：mygroup.filer@192.168.1.100:8888
	clientName = filerGroup + "." + clientType + "@" + string(clientAddress)
	glog.V(0).Infof("+ client %v", clientName)

	// 创建消息通道
	// 使用大缓冲区避免潜在的死锁：
	// 如果 KeepConnected 循环不再监听此通道，但 SendHeartbeat 还在尝试发送，
	// 没有缓冲区会导致 SendHeartbeat 永久阻塞在 clientChansLock 上
	messageChan = make(chan *master_pb.KeepConnectedResponse, 10000)

	// 加锁注册客户端
	ms.clientChansLock.Lock()
	ms.clientChans[clientName] = messageChan
	ms.clientChansLock.Unlock()

	return
}

// deleteClient 从广播列表中移除客户端
//
// 功能说明：
// 当客户端断开连接时，关闭其消息通道并从广播列表中移除
// 确保不再向该客户端发送消息，避免资源泄漏
//
// 参数:
//   - clientName: 客户端的唯一标识
//
// 清理步骤：
// 1. 关闭消息通道（触发 KeepConnected 协程退出）
// 2. 从 clientChans map 中删除
// 3. 允许垃圾回收器回收相关资源
func (ms *MasterServer) deleteClient(clientName string) {
	glog.V(0).Infof("- client %v", clientName)

	// 加锁删除客户端
	ms.clientChansLock.Lock()

	// 关闭消息通道，使得 KeepConnected 协程可以退出
	if clientChan, ok := ms.clientChans[clientName]; ok {
		close(clientChan)
		delete(ms.clientChans, clientName)
	}

	ms.clientChansLock.Unlock()
}

// findClientAddress 从 gRPC 上下文中提取客户端地址
//
// 功能说明：
// 从 gRPC 请求的上下文中获取客户端的 IP 地址和端口
// 如果指定了 gRPC 端口，则使用该端口替换客户端的实际端口
//
// 参数:
//   - ctx: gRPC 请求上下文
//   - grpcPort: 客户端声明的 gRPC 端口（如果非 0）
//
// 返回:
//   - string: 客户端地址，格式：IP:Port
//
// 应用场景：
// 1. 客户端通过 NAT 连接，需要记录其真实 IP
// 2. 客户端使用不同的端口监听 gRPC 服务
// 3. 用于日志记录和客户端识别
func findClientAddress(ctx context.Context, grpcPort uint32) string {
	// fmt.Printf("FromContext %+v\n", ctx)

	// 从上下文中提取 peer 信息
	pr, ok := peer.FromContext(ctx)
	if !ok {
		glog.Error("failed to get peer from ctx")
		return ""
	}

	// 检查地址是否有效
	if pr.Addr == net.Addr(nil) {
		glog.Error("failed to get peer address")
		return ""
	}

	// 如果未指定 gRPC 端口，直接返回客户端地址
	if grpcPort == 0 {
		return pr.Addr.String()
	}

	// 如果是 TCP 连接，提取 IP 并替换端口
	if tcpAddr, ok := pr.Addr.(*net.TCPAddr); ok {
		externalIP := tcpAddr.IP
		// 使用客户端声明的 gRPC 端口
		return util.JoinHostPort(externalIP.String(), int(grpcPort))
	}

	return pr.Addr.String()

}

// GetMasterConfiguration 获取 Master 的配置信息
//
// 功能说明：
// 返回 Master Server 的配置信息，供客户端使用
// 包括监控地址、存储后端、副本策略、Volume 大小限制等
//
// 参数:
//   - ctx: gRPC 请求上下文
//   - req: 配置请求（目前未使用）
//
// 返回:
//   - GetMasterConfigurationResponse: 包含 Master 配置的响应
//   - error: 错误信息（目前总是返回 nil）
//
// 配置项说明：
//   - MetricsAddress: Prometheus 监控地址
//   - MetricsIntervalSeconds: 监控数据上报间隔（秒）
//   - StorageBackends: 支持的存储后端列表（本地磁盘、S3、GCS 等）
//   - DefaultReplication: 默认副本策略（如 "000"、"001"）
//   - VolumeSizeLimitMB: 单个 Volume 的大小限制（MB）
//   - VolumePreallocate: 是否预分配 Volume 磁盘空间
//   - Leader: 当前的 Leader 地址
//
// 调用时机：
// 1. Volume Server 启动时，查询 Master 配置
// 2. Filer 启动时，获取存储后端配置
// 3. 运维工具查询集群配置
func (ms *MasterServer) GetMasterConfiguration(ctx context.Context, req *master_pb.GetMasterConfigurationRequest) (*master_pb.GetMasterConfigurationResponse, error) {

	// 获取当前的 Leader 地址
	leader, _ := ms.Topo.Leader()

	// 构造响应消息
	resp := &master_pb.GetMasterConfigurationResponse{
		MetricsAddress:         ms.option.MetricsAddress,        // Prometheus 监控地址
		MetricsIntervalSeconds: uint32(ms.option.MetricsIntervalSec), // 监控间隔
		StorageBackends:        backend.ToPbStorageBackends(),    // 存储后端列表
		DefaultReplication:     ms.option.DefaultReplicaPlacement, // 默认副本策略
		VolumeSizeLimitMB:      uint32(ms.option.VolumeSizeLimitMB), // Volume 大小限制
		VolumePreallocate:      ms.option.VolumePreallocate,      // 是否预分配空间
		Leader:                 string(leader),                   // Leader 地址
	}

	return resp, nil
}
