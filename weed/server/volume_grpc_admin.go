// Package weed_server 实现 Volume Server 的管理操作 gRPC 接口
// 本文件提供 Volume Server 的各种管理功能
//
// 核心功能:
//   - Collection 管理：删除集合
//   - Volume 生命周期：分配、挂载、卸载、删除
//   - Volume 配置：副本策略、读写权限
//   - 状态查询：卷状态、服务器状态、Needle 状态
//   - 连接测试：Ping Filer/Volume/Master
//
// gRPC 服务实现:
//   - volume_server_pb.VolumeServerServer
//
// 调用方:
//   - Master Server：管理 Volume 生命周期
//   - 其他 Volume Server：副本同步、状态查询
//   - 管理工具：weed shell、运维脚本
//
// 关键设计:
//   - 所有操作通过 vs.store 进行，确保数据一致性
//   - 部分操作需要通知 Master（如标记只读）
//   - 支持 EC 卷和普通卷
package weed_server

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util/version"

	"github.com/seaweedfs/seaweedfs/weed/storage"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// DeleteCollection 删除指定 Collection 下的所有 Volume
// gRPC API: DeleteCollection
//
// 功能:
//   - 删除属于指定集合的所有卷文件
//   - 包括 .dat、.idx 等所有相关文件
//
// 参数:
//   - Collection: 集合名称，例如 "pictures"、"videos"
//
// 使用场景:
//   - 清理不再使用的集合
//   - 释放存储空间
//   - 数据迁移后的清理
//
// 注意:
//   - 此操作不可逆，会永久删除数据
//   - 删除前需确保集合不再使用
//   - Master 会在元数据中移除此集合
func (vs *VolumeServer) DeleteCollection(ctx context.Context, req *volume_server_pb.DeleteCollectionRequest) (*volume_server_pb.DeleteCollectionResponse, error) {

	resp := &volume_server_pb.DeleteCollectionResponse{}

	// 调用 store 删除集合
	// 会删除所有属于此集合的 volume 文件
	err := vs.store.DeleteCollection(req.Collection)

	if err != nil {
		glog.Errorf("delete collection %s: %v", req.Collection, err)
	} else {
		glog.V(2).Infof("delete collection %v", req)
	}

	return resp, err

}

// AllocateVolume 在 Volume Server 上分配（创建）新 Volume
// gRPC API: AllocateVolume
//
// 功能:
//   - 创建新的 Volume 文件（.dat 和 .idx）
//   - 配置副本策略、TTL、预分配等参数
//   - 初始化 Volume 的 SuperBlock
//
// 参数:
//   - VolumeId: 卷 ID，由 Master 分配
//   - Collection: 集合名称，用于逻辑分组
//   - Replication: 副本策略，如 "001"、"010"
//   - Ttl: 生存时间，如 "3d"、"1w"
//   - Preallocate: 是否预分配磁盘空间（默认 32GB）
//   - Version: Needle 格式版本（1/2/3）
//   - MemoryMapMaxSizeMb: 内存映射最大大小
//   - DiskType: 磁盘类型（hdd/ssd）
//
// 创建流程:
//   1. 选择合适的存储目录（空间最多的）
//   2. 创建 .dat 文件（数据文件）
//   3. 写入 SuperBlock（8 字节头）
//   4. 创建 .idx 文件（索引文件）
//   5. 如果 Preallocate=true，预分配 32GB 空间
//
// 使用场景:
//   - Master 发现可写 Volume 不足时触发
//   - 管理员手动创建 Volume
//
// 注意:
//   - 同一个 VolumeId 不能重复分配
//   - 预分配会占用磁盘空间但提升写入性能
func (vs *VolumeServer) AllocateVolume(ctx context.Context, req *volume_server_pb.AllocateVolumeRequest) (*volume_server_pb.AllocateVolumeResponse, error) {

	resp := &volume_server_pb.AllocateVolumeResponse{}

	// 调用 store 添加新 Volume
	// needleMapKind: 索引类型（memory/leveldb/leveldbMedium/leveldbLarge）
	// ldbTimout: LevelDB 超时时间
	err := vs.store.AddVolume(
		needle.VolumeId(req.VolumeId),
		req.Collection,
		vs.needleMapKind,
		req.Replication,
		req.Ttl,
		req.Preallocate,
		needle.Version(req.Version),
		req.MemoryMapMaxSizeMb,
		types.ToDiskType(req.DiskType),
		vs.ldbTimout,
	)

	if err != nil {
		glog.Errorf("assign volume %v: %v", req, err)
	} else {
		glog.V(2).Infof("assign volume %v", req)
	}

	return resp, err

}

// VolumeMount 挂载已存在的 Volume
// gRPC API: VolumeMount
//
// 功能:
//   - 加载 Volume 的元数据到内存
//   - 打开 .dat 和 .idx 文件
//   - 构建 Needle 索引
//
// 挂载流程:
//   1. 读取 .dat 文件的 SuperBlock（前 8 字节）
//   2. 根据 SuperBlock 确定 Needle 版本和副本策略
//   3. 加载 .idx 索引文件到内存或 LevelDB
//   4. 将 Volume 加入 store 的管理列表
//
// 使用场景:
//   - Volume Server 启动时自动挂载所有 Volume
//   - 添加新磁盘后挂载其中的 Volume
//   - Volume 维护（compact）后重新挂载
//   - 从只读改为可写前需要先挂载
//
// 注意:
//   - 挂载不会修改文件内容
//   - 索引类型（memory/leveldb）在启动时配置
//   - 重复挂载会返回错误
func (vs *VolumeServer) VolumeMount(ctx context.Context, req *volume_server_pb.VolumeMountRequest) (*volume_server_pb.VolumeMountResponse, error) {

	resp := &volume_server_pb.VolumeMountResponse{}

	// 调用 store 挂载 Volume
	// 会加载索引并打开文件句柄
	err := vs.store.MountVolume(needle.VolumeId(req.VolumeId))

	if err != nil {
		glog.Errorf("volume mount %v: %v", req, err)
	} else {
		glog.V(2).Infof("volume mount %v", req)
	}

	return resp, err

}

// VolumeUnmount 卸载 Volume
// gRPC API: VolumeUnmount
//
// 功能:
//   - 关闭 Volume 的文件句柄
//   - 释放索引占用的内存或关闭 LevelDB
//   - 从 store 的管理列表中移除
//
// 卸载流程:
//   1. 检查 Volume 是否正在使用
//   2. 关闭 .dat 和 .idx 文件
//   3. 释放内存索引或关闭 LevelDB
//   4. 从内存中移除 Volume 对象
//
// 使用场景:
//   - 删除 Volume 前先卸载
//   - 修改 Volume 配置前卸载
//   - 迁移 Volume 到其他服务器前卸载
//   - 释放内存或文件句柄
//
// 注意:
//   - 卸载后无法读写此 Volume
//   - 不会删除文件，只是断开连接
//   - 卸载后可以重新挂载
func (vs *VolumeServer) VolumeUnmount(ctx context.Context, req *volume_server_pb.VolumeUnmountRequest) (*volume_server_pb.VolumeUnmountResponse, error) {

	resp := &volume_server_pb.VolumeUnmountResponse{}

	// 调用 store 卸载 Volume
	// 会关闭文件句柄并释放资源
	err := vs.store.UnmountVolume(needle.VolumeId(req.VolumeId))

	if err != nil {
		glog.Errorf("volume unmount %v: %v", req, err)
	} else {
		glog.V(2).Infof("volume unmount %v", req)
	}

	return resp, err

}

// VolumeDelete 删除 Volume 文件
// gRPC API: VolumeDelete
//
// 功能:
//   - 删除 Volume 的所有文件（.dat、.idx、.vif 等）
//   - 可选择只删除空 Volume
//
// 参数:
//   - VolumeId: 要删除的卷 ID
//   - OnlyEmpty: 是否只删除空卷（FileCount == 0）
//
// 删除流程:
//   1. 先卸载 Volume（如果已挂载）
//   2. 检查是否满足删除条件（OnlyEmpty）
//   3. 删除 .dat 文件（数据文件）
//   4. 删除 .idx 文件（索引文件）
//   5. 删除 .vif 文件（Volume 信息文件）
//   6. 删除其他相关文件（EC 分片、临时文件）
//
// 使用场景:
//   - Vacuum 清理空 Volume
//   - 数据迁移后清理旧 Volume
//   - 释放磁盘空间
//   - 删除损坏的 Volume
//
// 注意:
//   - 此操作不可逆，会永久删除数据
//   - OnlyEmpty=true 时只删除 FileCount=0 的卷
//   - 删除前确保副本存在于其他节点
func (vs *VolumeServer) VolumeDelete(ctx context.Context, req *volume_server_pb.VolumeDeleteRequest) (*volume_server_pb.VolumeDeleteResponse, error) {

	resp := &volume_server_pb.VolumeDeleteResponse{}

	// 调用 store 删除 Volume
	// OnlyEmpty=true 时只删除空卷
	err := vs.store.DeleteVolume(needle.VolumeId(req.VolumeId), req.OnlyEmpty)

	if err != nil {
		glog.Errorf("volume delete %v: %v", req, err)
	} else {
		glog.V(2).Infof("volume delete %v", req)
	}

	return resp, err

}

// VolumeConfigure 配置 Volume 的副本策略
// gRPC API: VolumeConfigure
//
// 功能:
//   - 修改 Volume 的副本策略（Replication）
//   - 更新 Volume 信息文件（.vif）
//
// 参数:
//   - VolumeId: 要配置的卷 ID
//   - Replication: 新的副本策略，如 "001"、"010"、"100"
//
// 配置流程:
//   1. 验证副本策略格式是否正确
//   2. 卸载 Volume（关闭文件句柄）
//   3. 修改 .vif 文件中的副本策略
//   4. 重新挂载 Volume
//
// 副本策略格式:
//   - 三位数字 XYZ
//   - X: 不同数据中心的副本数
//   - Y: 不同机架的副本数（同数据中心）
//   - Z: 不同服务器的副本数（同机架）
//   - 例如: "001" = 同机架 1 副本，"100" = 跨数据中心 1 副本
//
// 使用场景:
//   - 调整副本策略以应对不同需求
//   - 增加副本提升可靠性
//   - 减少副本降低存储成本
//
// 注意:
//   - 只修改元数据，不会自动创建或删除副本
//   - 需要手动同步副本到其他节点
//   - 配置过程中 Volume 暂时不可用
func (vs *VolumeServer) VolumeConfigure(ctx context.Context, req *volume_server_pb.VolumeConfigureRequest) (*volume_server_pb.VolumeConfigureResponse, error) {

	resp := &volume_server_pb.VolumeConfigureResponse{}

	// 【步骤 1：验证副本策略格式】
	// 检查格式是否为三位数字（如 "001"、"010"）
	if _, err := super_block.NewReplicaPlacementFromString(req.Replication); err != nil {
		resp.Error = fmt.Sprintf("volume configure replication %v: %v", req, err)
		return resp, nil
	}

	// 【步骤 2：卸载 Volume】
	// 关闭文件句柄，释放资源
	if err := vs.store.UnmountVolume(needle.VolumeId(req.VolumeId)); err != nil {
		glog.Errorf("volume configure unmount %v: %v", req, err)
		resp.Error = fmt.Sprintf("volume configure unmount %v: %v", req, err)
		return resp, nil
	}

	// 【步骤 3：修改 Volume 信息文件】
	// 更新 .vif 文件中的副本策略
	if err := vs.store.ConfigureVolume(needle.VolumeId(req.VolumeId), req.Replication); err != nil {
		glog.Errorf("volume configure %v: %v", req, err)
		resp.Error = fmt.Sprintf("volume configure %v: %v", req, err)
		return resp, nil
	}

	// 【步骤 4：重新挂载 Volume】
	// 使用新的配置重新加载 Volume
	if err := vs.store.MountVolume(needle.VolumeId(req.VolumeId)); err != nil {
		glog.Errorf("volume configure mount %v: %v", req, err)
		resp.Error = fmt.Sprintf("volume configure mount %v: %v", req, err)
		return resp, nil
	}

	return resp, nil

}

// VolumeMarkReadonly 将 Volume 标记为只读
// gRPC API: VolumeMarkReadonly
//
// 功能:
//   - 将 Volume 设置为只读模式
//   - 通知 Master 停止向此 Volume 分配写入请求
//   - 可选择是否持久化只读状态
//
// 参数:
//   - VolumeId: 要标记的卷 ID
//   - Persist: 是否持久化只读状态（写入 .vif 文件）
//
// 标记流程（三步走，避免竞态条件）:
//   【步骤 1】通知 Master 标记为只读
//     - 停止 Master 向此 Volume 分配新文件
//
//   【稀有情况 1.5】
//     - 如果心跳恰好在步骤 1 和 2 之间发生
//     - 可能导致 Master 又将 Volume 标记为可写
//
//   【步骤 2】标记本地 Volume 为只读
//     - 设置 Volume 的只读标志
//     - 如果 Persist=true，写入 .vif 文件
//
//   【步骤 3】再次通知 Master 标记为只读
//     - 防止稀有情况 1.5 发生
//     - 确保 Master 和 Volume Server 状态一致
//
// 使用场景:
//   - Volume 即将满，需要停止写入
//   - 准备进行 compact 或迁移操作
//   - 数据归档，转为只读模式
//   - 紧急维护，临时禁止写入
//
// 只读效果:
//   - 允许读取（GET 请求）
//   - 禁止写入（POST/PUT 请求）
//   - 禁止删除（DELETE 请求）
//
// 注意:
//   - Persist=false 时重启后恢复为可写
//   - Persist=true 时需要手动标记为可写
//   - 已经在执行的写入请求不会中断
func (vs *VolumeServer) VolumeMarkReadonly(ctx context.Context, req *volume_server_pb.VolumeMarkReadonlyRequest) (*volume_server_pb.VolumeMarkReadonlyResponse, error) {

	resp := &volume_server_pb.VolumeMarkReadonlyResponse{}

	// 获取 Volume 对象
	v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
	if v == nil {
		return nil, fmt.Errorf("volume %d not found", req.VolumeId)
	}

	// 【步骤 1】通知 Master 停止向此 Volume 分配写入
	if err := vs.notifyMasterVolumeReadonly(v, true); err != nil {
		return resp, err
	}

	// 【稀有情况 1.5】
	// 如果心跳恰好在步骤 1 和 2 之间发生，Master 可能又将 Volume 标记为可写
	// 所以步骤 3 会再次通知 Master

	// 【步骤 2】标记本地 Volume 为只读
	// Persist=true 时会写入 .vif 文件
	err := vs.store.MarkVolumeReadonly(needle.VolumeId(req.VolumeId), req.GetPersist())

	if err != nil {
		glog.Errorf("volume mark readonly %v: %v", req, err)
	} else {
		glog.V(2).Infof("volume mark readonly %v", req)
	}

	// 【步骤 3】再次通知 Master，防止稀有情况 1.5
	if err := vs.notifyMasterVolumeReadonly(v, true); err != nil {
		return resp, err
	}

	return resp, err
}

// notifyMasterVolumeReadonly 通知 Master 更新 Volume 的只读状态
// 内部辅助函数，由 VolumeMarkReadonly 和 VolumeMarkWritable 调用
//
// 功能:
//   - 通过 gRPC 通知 Master 更新 Volume 状态
//   - Master 会在拓扑结构中更新此 Volume 的可写状态
//
// 参数:
//   - v: Volume 对象
//   - isReadOnly: true=只读，false=可写
//
// 请求信息:
//   - Ip/Port: Volume Server 地址
//   - VolumeId: 卷 ID
//   - Collection: 集合名称
//   - ReplicaPlacement: 副本策略
//   - Ttl: 生存时间
//   - DiskType: 磁盘类型
//   - IsReadonly: 只读状态
//
// Master 行为:
//   - IsReadonly=true: 停止分配新文件到此 Volume
//   - IsReadonly=false: 允许分配新文件到此 Volume
//
// 返回:
//   - error: 通知失败错误
func (vs *VolumeServer) notifyMasterVolumeReadonly(v *storage.Volume, isReadOnly bool) error {
	// 使用 WithMasterClient 建立 gRPC 连接
	if grpcErr := pb.WithMasterClient(false, vs.GetMaster(context.Background()), vs.grpcDialOption, false, func(client master_pb.SeaweedClient) error {
		// 调用 Master 的 VolumeMarkReadonly RPC
		_, err := client.VolumeMarkReadonly(context.Background(), &master_pb.VolumeMarkReadonlyRequest{
			Ip:               vs.store.Ip,
			Port:             uint32(vs.store.Port),
			VolumeId:         uint32(v.Id),
			Collection:       v.Collection,
			ReplicaPlacement: uint32(v.ReplicaPlacement.Byte()),
			Ttl:              v.Ttl.ToUint32(),
			DiskType:         string(v.DiskType()),
			IsReadonly:       isReadOnly,
		})
		if err != nil {
			return fmt.Errorf("set volume %d to read only on master: %v", v.Id, err)
		}
		return nil
	}); grpcErr != nil {
		glog.V(0).Infof("connect to %s: %v", vs.GetMaster(context.Background()), grpcErr)
		return fmt.Errorf("grpc VolumeMarkReadonly with master %s: %v", vs.GetMaster(context.Background()), grpcErr)
	}
	return nil
}

// VolumeMarkWritable 将 Volume 标记为可写
// gRPC API: VolumeMarkWritable
//
// 功能:
//   - 将 Volume 设置为可写模式
//   - 通知 Master 允许向此 Volume 分配写入请求
//
// 参数:
//   - VolumeId: 要标记的卷 ID
//
// 标记流程:
//   1. 获取 Volume 对象
//   2. 标记本地 Volume 为可写
//   3. 通知 Master 允许分配新文件
//
// 使用场景:
//   - 恢复之前标记为只读的 Volume
//   - 维护完成后重新开放写入
//   - 手动解除只读限制
//
// 注意:
//   - 只能标记本地已挂载的 Volume
//   - 如果之前用 Persist=true 标记只读，需要手动改为可写
//   - Master 收到通知后会立即分配新文件
func (vs *VolumeServer) VolumeMarkWritable(ctx context.Context, req *volume_server_pb.VolumeMarkWritableRequest) (*volume_server_pb.VolumeMarkWritableResponse, error) {

	resp := &volume_server_pb.VolumeMarkWritableResponse{}

	// 获取 Volume 对象
	v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
	if v == nil {
		return nil, fmt.Errorf("volume %d not found", req.VolumeId)
	}

	// 标记本地 Volume 为可写
	err := vs.store.MarkVolumeWritable(needle.VolumeId(req.VolumeId))

	if err != nil {
		glog.Errorf("volume mark writable %v: %v", req, err)
	} else {
		glog.V(2).Infof("volume mark writable %v", req)
	}

	// 通知 Master 允许向此 Volume 分配流量
	if err := vs.notifyMasterVolumeReadonly(v, false); err != nil {
		return resp, err
	}

	return resp, err
}

// VolumeStatus 查询 Volume 的状态信息
// gRPC API: VolumeStatus
//
// 功能:
//   - 返回 Volume 的详细状态信息
//   - 包括大小、文件数、删除数、只读状态等
//
// 参数:
//   - VolumeId: 要查询的卷 ID
//
// 返回信息:
//   - IsReadOnly: 是否只读
//   - VolumeSize: 卷文件大小（字节）
//   - FileCount: 当前文件数量
//   - FileDeletedCount: 已删除文件数量
//
// 使用场景:
//   - 监控 Volume 使用情况
//   - 判断是否需要 compact（FileDeletedCount 较高）
//   - 判断是否接近容量上限
//   - 健康检查和诊断
//
// 注意:
//   - Volume 必须已挂载
//   - VolumeSize 是实际文件大小，不是逻辑容量
func (vs *VolumeServer) VolumeStatus(ctx context.Context, req *volume_server_pb.VolumeStatusRequest) (*volume_server_pb.VolumeStatusResponse, error) {

	resp := &volume_server_pb.VolumeStatusResponse{}

	// 获取 Volume 对象
	v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
	if v == nil {
		return nil, fmt.Errorf("not found volume id %d", req.VolumeId)
	}
	if v.DataBackend == nil {
		return nil, fmt.Errorf("volume %d data backend not found", req.VolumeId)
	}

	// 获取 Volume 文件大小
	volumeSize, _, _ := v.DataBackend.GetStat()

	// 填充返回信息
	resp.IsReadOnly = v.IsReadOnly()
	resp.VolumeSize = uint64(volumeSize)
	resp.FileCount = v.FileCount()
	resp.FileDeletedCount = v.DeletedCount()

	return resp, nil
}

// VolumeServerStatus 查询 Volume Server 的整体状态
// gRPC API: VolumeServerStatus
//
// 功能:
//   - 返回 Volume Server 的系统信息
//   - 包括内存、版本、位置、磁盘状态等
//
// 返回信息:
//   - MemoryStatus: 内存使用情况（总量、已用、可用）
//   - Version: SeaweedFS 版本号
//   - DataCenter: 数据中心名称
//   - Rack: 机架名称
//   - DiskStatuses: 所有存储目录的磁盘状态
//     - Directory: 目录路径
//     - All/Used/Free: 总量/已用/可用空间
//     - PercentUsed: 使用百分比
//
// 使用场景:
//   - 监控 Volume Server 健康状态
//   - 容量规划和预警
//   - 故障诊断
//   - 集群管理工具展示信息
//
// 注意:
//   - 磁盘状态基于文件系统统计
//   - 内存状态来自 runtime.MemStats
func (vs *VolumeServer) VolumeServerStatus(ctx context.Context, req *volume_server_pb.VolumeServerStatusRequest) (*volume_server_pb.VolumeServerStatusResponse, error) {

	resp := &volume_server_pb.VolumeServerStatusResponse{
		MemoryStatus: stats.MemStat(),        // 内存使用统计
		Version:      version.Version(),      // SeaweedFS 版本
		DataCenter:   vs.dataCenter,          // 数据中心
		Rack:         vs.rack,                // 机架
	}

	// 遍历所有存储目录，获取磁盘状态
	for _, loc := range vs.store.Locations {
		if dir, e := filepath.Abs(loc.Directory); e == nil {
			// 获取磁盘使用情况（总量、已用、可用）
			resp.DiskStatuses = append(resp.DiskStatuses, stats.NewDiskStatus(dir))
		}
	}

	return resp, nil

}

// VolumeServerLeave 让 Volume Server 离开集群
// gRPC API: VolumeServerLeave
//
// 功能:
//   - 停止向 Master 发送心跳
//   - 让 Master 将此节点标记为离线
//
// 使用场景:
//   - 优雅下线 Volume Server
//   - 维护前临时移除节点
//   - 迁移前停止服务
//
// 效果:
//   - Master 不再向此节点分配新文件
//   - 已存在的 Volume 仍可访问
//   - 需要手动停止进程
//
// 注意:
//   - 只停止心跳，不关闭服务
//   - 不会卸载 Volume
//   - 重启后会自动重新加入
func (vs *VolumeServer) VolumeServerLeave(ctx context.Context, req *volume_server_pb.VolumeServerLeaveRequest) (*volume_server_pb.VolumeServerLeaveResponse, error) {

	resp := &volume_server_pb.VolumeServerLeaveResponse{}

	// 停止心跳，Master 会将节点标记为离线
	vs.StopHeartbeat()

	return resp, nil

}

// VolumeNeedleStatus 查询特定 Needle 的状态
// gRPC API: VolumeNeedleStatus
//
// 功能:
//   - 查询指定 Needle 的元数据
//   - 支持普通 Volume 和 EC Volume
//
// 参数:
//   - VolumeId: 卷 ID
//   - NeedleId: Needle ID（文件唯一标识）
//
// 返回信息:
//   - NeedleId: Needle ID
//   - Cookie: Cookie 值（用于验证）
//   - Size: Needle 数据大小
//   - LastModified: 最后修改时间（Unix 时间戳）
//   - Crc: CRC 校验和
//   - Ttl: 生存时间（如果有）
//
// 查询流程:
//   1. 检查是否为普通 Volume
//   2. 如果不是，检查是否为 EC Volume
//   3. 从相应类型的 Volume 读取 Needle
//   4. 返回 Needle 元数据
//
// 使用场景:
//   - 验证文件是否存在
//   - 检查文件元数据
//   - 调试和诊断
//   - 数据完整性检查
//
// 注意:
//   - 只返回元数据，不返回文件内容
//   - Needle 不存在时返回错误
//   - 支持 EC 卷和普通卷
func (vs *VolumeServer) VolumeNeedleStatus(ctx context.Context, req *volume_server_pb.VolumeNeedleStatusRequest) (*volume_server_pb.VolumeNeedleStatusResponse, error) {

	resp := &volume_server_pb.VolumeNeedleStatusResponse{}

	volumeId := needle.VolumeId(req.VolumeId)

	// 创建 Needle 对象，设置要查询的 ID
	n := &needle.Needle{
		Id: types.NeedleId(req.NeedleId),
	}

	var count int
	var err error

	// 检查是否为普通 Volume
	hasVolume := vs.store.HasVolume(volumeId)
	if !hasVolume {
		// 检查是否为 EC Volume
		_, hasEcVolume := vs.store.FindEcVolume(volumeId)
		if !hasEcVolume {
			return nil, fmt.Errorf("volume not found %d", req.VolumeId)
		}
		// 从 EC Volume 读取 Needle
		count, err = vs.store.ReadEcShardNeedle(volumeId, n, nil)
	} else {
		// 从普通 Volume 读取 Needle
		count, err = vs.store.ReadVolumeNeedle(volumeId, n, nil, nil)
	}

	if err != nil {
		return nil, err
	}
	if count < 0 {
		return nil, fmt.Errorf("needle not found %d", n.Id)
	}

	// 填充返回信息
	resp.NeedleId = uint64(n.Id)
	resp.Cookie = uint32(n.Cookie)
	resp.Size = uint32(n.Size)
	resp.LastModified = n.LastModified
	resp.Crc = n.Checksum.Value()
	if n.HasTtl() {
		resp.Ttl = n.Ttl.String()
	}

	return resp, nil

}

// Ping 测试与其他 SeaweedFS 组件的连接
// gRPC API: Ping
//
// 功能:
//   - 测试与 Filer/Volume/Master 的网络连接
//   - 测量网络延迟
//   - 验证服务可用性
//
// 参数:
//   - Target: 目标地址（如 "192.168.1.10:8080"）
//   - TargetType: 目标类型（Filer/Volume/Master）
//
// 返回信息:
//   - StartTimeNs: 本地请求开始时间（纳秒）
//   - RemoteTimeNs: 远程服务器时间（纳秒）
//   - StopTimeNs: 本地请求结束时间（纳秒）
//
// 延迟计算:
//   - 往返延迟 = StopTimeNs - StartTimeNs
//   - 时钟偏差 = RemoteTimeNs - StartTimeNs（粗略估计）
//
// 使用场景:
//   - 健康检查
//   - 网络诊断
//   - 监控系统延迟
//   - 验证集群配置
//
// 注意:
//   - 时钟偏差受网络延迟影响，只能作为参考
//   - 需要目标服务支持 Ping RPC
func (vs *VolumeServer) Ping(ctx context.Context, req *volume_server_pb.PingRequest) (resp *volume_server_pb.PingResponse, pingErr error) {
	// 记录请求开始时间
	resp = &volume_server_pb.PingResponse{
		StartTimeNs: time.Now().UnixNano(),
	}

	// 根据目标类型选择不同的客户端
	if req.TargetType == cluster.FilerType {
		// Ping Filer
		pingErr = pb.WithFilerClient(false, 0, pb.ServerAddress(req.Target), vs.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
			pingResp, err := client.Ping(ctx, &filer_pb.PingRequest{})
			if pingResp != nil {
				resp.RemoteTimeNs = pingResp.StartTimeNs
			}
			return err
		})
	}

	if req.TargetType == cluster.VolumeServerType {
		// Ping Volume Server
		pingErr = pb.WithVolumeServerClient(false, pb.ServerAddress(req.Target), vs.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
			pingResp, err := client.Ping(ctx, &volume_server_pb.PingRequest{})
			if pingResp != nil {
				resp.RemoteTimeNs = pingResp.StartTimeNs
			}
			return err
		})
	}

	if req.TargetType == cluster.MasterType {
		// Ping Master
		pingErr = pb.WithMasterClient(false, pb.ServerAddress(req.Target), vs.grpcDialOption, false, func(client master_pb.SeaweedClient) error {
			pingResp, err := client.Ping(ctx, &master_pb.PingRequest{})
			if pingResp != nil {
				resp.RemoteTimeNs = pingResp.StartTimeNs
			}
			return err
		})
	}

	// 记录错误（如果有）
	if pingErr != nil {
		pingErr = fmt.Errorf("ping %s %s: %v", req.TargetType, req.Target, pingErr)
	}

	// 记录请求结束时间
	resp.StopTimeNs = time.Now().UnixNano()
	return
}
