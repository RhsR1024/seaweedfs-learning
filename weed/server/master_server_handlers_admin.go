// Package weed_server 包含 Master Server 的管理 API 处理函数
// 本文件实现了通过 HTTP API 进行集群管理的处理函数，包括：
//   - Collection 管理（删除、查询统计）
//   - 卷管理（扩容、压缩、状态查询）
//   - 拓扑查询与重定向
//   - 文件上传（/submit 接口）
package weed_server

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/http"
	"strconv"

	"github.com/seaweedfs/seaweedfs/weed/util/version"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend/memory_map"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/topology"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

// collectionDeleteHandler 处理删除 Collection 的 HTTP 请求
// API 端点: DELETE /col/delete?collection={name}
//
// 工作流程:
//   1. 从请求参数中获取 collection 名称
//   2. 在拓扑中查找该 collection 是否存在
//   3. 遍历所有持有该 collection 卷的 Volume Server
//   4. 通过 gRPC 调用每个 Volume Server 删除该 collection 的所有卷
//   5. 从 Master 的拓扑结构中移除该 collection
//
// 参数:
//   - collection: Collection 名称（必需）
//
// 返回:
//   - 成功: HTTP 204 No Content
//   - 失败: HTTP 400 (collection 不存在) 或 HTTP 500 (删除失败)
//
// 注意:
//   - 这是一个危险操作，会删除 collection 下的所有数据
//   - 删除操作是同步的，会等待所有 Volume Server 完成删除
func (ms *MasterServer) collectionDeleteHandler(w http.ResponseWriter, r *http.Request) {
	collectionName := r.FormValue("collection")

	// 在拓扑中查找 collection
	collection, ok := ms.Topo.FindCollection(collectionName)
	if !ok {
		writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("collection %s does not exist", collectionName))
		return
	}

	// 遍历所有持有该 collection 卷的 Volume Server
	for _, server := range collection.ListVolumeServers() {
		// 通过 gRPC 调用 Volume Server 删除该 collection
		err := operation.WithVolumeServerClient(false, server.ServerAddress(), ms.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
			_, deleteErr := client.DeleteCollection(context.Background(), &volume_server_pb.DeleteCollectionRequest{
				Collection: collection.Name,
			})
			return deleteErr
		})
		if err != nil {
			writeJsonError(w, r, http.StatusInternalServerError, err)
			return
		}
	}

	// 从 Master 拓扑中移除该 collection
	ms.Topo.DeleteCollection(collectionName)

	w.WriteHeader(http.StatusNoContent)
}

// dirStatusHandler 返回 Master 的状态信息和拓扑结构
// API 端点: GET /dir/status
//
// 返回信息:
//   - Version: SeaweedFS 版本号
//   - Topology: 完整的拓扑结构信息
//     - DataCenters: 数据中心列表
//     - Racks: 机架列表
//     - DataNodes: 数据节点列表
//     - Volumes: 卷分布信息
//
// 示例响应:
//   {
//     "Version": "3.0",
//     "Topology": {
//       "DataCenters": [...],
//       "Free": 100,
//       "Max": 200
//     }
//   }
//
// 用途:
//   - 监控集群状态
//   - 查看拓扑结构
//   - 获取容量信息
func (ms *MasterServer) dirStatusHandler(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	m["Version"] = version.Version()
	m["Topology"] = ms.Topo.ToInfo()
	writeJsonQuiet(w, r, http.StatusOK, m)
}

// volumeVacuumHandler 触发卷压缩（Vacuum）操作
// API 端点: GET /vol/vacuum?garbageThreshold={threshold}
//
// Vacuum 操作的作用:
//   - 删除已标记为删除的文件，回收磁盘空间
//   - 重新组织卷文件，消除碎片
//   - 更新索引文件
//
// 参数:
//   - garbageThreshold: 垃圾比例阈值（可选）
//     - 取值范围: 0.0 ~ 1.0
//     - 默认值: 使用启动时配置的 GarbageThreshold
//     - 只有当卷的垃圾比例超过此阈值时才会被压缩
//     - 例如: 0.3 表示当垃圾数据占比超过 30% 时触发压缩
//
// 工作流程:
//   1. 解析垃圾阈值参数
//   2. 调用 Topology.Vacuum() 触发压缩
//   3. Vacuum 会遍历所有卷，对超过阈值的卷进行压缩
//   4. 返回当前拓扑状态
//
// 返回:
//   - 与 /dir/status 相同的拓扑信息
//
// 注意:
//   - Vacuum 是 I/O 密集型操作，会影响性能
//   - MaxParallelVacuumPerServer 控制每台服务器的并发压缩数
//   - 压缩过程中卷仍然可读，但会暂时变为只读
func (ms *MasterServer) volumeVacuumHandler(w http.ResponseWriter, r *http.Request) {
	gcString := r.FormValue("garbageThreshold")
	gcThreshold := ms.option.GarbageThreshold

	// 如果请求中指定了垃圾阈值，则解析并使用
	if gcString != "" {
		var err error
		gcThreshold, err = strconv.ParseFloat(gcString, 32)
		if err != nil {
			glog.V(0).Infof("garbageThreshold %s is not a valid float number: %v", gcString, err)
			writeJsonError(w, r, http.StatusNotAcceptable, fmt.Errorf("garbageThreshold %s is not a valid float number", gcString))
			return
		}
	}

	// 触发全局 Vacuum 操作
	// 参数说明:
	//   - gcThreshold: 垃圾阈值
	//   - MaxParallelVacuumPerServer: 每台服务器最大并发压缩数
	//   - 0: volumeId=0 表示处理所有卷
	//   - "": collection="" 表示处理所有 collection
	//   - preallocateSize: 预分配大小
	//   - false: 不强制压缩索引
	ms.Topo.Vacuum(ms.grpcDialOption, gcThreshold, ms.option.MaxParallelVacuumPerServer, 0, "", ms.preallocateSize, false)

	// 返回拓扑状态
	ms.dirStatusHandler(w, r)
}

// volumeGrowHandler 处理卷扩容请求
// API 端点: GET /vol/grow?count={count}&collection={name}&replication={type}&ttl={ttl}&dataCenter={dc}&rack={rack}&dataNode={node}
//
// 卷扩容的作用:
//   - 主动增加可写卷的数量
//   - 为即将到来的写入请求预留空间
//   - 在特定数据中心/机架/节点上创建卷
//
// 参数:
//   - count: 要创建的卷数量（必需）
//   - collection: Collection 名称（可选）
//   - replication: 副本策略，格式为 "XYZ"（可选，默认使用启动配置）
//     - X: 不同数据中心的副本数
//     - Y: 不同机架的副本数
//     - Z: 不同服务器的副本数
//     - 例如: "001" 表示同机架不同服务器 1 个副本
//   - ttl: 文件过期时间（可选）
//     - 格式: 3m（3分钟）、2h（2小时）、1d（1天）
//   - dataCenter: 指定数据中心（可选）
//   - rack: 指定机架（可选）
//   - dataNode: 指定数据节点（可选）
//   - disk: 磁盘类型，如 "hdd" 或 "ssd"（可选）
//   - preallocate: 预分配大小（可选）
//
// 工作流程:
//   1. 解析请求参数，构建 VolumeGrowOption
//   2. 计算所需的总副本数 = count × 副本数
//   3. 检查拓扑中是否有足够的可用空间
//   4. 调用 VolumeGrowth 模块在指定位置创建卷
//   5. 返回实际创建的卷数量
//
// 返回:
//   - 成功: {"count": 实际创建的卷数}
//   - 失败: 错误信息
//
// 示例:
//   curl "http://localhost:9333/vol/grow?count=3&replication=001"
func (ms *MasterServer) volumeGrowHandler(w http.ResponseWriter, r *http.Request) {
	count := uint64(0)

	// 解析请求参数，构建卷扩容选项
	option, err := ms.getVolumeGrowOption(r)
	if err != nil {
		writeJsonError(w, r, http.StatusNotAcceptable, err)
		return
	}
	glog.V(0).Infof("volumeGrowHandler received %v from %v", option.String(), r.RemoteAddr)

	// 解析要创建的卷数量
	if count, err = strconv.ParseUint(r.FormValue("count"), 10, 32); err == nil {
		// 计算所需的总副本数
		// 例如: count=3, 副本策略="001" (2个副本), 则需要 3 × 2 = 6 个卷空间
		replicaCount := int64(count * uint64(option.ReplicaPlacement.GetCopyCount()))

		if ms.Topo.AvailableSpaceFor(option) < replicaCount {
			// 可用空间不足
			err = fmt.Errorf("only %d volumes left, not enough for %d", ms.Topo.AvailableSpaceFor(option), replicaCount)
		} else if !ms.Topo.DataCenterExists(option.DataCenter) {
			// 指定的数据中心不存在
			err = fmt.Errorf("data center %v not found in topology", option.DataCenter)
		} else {
			// 执行卷扩容
			var newVidLocations []*master_pb.VolumeLocation
			newVidLocations, err = ms.vg.GrowByCountAndType(ms.grpcDialOption, uint32(count), option, ms.Topo)
			// 更新实际创建的卷数量
			count = uint64(len(newVidLocations))
		}
	} else {
		err = fmt.Errorf("can not parse parameter count %s", r.FormValue("count"))
	}

	if err != nil {
		writeJsonError(w, r, http.StatusNotAcceptable, err)
	} else {
		writeJsonQuiet(w, r, http.StatusOK, map[string]interface{}{"count": count})
	}
}

// volumeStatusHandler 返回所有卷的状态信息
// API 端点: GET /vol/status
//
// 返回信息:
//   - Version: SeaweedFS 版本号
//   - Volumes: 所有卷的详细信息映射表
//     - Key: VolumeId
//     - Value: 卷的位置、大小、副本等信息
//
// 用途:
//   - 监控卷的分布情况
//   - 查看每个卷的状态
//   - 调试卷相关问题
func (ms *MasterServer) volumeStatusHandler(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	m["Version"] = version.Version()
	m["Volumes"] = ms.Topo.ToVolumeMap()
	writeJsonQuiet(w, r, http.StatusOK, m)
}

// redirectHandler 处理文件访问重定向请求
// API 端点: GET /{volumeId},{fileKey}
//
// 工作流程:
//   1. 从 URL 路径中解析 VolumeId
//   2. 在拓扑中查找该卷的位置
//   3. 随机选择一个副本位置
//   4. 返回 HTTP 308 永久重定向到该 Volume Server
//
// 参数:
//   - collection: Collection 名称（可选，URL 参数）
//   - URL 路径格式: /{volumeId},{fileKey}
//
// 返回:
//   - 成功: HTTP 308 重定向到 Volume Server
//   - 失败: HTTP 404 卷未找到
//
// 用途:
//   - 客户端不知道卷位置时的访问入口
//   - 负载均衡（随机选择副本）
//
// 注意:
//   - 这会增加 Master 的负载
//   - 建议客户端缓存卷位置，直接访问 Volume Server
func (ms *MasterServer) redirectHandler(w http.ResponseWriter, r *http.Request) {
	// 从 URL 路径解析 volumeId
	vid, _, _, _, _ := parseURLPath(r.URL.Path)
	collection := r.FormValue("collection")

	// 查找卷的位置
	location := ms.findVolumeLocation(collection, vid)
	if location.Error == "" {
		// 随机选择一个副本位置（负载均衡）
		loc := location.Locations[rand.IntN(len(location.Locations))]
		url, _ := util_http.NormalizeUrl(loc.PublicUrl)

		// 构建完整的重定向 URL
		if r.URL.RawQuery != "" {
			url = url + r.URL.Path + "?" + r.URL.RawQuery
		} else {
			url = url + r.URL.Path
		}

		// 返回永久重定向
		http.Redirect(w, r, url, http.StatusPermanentRedirect)
	} else {
		writeJsonError(w, r, http.StatusNotFound, fmt.Errorf("volume id %s not found: %s", vid, location.Error))
	}
}

// submitFromMasterServerHandler 处理 /submit 请求
// API 端点: POST /submit
//
// /submit 接口提供一站式文件上传服务，自动完成以下步骤:
//   1. 向 Master 申请 FileId
//   2. 将文件上传到分配的 Volume Server
//   3. 返回上传结果和文件访问地址
//
// 工作流程:
//   1. 检查当前 Master 是否是 Leader
//   2. 如果是 Leader，直接处理请求
//   3. 如果不是 Leader，转发给 Leader 处理
//   4. 在 submitForClientHandler 中完成文件上传
//
// 参数（multipart/form-data）:
//   - file: 要上传的文件（必需）
//   - collection: Collection 名称（可选）
//   - replication: 副本策略（可选）
//   - ttl: 文件过期时间（可选）
//   - dataCenter: 指定数据中心（可选）
//
// 返回:
//   - 成功: {
//       "fid": "3,01e3b0756f",
//       "url": "127.0.0.1:8080/3,01e3b0756f",
//       "publicUrl": "localhost:8080/3,01e3b0756f",
//       "size": 1234
//     }
//   - 失败: 错误信息
//
// API 示例:
//   curl -F file=@/etc/hosts "http://127.0.0.1:9333/submit"
//   curl -F file=@photo.jpg "http://127.0.0.1:9333/submit?collection=photos&ttl=1d"
//
// 注意:
//   - 这是简化的上传接口，适合简单场景
//   - 性能要求高的场景建议使用两步上传：先 /dir/assign 再上传到 Volume Server
//   - 大文件上传可能超时，建议使用直接上传到 Volume Server 的方式
func (ms *MasterServer) submitFromMasterServerHandler(w http.ResponseWriter, r *http.Request) {
	if ms.Topo.IsLeader() {
		// 当前节点是 Leader，直接处理上传请求
		submitForClientHandler(w, r, func(ctx context.Context) pb.ServerAddress { return ms.option.Master }, ms.grpcDialOption)
	} else {
		// 当前节点不是 Leader，需要转发给 Leader
		masterUrl, err := ms.Topo.Leader()
		if err != nil {
			writeJsonError(w, r, http.StatusInternalServerError, err)
		} else {
			// 使用 Leader 的地址处理请求
			submitForClientHandler(w, r, func(ctx context.Context) pb.ServerAddress { return masterUrl }, ms.grpcDialOption)
		}
	}
}

// getVolumeGrowOption 从 HTTP 请求中解析卷扩容选项
// 这是一个辅助函数，被多个 handler 复用
//
// 参数:
//   - r: HTTP 请求对象
//
// 解析的参数包括:
//   - replication: 副本策略字符串（如 "001"）
//   - ttl: 文件过期时间（如 "3d", "2h"）
//   - memoryMapMaxSizeMb: 内存映射最大大小
//   - disk: 磁盘类型（"hdd" 或 "ssd"）
//   - preallocate: 预分配大小（字节）
//   - collection: Collection 名称
//   - dataCenter: 数据中心名称
//   - rack: 机架名称
//   - dataNode: 数据节点地址
//
// 返回:
//   - *topology.VolumeGrowOption: 解析后的卷扩容选项
//   - error: 解析失败时返回错误
//
// 注意:
//   - 如果 replication 为空，使用启动时配置的默认值
//   - 如果 preallocate 为空，使用服务器配置的默认值
func (ms *MasterServer) getVolumeGrowOption(r *http.Request) (*topology.VolumeGrowOption, error) {
	// 解析副本策略
	replicationString := r.FormValue("replication")
	if replicationString == "" {
		replicationString = ms.option.DefaultReplicaPlacement
	}
	replicaPlacement, err := super_block.NewReplicaPlacementFromString(replicationString)
	if err != nil {
		return nil, err
	}

	// 解析 TTL（文件过期时间）
	ttl, err := needle.ReadTTL(r.FormValue("ttl"))
	if err != nil {
		return nil, err
	}

	// 解析内存映射最大大小
	memoryMapMaxSizeMb, err := memory_map.ReadMemoryMapMaxSizeMb(r.FormValue("memoryMapMaxSizeMb"))
	if err != nil {
		return nil, err
	}

	// 解析磁盘类型
	diskType := types.ToDiskType(r.FormValue("disk"))

	// 解析预分配大小
	preallocate := ms.preallocateSize
	if r.FormValue("preallocate") != "" {
		preallocate, err = strconv.ParseInt(r.FormValue("preallocate"), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("Failed to parse int64 preallocate = %s: %v", r.FormValue("preallocate"), err)
		}
	}

	// 获取当前 Needle 版本
	ver := needle.GetCurrentVersion()

	// 构建卷扩容选项
	volumeGrowOption := &topology.VolumeGrowOption{
		Collection:         r.FormValue("collection"),
		ReplicaPlacement:   replicaPlacement,
		Ttl:                ttl,
		DiskType:           diskType,
		Preallocate:        preallocate,
		DataCenter:         r.FormValue("dataCenter"),
		Rack:               r.FormValue("rack"),
		DataNode:           r.FormValue("dataNode"),
		MemoryMapMaxSizeMb: memoryMapMaxSizeMb,
		Version:            uint32(ver),
	}
	return volumeGrowOption, nil
}

// collectionInfoHandler 返回指定 Collection 的统计信息
// API 端点: GET /col/info?collection={name}&detail={true|false}
//
// 功能:
//   - 查询 Collection 的总容量、已用容量、文件数等统计信息
//   - 支持详细模式，返回每个 VolumeLayout 的单独统计
//
// 参数:
//   - collection: Collection 名称（必需）
//   - detail: 是否返回详细信息（可选，默认 false）
//     - true: 返回每个 VolumeLayout 的统计数组
//     - false: 返回汇总的统计信息
//
// 返回（detail=false）:
//   {
//     "Version": "3.0",
//     "Collection": "photos",
//     "TotalSize": 1073741824,    // 总容量（字节）
//     "FileCount": 12345,         // 文件总数
//     "UsedSize": 536870912,      // 已用容量（字节）
//     "VolumeCount": 10           // 卷数量
//   }
//
// 返回（detail=true）:
//   [
//     {
//       "Version": "3.0",
//       "Collection": "photos",
//       "TotalSize": 536870912,
//       "FileCount": 6000,
//       "UsedSize": 268435456
//     },
//     ...
//   ]
//
// VolumeLayout 说明:
//   - 每个 Collection 可能有多个 VolumeLayout
//   - 不同的副本策略、TTL、磁盘类型会创建不同的 VolumeLayout
//   - detail=true 时可以看到每个 VolumeLayout 的独立统计
//
// 用途:
//   - 监控 Collection 的存储使用情况
//   - 容量规划
//   - 了解数据分布
func (ms *MasterServer) collectionInfoHandler(w http.ResponseWriter, r *http.Request) {
	// 获取 collection 名称
	collectionName := r.FormValue("collection")
	if collectionName == "" {
		writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("collection is required"))
		return
	}

	// 是否输出详细信息
	detail := r.FormValue("detail") == "true"

	// 在拓扑中查找 collection
	collection, ok := ms.Topo.FindCollection(collectionName)
	if !ok {
		writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("collection %s does not exist", collectionName))
		return
	}

	// 获取该 collection 的所有 VolumeLayout
	volumeLayouts := collection.GetAllVolumeLayouts()

	if detail {
		// 详细模式：返回每个 VolumeLayout 的统计信息
		all_stats := make([]map[string]interface{}, len(volumeLayouts))
		for i, volumeLayout := range volumeLayouts {
			volumeLayoutStats := volumeLayout.Stats()
			m := make(map[string]interface{})
			m["Version"] = version.Version()
			m["Collection"] = collectionName
			m["TotalSize"] = volumeLayoutStats.TotalSize
			m["FileCount"] = volumeLayoutStats.FileCount
			m["UsedSize"] = volumeLayoutStats.UsedSize
			all_stats[i] = m
		}
		writeJsonQuiet(w, r, http.StatusOK, all_stats)
	} else {
		// 汇总模式：返回整个 collection 的统计信息
		collectionStats := map[string]interface{}{
			"Version":     version.Version(),
			"Collection":  collectionName,
			"TotalSize":   uint64(0),
			"FileCount":   uint64(0),
			"UsedSize":    uint64(0),
			"VolumeCount": uint64(0),
		}

		// 累加所有 VolumeLayout 的统计信息
		for _, volumeLayout := range volumeLayouts {
			volumeLayoutStats := volumeLayout.Stats()
			collectionStats["TotalSize"] = collectionStats["TotalSize"].(uint64) + volumeLayoutStats.TotalSize
			collectionStats["FileCount"] = collectionStats["FileCount"].(uint64) + volumeLayoutStats.FileCount
			collectionStats["UsedSize"] = collectionStats["UsedSize"].(uint64) + volumeLayoutStats.UsedSize
			collectionStats["VolumeCount"] = collectionStats["VolumeCount"].(uint64) + 1
		}

		writeJsonQuiet(w, r, http.StatusOK, collectionStats)
	}
}
