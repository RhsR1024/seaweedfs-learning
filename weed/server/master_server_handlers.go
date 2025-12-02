// Package weed_server 实现 Master Server 的核心 HTTP 处理函数
// 本文件包含文件 ID 分配、卷位置查询等核心 API
package weed_server

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/topology"
)

// lookupVolumeId 批量查询卷或文件的位置信息
// 这是一个内部辅助函数，被 dirLookupHandler 调用
//
// 参数:
//   - vids: VolumeId 或 FileId 的列表
//   - collection: Collection 名称（可选，用于过滤）
//
// 返回:
//   - volumeLocations: VolumeId 到位置信息的映射
//     - Key: VolumeId 字符串
//     - Value: LookupResult，包含 Locations（副本位置列表）和 Error
//
// 处理逻辑:
//   - 如果输入是 FileId（包含逗号），则提取 VolumeId 部分
//   - 去重：相同的 VolumeId 只查询一次
//   - 对每个 VolumeId 调用 findVolumeLocation 查询位置
func (ms *MasterServer) lookupVolumeId(vids []string, collection string) (volumeLocations map[string]operation.LookupResult) {
	volumeLocations = make(map[string]operation.LookupResult)
	for _, vid := range vids {
		// 如果是 FileId 格式（volumeId,fileKey），提取 volumeId 部分
		commaSep := strings.Index(vid, ",")
		if commaSep > 0 {
			vid = vid[0:commaSep]
		}

		// 去重：已查询过的跳过
		if _, ok := volumeLocations[vid]; ok {
			continue
		}

		// 查询卷的位置信息
		volumeLocations[vid] = ms.findVolumeLocation(collection, vid)
	}
	return
}

// dirLookupHandler 处理卷或文件位置查询请求
// API 端点: GET /dir/lookup?volumeId={vid} 或 GET /dir/lookup?fileId={fid}
//
// 这是 SeaweedFS 最核心的 API 之一，客户端通过此接口获取文件所在的 Volume Server 地址
//
// 参数:
//   - volumeId: 卷 ID（可选）
//   - fileId: 文件 ID（可选）
//   - collection: Collection 名称（可选，可加速查询）
//   - read: 是否为读取操作（可选，"yes" 表示读取）
//
// 返回:
//   {
//     "volumeOrFileId": "3,01e3b0756f",
//     "locations": [
//       {
//         "url": "127.0.0.1:8080",
//         "publicUrl": "localhost:8080",
//         "dataCenter": "dc1",
//         "grpcPort": 18080
//       }
//     ]
//   }
//
// JWT 鉴权:
//   - 如果提供 fileId，响应会在 Header 中包含 JWT token
//   - 读取操作使用 ReadSigningKey 生成只读 token
//   - 写入/删除操作使用 SigningKey 生成读写 token
//   - Volume Server 会验证 JWT 的有效性
//
// 工作流程:
//   1. 从参数中提取 VolumeId（兼容 volumeId 和 fileId 参数）
//   2. 调用 findVolumeLocation 在拓扑中查找卷的位置
//   3. 如果是 fileId 请求，生成 JWT 并添加到响应头
//   4. 返回位置信息
//
// 使用场景:
//   - 客户端上传文件后，需要查询 fileId 对应的 Volume Server 位置
//   - 客户端下载文件时，查询 fileId 的位置并获取读取权限
//   - 客户端删除文件时，查询 fileId 的位置并获取写入权限
//
// API 示例:
//   curl "http://localhost:9333/dir/lookup?volumeId=3"
//   curl "http://localhost:9333/dir/lookup?fileId=3,01e3b0756f&read=yes"
func (ms *MasterServer) dirLookupHandler(w http.ResponseWriter, r *http.Request) {
	vid := r.FormValue("volumeId")
	if vid != "" {
		// 向后兼容：处理包含逗号的 volumeId
		commaSep := strings.Index(vid, ",")
		if commaSep > 0 {
			vid = vid[0:commaSep]
		}
	}

	// 处理 fileId 参数，提取 volumeId
	fileId := r.FormValue("fileId")
	if fileId != "" {
		commaSep := strings.Index(fileId, ",")
		if commaSep > 0 {
			vid = fileId[0:commaSep]
		}
	}

	// collection 可选参数，在有大量 collection 时可以加速查询
	collection := r.FormValue("collection")

	// 查找卷的位置
	location := ms.findVolumeLocation(collection, vid)

	httpStatus := http.StatusOK
	if location.Error != "" || location.Locations == nil {
		// 卷未找到
		httpStatus = http.StatusNotFound
	} else {
		// 如果是文件查询，添加 JWT 鉴权
		forRead := r.FormValue("read")
		isRead := forRead == "yes"
		ms.maybeAddJwtAuthorization(w, fileId, !isRead)
	}

	writeJsonQuiet(w, r, httpStatus, location)
}

// findVolumeLocation 查找卷的位置信息
// 根据当前节点是否为 Leader 选择不同的查询方式
//
// 参数:
//   - collection: Collection 名称
//   - vid: VolumeId 字符串
//
// 返回:
//   - operation.LookupResult: 包含卷的所有副本位置
//
// 查询策略:
//   - 如果当前是 Leader：直接从本地拓扑结构查询
//   - 如果不是 Leader：通过 MasterClient 向 Leader 查询
//
// 这种设计确保了:
//   - Leader 节点可以快速响应查询（避免网络开销）
//   - Follower 节点可以转发查询到 Leader（保证数据一致性）
func (ms *MasterServer) findVolumeLocation(collection, vid string) operation.LookupResult {
	var locations []operation.Location
	var err error

	if ms.Topo.IsLeader() {
		// 当前是 Leader，直接从拓扑中查询
		volumeId, newVolumeIdErr := needle.NewVolumeId(vid)
		if newVolumeIdErr != nil {
			err = fmt.Errorf("Unknown volume id %s", vid)
		} else {
			// 在拓扑中查找该卷的所有副本位置
			machines := ms.Topo.Lookup(collection, volumeId)
			for _, loc := range machines {
				locations = append(locations, operation.Location{
					Url:        loc.Url(),
					PublicUrl:  loc.PublicUrl,
					DataCenter: loc.GetDataCenterId(),
					GrpcPort:   loc.GrpcPort,
				})
			}
		}
	} else {
		// 当前不是 Leader，通过 MasterClient 向 Leader 查询
		machines, getVidLocationsErr := ms.MasterClient.GetVidLocations(vid)
		for _, loc := range machines {
			locations = append(locations, operation.Location{
				Url:        loc.Url,
				PublicUrl:  loc.PublicUrl,
				DataCenter: loc.DataCenter,
				GrpcPort:   loc.GrpcPort,
			})
		}
		err = getVidLocationsErr
	}

	// 如果没有找到任何位置，返回错误
	if len(locations) == 0 && err == nil {
		err = fmt.Errorf("volume id %s not found", vid)
	}

	// 构建返回结果
	ret := operation.LookupResult{
		VolumeOrFileId: vid,
		Locations:      locations,
	}
	if err != nil {
		ret.Error = err.Error()
	}
	return ret
}

// dirAssignHandler 处理文件 ID 分配请求
// API 端点: GET /dir/assign?count={n}&collection={name}&replication={type}&ttl={ttl}
//
// 这是 SeaweedFS 上传文件的第一步：申请文件 ID
//
// 参数:
//   - count: 要分配的文件 ID 数量（可选，默认 1）
//   - writableVolumeCount: 期望的可写卷数量（可选，用于触发扩容）
//   - collection: Collection 名称（可选）
//   - replication: 副本策略（可选）
//   - ttl: 文件过期时间（可选）
//   - dataCenter: 指定数据中心（可选）
//   - rack: 指定机架（可选）
//   - dataNode: 指定数据节点（可选）
//   - disk: 磁盘类型（可选）
//
// 返回:
//   {
//     "fid": "3,01e3b0756f",          // 分配的文件 ID
//     "url": "127.0.0.1:8080",         // Volume Server 地址
//     "publicUrl": "localhost:8080",   // Volume Server 公网地址
//     "count": 1                       // 实际分配的数量
//   }
//
// 工作流程:
//   1. 解析请求参数，构建卷扩容选项
//   2. 获取对应的 VolumeLayout
//   3. 调用 Topo.PickForWrite() 尝试分配文件 ID
//   4. 如果需要扩容且未被禁用，触发自动扩容
//   5. 在 10 秒超时内重试，直到分配成功
//   6. 生成 JWT token 并返回结果
//
// 自动扩容逻辑:
//   - 当 shouldGrow=true 且 VolumeLayout 没有正在进行的扩容请求时
//   - 向 volumeGrowthRequestChan 发送扩容请求
//   - 后台协程会处理扩容任务
//
// 重试机制:
//   - 最大超时时间：10 秒
//   - 失败后等待 200ms 再重试
//   - 这确保了在扩容完成前客户端可以等待
//
// API 示例:
//   curl "http://localhost:9333/dir/assign"
//   curl "http://localhost:9333/dir/assign?count=5&collection=photos&ttl=1d"
//
// 注意:
//   - 分配的文件 ID 不代表文件已存在，只是预留了位置
//   - 客户端需要将文件上传到返回的 url 才能真正写入
//   - JWT token 在响应头的 Authorization 字段中
func (ms *MasterServer) dirAssignHandler(w http.ResponseWriter, r *http.Request) {
	// 统计分配请求数
	stats.AssignRequest()

	// 解析要分配的文件 ID 数量
	requestedCount, e := strconv.ParseUint(r.FormValue("count"), 10, 64)
	if e != nil || requestedCount == 0 {
		requestedCount = 1
	}

	// 解析期望的可写卷数量（用于触发扩容）
	writableVolumeCount, e := strconv.ParseUint(r.FormValue("writableVolumeCount"), 10, 32)
	if e != nil {
		writableVolumeCount = 0
	}

	// 解析卷扩容选项
	option, err := ms.getVolumeGrowOption(r)
	if err != nil {
		writeJsonQuiet(w, r, http.StatusNotAcceptable, operation.AssignResult{Error: err.Error()})
		return
	}

	// 获取对应的 VolumeLayout
	vl := ms.Topo.GetVolumeLayout(option.Collection, option.ReplicaPlacement, option.Ttl, option.DiskType)

	var (
		lastErr    error
		maxTimeout = time.Second * 10 // 最大超时时间
		startTime  = time.Now()
	)

	// 检查数据中心是否存在
	if !ms.Topo.DataCenterExists(option.DataCenter) {
		writeJsonQuiet(w, r, http.StatusBadRequest, operation.AssignResult{
			Error: fmt.Sprintf("data center %v not found in topology", option.DataCenter),
		})
		return
	}

	// 在超时时间内重试分配
	for time.Since(startTime) < maxTimeout {
		// 尝试从拓扑中挑选可写卷并分配文件 ID
		fid, count, dnList, shouldGrow, err := ms.Topo.PickForWrite(requestedCount, option, vl)

		if shouldGrow && !vl.HasGrowRequest() && !ms.option.VolumeGrowthDisabled {
			// 需要扩容且未被禁用，触发自动扩容
			glog.V(0).Infof("dirAssign volume growth %v from %v", option.String(), r.RemoteAddr)
			if err != nil && ms.Topo.AvailableSpaceFor(option) <= 0 {
				err = fmt.Errorf("%s and no free volumes left for %s", err.Error(), option.String())
			}

			// 标记扩容请求正在进行
			vl.AddGrowRequest()

			// 向扩容请求通道发送扩容任务
			ms.volumeGrowthRequestChan <- &topology.VolumeGrowRequest{
				Option: option,
				Count:  uint32(writableVolumeCount),
				Reason: "http assign",
			}
		}

		if err != nil {
			// 分配失败，记录错误并等待重试
			stats.MasterPickForWriteErrorCounter.Inc()
			lastErr = err
			time.Sleep(200 * time.Millisecond)
			continue
		} else {
			// 分配成功，生成 JWT token
			ms.maybeAddJwtAuthorization(w, fid, true)

			// 获取数据节点
			dn := dnList.Head()
			if dn == nil {
				continue
			}

			// 返回分配结果
			writeJsonQuiet(w, r, http.StatusOK, operation.AssignResult{
				Fid:       fid,
				Url:       dn.Url(),
				PublicUrl: dn.PublicUrl,
				Count:     count,
			})
			return
		}
	}

	// 超时或失败，返回错误
	if lastErr != nil {
		writeJsonQuiet(w, r, http.StatusNotAcceptable, operation.AssignResult{Error: lastErr.Error()})
	} else {
		writeJsonQuiet(w, r, http.StatusRequestTimeout, operation.AssignResult{Error: "request timeout"})
	}
}

// maybeAddJwtAuthorization 为文件操作生成并添加 JWT 鉴权 token
// 将生成的 JWT token 添加到响应头的 Authorization 字段
//
// 参数:
//   - w: HTTP 响应对象
//   - fileId: 文件 ID
//   - isWrite: 是否为写入操作
//
// JWT 生成策略:
//   - 写入操作（上传/删除）：使用 SigningKey 生成，过期时间为 ExpiresAfterSec
//   - 读取操作（下载）：使用 ReadSigningKey 生成，过期时间为 ReadExpiresAfterSec
//
// 安全机制:
//   - 每个文件操作都需要携带有效的 JWT token
//   - Volume Server 会验证 token 的签名和过期时间
//   - token 中包含文件 ID，防止跨文件访问
//   - 读写分离的密钥设计，提供更细粒度的权限控制
//
// 响应头格式:
//   Authorization: BEARER eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
//
// 注意:
//   - 如果 fileId 为空，不生成 token
//   - 如果未配置 SigningKey，不生成 token
//   - 客户端需要在访问 Volume Server 时携带此 token
func (ms *MasterServer) maybeAddJwtAuthorization(w http.ResponseWriter, fileId string, isWrite bool) {
	if fileId == "" {
		return
	}

	var encodedJwt security.EncodedJwt
	if isWrite {
		// 写入操作：使用写入密钥生成 token
		encodedJwt = security.GenJwtForVolumeServer(ms.guard.SigningKey, ms.guard.ExpiresAfterSec, fileId)
	} else {
		// 读取操作：使用只读密钥生成 token
		encodedJwt = security.GenJwtForVolumeServer(ms.guard.ReadSigningKey, ms.guard.ReadExpiresAfterSec, fileId)
	}

	if encodedJwt == "" {
		return
	}

	// 将 JWT token 添加到响应头
	w.Header().Set("Authorization", "BEARER "+string(encodedJwt))
}
