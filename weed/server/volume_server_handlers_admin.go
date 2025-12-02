// Package weed_server 实现 Volume Server 的 HTTP 管理接口
// 本文件提供健康检查、状态查询和磁盘统计的 HTTP Handler
//
// 核心功能:
//   - healthzHandler: 健康检查接口，用于负载均衡器探活
//   - statusHandler: 状态查询接口，返回 Volume 信息和磁盘状态
//   - statsDiskHandler: 磁盘统计接口，只返回磁盘使用情况
//
// 使用场景:
//   - 负载均衡：负载均衡器通过 healthz 接口判断节点健康
//   - 监控系统：通过 status 接口采集 Volume Server 状态
//   - 容量规划：通过 statsDisk 接口监控磁盘使用情况
//   - 运维告警：磁盘空间不足时告警
//
// HTTP 接口:
//   - GET /healthz
//     * 返回 200: 健康
//     * 返回 503: 不健康（副本不可达）
//   - GET /status
//     * 返回 JSON：包含版本、磁盘状态、Volume 列表
//   - GET /stats/disk
//     * 返回 JSON：只包含版本和磁盘状态
//
// 健康检查逻辑:
//   - 检查所有有副本的 Volume
//   - 确保至少一个副本可达
//   - 任何 Volume 的副本全部不可达时返回不健康
//
// 注意事项:
//   - healthz 检查可能较慢（需要连接副本）
//   - status 返回所有 Volume 信息（数据量可能较大）
//   - statsDisk 性能开销较小，适合高频采集
package weed_server

import (
	"net/http"
	"path/filepath"

	"github.com/seaweedfs/seaweedfs/weed/topology"
	"github.com/seaweedfs/seaweedfs/weed/util/version"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
)

// healthzHandler HTTP 健康检查接口
// 用于负载均衡器和监控系统判断 Volume Server 是否健康
//
// 路径: GET /healthz
//
// 返回:
//   - 200 OK: Volume Server 健康，所有有副本的 Volume 至少一个副本可达
//   - 503 Service Unavailable: 不健康，存在 Volume 的所有副本都不可达
//
// 健康检查逻辑:
//   1. 遍历所有 Volume
//   2. 跳过无 Collection 的 Volume（单副本 Volume）
//   3. 检查有副本的 Volume（ReplicaPlacement.CopyCount > 1）
//   4. 尝试获取可写副本列表
//   5. 任何一个 Volume 的副本全部不可达时返回 503
//
// 使用场景:
//   - Kubernetes Liveness Probe:
//     livenessProbe:
//       httpGet:
//         path: /healthz
//         port: 8080
//   - Nginx 负载均衡:
//     upstream backend {
//       server 192.168.1.10:8080 max_fails=3 fail_timeout=30s;
//       check interval=5000 rise=2 fall=3 timeout=1000 type=http;
//       check_http_send "GET /healthz HTTP/1.0\r\n\r\n";
//       check_http_expect_alive http_2xx;
//     }
//
// 性能考虑:
//   - 需要连接副本 Volume Server（可能较慢）
//   - 建议降低检查频率（如 30 秒一次）
//   - 超时设置要合理（如 5 秒）
//
// 响应头:
//   - Server: SeaweedFS Volume <version>
func (vs *VolumeServer) healthzHandler(w http.ResponseWriter, r *http.Request) {
	// 【设置响应头】
	// 标识服务器类型和版本
	w.Header().Set("Server", "SeaweedFS Volume "+version.VERSION)

	// 【获取所有 Volume 信息】
	volumeInfos := vs.store.VolumeInfos()

	// 【检查每个 Volume 的副本健康】
	for _, vinfo := range volumeInfos {
		// 【跳过无 Collection 的 Volume】
		// Collection 为空的 Volume 通常是单副本，不需要检查副本健康
		if len(vinfo.Collection) == 0 {
			continue
		}

		// 【检查副本数】
		// GetCopyCount() 返回副本数量（包括自己）
		// > 1 表示有远程副本需要检查
		if vinfo.ReplicaPlacement.GetCopyCount() > 1 {
			// 【获取可写副本列表】
			// GetWritableRemoteReplications 尝试连接所有副本 Volume Server
			// 返回可写的副本列表
			// 如果没有任何副本可达，返回错误
			_, err := topology.GetWritableRemoteReplications(vs.store, vs.grpcDialOption, vinfo.Id, vs.GetMaster)
			if err != nil {
				// 【副本不可达，返回不健康】
				// 至少一个 Volume 的所有副本都不可达
				// 标记 Volume Server 为不健康
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
		}
	}

	// 【所有检查通过，返回健康】
	w.WriteHeader(http.StatusOK)
}

// statusHandler HTTP 状态查询接口
// 返回 Volume Server 的详细状态信息
//
// 路径: GET /status
//
// 返回: JSON 格式，包含：
//   - Version: SeaweedFS 版本号
//   - DiskStatuses: 磁盘状态列表
//     * Dir: 磁盘目录
//     * DiskType: 磁盘类型（hdd、ssd、nvme 等）
//     * All: 总容量（字节）
//     * Used: 已使用（字节）
//     * Free: 可用空间（字节）
//     * PercentUsed: 使用百分比
//     * PercentFree: 剩余百分比
//   - Volumes: Volume 信息列表
//     * Id: Volume ID
//     * Collection: 集合名称
//     * ReplicaPlacement: 副本策略
//     * Ttl: TTL 配置
//     * Version: Needle 版本
//     * Size: Volume 大小
//     * FileCount: 文件数量
//     * ReadOnly: 是否只读
//     等等
//
// 使用场景:
//   - 监控系统：Prometheus、Grafana 采集指标
//   - 运维脚本：自动化运维工具查询状态
//   - 调试诊断：排查 Volume Server 问题
//
// 使用示例:
//   curl http://localhost:8080/status | jq .
//   {
//     "Version": "3.50",
//     "DiskStatuses": [
//       {
//         "dir": "/data1",
//         "diskType": "ssd",
//         "all": 1099511627776,
//         "used": 549755813888,
//         "free": 549755813888,
//         "percent_used": 50.0,
//         "percent_free": 50.0
//       }
//     ],
//     "Volumes": [
//       {
//         "id": 1,
//         "collection": "photos",
//         "replicaPlacement": "001",
//         ...
//       }
//     ]
//   }
//
// 响应头:
//   - Server: SeaweedFS Volume <version>
//   - Content-Type: application/json
func (vs *VolumeServer) statusHandler(w http.ResponseWriter, r *http.Request) {
	// 【设置响应头】
	w.Header().Set("Server", "SeaweedFS Volume "+version.VERSION)

	// 【构造响应 JSON】
	m := make(map[string]interface{})

	// 【添加版本信息】
	m["Version"] = version.Version()

	// 【收集磁盘状态】
	var ds []*volume_server_pb.DiskStatus
	for _, loc := range vs.store.Locations {
		// 获取目录的绝对路径
		if dir, e := filepath.Abs(loc.Directory); e == nil {
			// 【创建磁盘状态对象】
			// NewDiskStatus 读取磁盘使用情况
			// 使用系统调用（如 statfs）获取磁盘信息
			newDiskStatus := stats.NewDiskStatus(dir)

			// 【设置磁盘类型】
			// DiskType 标识磁盘性能等级（hdd、ssd、nvme 等）
			// 用于 Tiered Storage 决策
			newDiskStatus.DiskType = loc.DiskType.String()

			ds = append(ds, newDiskStatus)
		}
	}
	m["DiskStatuses"] = ds

	// 【收集 Volume 信息】
	// VolumeInfos 返回所有 Volume 的详细信息
	m["Volumes"] = vs.store.VolumeInfos()

	// 【返回 JSON】
	// writeJsonQuiet 序列化为 JSON 并写入响应
	// 状态码：200 OK
	writeJsonQuiet(w, r, http.StatusOK, m)
}

// statsDiskHandler HTTP 磁盘统计接口
// 只返回磁盘使用情况，不包含 Volume 信息
//
// 路径: GET /stats/disk
//
// 返回: JSON 格式，包含：
//   - Version: SeaweedFS 版本号
//   - DiskStatuses: 磁盘状态列表（同 statusHandler）
//
// 与 statusHandler 的区别:
//   - statsDiskHandler: 只返回磁盘状态，性能开销小
//   - statusHandler: 返回完整信息，包括所有 Volume
//
// 使用场景:
//   - 高频监控：每 10 秒采集一次磁盘使用情况
//   - 容量告警：磁盘空间不足时触发告警
//   - 自动扩容：根据磁盘使用率触发自动扩容
//
// 使用示例:
//   curl http://localhost:8080/stats/disk | jq .
//   {
//     "Version": "3.50",
//     "DiskStatuses": [
//       {
//         "dir": "/data1",
//         "diskType": "ssd",
//         "all": 1099511627776,
//         "used": 549755813888,
//         "free": 549755813888,
//         "percent_used": 50.0,
//         "percent_free": 50.0
//       }
//     ]
//   }
//
// Prometheus 监控示例:
//   # 抓取配置
//   - job_name: 'seaweedfs-volume'
//     static_configs:
//       - targets: ['localhost:8080']
//     metrics_path: '/stats/disk'
//     scrape_interval: 30s
//
// 响应头:
//   - Server: SeaweedFS Volume <version>
//   - Content-Type: application/json
func (vs *VolumeServer) statsDiskHandler(w http.ResponseWriter, r *http.Request) {
	// 【设置响应头】
	w.Header().Set("Server", "SeaweedFS Volume "+version.VERSION)

	// 【构造响应 JSON】
	m := make(map[string]interface{})

	// 【添加版本信息】
	m["Version"] = version.Version()

	// 【收集磁盘状态】
	var ds []*volume_server_pb.DiskStatus
	for _, loc := range vs.store.Locations {
		// 获取目录的绝对路径
		if dir, e := filepath.Abs(loc.Directory); e == nil {
			// 【创建磁盘状态对象】
			// NewDiskStatus 读取磁盘使用情况
			newDiskStatus := stats.NewDiskStatus(dir)

			// 【设置磁盘类型】
			newDiskStatus.DiskType = loc.DiskType.String()

			ds = append(ds, newDiskStatus)
		}
	}
	m["DiskStatuses"] = ds

	// 【返回 JSON】
	// 注意：不包含 Volume 信息，只有磁盘状态
	writeJsonQuiet(w, r, http.StatusOK, m)
}
