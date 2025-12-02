// Package weed_server 实现 Volume Server 的 Web UI 状态页面
// 本文件提供一个 HTML 页面，展示 Volume Server 的运行状态和详细信息
//
// 核心功能:
//   - uiStatusHandler: 渲染 HTML 状态页面，显示 Volume Server 的全面信息
//
// 展示内容:
//   - 基本信息：版本、运行时间、Master 节点列表
//   - 磁盘状态：所有存储目录的容量、使用率、磁盘类型
//   - Volume 信息：本地 Volume 列表（ID、大小、文件数、副本策略等）
//   - EC Volume 信息：纠删码 Volume 列表
//   - 远程 Volume 信息：存储在云端的 Volume 列表
//   - 统计数据：请求计数、字节传输、错误率等
//
// 使用场景:
//   - 运维监控：通过浏览器快速查看 Volume Server 状态
//   - 故障诊断：检查磁盘使用、Volume 分布、错误统计
//   - 容量规划：查看磁盘剩余空间，决定是否需要扩容
//   - 健康检查：确认所有 Volume 正常，无异常错误
//
// 访问方式:
//   浏览器访问：http://<volume-server>:8080/ui/index.html
//   或：http://<volume-server>:8080/
//
// 页面内容示例:
//   ┌─────────────────────────────────────────┐
//   │ SeaweedFS Volume Server                 │
//   │ Version: 3.50                           │
//   │ Up Time: 2 days 5 hours                 │
//   │                                         │
//   │ Masters:                                │
//   │   - 192.168.1.10:9333                   │
//   │   - 192.168.1.11:9333                   │
//   │                                         │
//   │ Disk Status:                            │
//   │   /data1: 500GB / 1TB (50%)  [SSD]      │
//   │   /data2: 200GB / 500GB (40%) [HDD]     │
//   │                                         │
//   │ Volumes: 150 volumes                    │
//   │   Volume 1: photos (20GB, 50000 files)  │
//   │   Volume 2: videos (30GB, 1000 files)   │
//   │   ...                                   │
//   │                                         │
//   │ EC Volumes: 10 volumes                  │
//   │ Remote Volumes: 5 volumes               │
//   │                                         │
//   │ Stats:                                  │
//   │   Requests: 1,234,567                   │
//   │   Bytes Read: 500GB                     │
//   │   Bytes Written: 200GB                  │
//   └─────────────────────────────────────────┘
//
// 注意事项:
//   - 页面数据是实时查询的（不缓存）
//   - Volume 列表可能很长（数百个），加载时间取决于 Volume 数量
//   - 不建议在生产环境暴露此页面（可能泄露敏感信息）
package weed_server

import (
	"net/http"
	"path/filepath"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/util/version"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	ui "github.com/seaweedfs/seaweedfs/weed/server/volume_server_ui"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage"
)

// uiStatusHandler 渲染 HTML 状态页面，显示 Volume Server 的全面信息
// 用于运维人员通过浏览器查看 Volume Server 状态
//
// 路径: GET /ui/index.html 或 GET /
//
// 返回:
//   - HTML 页面，包含版本、运行时间、磁盘状态、Volume 列表、统计数据等
//
// 页面数据:
//   - Version: SeaweedFS 版本号
//   - Masters: 配置的 Master 节点列表
//   - Up Time: Volume Server 运行时间
//   - DiskStatuses: 所有存储目录的磁盘状态
//   - Volumes: 本地 Volume 列表（不包含远程 Volume）
//   - EcVolumes: EC（纠删码）Volume 列表
//   - RemoteVolumes: 存储在云端的 Volume 列表
//   - Stats: 统计信息（如运行时间）
//   - Counters: 请求计数、字节传输、错误率等
//
// Volume 分类逻辑:
//   - 本地 Volume：数据存储在本地磁盘，性能最高
//   - 远程 Volume：数据存储在云端（S3/Azure/GCS），成本最低
//   - EC Volume：使用纠删码存储，平衡性能和成本
//
// 使用示例:
//   # 打开浏览器访问
//   http://localhost:8080/ui/index.html
//
//   # 或使用 curl 获取 HTML
//   curl http://localhost:8080/ui/index.html
//
// 模板渲染:
//   使用 volume_server_ui.StatusTpl 模板渲染 HTML
//   模板文件：weed/server/volume_server_ui/templates.go
func (vs *VolumeServer) uiStatusHandler(w http.ResponseWriter, r *http.Request) {
	// 【设置响应头】
	w.Header().Set("Server", "SeaweedFS Volume "+version.VERSION)

	// 【收集基本信息】
	infos := make(map[string]interface{})
	// 计算运行时间（从 Volume Server 启动到现在）
	infos["Up Time"] = time.Now().Sub(startTime).String()

	// 【收集磁盘状态】
	var ds []*volume_server_pb.DiskStatus
	for _, loc := range vs.store.Locations {
		// 获取目录的绝对路径
		if dir, e := filepath.Abs(loc.Directory); e == nil {
			// 创建磁盘状态对象
			// NewDiskStatus 读取磁盘使用情况（总容量、已使用、可用）
			newDiskStatus := stats.NewDiskStatus(dir)

			// 设置磁盘类型（hdd、ssd、nvme 等）
			newDiskStatus.DiskType = loc.DiskType.String()

			ds = append(ds, newDiskStatus)
		}
	}

	// 【收集 Volume 信息】
	volumeInfos := vs.store.VolumeInfos()

	// 【分类 Volume】
	// 区分本地 Volume 和远程 Volume
	var normalVolumeInfos, remoteVolumeInfos []*storage.VolumeInfo
	for _, vinfo := range volumeInfos {
		if vinfo.IsRemote() {
			// 远程 Volume：数据存储在云端（S3/Azure/GCS）
			// 访问延迟高，但成本低
			remoteVolumeInfos = append(remoteVolumeInfos, vinfo)
		} else {
			// 本地 Volume：数据存储在本地磁盘
			// 访问延迟低，但磁盘成本高
			normalVolumeInfos = append(normalVolumeInfos, vinfo)
		}
	}

	// 【构造模板参数】
	// 传递给 HTML 模板的数据结构
	args := struct {
		Version       string                 // SeaweedFS 版本号
		Masters       []pb.ServerAddress     // Master 节点列表
		Volumes       interface{}            // 本地 Volume 列表
		EcVolumes     interface{}            // EC（纠删码）Volume 列表
		RemoteVolumes interface{}            // 远程 Volume 列表
		DiskStatuses  interface{}            // 磁盘状态列表
		Stats         interface{}            // 统计信息（如运行时间）
		Counters      *stats.ServerStats     // 请求计数、字节传输等
	}{
		version.Version(),                   // 版本信息
		vs.SeedMasterNodes,                  // 配置的 Master 节点
		normalVolumeInfos,                   // 本地 Volume
		vs.store.EcVolumes(),                // EC Volume
		remoteVolumeInfos,                   // 远程 Volume
		ds,                                  // 磁盘状态
		infos,                               // 基本信息（运行时间）
		serverStats,                         // 统计数据
	}

	// 【渲染 HTML 模板】
	// StatusTpl 是预编译的 HTML 模板
	// 模板文件：weed/server/volume_server_ui/templates.go
	if err := ui.StatusTpl.Execute(w, args); err != nil {
		// 模板渲染失败（通常是编程错误）
		glog.Errorf("template execution error: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}
