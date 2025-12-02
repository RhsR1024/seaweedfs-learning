// Package weed_server 实现 Master Server 的 UI 处理函数
// 本文件提供 Web UI 界面，用于可视化展示集群状态
package weed_server

import (
	"github.com/seaweedfs/seaweedfs/weed/util/version"
	"net/http"
	"time"

	hashicorpRaft "github.com/hashicorp/raft"
	"github.com/seaweedfs/raft"

	ui "github.com/seaweedfs/seaweedfs/weed/server/master_ui"
	"github.com/seaweedfs/seaweedfs/weed/stats"
)

// uiStatusHandler 渲染 Master 的 Web UI 状态页面
// API 端点: GET /ui/index.html 或 GET /
//
// 功能:
//   - 展示集群拓扑结构（数据中心、机架、节点、卷）
//   - 显示 Raft 集群状态（Leader、Peers、日志索引等）
//   - 显示运行时统计信息（运行时间、请求计数等）
//   - 显示卷容量限制配置
//
// UI 显示的信息:
//   - Version: SeaweedFS 版本号
//   - Topology: 完整的拓扑树结构
//     - DataCenters: 数据中心及其下的机架和节点
//     - Free/Max: 可用/最大卷数
//   - RaftServer: Raft 集群信息
//     - Leader: 当前 Leader 地址
//     - Peers: 所有节点列表
//     - State: 节点状态（Leader/Follower/Candidate）
//   - Stats: 运行时统计
//     - Up Time: 运行时长
//     - Max Volume Id: 当前最大卷 ID
//   - Counters: 请求计数器
//     - 分配请求数、查询请求数等
//   - VolumeSizeLimitMB: 单个卷的大小限制（MB）
//
// Raft 实现:
//   - 支持两种 Raft 实现：原生 SeaweedFS Raft 和 HashiCorp Raft
//   - 根据实际使用的实现选择不同的模板渲染
//
// 模板文件:
//   - StatusTpl: 原生 Raft 的模板
//   - StatusNewRaftTpl: HashiCorp Raft 的模板
//
// 用途:
//   - 在浏览器中可视化监控集群状态
//   - 快速诊断集群问题
//   - 查看数据分布和容量使用情况
func (ms *MasterServer) uiStatusHandler(w http.ResponseWriter, r *http.Request) {
	// 准备统计信息
	infos := make(map[string]interface{})
	infos["Up Time"] = time.Now().Sub(startTime).String()        // 运行时长
	infos["Max Volume Id"] = ms.Topo.GetMaxVolumeId()            // 当前最大卷 ID

	// 获取 Raft 访问锁，确保读取一致性
	ms.Topo.RaftServerAccessLock.RLock()
	defer ms.Topo.RaftServerAccessLock.RUnlock()

	if ms.Topo.RaftServer != nil {
		// 使用原生 SeaweedFS Raft 实现
		args := struct {
			Version           string
			Topology          interface{}
			RaftServer        raft.Server
			Stats             map[string]interface{}
			Counters          *stats.ServerStats
			VolumeSizeLimitMB uint32
		}{
			version.Version(),
			ms.Topo.ToInfo(),
			ms.Topo.RaftServer,
			infos,
			serverStats,
			ms.option.VolumeSizeLimitMB,
		}
		ui.StatusTpl.Execute(w, args)
	} else if ms.Topo.HashicorpRaft != nil {
		// 使用 HashiCorp Raft 实现
		args := struct {
			Version           string
			Topology          interface{}
			RaftServer        *hashicorpRaft.Raft
			Stats             map[string]interface{}
			Counters          *stats.ServerStats
			VolumeSizeLimitMB uint32
		}{
			version.Version(),
			ms.Topo.ToInfo(),
			ms.Topo.HashicorpRaft,
			infos,
			serverStats,
			ms.option.VolumeSizeLimitMB,
		}
		ui.StatusNewRaftTpl.Execute(w, args)
	}
}
