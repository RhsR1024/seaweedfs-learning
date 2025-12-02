// Package weed_server 实现 Raft 集群状态查询的 HTTP 处理函数
// 本文件提供集群状态、健康检查和 Raft 统计的 API
package weed_server

import (
	"github.com/cenkalti/backoff/v4"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"net/http"
	"time"
)

// ClusterStatusResult 集群状态信息
// 用于 /cluster/status API 的返回结果
type ClusterStatusResult struct {
	IsLeader    bool             `json:"IsLeader,omitempty"`    // 当前节点是否是 Leader
	Leader      pb.ServerAddress `json:"Leader,omitempty"`      // Leader 节点地址
	Peers       []string         `json:"Peers,omitempty"`       // 所有 Peer 节点列表
	MaxVolumeId needle.VolumeId  `json:"MaxVolumeId,omitempty"` // 当前最大卷 ID
}

// StatusHandler 返回 Raft 集群的状态信息
// API 端点: GET /cluster/status
//
// 返回信息:
//   {
//     "IsLeader": true,
//     "Leader": "192.168.1.10:9333",
//     "Peers": ["192.168.1.10:9333", "192.168.1.11:9333", "192.168.1.12:9333"],
//     "MaxVolumeId": 12345
//   }
//
// 用途:
//   - 监控集群状态
//   - 判断当前节点是否是 Leader
//   - 获取 Leader 地址
//   - 查看集群成员列表
//   - 获取最大卷 ID
func (s *RaftServer) StatusHandler(w http.ResponseWriter, r *http.Request) {
	ret := ClusterStatusResult{
		IsLeader:    s.topo.IsLeader(),
		Peers:       s.Peers(),
		MaxVolumeId: s.topo.GetMaxVolumeId(),
	}

	// 获取 Leader 地址
	if leader, e := s.topo.Leader(); e == nil {
		ret.Leader = leader
	}

	writeJsonQuiet(w, r, http.StatusOK, ret)
}

// HealthzHandler 健康检查接口
// API 端点: GET /cluster/healthz
//
// 返回状态码:
//   - 200 OK: 集群健康
//   - 423 Locked: Leader 节点的子节点被锁定（正在执行关键操作）
//   - 503 Service Unavailable: 无法确定 Leader（集群不可用）
//
// 健康检查逻辑:
//   1. 检查是否能获取 Leader 地址
//   2. 如果当前节点是 Leader，额外检查子节点锁定状态
//   3. 使用指数退避重试机制，最多重试 5 秒
//
// 用途:
//   - 负载均衡器的健康检查
//   - Kubernetes liveness/readiness probe
//   - 监控系统的健康检查
//
// 注意:
//   - Follower 节点只检查是否有 Leader
//   - Leader 节点还会检查是否有子节点被锁定
//   - 锁定状态表示正在执行 Barrier 或其他关键操作
func (s *RaftServer) HealthzHandler(w http.ResponseWriter, r *http.Request) {
	// 检查是否能获取 Leader
	leader, err := s.topo.Leader()
	if err != nil {
		// 无法确定 Leader，集群不可用
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	// 如果当前节点是 Leader，检查子节点锁定状态
	if s.serverAddr == leader {
		// 配置指数退避策略
		expBackoff := backoff.NewExponentialBackOff()
		expBackoff.InitialInterval = 20 * time.Millisecond  // 初始重试间隔
		expBackoff.MaxInterval = 1 * time.Second             // 最大重试间隔
		expBackoff.MaxElapsedTime = 5 * time.Second          // 总超时时间

		// 使用指数退避重试检查锁定状态
		isLocked, err := backoff.RetryWithData(s.topo.IsChildLocked, expBackoff)
		if err != nil {
			glog.Errorf("HealthzHandler: %+v", err)
		}

		if isLocked {
			// 子节点被锁定，返回 423 Locked
			w.WriteHeader(http.StatusLocked)
			return
		}
	}

	// 健康检查通过
	w.WriteHeader(http.StatusOK)
}

// StatsRaftHandler 返回 Raft 的统计信息
// API 端点: GET /raft/stats
//
// 返回信息（来自 HashiCorp Raft）:
//   {
//     "applied_index": "12345",      // 已应用的日志索引
//     "commit_index": "12345",       // 已提交的日志索引
//     "fsm_pending": "0",            // 待应用到状态机的日志数
//     "last_contact": "0",           // 最后一次联系 Leader 的时间
//     "last_log_index": "12345",     // 最后一条日志的索引
//     "last_log_term": "3",          // 最后一条日志的任期
//     "last_snapshot_index": "10000",// 最后一次快照的索引
//     "last_snapshot_term": "2",     // 最后一次快照的任期
//     "num_peers": "2",              // Peer 节点数量
//     "state": "Leader",             // 当前状态：Leader/Follower/Candidate
//     "term": "3"                    // 当前任期
//   }
//
// 用途:
//   - 监控 Raft 集群的详细状态
//   - 调试 Raft 相关问题
//   - 了解日志复制进度
//
// 注意:
//   - 只在使用 HashiCorp Raft 实现时可用
//   - 原生 SeaweedFS Raft 不支持此接口
func (s *RaftServer) StatsRaftHandler(w http.ResponseWriter, r *http.Request) {
	if s.RaftHashicorp == nil {
		// 未使用 HashiCorp Raft
		writeJsonQuiet(w, r, http.StatusNotFound, nil)
		return
	}

	// 返回 Raft 统计信息
	writeJsonQuiet(w, r, http.StatusOK, s.RaftHashicorp.Stats())
}
