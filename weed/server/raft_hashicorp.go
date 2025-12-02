// Package weed_server 实现基于 HashiCorp Raft 的分布式共识
// 本文件是 SeaweedFS Master 高可用的核心实现
//
// Raft 共识算法简介:
//   - Raft 是一种分布式共识算法，用于在多个节点间保持数据一致性
//   - 通过 Leader 选举和日志复制，确保集群状态的强一致性
//   - 即使部分节点故障，集群仍可正常工作（需要多数节点存活）
//
// SeaweedFS 中的 Raft 应用:
//   - Master 节点通过 Raft 实现高可用
//   - 拓扑信息（卷位置、数据节点等）通过 Raft 日志同步
//   - 只有 Leader 可以处理写入请求
//   - Follower 会将写入请求转发给 Leader
//
// 参考资料:
//   - https://yusufs.medium.com/creating-distributed-kv-database-by-implementing-raft-consensus-using-golang-d0884eef2e28
//   - https://github.com/Jille/raft-grpc-example
package weed_server

import (
	"fmt"
	"math/rand/v2"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	transport "github.com/Jille/raft-grpc-transport"
	"github.com/armon/go-metrics"
	"github.com/armon/go-metrics/prometheus"
	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"google.golang.org/grpc"
)

const (
	// ldbFile 存储 Raft 日志的文件名
	// 使用 BoltDB 存储，支持持久化和崩溃恢复
	ldbFile = "logs.dat"

	// sdbFile 存储 Raft 稳定状态的文件名
	// 包括当前任期、投票记录等关键信息
	sdbFile = "stable.dat"

	// updatePeersTimeout Peer 更新的超时时间
	// 用于定期同步集群成员变更
	updatePeersTimeout = 15 * time.Minute
)

// getPeerIdx 计算当前节点在 Peer 列表中的索引位置
// 用于在集群初始化时错开各节点的启动时间，避免冲突
//
// 参数:
//   - self: 当前节点的地址
//   - mapPeers: 所有 Peer 节点的地址映射
//
// 返回:
//   - 当前节点的索引位置（从 0 开始）
//   - 如果未找到返回 -1
//
// 实现逻辑:
//   1. 将 Peer 地址按字母顺序排序
//   2. 查找当前节点在排序后列表中的位置
//   3. 返回索引
//
// 用途:
//   - 避免多个节点同时启动时的竞争
//   - 根据索引错开启动时间（sleep 不同时长）
func getPeerIdx(self pb.ServerAddress, mapPeers map[string]pb.ServerAddress) int {
	// 将 map 转换为 slice 以便排序
	peers := make([]pb.ServerAddress, 0, len(mapPeers))
	for _, peer := range mapPeers {
		peers = append(peers, peer)
	}

	// 按地址字符串排序，确保所有节点看到的顺序一致
	sort.Slice(peers, func(i, j int) bool {
		return strings.Compare(string(peers[i]), string(peers[j])) < 0
	})

	// 查找当前节点的索引
	for i, peer := range peers {
		if string(peer) == string(self) {
			return i
		}
	}
	return -1
}

// AddPeersConfiguration 构建 Raft 集群的初始配置
// 将所有 Peer 节点添加到 Raft 配置中
//
// 返回:
//   - raft.Configuration: Raft 集群配置
//
// 配置说明:
//   - Suffrage: raft.Voter 表示该节点有投票权
//   - ID: Peer 的唯一标识符
//   - Address: Peer 的 gRPC 地址
//
// 注意:
//   - 只有有投票权的节点才能参与 Leader 选举
//   - 集群需要多数节点存活才能正常工作
func (s *RaftServer) AddPeersConfiguration() (cfg raft.Configuration) {
	for _, peer := range s.peers {
		cfg.Servers = append(cfg.Servers, raft.Server{
			Suffrage: raft.Voter,                            // 投票权
			ID:       raft.ServerID(peer),                   // 节点 ID
			Address:  raft.ServerAddress(peer.ToGrpcAddress()), // gRPC 地址
		})
	}
	return cfg
}

// monitorLeaderLoop 监控 Raft Leader 变更事件
// 这是一个长期运行的协程，监听 Leader 状态变化并做相应处理
//
// 参数:
//   - updatePeers: 是否需要更新 Peer 列表
//
// 功能:
//   1. 监听 Leader 变更通知
//   2. 当成为 Leader 时，执行 Barrier 操作确保状态同步
//   3. 当失去 Leader 身份时，重置 Barrier 状态
//   4. 更新 Prometheus 指标和日志
//
// Barrier 机制:
//   - DoBarrier(): 确保所有之前的 Raft 日志都已应用到状态机
//   - 这保证了新 Leader 看到的是最新的集群状态
//   - BarrierReset(): Follower 重置 Barrier 状态
//
// Leader 变更流程:
//   1. 接收 Leader 变更事件
//   2. 如果成为 Leader：
//      - 更新 Peer 列表（首次）
//      - 执行 Barrier 确保状态同步
//      - 更新监控指标
//   3. 如果失去 Leader：
//      - 重置 Barrier 状态
//   4. 记录 Leader 变更时间
func (s *RaftServer) monitorLeaderLoop(updatePeers bool) {
	for {
		// 记录上一个 Leader
		prevLeader, _ := s.RaftHashicorp.LeaderWithID()

		select {
		case isLeader := <-s.RaftHashicorp.LeaderCh():
			// 获取当前 Leader
			leader, _ := s.RaftHashicorp.LeaderWithID()

			if isLeader {
				// 当前节点成为 Leader

				if updatePeers {
					// 首次成为 Leader 时更新 Peer 列表
					s.updatePeers()
					updatePeers = false
				}

				// 执行 Barrier，确保所有日志都已应用
				s.topo.DoBarrier()

				// 更新 Prometheus 指标
				stats.MasterLeaderChangeCounter.WithLabelValues(fmt.Sprintf("%+v", leader)).Inc()
			} else {
				// 当前节点不是 Leader（变为 Follower）
				s.topo.BarrierReset()
			}

			// 记录 Leader 变更日志
			glog.V(0).Infof("is leader %+v change event: %+v => %+v", isLeader, prevLeader, leader)
			prevLeader = leader

			// 更新最后一次 Leader 变更时间
			s.topo.LastLeaderChangeTime = time.Now()
		}
	}
}

// updatePeers 同步 Raft 集群成员列表
// 将配置中的 Peer 列表与 Raft 当前配置同步
//
// 功能:
//   1. 添加新加入的 Peer 节点
//   2. 移除已离开的 Peer 节点
//   3. 确保集群成员配置一致
//
// 实现逻辑:
//   1. 获取 Raft 当前配置中的所有节点
//   2. 对比配置文件中的 Peer 列表
//   3. 添加配置中有但 Raft 中没有的节点
//   4. 移除 Raft 中有但配置中没有的节点
//
// 使用场景:
//   - 动态扩容：添加新的 Master 节点
//   - 动态缩容：移除故障或下线的 Master 节点
//   - 配置更新：修改集群成员后同步
//
// 注意:
//   - 只有 Leader 才能修改集群配置
//   - 添加/移除节点是异步操作
//   - 集群成员变更需要多数节点同意
func (s *RaftServer) updatePeers() {
	peerLeader := string(s.serverAddr)
	existsPeerName := make(map[string]bool)

	// 获取 Raft 当前配置中的所有节点（除了 Leader）
	for _, server := range s.RaftHashicorp.GetConfiguration().Configuration().Servers {
		if string(server.ID) == peerLeader {
			continue
		}
		existsPeerName[string(server.ID)] = true
	}

	// 添加新的 Peer 节点
	for _, peer := range s.peers {
		peerName := string(peer)
		if peerName == peerLeader || existsPeerName[peerName] {
			// 跳过 Leader 自己和已存在的节点
			continue
		}
		glog.V(0).Infof("adding new peer: %s", peerName)
		// 添加为有投票权的节点
		s.RaftHashicorp.AddVoter(
			raft.ServerID(peerName), raft.ServerAddress(peer.ToGrpcAddress()), 0, 0)
	}

	// 移除已离开的 Peer 节点
	for peer := range existsPeerName {
		if _, found := s.peers[peer]; !found {
			glog.V(0).Infof("removing old peer: %s", peer)
			s.RaftHashicorp.RemoveServer(raft.ServerID(peer), 0, 0)
		}
	}

	// 如果当前 Leader 不在配置中，也移除
	if _, found := s.peers[peerLeader]; !found {
		glog.V(0).Infof("removing old leader peer: %s", peerLeader)
		s.RaftHashicorp.RemoveServer(raft.ServerID(peerLeader), 0, 0)
	}
}

// NewHashicorpRaftServer 创建并初始化 HashiCorp Raft 服务器
// 这是 SeaweedFS Master 高可用的核心初始化函数
//
// 参数:
//   - option: Raft 服务器配置选项
//
// 返回:
//   - *RaftServer: 初始化好的 Raft 服务器实例
//   - error: 初始化失败时返回错误
//
// 初始化流程（5个主要步骤）:
//
// 【步骤 1：创建 RaftServer 实例】
// 【步骤 2：配置 Raft 参数】
// 【步骤 3：初始化持久化存储】
// 【步骤 4：启动 Raft 实例】
// 【步骤 5：集群引导或加入】
//
// 详细说明见下方代码注释
func NewHashicorpRaftServer(option *RaftServerOption) (*RaftServer, error) {
	// 【步骤 1：创建 RaftServer 实例】
	s := &RaftServer{
		peers:      option.Peers,
		serverAddr: option.ServerAddr,
		dataDir:    option.DataDir,
		topo:       option.Topo,
	}

	// 【步骤 2：配置 Raft 参数】
	c := raft.DefaultConfig()

	// 设置节点唯一标识符
	c.LocalID = raft.ServerID(s.serverAddr) // TODO: IP:port 地址可能会变化

	// 心跳超时：Leader 向 Follower 发送心跳的间隔
	// 添加随机抖动避免多个节点同时超时
	c.HeartbeatTimeout = time.Duration(float64(option.HeartbeatInterval) * (rand.Float64()*0.25 + 1))

	// 选举超时：Follower 等待心跳的最长时间
	c.ElectionTimeout = option.ElectionTimeout

	// Leader 租约超时：Leader 保持领导权的最长时间
	// 不能超过心跳超时
	if c.LeaderLeaseTimeout > c.HeartbeatTimeout {
		c.LeaderLeaseTimeout = c.HeartbeatTimeout
	}

	// 根据日志级别设置 Raft 日志级别
	if glog.V(4) {
		c.LogLevel = "Debug"
	} else if glog.V(2) {
		c.LogLevel = "Info"
	} else if glog.V(1) {
		c.LogLevel = "Warn"
	} else if glog.V(0) {
		c.LogLevel = "Error"
	}

	// 验证配置有效性
	if err := raft.ValidateConfig(c); err != nil {
		return nil, fmt.Errorf("raft.ValidateConfig: %w", err)
	}

	// 【步骤 3：初始化持久化存储】

	// 如果是 Bootstrap 模式，清空所有旧数据
	// Bootstrap 用于初次启动集群或完全重建
	if option.RaftBootstrap {
		os.RemoveAll(path.Join(s.dataDir, ldbFile))      // 删除日志文件
		os.RemoveAll(path.Join(s.dataDir, sdbFile))      // 删除状态文件
		os.RemoveAll(path.Join(s.dataDir, "snapshots"))  // 删除快照文件
	}

	// 创建快照目录
	if err := os.MkdirAll(path.Join(s.dataDir, "snapshots"), os.ModePerm); err != nil {
		return nil, err
	}
	baseDir := s.dataDir

	// 创建日志存储（LogStore）
	// 使用 BoltDB 存储 Raft 日志条目
	// 日志包含所有状态变更操作，用于复制和恢复
	ldb, err := boltdb.NewBoltStore(filepath.Join(baseDir, ldbFile))
	if err != nil {
		return nil, fmt.Errorf("boltdb.NewBoltStore(%q): %v", filepath.Join(baseDir, "logs.dat"), err)
	}

	// 创建稳定存储（StableStore）
	// 存储 Raft 的持久化状态，如当前任期、投票记录
	sdb, err := boltdb.NewBoltStore(filepath.Join(baseDir, sdbFile))
	if err != nil {
		return nil, fmt.Errorf("boltdb.NewBoltStore(%q): %v", filepath.Join(baseDir, "stable.dat"), err)
	}

	// 创建快照存储（SnapshotStore）
	// 快照用于压缩日志，保留 3 个历史快照
	fss, err := raft.NewFileSnapshotStore(baseDir, 3, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("raft.NewFileSnapshotStore(%q, ...): %v", baseDir, err)
	}

	// 创建 gRPC 传输层
	// 用于 Raft 节点间的通信（心跳、日志复制、投票等）
	s.TransportManager = transport.New(raft.ServerAddress(s.serverAddr), []grpc.DialOption{option.GrpcDialOption})

	// 【步骤 4：启动 Raft 实例】

	// 创建状态机（FSM - Finite State Machine）
	// 状态机负责应用 Raft 日志到实际的业务状态（拓扑信息）
	stateMachine := StateMachine{topo: option.Topo}

	// 创建 Raft 实例
	// 参数说明:
	//   - c: Raft 配置
	//   - stateMachine: 状态机，应用日志到拓扑
	//   - ldb: 日志存储
	//   - sdb: 稳定存储
	//   - fss: 快照存储
	//   - transport: 节点间通信传输层
	s.RaftHashicorp, err = raft.NewRaft(c, &stateMachine, ldb, sdb, fss, s.TransportManager.Transport())
	if err != nil {
		return nil, fmt.Errorf("raft.NewRaft: %w", err)
	}

	// 【步骤 5：集群引导或加入】

	updatePeers := false

	// 判断是否需要引导新集群
	// 条件：Bootstrap 模式 或 当前无集群配置
	if option.RaftBootstrap || len(s.RaftHashicorp.GetConfiguration().Configuration().Servers) == 0 {
		// 构建初始集群配置
		cfg := s.AddPeersConfiguration()

		// 计算当前节点的启动延迟
		// 目的：避免所有节点同时引导集群导致冲突
		// 每个节点根据其索引位置延迟不同的时间
		peerIdx := getPeerIdx(s.serverAddr, s.peers)
		timeSleep := time.Duration(float64(c.LeaderLeaseTimeout) * (rand.Float64()*0.25 + 1) * float64(peerIdx))
		glog.V(0).Infof("Bootstrapping idx: %d sleep: %v new cluster: %+v", peerIdx, timeSleep, cfg)

		// 延迟启动
		time.Sleep(timeSleep)

		// 引导集群
		// 将初始配置写入 Raft 日志，所有节点从此配置开始
		f := s.RaftHashicorp.BootstrapCluster(cfg)
		if err := f.Error(); err != nil {
			return nil, fmt.Errorf("raft.Raft.BootstrapCluster: %w", err)
		}
	} else {
		// 加入已存在的集群，需要更新 Peer 列表
		updatePeers = true
	}

	// 启动 Leader 监控协程
	// 监听 Leader 变更事件并执行相应操作
	go s.monitorLeaderLoop(updatePeers)

	// 启动调试日志协程（仅在 Debug 级别）
	// 定期打印集群配置信息
	ticker := time.NewTicker(c.HeartbeatTimeout * 10)
	if glog.V(4) {
		go func() {
			for {
				select {
				case <-ticker.C:
					cfuture := s.RaftHashicorp.GetConfiguration()
					if err = cfuture.Error(); err != nil {
						glog.Fatalf("error getting config: %s", err)
					}
					configuration := cfuture.Configuration()
					glog.V(4).Infof("Showing peers known by %s:\n%+v", s.RaftHashicorp.String(), configuration.Servers)
				}
			}
		}()
	}

	// 配置 Prometheus 指标收集
	// 用于监控 Raft 集群的健康状况
	if sink, err := prometheus.NewPrometheusSinkFrom(prometheus.PrometheusOpts{
		Registerer: stats.Gather,
	}); err != nil {
		return nil, fmt.Errorf("NewPrometheusSink: %w", err)
	} else {
		metricsConf := metrics.DefaultConfig(stats.Namespace)
		metricsConf.EnableRuntimeMetrics = false // 禁用运行时指标，减少开销
		if _, err = metrics.NewGlobal(metricsConf, sink); err != nil {
			return nil, fmt.Errorf("metrics.NewGlobal: %w", err)
		}
	}

	return s, nil
}
