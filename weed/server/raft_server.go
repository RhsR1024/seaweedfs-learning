// Package weed_server 实现 SeaweedFS 原生 Raft 共识服务器
// 本文件提供基于 github.com/seaweedfs/raft 的 Raft 实现
// 与 raft_hashicorp.go 不同，这是 SeaweedFS 自研的 Raft 实现
//
// 核心功能:
//   - Master Server 的高可用性（HA）支持
//   - 分布式共识算法实现（Leader 选举、日志复制）
//   - 集群状态机同步（MaxVolumeId 等元数据）
//   - 节点动态加入和移除
//   - 快照和状态恢复
//
// Raft 实现对比:
//   - 原生 Raft（本文件）：SeaweedFS 自研，轻量级，功能简化
//   - HashiCorp Raft（raft_hashicorp.go）：生产级实现，功能完整
//
// 关键概念:
//   - StateMachine：Raft 状态机，负责执行日志命令
//   - Snapshot：快照机制，用于快速恢复和减少日志大小
//   - Leader Election：Leader 选举，确保集群只有一个 Leader
//   - Log Replication：日志复制，确保所有节点状态一致
//
// 使用场景:
//   - 小规模部署（3-5 个 Master 节点）
//   - 简单的元数据同步需求
//   - 对 Raft 实现有定制需求的场景
package weed_server

import (
	"encoding/json"
	"io"
	"math/rand/v2"
	"os"
	"path"
	"time"

	transport "github.com/Jille/raft-grpc-transport"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/pb"

	hashicorpRaft "github.com/hashicorp/raft"
	"github.com/seaweedfs/raft"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/topology"
)

// RaftServerOption Raft 服务器配置选项
// 包含启动 Raft 服务器所需的所有参数
type RaftServerOption struct {
	// GrpcDialOption gRPC 拨号选项，用于节点间通信
	// 可配置 TLS、连接池、超时等参数
	GrpcDialOption grpc.DialOption

	// Peers 集群中的其他节点地址映射
	// key: 节点名称（如 "master1"）
	// value: 节点服务器地址（如 "192.168.1.10:9333"）
	Peers map[string]pb.ServerAddress

	// ServerAddr 当前节点的服务器地址
	// 例如: "192.168.1.10:9333"
	ServerAddr pb.ServerAddress

	// DataDir Raft 数据目录
	// 存储日志、快照、配置等持久化数据
	// 例如: "/data/seaweedfs/master"
	DataDir string

	// Topo 集群拓扑结构引用
	// Raft 状态机操作的主要对象
	Topo *topology.Topology

	// RaftResumeState 是否恢复 Raft 状态
	// true: 从持久化数据恢复（保留配置和快照）
	// false: 清空所有持久化数据，重新初始化
	RaftResumeState bool

	// HeartbeatInterval 心跳间隔
	// Leader 发送心跳到 Follower 的时间间隔
	// 默认: 500ms，实际会加入随机抖动（0.25 倍）
	HeartbeatInterval time.Duration

	// ElectionTimeout 选举超时时间
	// Follower 在此时间内未收到心跳则发起选举
	// 默认: 1000ms
	ElectionTimeout time.Duration

	// RaftBootstrap 是否引导新集群
	// true: 作为第一个节点初始化新集群
	// false: 加入现有集群
	RaftBootstrap bool
}

// RaftServer Raft 服务器实例
// 封装原生 SeaweedFS Raft 或 HashiCorp Raft 实现
//
// 支持两种 Raft 实现:
//   - raftServer: 原生 SeaweedFS Raft（github.com/seaweedfs/raft）
//   - RaftHashicorp: HashiCorp Raft（github.com/hashicorp/raft）
//
// 两种实现互斥，同一时间只能使用一种
type RaftServer struct {
	// peers 初始集群节点列表
	// 用于加入集群时的初始连接
	peers map[string]pb.ServerAddress

	// raftServer 原生 Raft 服务器实例
	// 使用 github.com/seaweedfs/raft 实现
	raftServer raft.Server

	// RaftHashicorp HashiCorp Raft 实例
	// 使用 github.com/hashicorp/raft 实现
	// 与 raftServer 互斥，只能二选一
	RaftHashicorp *hashicorpRaft.Raft

	// TransportManager gRPC 传输管理器
	// 用于 HashiCorp Raft 的节点间通信
	TransportManager *transport.Manager

	// dataDir Raft 数据目录
	// 存储日志、快照、配置
	dataDir string

	// serverAddr 当前节点地址
	serverAddr pb.ServerAddress

	// topo 集群拓扑结构
	// Raft 状态机操作的主要对象
	topo *topology.Topology

	// GrpcServer gRPC 服务器
	// 提供 Raft RPC 接口
	*raft.GrpcServer
}

// StateMachine Raft 状态机实现
// 负责执行 Raft 日志中的命令，维护集群状态
//
// 实现接口:
//   - raft.StateMachine: SeaweedFS 原生 Raft 接口
//   - hashicorpRaft.FSM: HashiCorp Raft 接口
//
// 状态机职责:
//   - 执行日志命令（Apply）
//   - 生成快照（Snapshot）
//   - 恢复快照（Restore）
//   - 持久化状态（Save）
//   - 恢复状态（Recovery）
//
// 当前状态机管理的状态:
//   - MaxVolumeId: 当前最大卷 ID
//   - 集群拓扑结构（通过 topo 引用）
type StateMachine struct {
	raft.StateMachine
	// topo 集群拓扑结构
	// 状态机所有操作都针对此对象
	topo *topology.Topology
}

// 编译时检查：确保 StateMachine 实现了 hashicorpRaft.FSM 接口
var _ hashicorpRaft.FSM = &StateMachine{}

// Save 保存 Raft 状态机的当前状态
// 用于原生 SeaweedFS Raft 的持久化机制
//
// 功能:
//   - 将状态机的当前状态序列化为 JSON
//   - 返回的字节数组将被持久化到磁盘
//
// 当前保存的状态:
//   - MaxVolumeId: 当前最大卷 ID
//
// 调用时机:
//   - 生成快照时
//   - 定期持久化状态时
//
// 返回:
//   - []byte: 序列化后的状态数据（JSON 格式）
//   - error: 序列化错误
func (s StateMachine) Save() ([]byte, error) {
	// 构建状态命令对象
	// 目前只保存 MaxVolumeId
	state := topology.MaxVolumeIdCommand{
		MaxVolumeId: s.topo.GetMaxVolumeId(),
	}
	glog.V(1).Infof("Save raft state %+v", state)

	// 序列化为 JSON
	return json.Marshal(state)
}

// Recovery 从持久化数据恢复 Raft 状态机
// 用于原生 SeaweedFS Raft 的状态恢复
//
// 功能:
//   - 从 JSON 数据反序列化状态
//   - 更新拓扑结构中的 MaxVolumeId
//
// 恢复的状态:
//   - MaxVolumeId: 恢复最大卷 ID
//
// 调用时机:
//   - 服务器启动时加载快照
//   - 从 Follower 同步快照时
//
// 参数:
//   - data: 序列化的状态数据（JSON 格式）
//
// 返回:
//   - error: 反序列化错误或恢复失败
func (s StateMachine) Recovery(data []byte) error {
	// 反序列化状态命令
	state := topology.MaxVolumeIdCommand{}
	err := json.Unmarshal(data, &state)
	if err != nil {
		return err
	}
	glog.V(1).Infof("Recovery raft state %+v", state)

	// 更新拓扑结构中的 MaxVolumeId
	// UpAdjustMaxVolumeId 只会向上调整，不会向下调整
	s.topo.UpAdjustMaxVolumeId(state.MaxVolumeId)
	return nil
}

// Apply 应用 Raft 日志到状态机
// 实现 hashicorpRaft.FSM 接口，用于 HashiCorp Raft
//
// 功能:
//   - 从 Raft 日志中提取命令
//   - 执行命令，更新状态机
//   - 所有节点按相同顺序应用相同的日志，保证一致性
//
// Raft 工作原理:
//   1. Leader 接收客户端请求（如分配新卷）
//   2. Leader 将命令写入本地日志
//   3. Leader 复制日志到 Follower
//   4. 大多数节点确认后，日志被提交（Committed）
//   5. 所有节点调用 Apply() 执行已提交的日志
//
// 当前支持的命令:
//   - MaxVolumeIdCommand: 更新最大卷 ID
//
// 参数:
//   - l: Raft 日志条目，包含命令数据和元数据
//
// 返回:
//   - interface{}: 命令执行结果（nil 表示成功，error 表示失败）
func (s *StateMachine) Apply(l *hashicorpRaft.Log) interface{} {
	// 记录应用前的 MaxVolumeId，用于日志对比
	before := s.topo.GetMaxVolumeId()

	// 从日志中反序列化命令
	state := topology.MaxVolumeIdCommand{}
	err := json.Unmarshal(l.Data, &state)
	if err != nil {
		return err
	}

	// 应用命令：更新 MaxVolumeId
	// 只会向上调整，确保 ID 单调递增
	s.topo.UpAdjustMaxVolumeId(state.MaxVolumeId)

	// 记录状态变化
	glog.V(1).Infoln("max volume id", before, "==>", s.topo.GetMaxVolumeId())
	return nil
}

// Snapshot 生成 Raft 状态机快照
// 实现 hashicorpRaft.FSM 接口
//
// 功能:
//   - 创建状态机的快照对象
//   - 快照用于加速新节点加入和故障恢复
//
// 快照机制:
//   - 定期生成快照，压缩历史日志
//   - 新节点加入时，先恢复快照，再应用增量日志
//   - 减少存储空间和恢复时间
//
// 快照内容:
//   - MaxVolumeId: 当前最大卷 ID
//
// 触发时机:
//   - 日志数量达到阈值
//   - 手动触发快照
//   - 新节点加入需要快照
//
// 返回:
//   - hashicorpRaft.FSMSnapshot: 快照对象
//   - error: 生成快照失败
func (s *StateMachine) Snapshot() (hashicorpRaft.FSMSnapshot, error) {
	// 创建快照对象
	// MaxVolumeIdCommand 实现了 FSMSnapshot 接口
	return &topology.MaxVolumeIdCommand{
		MaxVolumeId: s.topo.GetMaxVolumeId(),
	}, nil
}

// Restore 从快照恢复 Raft 状态机
// 实现 hashicorpRaft.FSM 接口
//
// 功能:
//   - 从快照数据流恢复状态
//   - 替换当前状态机的全部状态
//
// 恢复场景:
//   - 新节点加入集群，从 Leader 获取快照
//   - 节点重启，从本地快照恢复
//   - Follower 落后太多，直接恢复快照而不是重放日志
//
// 参数:
//   - r: 快照数据流（ReadCloser）
//
// 返回:
//   - error: 恢复失败
func (s *StateMachine) Restore(r io.ReadCloser) error {
	// 读取完整的快照数据
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	// 使用 Recovery() 方法恢复状态
	if err := s.Recovery(b); err != nil {
		return err
	}
	return nil
}

// NewRaftServer 创建并启动原生 SeaweedFS Raft 服务器
// 用于 Master Server 的高可用性实现
//
// 功能:
//   - 初始化 Raft 服务器实例
//   - 配置心跳和选举超时
//   - 加载快照恢复状态
//   - 同步集群成员列表
//   - 启动 gRPC 服务
//
// 初始化流程:
//   【阶段 1：创建实例和配置】
//   1. 创建 RaftServer 实例
//   2. 设置日志级别
//   3. 注册 Raft 命令（MaxVolumeIdCommand）
//   4. 创建 gRPC 传输器
//
//   【阶段 2：清理持久化数据】
//   5. 清理旧的日志文件（避免节点可提升为 Leader）
//   6. 根据 RaftResumeState 决定是否清理配置和快照
//   7. 创建快照目录
//
//   【阶段 3：启动 Raft 服务】
//   8. 创建状态机实例
//   9. 创建 Raft Server
//   10. 配置心跳间隔（加入随机抖动）
//   11. 配置选举超时
//   12. 加载快照恢复状态
//   13. 启动 Raft 服务
//
//   【阶段 4：同步集群成员】
//   14. 添加配置中的所有 Peer
//   15. 移除已删除的 Peer
//   16. 创建 gRPC Server
//   17. 输出当前 Leader 信息
//
// 参数:
//   - option: Raft 服务器配置选项
//
// 返回:
//   - *RaftServer: Raft 服务器实例
//   - error: 初始化失败错误
//
// 重要配置:
//   - HeartbeatInterval: 心跳间隔，默认 500ms，实际会加入 0-25% 的随机抖动
//   - ElectionTimeout: 选举超时，默认 1000ms
//   - RaftResumeState: 是否保留持久化状态
//
// 持久化数据目录:
//   - log/: Raft 日志（总是清空）
//   - conf/: 集群配置（RaftResumeState=false 时清空）
//   - snapshot/: 状态快照（RaftResumeState=false 时清空）
func NewRaftServer(option *RaftServerOption) (*RaftServer, error) {
	// 【阶段 1：创建实例和配置】
	// 创建 RaftServer 实例
	s := &RaftServer{
		peers:      option.Peers,
		serverAddr: option.ServerAddr,
		dataDir:    option.DataDir,
		topo:       option.Topo,
	}

	// 设置日志级别
	// glog.V(4) 时启用详细的 Raft 日志
	if glog.V(4) {
		raft.SetLogLevel(2)
	}

	// 注册 Raft 命令
	// MaxVolumeIdCommand 用于同步最大卷 ID
	raft.RegisterCommand(&topology.MaxVolumeIdCommand{})

	var err error
	// 创建 gRPC 传输器，用于节点间通信
	transporter := raft.NewGrpcTransporter(option.GrpcDialOption)
	glog.V(0).Infof("Starting RaftServer with %v", option.ServerAddr)

	// 【阶段 2：清理持久化数据】
	// 总是清理旧的日志文件
	// 避免节点在重启后仍然认为自己是可提升的（promotable）
	os.RemoveAll(path.Join(s.dataDir, "log"))

	if !option.RaftResumeState {
		// 如果不恢复状态，清理所有持久化元数据
		// conf/: 集群配置（节点列表、任期等）
		// snapshot/: 状态快照
		os.RemoveAll(path.Join(s.dataDir, "conf"))
		os.RemoveAll(path.Join(s.dataDir, "snapshot"))
	}

	// 确保快照目录存在
	if err := os.MkdirAll(path.Join(s.dataDir, "snapshot"), os.ModePerm); err != nil {
		return nil, err
	}

	// 【阶段 3：启动 Raft 服务】
	// 创建状态机实例
	stateMachine := StateMachine{topo: option.Topo}

	// 创建 Raft Server
	// 参数:
	//   - name: 节点名称（使用服务器地址）
	//   - dataDir: 数据目录
	//   - transporter: 传输器
	//   - stateMachine: 状态机
	//   - context: 上下文（拓扑结构）
	//   - connectionString: gRPC 连接地址
	s.raftServer, err = raft.NewServer(string(s.serverAddr), s.dataDir, transporter, stateMachine, option.Topo, s.serverAddr.ToGrpcAddress())
	if err != nil {
		glog.V(0).Infoln(err)
		return nil, err
	}

	// 配置心跳间隔，加入随机抖动（0-25%）
	// 抖动的作用：避免所有节点同时发送心跳导致网络拥塞
	// 例如：HeartbeatInterval=500ms，实际间隔为 500ms ~ 625ms
	heartbeatInterval := time.Duration(float64(option.HeartbeatInterval) * (rand.Float64()*0.25 + 1))
	s.raftServer.SetHeartbeatInterval(heartbeatInterval)

	// 配置选举超时
	// Follower 在此时间内未收到心跳则发起选举
	s.raftServer.SetElectionTimeout(option.ElectionTimeout)

	// 加载快照恢复状态
	// 如果 RaftResumeState=true，会恢复之前的状态
	if err := s.raftServer.LoadSnapshot(); err != nil {
		return nil, err
	}

	// 启动 Raft 服务
	if err := s.raftServer.Start(); err != nil {
		return nil, err
	}

	// 【阶段 4：同步集群成员】
	// 添加配置中的所有 Peer
	for name, peer := range s.peers {
		if err := s.raftServer.AddPeer(name, peer.ToGrpcAddress()); err != nil {
			return nil, err
		}
	}

	// 移除已删除的 Peer
	// 对比当前 Raft 中的 Peer 和配置中的 Peer
	// 将不在配置中的 Peer 移除
	for existsPeerName := range s.raftServer.Peers() {
		if existingPeer, found := s.peers[existsPeerName]; !found {
			if err := s.raftServer.RemovePeer(existsPeerName); err != nil {
				glog.V(0).Infoln(err)
				return nil, err
			} else {
				glog.V(0).Infof("removing old peer: %s", existingPeer)
			}
		}
	}

	// 创建 gRPC Server，提供 Raft RPC 接口
	s.GrpcServer = raft.NewGrpcServer(s.raftServer)

	// 输出当前集群的 Leader
	glog.V(0).Infof("current cluster leader: %v", s.raftServer.Leader())

	return s, nil
}

// Peers 返回集群中所有节点的名称列表
// 兼容原生 Raft 和 HashiCorp Raft 两种实现
//
// 功能:
//   - 从 Raft 实例中获取所有节点信息
//   - 提取节点名称返回
//
// 支持的 Raft 实现:
//   - 原生 SeaweedFS Raft (raftServer)
//   - HashiCorp Raft (RaftHashicorp)
//
// 返回:
//   - members: 节点名称列表，例如: ["master1", "master2", "master3"]
//
// 用途:
//   - 集群状态查询 API
//   - 监控集群成员
//   - 健康检查
func (s *RaftServer) Peers() (members []string) {
	if s.raftServer != nil {
		// 原生 SeaweedFS Raft 实现
		peers := s.raftServer.Peers()
		for _, p := range peers {
			// 提取每个节点的名称
			members = append(members, p.Name)
		}
	} else if s.RaftHashicorp != nil {
		// HashiCorp Raft 实现
		cfg := s.RaftHashicorp.GetConfiguration()
		for _, p := range cfg.Configuration().Servers {
			// 提取每个节点的 ID（作为名称）
			members = append(members, string(p.ID))
		}
	}
	return
}

// DoJoinCommand 执行加入集群命令
// 用于引导新集群或加入现有集群
//
// 功能:
//   - 发送 Join 命令到 Raft 集群
//   - 将当前节点添加到集群成员列表
//
// 使用场景:
//   - 初始化新集群时，第一个节点执行此命令
//   - 新节点加入现有集群时执行
//
// Join 命令内容:
//   - Name: 节点名称（使用服务器地址）
//   - ConnectionString: 节点的 gRPC 连接地址
//
// 注意:
//   - 只在使用原生 SeaweedFS Raft 时有效
//   - HashiCorp Raft 有不同的加入机制
//   - 失败时只记录错误，不会中断服务器启动
func (s *RaftServer) DoJoinCommand() {
	glog.V(0).Infoln("Initializing new cluster")

	// 发送 Join 命令到 Raft 集群
	// DefaultJoinCommand 是默认的加入命令实现
	if _, err := s.raftServer.Do(&raft.DefaultJoinCommand{
		Name:             s.raftServer.Name(),           // 节点名称
		ConnectionString: s.serverAddr.ToGrpcAddress(),  // gRPC 连接地址
	}); err != nil {
		// 失败时记录错误
		// 不返回错误，因为可能是重复加入等非致命错误
		glog.Errorf("fail to send join command: %v", err)
	}
}
