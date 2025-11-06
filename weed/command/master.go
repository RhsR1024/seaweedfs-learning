// Package command 包含 Master Server 的启动命令实现
// Master Server 是 SeaweedFS 的核心组件，负责：
// 1. 管理卷（Volume）的分配和位置映射
// 2. 生成文件 ID（包含序列号）
// 3. 维护集群拓扑结构（数据中心、机架、节点）
// 4. 通过 Raft 协议实现高可用和一致性
package command

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"path"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util/version"

	hashicorpRaft "github.com/hashicorp/raft" // Hashicorp Raft 实现（可选）

	"slices"

	"github.com/gorilla/mux"                   // HTTP 路由
	"github.com/seaweedfs/raft/protobuf"       // SeaweedFS 自己的 Raft 实现
	"github.com/spf13/viper"                   // 配置管理
	"google.golang.org/grpc/reflection"        // gRPC 反射服务

	stats_collect "github.com/seaweedfs/seaweedfs/weed/stats" // 指标收集

	"github.com/seaweedfs/seaweedfs/weed/util/grace" // 优雅关闭

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	weed_server "github.com/seaweedfs/seaweedfs/weed/server"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var (
	// m 全局的 Master 配置选项
	m MasterOptions
)

const (
	// raftJoinCheckDelay 检查是否应该加入 Raft 集群之前的延迟
	// 给其他节点一些启动时间，避免过早做出决策
	raftJoinCheckDelay = 1500 * time.Millisecond // delay before checking if we should join a raft cluster
)

// MasterOptions Master Server 的配置选项
// 包含网络、存储、复制、监控等各方面的配置
type MasterOptions struct {
	port                       *int           // HTTP 服务端口，默认 9333
	portGrpc                   *int           // gRPC 服务端口，默认 HTTP端口+10000
	ip                         *string        // Master 的 IP 地址，用作标识符
	ipBind                     *string        // 绑定的 IP 地址（可以与 ip 不同，用于多网卡场景）
	metaFolder                 *string        // 元数据存储目录
	peers                      *string        // 所有 Master 节点的列表（用于 Raft 集群）
	mastersDeprecated          *string        // deprecated, for backward compatibility in master.follower
	volumeSizeLimitMB          *uint          // 卷大小限制（MB），超过此大小停止写入
	volumePreallocate          *bool          // 是否预分配磁盘空间
	maxParallelVacuumPerServer *int           // 每个 Volume Server 同时进行垃圾回收的最大卷数
	// pulseSeconds       *int                // 心跳间隔秒数（已废弃）
	defaultReplication *string        // 默认的复制策略（如 "001" 表示 1 个副本）
	garbageThreshold   *float64       // 垃圾回收阈值（0.3 表示 30% 空间为垃圾时触发）
	whiteList          *string        // IP 白名单，只有这些 IP 可以写入
	disableHttp        *bool          // 是否禁用 HTTP，只允许 gRPC
	metricsAddress     *string        // Prometheus 网关地址
	metricsIntervalSec *int           // 指标推送间隔（秒）
	raftResumeState    *bool          // 启动时是否恢复之前的状态
	metricsHttpPort    *int           // Prometheus 指标 HTTP 端口
	metricsHttpIp      *string        // Prometheus 指标监听 IP
	heartbeatInterval  *time.Duration // Master 之间的心跳间隔
	electionTimeout    *time.Duration // Raft 选举超时时间
	raftHashicorp      *bool          // 是否使用 Hashicorp Raft 实现
	raftBootstrap      *bool          // 是否启动 Raft 集群（第一次启动时需要）
	telemetryUrl       *string        // 遥测数据上报 URL
	telemetryEnabled   *bool          // 是否启用遥测上报
}

// init 初始化 Master 命令的参数
// 这里定义了所有命令行参数的默认值和说明
func init() {
	cmdMaster.Run = runMaster // break init cycle  // 设置命令的执行函数
	m.port = cmdMaster.Flag.Int("port", 9333, "http listen port")
	m.portGrpc = cmdMaster.Flag.Int("port.grpc", 0, "grpc listen port")
	m.ip = cmdMaster.Flag.String("ip", util.DetectedHostAddress(), "master <ip>|<server> address, also used as identifier")
	m.ipBind = cmdMaster.Flag.String("ip.bind", "", "ip address to bind to. If empty, default to same as -ip option.")
	m.metaFolder = cmdMaster.Flag.String("mdir", os.TempDir(), "data directory to store meta data")
	// peers 参数很重要：
	// - 多 Master 模式：列出所有 Master 节点，如 "127.0.0.1:9093,127.0.0.1:9094,127.0.0.1:9095"
	// - 单 Master 模式：使用 "none" 跳过 Raft 仲裁，立即启动
	m.peers = cmdMaster.Flag.String("peers", "", "all master nodes in comma separated ip:port list, example: 127.0.0.1:9093,127.0.0.1:9094,127.0.0.1:9095; use 'none' for single-master mode")
	m.volumeSizeLimitMB = cmdMaster.Flag.Uint("volumeSizeLimitMB", 30*1000, "Master stops directing writes to oversized volumes.")
	m.volumePreallocate = cmdMaster.Flag.Bool("volumePreallocate", false, "Preallocate disk space for volumes.")
	m.maxParallelVacuumPerServer = cmdMaster.Flag.Int("maxParallelVacuumPerServer", 1, "maximum number of volumes to vacuum in parallel per volume server")
	// m.pulseSeconds = cmdMaster.Flag.Int("pulseSeconds", 5, "number of seconds between heartbeats")
	m.defaultReplication = cmdMaster.Flag.String("defaultReplication", "", "Default replication type if not specified.")
	m.garbageThreshold = cmdMaster.Flag.Float64("garbageThreshold", 0.3, "threshold to vacuum and reclaim spaces")
	m.whiteList = cmdMaster.Flag.String("whiteList", "", "comma separated Ip addresses having write permission. No limit if empty.")
	m.disableHttp = cmdMaster.Flag.Bool("disableHttp", false, "disable http requests, only gRPC operations are allowed.")
	m.metricsAddress = cmdMaster.Flag.String("metrics.address", "", "Prometheus gateway address <host>:<port>")
	m.metricsIntervalSec = cmdMaster.Flag.Int("metrics.intervalSeconds", 15, "Prometheus push interval in seconds")
	m.metricsHttpPort = cmdMaster.Flag.Int("metricsPort", 0, "Prometheus metrics listen port")
	m.metricsHttpIp = cmdMaster.Flag.String("metricsIp", "", "metrics listen ip. If empty, default to same as -ip.bind option.")
	m.raftResumeState = cmdMaster.Flag.Bool("resumeState", false, "resume previous state on start master server")
	m.heartbeatInterval = cmdMaster.Flag.Duration("heartbeatInterval", 300*time.Millisecond, "heartbeat interval of master servers, and will be randomly multiplied by [1, 1.25)")
	m.electionTimeout = cmdMaster.Flag.Duration("electionTimeout", 10*time.Second, "election timeout of master servers")
	m.raftHashicorp = cmdMaster.Flag.Bool("raftHashicorp", false, "use hashicorp raft")
	m.raftBootstrap = cmdMaster.Flag.Bool("raftBootstrap", false, "Whether to bootstrap the Raft cluster")
	m.telemetryUrl = cmdMaster.Flag.String("telemetry.url", "https://telemetry.seaweedfs.com/api/collect", "telemetry server URL to send usage statistics")
	m.telemetryEnabled = cmdMaster.Flag.Bool("telemetry", false, "enable telemetry reporting")
}

// cmdMaster Master 命令的定义
// 包含命令的用法、简短描述和详细说明
var cmdMaster = &Command{
	UsageLine: "master -port=9333",
	Short:     "start a master server",
	Long: `start a master server to provide volume=>location mapping service and sequence number of file ids

	The configuration file "security.toml" is read from ".", "$HOME/.seaweedfs/", "/usr/local/etc/seaweedfs/", or "/etc/seaweedfs/", in that order.

	The example security.toml configuration file can be generated by "weed scaffold -config=security"

	For single-master setups, use -peers=none to skip Raft quorum wait and enable instant startup.
	This is ideal for development or standalone deployments.

  `,
}

var (
	// masterCpuProfile CPU 性能分析输出文件路径
	masterCpuProfile = cmdMaster.Flag.String("cpuprofile", "", "cpu profile output file")
	// masterMemProfile 内存性能分析输出文件路径
	masterMemProfile = cmdMaster.Flag.String("memprofile", "", "memory profile output file")
)

// runMaster Master 命令的执行函数
// 负责加载配置、检查环境、启动指标收集，然后调用 startMaster
func runMaster(cmd *Command, args []string) bool {

	// 加载安全配置（TLS 证书等）
	util.LoadSecurityConfiguration()
	// 加载 master 的配置文件（从 TOML 文件）
	util.LoadConfiguration("master", false)

	// bind viper configuration to command line flags
	// 如果配置文件中指定了 mdir，覆盖命令行参数
	if v := util.GetViper().GetString("master.mdir"); v != "" {
		*m.metaFolder = v
	}

	// 设置性能分析（CPU 和内存），用于性能调优
	grace.SetupProfiling(*masterCpuProfile, *masterMemProfile)

	// 检查元数据目录是否存在，不存在则创建
	parent, _ := util.FullPath(*m.metaFolder).DirAndName()
	if util.FileExists(string(parent)) && !util.FileExists(*m.metaFolder) {
		os.MkdirAll(*m.metaFolder, 0755)
	}
	// 测试元数据目录是否可写，这是启动的前提条件
	if err := util.TestFolderWritable(util.ResolvePath(*m.metaFolder)); err != nil {
		glog.Fatalf("Check Meta Folder (-mdir) Writable %s : %s", *m.metaFolder, err)
	}

	// 解析 IP 白名单
	masterWhiteList := util.StringSplit(*m.whiteList, ",")
	// 检查卷大小限制是否合理（不能超过 30TB）
	if *m.volumeSizeLimitMB > util.VolumeSizeLimitGB*1000 {
		glog.Fatalf("volumeSizeLimitMB should be smaller than 30000")
	}

	// 确定指标服务器的监听 IP
	// 优先级：metricsHttpIp > ipBind > ip
	switch {
	case *m.metricsHttpIp != "":
		// noting to do, use m.metricsHttpIp
	case *m.ipBind != "":
		*m.metricsHttpIp = *m.ipBind
	case *m.ip != "":
		*m.metricsHttpIp = *m.ip
	}
	// 启动 Prometheus 指标服务器（HTTP endpoint）
	go stats_collect.StartMetricsServer(*m.metricsHttpIp, *m.metricsHttpPort)
	// 定期向 Prometheus Gateway 推送指标
	go stats_collect.LoopPushingMetric("masterServer", util.JoinHostPort(*m.ip, *m.port), *m.metricsAddress, *m.metricsIntervalSec)

	// 启动 Master Server
	startMaster(m, masterWhiteList)
	return true
}

// startMaster 启动 Master Server 的核心逻辑
// 包括：
// 1. 创建 HTTP 和 gRPC 服务器
// 2. 启动 Raft 一致性协议
// 3. 注册 API 路由
// 4. 设置优雅关闭
func startMaster(masterOption MasterOptions, masterWhiteList []string) {

	// 加载后端存储配置（如 S3、Azure 等远程存储）
	backend.LoadConfiguration(util.GetViper())

	// 如果没有指定 gRPC 端口，默认为 HTTP 端口 + 10000
	// 例如：HTTP 9333 -> gRPC 19333
	if *masterOption.portGrpc == 0 {
		*masterOption.portGrpc = 10000 + *masterOption.port
	}
	// 如果没有指定绑定 IP，使用主 IP
	if *masterOption.ipBind == "" {
		*masterOption.ipBind = *masterOption.ip
	}

	// 检查并规范化 peers 配置，返回本机地址和所有 peer 地址
	myMasterAddress, peers := checkPeers(*masterOption.ip, *masterOption.port, *masterOption.portGrpc, *masterOption.peers)

	// 构建 Master 节点的地址映射表（用于 Raft 通信）
	masterPeers := make(map[string]pb.ServerAddress)
	for _, peer := range peers {
		masterPeers[string(peer)] = peer
	}

	// 创建 HTTP 路由器
	r := mux.NewRouter()
	// 创建 Master Server 实例
	ms := weed_server.NewMasterServer(r, masterOption.toMasterOption(masterWhiteList), masterPeers)
	listeningAddress := util.JoinHostPort(*masterOption.ipBind, *masterOption.port)
	glog.V(0).Infof("Start Seaweed Master %s at %s", version.Version(), listeningAddress)

	// 创建 HTTP 监听器（包括外部网络和本地 Unix socket）
	masterListener, masterLocalListener, e := util.NewIpAndLocalListeners(*masterOption.ipBind, *masterOption.port, 0)
	if e != nil {
		glog.Fatalf("Master startup error: %v", e)
	}

	// start raftServer
	// 准备 Raft 数据目录（每个 Master 端口对应一个目录）
	metaDir := path.Join(*masterOption.metaFolder, fmt.Sprintf("m%d", *masterOption.port))

	// 判断是否为单 Master 模式（peers=none）
	isSingleMaster := isSingleMasterMode(*masterOption.peers)

	// 配置 Raft Server 选项
	raftServerOption := &weed_server.RaftServerOption{
		GrpcDialOption:    security.LoadClientTLS(util.GetViper(), "grpc.master"), // TLS 配置
		Peers:             masterPeers,                                             // 所有 Master 节点
		ServerAddr:        myMasterAddress,                                         // 本机地址
		DataDir:           util.ResolvePath(metaDir),                               // Raft 日志存储目录
		Topo:              ms.Topo,                                                 // 拓扑结构对象
		RaftResumeState:   *masterOption.raftResumeState,                           // 是否恢复之前状态
		HeartbeatInterval: *masterOption.heartbeatInterval,                         // 心跳间隔
		ElectionTimeout:   *masterOption.electionTimeout,                           // 选举超时
		RaftBootstrap:     *masterOption.raftBootstrap,                             // 是否引导集群
	}
	var raftServer *weed_server.RaftServer
	var err error
	// 根据配置选择 Raft 实现：Hashicorp Raft 或 SeaweedFS 自己的 Raft
	if *masterOption.raftHashicorp {
		if raftServer, err = weed_server.NewHashicorpRaftServer(raftServerOption); err != nil {
			glog.Fatalf("NewHashicorpRaftServer: %s", err)
		}
	} else {
		// 使用 SeaweedFS 自己的 Raft 实现
		raftServer, err = weed_server.NewRaftServer(raftServerOption)
		if raftServer == nil {
			glog.Fatalf("please verify %s is writable, see https://github.com/seaweedfs/seaweedfs/issues/717: %s", *masterOption.metaFolder, err)
		}
		// For single-master mode, initialize cluster immediately without waiting
		// 单 Master 模式：立即初始化集群，无需等待仲裁
		if isSingleMaster {
			glog.V(0).Infof("Single-master mode: initializing cluster immediately")
			raftServer.DoJoinCommand()
		}
	}
	// 将 Raft Server 设置到 Master Server 中
	ms.SetRaftServer(raftServer)
	// 注册 Raft 集群状态相关的 HTTP API
	r.HandleFunc("/cluster/status", raftServer.StatusHandler).Methods(http.MethodGet, http.MethodHead)
	r.HandleFunc("/cluster/healthz", raftServer.HealthzHandler).Methods(http.MethodGet, http.MethodHead)
	if *masterOption.raftHashicorp {
		r.HandleFunc("/raft/stats", raftServer.StatsRaftHandler).Methods(http.MethodGet)
	}

	// starting grpc server
	// 启动 gRPC 服务器
	grpcPort := *masterOption.portGrpc
	grpcL, grpcLocalL, err := util.NewIpAndLocalListeners(*masterOption.ipBind, grpcPort, 0)
	if err != nil {
		glog.Fatalf("master failed to listen on grpc port %d: %v", grpcPort, err)
	}
	// 创建 gRPC 服务器（带 TLS 配置）
	grpcS := pb.NewGrpcServer(security.LoadServerTLS(util.GetViper(), "grpc.master"))
	// 注册 Master 的 gRPC 服务
	master_pb.RegisterSeaweedServer(grpcS, ms)
	// 注册 Raft 的 gRPC 服务（用于节点间通信）
	if *masterOption.raftHashicorp {
		raftServer.TransportManager.Register(grpcS)
	} else {
		protobuf.RegisterRaftServer(grpcS, raftServer)
	}
	// 注册 gRPC 反射服务（方便调试和工具使用）
	reflection.Register(grpcS)
	glog.V(0).Infof("Start Seaweed Master %s grpc server at %s:%d", version.Version(), *masterOption.ipBind, grpcPort)
	// 启动 gRPC 服务器（本地和网络）
	if grpcLocalL != nil {
		go grpcS.Serve(grpcLocalL)
	}
	go grpcS.Serve(grpcL)

	// For multi-master mode with non-Hashicorp raft, wait and check if we should join
	// 多 Master 模式（非 Hashicorp Raft）：延迟检查是否应该加入集群
	if !*masterOption.raftHashicorp && !isSingleMaster {
		go func() {
			// 等待一段时间，让其他节点有机会启动
			time.Sleep(raftJoinCheckDelay)

			ms.Topo.RaftServerAccessLock.RLock()
			// 检查是否是空的 Master（没有 Leader，日志为空）
			isEmptyMaster := ms.Topo.RaftServer.Leader() == "" && ms.Topo.RaftServer.IsLogEmpty()
			// 如果是第一个节点，且找不到其他 Leader，则初始化集群
			if isEmptyMaster && isTheFirstOne(myMasterAddress, peers) && ms.MasterClient.FindLeaderFromOtherPeers(myMasterAddress) == "" {
				raftServer.DoJoinCommand()
			}
			ms.Topo.RaftServerAccessLock.RUnlock()
		}()
	}

	// 保持与其他 Master 的连接（用于通信和同步）
	go ms.MasterClient.KeepConnectedToMaster(context.Background())

	// start http server
	// 启动 HTTP 服务器
	var (
		clientCertFile,
		certFile,
		keyFile string
	)
	useTLS := false
	useMTLS := false

	// 检查是否配置了 HTTPS
	if viper.GetString("https.master.key") != "" {
		useTLS = true
		certFile = viper.GetString("https.master.cert")
		keyFile = viper.GetString("https.master.key")
	}

	// 检查是否配置了双向 TLS（mTLS）
	if viper.GetString("https.master.ca") != "" {
		useMTLS = true
		clientCertFile = viper.GetString("https.master.ca")
	}

	// 启动本地 HTTP 服务器（Unix socket）
	if masterLocalListener != nil {
		go newHttpServer(r, nil).Serve(masterLocalListener)
	}

	var tlsConfig *tls.Config
	// 配置 mTLS
	if useMTLS {
		tlsConfig = security.LoadClientTLSHTTP(clientCertFile)
		security.FixTlsConfig(util.GetViper(), tlsConfig)
	}

	// 启动网络 HTTP 服务器（根据是否使用 TLS）
	if useTLS {
		go newHttpServer(r, tlsConfig).ServeTLS(masterListener, certFile, keyFile)
	} else {
		go newHttpServer(r, nil).Serve(masterListener)
	}

	// 注册优雅关闭回调
	grace.OnInterrupt(ms.Shutdown)
	grace.OnInterrupt(grpcS.Stop)
	// 注册重载回调（如果是 Hashicorp Raft 的 Leader，先转移领导权）
	grace.OnReload(func() {
		if ms.Topo.HashicorpRaft != nil && ms.Topo.HashicorpRaft.State() == hashicorpRaft.Leader {
			ms.Topo.HashicorpRaft.LeadershipTransfer()
		}
	})
	// 阻塞主线程，防止程序退出
	select {}
}

// isSingleMasterMode 判断是否为单 Master 模式
// 单 Master 模式使用 peers=none，无需 Raft 仲裁，立即启动
// 适合开发环境或不需要高可用的场景
func isSingleMasterMode(peers string) bool {
	p := strings.ToLower(strings.TrimSpace(peers))
	return p == "none"
}

// checkPeers 检查并规范化 peers 配置
// 返回值：
//   - masterAddress: 本机的完整地址（IP:Port:GrpcPort）
//   - cleanedPeers: 所有 Master 节点的地址列表（包括本机）
//
// 功能：
// 1. 处理单 Master 模式（peers=none）
// 2. 解析 peers 字符串为地址列表
// 3. 确保本机地址在 peers 列表中
// 4. 验证 Master 数量为奇数（Raft 要求）
func checkPeers(masterIp string, masterPort int, masterGrpcPort int, peers string) (masterAddress pb.ServerAddress, cleanedPeers []pb.ServerAddress) {
	glog.V(0).Infof("current: %s:%d peers:%s", masterIp, masterPort, peers)
	// 构建本机的完整地址
	masterAddress = pb.NewServerAddress(masterIp, masterPort, masterGrpcPort)

	// Handle special case: -peers=none for single-master setup
	// 处理特殊情况：单 Master 模式
	if isSingleMasterMode(peers) {
		glog.V(0).Infof("Running in single-master mode (peers=none), no quorum required")
		cleanedPeers = []pb.ServerAddress{masterAddress}
		return
	}

	peers = strings.TrimSpace(peers)

	// 解析 peers 字符串为地址列表
	cleanedPeers = pb.ServerAddresses(peers).ToAddresses()

	// 检查本机是否在 peers 列表中
	hasSelf := false
	for _, peer := range cleanedPeers {
		if peer.ToHttpAddress() == masterAddress.ToHttpAddress() {
			hasSelf = true
			break
		}
	}

	// 如果本机不在列表中，添加进去
	if !hasSelf {
		cleanedPeers = append(cleanedPeers, masterAddress)
	}
	// Raft 协议要求奇数个节点（2n+1），以保证能够形成多数派
	// 偶数个节点无法有效处理网络分区
	if len(cleanedPeers)%2 == 0 {
		glog.Fatalf("Only odd number of masters are supported: %+v", cleanedPeers)
	}
	return
}

// isTheFirstOne 判断本机是否是 peers 列表中的第一个节点（按字典序）
// 用于决定哪个节点应该初始化 Raft 集群
// 参数：
//   - self: 本机地址
//   - peers: 所有 Master 节点地址列表
//
// 返回值：
//   - true: 本机是第一个节点，应该初始化集群
//   - false: 本机不是第一个节点，应该等待加入
func isTheFirstOne(self pb.ServerAddress, peers []pb.ServerAddress) bool {
	// 按字典序排序所有节点
	slices.SortFunc(peers, func(a, b pb.ServerAddress) int {
		return strings.Compare(string(a), string(b))
	})
	if len(peers) <= 0 {
		return true
	}
	// 检查本机是否是排序后的第一个节点
	return self == peers[0]
}

// toMasterOption 将命令行选项转换为 MasterServer 需要的选项结构
// 这是一个适配器方法，用于桥接命令行参数和服务器内部配置
func (m *MasterOptions) toMasterOption(whiteList []string) *weed_server.MasterOption {
	masterAddress := pb.NewServerAddress(*m.ip, *m.port, *m.portGrpc)
	return &weed_server.MasterOption{
		Master:                     masterAddress,                   // Master 地址
		MetaFolder:                 *m.metaFolder,                   // 元数据目录
		VolumeSizeLimitMB:          uint32(*m.volumeSizeLimitMB),    // 卷大小限制
		VolumePreallocate:          *m.volumePreallocate,            // 是否预分配空间
		MaxParallelVacuumPerServer: *m.maxParallelVacuumPerServer,   // 并发垃圾回收数
		// PulseSeconds:            *m.pulseSeconds,                 // 心跳间隔（已废弃）
		DefaultReplicaPlacement: *m.defaultReplication, // 默认复制策略
		GarbageThreshold:        *m.garbageThreshold,   // 垃圾回收阈值
		WhiteList:               whiteList,             // IP 白名单
		DisableHttp:             *m.disableHttp,        // 是否禁用 HTTP
		MetricsAddress:          *m.metricsAddress,     // Prometheus 地址
		MetricsIntervalSec:      *m.metricsIntervalSec, // 指标推送间隔
		TelemetryUrl:            *m.telemetryUrl,       // 遥测 URL
		TelemetryEnabled:        *m.telemetryEnabled,   // 是否启用遥测
	}
}
