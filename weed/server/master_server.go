// Package weed_server 实现了 SeaweedFS 的核心服务器组件
// 包括 Master 服务器、Volume 服务器、Filer 服务器等
package weed_server

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/telemetry"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/pb"

	"github.com/gorilla/mux"
	hashicorpRaft "github.com/hashicorp/raft"
	"github.com/seaweedfs/raft"
	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/sequence"
	"github.com/seaweedfs/seaweedfs/weed/shell"
	"github.com/seaweedfs/seaweedfs/weed/topology"
	"github.com/seaweedfs/seaweedfs/weed/util"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
	"github.com/seaweedfs/seaweedfs/weed/util/version"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

const (
	// SequencerType 序列号生成器类型配置键
	// 支持 "raft"(默认) 和 "snowflake" 两种类型
	SequencerType        = "master.sequencer.type"
	// SequencerSnowflakeId Snowflake 序列号生成器的节点 ID 配置键
	// 当使用 Snowflake 算法时需要配置唯一的节点 ID
	SequencerSnowflakeId = "master.sequencer.sequencer_snowflake_id"
)

// MasterOption Master 服务器的配置选项
// 包含了 Master 节点运行所需的所有配置参数
type MasterOption struct {
	Master                     pb.ServerAddress // Master 服务器地址 (host:port)
	MetaFolder                 string           // 元数据存储文件夹路径
	VolumeSizeLimitMB          uint32           // 单个 Volume 的大小限制(MB)
	VolumePreallocate          bool             // 是否预分配 Volume 空间
	MaxParallelVacuumPerServer int              // 每个服务器最大并行清理任务数
	// PulseSeconds            int              // 心跳间隔(已废弃)
	DefaultReplicaPlacement string   // 默认副本放置策略 (如 "001" 表示1个数据中心,0个机架,1个副本)
	GarbageThreshold        float64  // 垃圾回收阈值,当删除比例超过此值时触发清理
	WhiteList               []string // IP 白名单列表
	DisableHttp             bool     // 是否禁用 HTTP 接口
	MetricsAddress          string   // Metrics 监控地址
	MetricsIntervalSec      int      // Metrics 采集间隔(秒)
	IsFollower              bool     // 是否为 Follower 节点(不执行管理脚本)
	TelemetryUrl            string   // 遥测数据上报 URL
	TelemetryEnabled        bool     // 是否启用遥测功能
	VolumeGrowthDisabled    bool     // 是否禁用 Volume 自动增长
}

// MasterServer Master 服务器核心结构体
// 负责管理整个 SeaweedFS 集群的元数据、拓扑结构和资源调度
type MasterServer struct {
	master_pb.UnimplementedSeaweedServer // gRPC 服务未实现接口
	option *MasterOption                 // Master 配置选项
	guard  *security.Guard                // 安全守卫,处理认证和授权

	preallocateSize int64 // Volume 预分配大小(字节)

	Topo                    *topology.Topology            // 拓扑结构,管理数据中心、机架、节点等层次关系
	vg                      *topology.VolumeGrowth        // Volume 增长策略管理器
	volumeGrowthRequestChan chan *topology.VolumeGrowRequest // Volume 增长请求通道

	// 客户端通知机制
	// 用于向连接的客户端推送集群状态变更
	clientChansLock sync.RWMutex                                  // 客户端通道映射的读写锁
	clientChans     map[string]chan *master_pb.KeepConnectedResponse // 客户端标识 -> 通知通道映射

	grpcDialOption grpc.DialOption // gRPC 连接选项(包含 TLS 等配置)

	MasterClient *wdclient.MasterClient // Master 客户端,用于与其他 Master 节点通信

	adminLocks *AdminLocks // 管理员锁,用于集群维护操作的互斥控制

	Cluster *cluster.Cluster // 集群信息管理

	// 遥测相关
	telemetryCollector *telemetry.Collector // 遥测数据收集器,用于收集和上报集群使用统计
}

// NewMasterServer 创建并初始化一个新的 Master 服务器实例
// 参数:
//   r: HTTP 路由器,用于注册 RESTful API 端点
//   option: Master 服务器配置选项
//   peers: 集群中其他 Master 节点的地址映射
// 返回:
//   初始化完成的 MasterServer 实例
func NewMasterServer(r *mux.Router, option *MasterOption, peers map[string]pb.ServerAddress) *MasterServer {

	v := util.GetViper()
	// 获取 JWT 签名密钥配置
	signingKey := v.GetString("jwt.signing.key")
	v.SetDefault("jwt.signing.expires_after_seconds", 10)
	expiresAfterSec := v.GetInt("jwt.signing.expires_after_seconds")

	// 获取只读 JWT 签名密钥配置(用于读操作的 token)
	readSigningKey := v.GetString("jwt.signing.read.key")
	v.SetDefault("jwt.signing.read.expires_after_seconds", 60)
	readExpiresAfterSec := v.GetInt("jwt.signing.read.expires_after_seconds")

	// 副本放置策略配置
	// treat_replication_as_minimums: 是否将副本数视为最小值(而非精确值)
	v.SetDefault("master.replication.treat_replication_as_minimums", false)
	replicationAsMin := v.GetBool("master.replication.treat_replication_as_minimums")

	// Volume 增长策略配置
	// 定义不同副本数量下每次增长创建的 Volume 数量
	v.SetDefault("master.volume_growth.copy_1", topology.VolumeGrowStrategy.Copy1Count)
	v.SetDefault("master.volume_growth.copy_2", topology.VolumeGrowStrategy.Copy2Count)
	v.SetDefault("master.volume_growth.copy_3", topology.VolumeGrowStrategy.Copy3Count)
	v.SetDefault("master.volume_growth.copy_other", topology.VolumeGrowStrategy.CopyOtherCount)
	v.SetDefault("master.volume_growth.threshold", topology.VolumeGrowStrategy.Threshold)
	v.SetDefault("master.volume_growth.disable", false)
	option.VolumeGrowthDisabled = v.GetBool("master.volume_growth.disable")

	// 应用 Volume 增长策略配置
	topology.VolumeGrowStrategy.Copy1Count = v.GetUint32("master.volume_growth.copy_1")       // 单副本时每次增长数量
	topology.VolumeGrowStrategy.Copy2Count = v.GetUint32("master.volume_growth.copy_2")       // 双副本时每次增长数量
	topology.VolumeGrowStrategy.Copy3Count = v.GetUint32("master.volume_growth.copy_3")       // 三副本时每次增长数量
	topology.VolumeGrowStrategy.CopyOtherCount = v.GetUint32("master.volume_growth.copy_other") // 其他副本数时每次增长数量
	topology.VolumeGrowStrategy.Threshold = v.GetFloat64("master.volume_growth.threshold")    // 触发增长的阈值
	whiteList := util.StringSplit(v.GetString("guard.white_list"), ",")

	// 计算 Volume 预分配大小
	var preallocateSize int64
	if option.VolumePreallocate {
		preallocateSize = int64(option.VolumeSizeLimitMB) * (1 << 20) // 转换为字节
	}

	// 加载 gRPC 客户端 TLS 配置
	grpcDialOption := security.LoadClientTLS(v, "grpc.master")
	// 创建 MasterServer 实例
	ms := &MasterServer{
		option:                  option,
		preallocateSize:         preallocateSize,
		volumeGrowthRequestChan: make(chan *topology.VolumeGrowRequest, 1<<6), // 缓冲大小为 64
		clientChans:             make(map[string]chan *master_pb.KeepConnectedResponse),
		grpcDialOption:          grpcDialOption,
		MasterClient:            wdclient.NewMasterClient(grpcDialOption, "", cluster.MasterType, option.Master, "", "", *pb.NewServiceDiscoveryFromMap(peers)),
		adminLocks:              NewAdminLocks(),
		Cluster:                 cluster.NewCluster(),
	}

	// 设置集群节点更新回调函数
	ms.MasterClient.SetOnPeerUpdateFn(ms.OnPeerUpdate)

	// 创建序列号生成器(用于生成唯一的文件 ID)
	seq := ms.createSequencer(option)
	if nil == seq {
		glog.Fatalf("create sequencer failed.")
	}
	// 创建拓扑结构
	// 参数: 名称, 序列号生成器, Volume大小限制(字节), 脉冲间隔(秒), 副本数是否为最小值
	ms.Topo = topology.NewTopology("topo", seq, uint64(ms.option.VolumeSizeLimitMB)*1024*1024, 5, replicationAsMin)
	ms.vg = topology.NewDefaultVolumeGrowth()
	glog.V(0).Infoln("Volume Size Limit is", ms.option.VolumeSizeLimitMB, "MB")

	// 初始化遥测收集器(在拓扑创建之后)
	// 用于收集集群使用统计信息并上报
	if option.TelemetryEnabled && option.TelemetryUrl != "" {
		telemetryClient := telemetry.NewClient(option.TelemetryUrl, option.TelemetryEnabled)
		ms.telemetryCollector = telemetry.NewCollector(telemetryClient, ms.Topo, ms.Cluster)
		ms.telemetryCollector.SetMasterServer(ms)

		// 设置版本和操作系统信息
		ms.telemetryCollector.SetVersion(version.VERSION_NUMBER)
		ms.telemetryCollector.SetOS(runtime.GOOS + "/" + runtime.GOARCH)

		// 启动定期遥测数据收集(每24小时)
		ms.telemetryCollector.StartPeriodicCollection(24 * time.Hour)
	}

	// 创建安全守卫,处理 JWT 认证和 IP 白名单
	ms.guard = security.NewGuard(append(ms.option.WhiteList, whiteList...), signingKey, expiresAfterSec, readSigningKey, readExpiresAfterSec)

	// 注册 HTTP 路由处理器
	handleStaticResources2(r) // 处理静态资源(CSS, JS 等)
	r.HandleFunc("/", ms.proxyToLeader(requestIDMiddleware(ms.uiStatusHandler))) // 首页,显示集群状态
	r.HandleFunc("/ui/index.html", requestIDMiddleware(ms.uiStatusHandler))
	if !ms.option.DisableHttp {
		// 文件操作相关 API
		r.HandleFunc("/dir/assign", ms.proxyToLeader(ms.guard.WhiteList(requestIDMiddleware(ms.dirAssignHandler))))     // 分配文件 ID 和存储位置
		r.HandleFunc("/dir/lookup", ms.guard.WhiteList(requestIDMiddleware(ms.dirLookupHandler)))                        // 查找文件存储位置
		r.HandleFunc("/dir/status", ms.proxyToLeader(ms.guard.WhiteList(requestIDMiddleware(ms.dirStatusHandler))))      // 查看目录状态
		// Collection 管理 API
		r.HandleFunc("/col/delete", ms.proxyToLeader(ms.guard.WhiteList(requestIDMiddleware(ms.collectionDeleteHandler)))) // 删除 Collection
		r.HandleFunc("/collection/info", ms.guard.WhiteList(requestIDMiddleware(ms.collectionInfoHandler)))                // 查看 Collection 信息
		// Volume 管理 API
		r.HandleFunc("/vol/grow", ms.proxyToLeader(ms.guard.WhiteList(requestIDMiddleware(ms.volumeGrowHandler))))    // 手动触发 Volume 增长
		r.HandleFunc("/vol/status", ms.proxyToLeader(ms.guard.WhiteList(requestIDMiddleware(ms.volumeStatusHandler)))) // 查看 Volume 状态
		r.HandleFunc("/vol/vacuum", ms.proxyToLeader(ms.guard.WhiteList(requestIDMiddleware(ms.volumeVacuumHandler)))) // 触发 Volume 清理
		// 其他 API
		r.HandleFunc("/submit", ms.guard.WhiteList(requestIDMiddleware(ms.submitFromMasterServerHandler))) // 从 Master 提交文件
		/*
			// 统计信息 API (已注释)
			r.HandleFunc("/stats/health", ms.guard.WhiteList(statsHealthHandler))
			r.HandleFunc("/stats/counter", ms.guard.WhiteList(statsCounterHandler))
			r.HandleFunc("/stats/memory", ms.guard.WhiteList(statsMemoryHandler))
		*/
		r.HandleFunc("/{fileId}", requestIDMiddleware(ms.redirectHandler)) // 文件重定向,将请求转发到实际存储位置
	}

	// 启动可写 Volume 的定期刷新
	// 定期检查各个 Volume 的状态,更新可写 Volume 列表
	ms.Topo.StartRefreshWritableVolumes(
		ms.grpcDialOption,                     // gRPC 连接选项
		ms.option.GarbageThreshold,            // 垃圾回收阈值
		ms.option.MaxParallelVacuumPerServer,  // 每服务器最大并行清理数
		topology.VolumeGrowStrategy.Threshold, // Volume 增长阈值
		ms.preallocateSize,                    // 预分配大小
	)

	// 启动 Volume 增长请求处理协程
	ms.ProcessGrowRequest()

	// 如果不是 Follower 节点,启动管理脚本
	// Follower 节点不执行管理任务,只负责数据同步
	if !option.IsFollower {
		ms.startAdminScripts()
	}

	return ms
}

// SetRaftServer 设置 Raft 服务器实例
// SeaweedFS 支持两种 Raft 实现:
//  1. seaweedfs/raft (自研实现)
//  2. hashicorp/raft (Hashicorp 实现)
// 参数:
//   raftServer: Raft 服务器封装对象
func (ms *MasterServer) SetRaftServer(raftServer *RaftServer) {
	var raftServerName string

	ms.Topo.RaftServerAccessLock.Lock()
	// 使用 SeaweedFS 自研的 Raft 实现
	if raftServer.raftServer != nil {
		ms.Topo.RaftServer = raftServer.raftServer
		// 添加 Leader 变更事件监听器
		ms.Topo.RaftServer.AddEventListener(raft.LeaderChangeEventType, func(e raft.Event) {
			glog.V(0).Infof("leader change event: %+v => %+v", e.PrevValue(), e.Value())
			stats.MasterLeaderChangeCounter.WithLabelValues(fmt.Sprintf("%+v", e.Value())).Inc()
			if ms.Topo.RaftServer.Leader() != "" {
				glog.V(0).Infof("[%s] %s becomes leader.", ms.Topo.RaftServer.Name(), ms.Topo.RaftServer.Leader())
				ms.Topo.LastLeaderChangeTime = time.Now()
			}
		})
		raftServerName = fmt.Sprintf("[%s]", ms.Topo.RaftServer.Name())
	} else if raftServer.RaftHashicorp != nil {
		// 使用 Hashicorp Raft 实现
		ms.Topo.HashicorpRaft = raftServer.RaftHashicorp
		raftServerName = ms.Topo.HashicorpRaft.String()
		ms.Topo.LastLeaderChangeTime = time.Now()
	}
	ms.Topo.RaftServerAccessLock.Unlock()

	// 判断当前节点是否为 Leader
	if ms.Topo.IsLeader() {
		glog.V(0).Infof("%s I am the leader!", raftServerName)
	} else {
		// 如果不是 Leader,获取并记录当前 Leader 信息
		var raftServerLeader string
		ms.Topo.RaftServerAccessLock.RLock()
		if ms.Topo.RaftServer != nil {
			raftServerLeader = ms.Topo.RaftServer.Leader()
		} else if ms.Topo.HashicorpRaft != nil {
			raftServerName = ms.Topo.HashicorpRaft.String()
			raftServerLeaderAddr, _ := ms.Topo.HashicorpRaft.LeaderWithID()
			raftServerLeader = string(raftServerLeaderAddr)
		}
		ms.Topo.RaftServerAccessLock.RUnlock()
		glog.V(0).Infof("%s %s - is the leader.", raftServerName, raftServerLeader)
	}
}

// proxyToLeader 创建一个代理中间件,将写操作请求转发到 Leader 节点
// 在 Raft 集群中,只有 Leader 节点可以处理写操作,Follower 节点需要将写请求转发给 Leader
// 参数:
//   f: 实际的处理函数
// 返回:
//   包装后的处理函数,会在必要时进行请求转发
func (ms *MasterServer) proxyToLeader(f http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// 如果当前节点是 Leader,直接处理请求
		if ms.Topo.IsLeader() {
			f(w, r)
			return
		}

		// 获取当前 Raft Leader 地址
		leaderAddr, _ := ms.Topo.MaybeLeader()
		raftServerLeader := leaderAddr.ToHttpAddress()
		if raftServerLeader == "" {
			// 如果无法获取 Leader 地址,尝试直接处理
			f(w, r)
			return
		}

		// 根据 HTTPS 客户端配置确定协议方案(http/https)
		scheme := util_http.GetGlobalHttpClient().GetHttpScheme()

		targetUrl, err := url.Parse(scheme + "://" + raftServerLeader)
		if err != nil {
			writeJsonError(w, r, http.StatusInternalServerError,
				fmt.Errorf("Leader URL %s://%s Parse Error: %v", scheme, raftServerLeader, err))
			return
		}

		// 创建反向代理并转发请求到 Leader
		glog.V(4).Infoln("proxying to leader", raftServerLeader, "using", scheme)
		proxy := httputil.NewSingleHostReverseProxy(targetUrl)
		proxy.Transport = util_http.GetGlobalHttpClient().GetClientTransport()
		proxy.ServeHTTP(w, r)
	}
}

// startAdminScripts 启动管理维护脚本
// 定期执行配置的管理命令,用于集群自动维护任务(如 volume.balance, volume.fix.replication 等)
func (ms *MasterServer) startAdminScripts() {
	v := util.GetViper()
	adminScripts := v.GetString("master.maintenance.scripts")
	if adminScripts == "" {
		return
	}
	glog.V(0).Infof("adminScripts: %v", adminScripts)

	// 设置脚本执行间隔,默认17分钟
	v.SetDefault("master.maintenance.sleep_minutes", 17)
	sleepMinutes := v.GetFloat64("master.maintenance.sleep_minutes")

	// 解析脚本行
	scriptLines := strings.Split(adminScripts, "\n")
	// 如果脚本中没有 lock 命令,自动添加 lock/unlock 包裹
	// 确保维护操作期间不会有冲突的操作
	if !strings.Contains(adminScripts, "lock") {
		scriptLines = append(append([]string{}, "lock"), scriptLines...)
		scriptLines = append(scriptLines, "unlock")
	}

	masterAddress := string(ms.option.Master)

	// 创建 Shell 命令执行环境
	var shellOptions shell.ShellOptions
	shellOptions.GrpcDialOption = security.LoadClientTLS(v, "grpc.master")
	shellOptions.Masters = &masterAddress

	shellOptions.Directory = "/"
	emptyFilerGroup := ""
	shellOptions.FilerGroup = &emptyFilerGroup

	commandEnv := shell.NewCommandEnv(&shellOptions)

	// 编译正则表达式,用于解析命令行参数(支持引号包裹的参数)
	reg, _ := regexp.Compile(`'.*?'|".*?"|\S+`)

	// 保持与 Master 的连接
	go commandEnv.MasterClient.KeepConnectedToMaster(context.Background())

	// 启动定期执行协程
	go func() {
		for {
			time.Sleep(time.Duration(sleepMinutes) * time.Minute)
			// 只有 Leader 节点才执行管理脚本
			if ms.Topo.IsLeader() && ms.MasterClient.GetMaster(context.Background()) != "" {
				shellOptions.FilerAddress = ms.GetOneFiler(cluster.FilerGroupName(*shellOptions.FilerGroup))
				if shellOptions.FilerAddress == "" {
					continue
				}
				// 逐行执行脚本
				for _, line := range scriptLines {
					// 支持分号分隔的多个命令
					for _, c := range strings.Split(line, ";") {
						processEachCmd(reg, c, commandEnv)
					}
				}
			}
		}
	}()
}

// processEachCmd 处理单个管理命令
// 参数:
//   reg: 用于解析命令行参数的正则表达式
//   line: 命令行字符串
//   commandEnv: Shell 命令执行环境
func processEachCmd(reg *regexp.Regexp, line string, commandEnv *shell.CommandEnv) {
	// 使用正则表达式分割命令和参数(处理引号包裹的参数)
	cmds := reg.FindAllString(line, -1)
	if len(cmds) == 0 {
		return
	}
	// 提取参数并去除引号
	args := make([]string, len(cmds[1:]))
	for i := range args {
		args[i] = strings.Trim(string(cmds[1+i]), "\"'")
	}
	cmd := cmds[0]

	// 在注册的命令中查找匹配的命令并执行
	for _, c := range shell.Commands {
		if c.Name() == cmd {
			// 检查是否为资源密集型命令
			// 资源密集型命令(如 volume.balance)不应该在 Master 上运行
			if c.HasTag(shell.ResourceHeavy) {
				glog.Warningf("%s is resource heavy and should not run on master", cmd)
				continue
			}
			glog.V(0).Infof("executing: %s %v", cmd, args)
			if err := c.Do(args, commandEnv, os.Stdout); err != nil {
				glog.V(0).Infof("error: %v", err)
			}
		}
	}
}

// createSequencer 创建序列号生成器
// 序列号生成器用于生成唯一的文件 ID
// 支持两种类型:
//  1. raft/default: 基于内存的序列号生成器(适合单 Master 或 Raft 集群)
//  2. snowflake: Snowflake 算法生成器(适合分布式环境,无需协调)
// 参数:
//   option: Master 配置选项
// 返回:
//   序列号生成器实例
func (ms *MasterServer) createSequencer(option *MasterOption) sequence.Sequencer {
	var seq sequence.Sequencer
	v := util.GetViper()
	seqType := strings.ToLower(v.GetString(SequencerType))
	glog.V(1).Infof("[%s] : [%s]", SequencerType, seqType)
	switch strings.ToLower(seqType) {
	case "snowflake":
		// Snowflake 算法: Twitter 开源的分布式 ID 生成算法
		// 生成的 ID 包含时间戳、机器 ID 和序列号,保证全局唯一
		var err error
		snowflakeId := v.GetInt(SequencerSnowflakeId)
		seq, err = sequence.NewSnowflakeSequencer(string(option.Master), snowflakeId)
		if err != nil {
			glog.Error(err)
			seq = nil
		}
	case "raft":
		fallthrough
	default:
		// 内存序列号生成器: 简单递增,由 Raft 保证一致性
		seq = sequence.NewMemorySequencer()
	}
	return seq
}

// OnPeerUpdate 处理集群节点更新事件
// 当集群中有 Master 节点加入或离开时,自动更新 Raft 集群配置
// 参数:
//   update: 集群节点更新信息
//   startFrom: 更新开始时间
func (ms *MasterServer) OnPeerUpdate(update *master_pb.ClusterNodeUpdate, startFrom time.Time) {
	ms.Topo.RaftServerAccessLock.RLock()
	defer ms.Topo.RaftServerAccessLock.RUnlock()

	// 只处理 Master 类型节点,且只在使用 Hashicorp Raft 时处理
	if update.NodeType != cluster.MasterType || ms.Topo.HashicorpRaft == nil {
		return
	}
	glog.V(4).Infof("OnPeerUpdate: %+v", update)

	peerAddress := pb.ServerAddress(update.Address)
	peerName := string(peerAddress)
	// 只有 Leader 节点才能修改 Raft 集群配置
	if ms.Topo.HashicorpRaft.State() != hashicorpRaft.Leader {
		return
	}
	if update.IsAdd {
		// 添加新节点
		raftServerFound := false
		// 检查节点是否已存在于 Raft 配置中
		for _, server := range ms.Topo.HashicorpRaft.GetConfiguration().Configuration().Servers {
			if string(server.ID) == peerName {
				raftServerFound = true
			}
		}
		if !raftServerFound {
			glog.V(0).Infof("adding new raft server: %s", peerName)
			// 将新节点添加为 Voter(有投票权的节点)
			ms.Topo.HashicorpRaft.AddVoter(
				hashicorpRaft.ServerID(peerName),
				hashicorpRaft.ServerAddress(peerAddress.ToGrpcAddress()), 0, 0)
		}
	} else {
		// 移除节点
		// 先 ping 节点确认是否真的不可用
		pb.WithMasterClient(false, peerAddress, ms.grpcDialOption, true, func(client master_pb.SeaweedClient) error {
			ctx, cancel := context.WithTimeout(context.TODO(), 15*time.Second)
			defer cancel()
			if _, err := client.Ping(ctx, &master_pb.PingRequest{Target: string(peerAddress), TargetType: cluster.MasterType}); err != nil {
				// Ping 失败,节点确实不可用,从 Raft 集群中移除
				glog.V(0).Infof("master %s didn't respond to pings. remove raft server", peerName)
				if err := ms.MasterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
					_, err := client.RaftRemoveServer(context.Background(), &master_pb.RaftRemoveServerRequest{
						Id:    peerName,
						Force: false,
					})
					return err
				}); err != nil {
					glog.Warningf("failed removing old raft server: %v", err)
					return err
				}
			} else {
				// Ping 成功,节点仍然可用,不移除
				glog.V(0).Infof("master %s successfully responded to ping", peerName)
			}
			return nil
		})
	}
}

// Shutdown 优雅关闭 Master 服务器
// 如果当前节点是 Leader,先进行 Leader 转移,然后关闭 Raft 服务
func (ms *MasterServer) Shutdown() {
	if ms.Topo == nil || ms.Topo.HashicorpRaft == nil {
		return
	}
	// 如果是 Leader,先将 Leader 角色转移给其他节点
	// 避免关闭 Leader 导致集群暂时不可用
	if ms.Topo.HashicorpRaft.State() == hashicorpRaft.Leader {
		ms.Topo.HashicorpRaft.LeadershipTransfer()
	}
	// 关闭 Raft 服务
	ms.Topo.HashicorpRaft.Shutdown()
}

// Reload 重新加载 Master 服务器配置
// 支持热更新安全配置(如 IP 白名单),无需重启服务
func (ms *MasterServer) Reload() {
	glog.V(0).Infoln("Reload master server...")

	// 重新加载安全配置文件
	util.LoadConfiguration("security", false)
	v := util.GetViper()
	// 更新 IP 白名单
	ms.guard.UpdateWhiteList(append(ms.option.WhiteList,
		util.StringSplit(v.GetString("guard.white_list"), ",")...),
	)
}
