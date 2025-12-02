// Package weed_server 中的 filer_server.go 负责 Filer 服务本体的初始化、配置加载与生命周期管理
//
// 核心职责：
//   1. Filer Server 初始化：创建和配置 FilerServer 实例
//   2. 存储后端管理：自动注册和加载可插拔的存储后端（MySQL、Postgres、LevelDB 等）
//   3. 通知系统集成：支持多种消息队列（Kafka、AWS SQS、Google Pub/Sub 等）
//   4. HTTP/gRPC 路由：注册 HTTP 处理器和 gRPC 服务
//   5. Master 集群交互：心跳、配置同步、metrics 上报
//   6. 集群同步：Filer 节点间的元数据同步和 bootstrap
//
// Filer Server 在 SeaweedFS 架构中的作用：
//   - 提供类 POSIX 文件系统接口（目录树、文件元数据）
//   - 支持 S3 兼容 API、WebDAV、FUSE 挂载
//   - 将文件元数据存储在可插拔的数据库中
//   - 将实际文件数据分块存储在 Volume Server 上
//
// 存储后端（通过 blank import 自动注册）：
//   - SQL：MySQL、Postgres、SQLite
//   - NoSQL：MongoDB、Cassandra、Redis、Elasticsearch、HBase
//   - KV：LevelDB、RocksDB、Etcd、YDB、Tarantool
//   - ArangoDB：多模型数据库
//
// 通知系统（通过 blank import 自动注册）：
//   - Kafka：高吞吐消息队列
//   - AWS SQS：AWS 托管消息队列
//   - Google Pub/Sub：GCP 托管消息队列
//   - GoCloud：跨云平台抽象
//   - Webhook：HTTP 回调
//   - Log：本地日志文件
//
// 配置文件：
//   - filer.toml：存储后端配置
//   - notification.toml：通知系统配置
//   - security.toml：JWT 认证配置
package weed_server

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/stats"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/util/grace"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"

	// 【Filer 核心包】
	"github.com/seaweedfs/seaweedfs/weed/filer"

	// 【存储后端自动注册】
	// 以下 blank import (_) 会触发各存储后端的 init() 函数
	// init() 函数会调用 filer.Stores.Register() 注册存储类型
	// 使用时在 filer.toml 中配置对应的存储类型即可

	// NoSQL 数据库
	_ "github.com/seaweedfs/seaweedfs/weed/filer/arangodb"      // ArangoDB 多模型数据库
	_ "github.com/seaweedfs/seaweedfs/weed/filer/cassandra"     // Cassandra v1（旧版）
	_ "github.com/seaweedfs/seaweedfs/weed/filer/cassandra2"    // Cassandra v2（推荐）
	_ "github.com/seaweedfs/seaweedfs/weed/filer/mongodb"       // MongoDB 文档数据库
	_ "github.com/seaweedfs/seaweedfs/weed/filer/hbase"         // HBase 列式存储

	// SQL 数据库
	_ "github.com/seaweedfs/seaweedfs/weed/filer/mysql"         // MySQL v1（旧版）
	_ "github.com/seaweedfs/seaweedfs/weed/filer/mysql2"        // MySQL v2（推荐）
	_ "github.com/seaweedfs/seaweedfs/weed/filer/postgres"      // PostgreSQL v1（旧版）
	_ "github.com/seaweedfs/seaweedfs/weed/filer/postgres2"     // PostgreSQL v2（推荐）
	_ "github.com/seaweedfs/seaweedfs/weed/filer/sqlite"        // SQLite 嵌入式数据库

	// KV 存储
	_ "github.com/seaweedfs/seaweedfs/weed/filer/leveldb"       // LevelDB v1（旧版）
	_ "github.com/seaweedfs/seaweedfs/weed/filer/leveldb2"      // LevelDB v2（推荐）
	_ "github.com/seaweedfs/seaweedfs/weed/filer/leveldb3"      // LevelDB v3（最新）
	_ "github.com/seaweedfs/seaweedfs/weed/filer/redis"         // Redis v1（旧版）
	_ "github.com/seaweedfs/seaweedfs/weed/filer/redis2"        // Redis v2（推荐）
	_ "github.com/seaweedfs/seaweedfs/weed/filer/redis3"        // Redis v3（最新，支持 Redis Cluster）
	_ "github.com/seaweedfs/seaweedfs/weed/filer/etcd"          // Etcd 分布式 KV 存储
	_ "github.com/seaweedfs/seaweedfs/weed/filer/ydb"           // YDB（Yandex Database）
	_ "github.com/seaweedfs/seaweedfs/weed/filer/tarantool"     // Tarantool 内存数据库

	// 搜索引擎
	_ "github.com/seaweedfs/seaweedfs/weed/filer/elastic/v7"    // Elasticsearch v7

	"github.com/seaweedfs/seaweedfs/weed/glog"

	// 【通知系统自动注册】
	// 以下 blank import (_) 会触发各通知系统的 init() 函数
	// init() 函数会调用 notification.Register() 注册通知类型
	// 使用时在 notification.toml 中配置对应的通知类型即可
	"github.com/seaweedfs/seaweedfs/weed/notification"
	_ "github.com/seaweedfs/seaweedfs/weed/notification/aws_sqs"        // AWS SQS 消息队列
	_ "github.com/seaweedfs/seaweedfs/weed/notification/gocdk_pub_sub"  // GoCloud Pub/Sub（跨云平台）
	_ "github.com/seaweedfs/seaweedfs/weed/notification/google_pub_sub" // Google Pub/Sub
	_ "github.com/seaweedfs/seaweedfs/weed/notification/kafka"          // Kafka 消息队列
	_ "github.com/seaweedfs/seaweedfs/weed/notification/log"            // 本地日志文件
	_ "github.com/seaweedfs/seaweedfs/weed/notification/webhook"        // HTTP Webhook 回调

	"github.com/seaweedfs/seaweedfs/weed/security"
)

// FilerOption 描述 Filer 进程的运行参数
//
// 包含 master 自动发现、默认副本策略、HTTP 暴露开关、上传限制等配置项
// 这些字段通常来自命令行参数或 YAML/Viper 配置文件
//
// 配置来源优先级：
//   1. 命令行参数（最高优先级）
//   2. 环境变量
//   3. 配置文件（filer.toml、security.toml 等）
//   4. 默认值（最低优先级）
type FilerOption struct {
	// 【Master 集群配置】
	Masters *pb.ServerDiscovery // Master 服务器列表，支持自动发现（DNS SRV 记录）

	// 【Filer 集群配置】
	FilerGroup string // Filer 分组名称，用于区分不同的 Filer 集群

	// 【默认存储配置】
	Collection         string // 默认集合名称，用于文件逻辑分组
	DefaultReplication string // 默认副本策略（如 "001"、"010"、"100"）
	DataCenter         string // 数据中心名称
	Rack               string // 机架名称
	DataNode           string // 数据节点名称
	DiskType           string // 磁盘类型（如 "hdd"、"ssd"）

	// 【HTTP 服务配置】
	DisableHttp           bool              // 是否禁用 HTTP 接口（仅保留 gRPC）
	DisableDirListing     bool              // 是否禁用目录列表功能
	DirListingLimit       int               // 目录列表最大条目数
	ShowUIDirectoryDelete bool              // 是否在 UI 中显示删除按钮
	AllowedOrigins        []string          // CORS 允许的来源域名列表
	ExposeDirectoryData   bool              // 是否暴露目录元数据（如文件数量、大小）
	Host                  pb.ServerAddress  // Filer 监听地址（如 "localhost:8888"）

	// 【上传配置】
	MaxMB                 int   // 单个 chunk 的最大大小（MB）
	SaveToFilerLimit      int64 // 小文件内联阈值（字节），小于此值的文件直接存储在元数据中
	ConcurrentUploadLimit int64 // 并发上传限制（同时处理的上传请求数）

	// 【下载配置】
	DownloadMaxBytesPs int64 // 下载限速（字节/秒），0 表示不限速

	// 【存储后端配置】
	DefaultLevelDbDir string // LevelDB 默认存储目录（如果使用 LevelDB 作为元数据存储）

	// 【安全配置】
	Cipher bool // 是否启用数据加密

	// 【高级配置】
	recursiveDelete bool // 是否允许递归删除目录（内部使用，不直接暴露）
}

// FilerServer 封装一个运行中的 Filer 节点
//
// 核心组件：
//   - filer.Filer：元数据管理核心
//   - Guard：JWT 认证和权限控制
//   - gRPC Server：元数据同步、订阅通知
//   - HTTP Server：文件上传下载、目录浏览
//
// 并发控制：
//   - inFlightDataSize：当前正在上传的数据量（字节）
//   - inFlightDataLimitCond：上传流量控制条件变量
//   - listenersLock/listenersCond：元数据监听者同步
//
// 安全机制：
//   - filerGuard：Filer 操作的 JWT 认证
//   - volumeGuard：Volume 操作的 JWT 认证
//   - secret：JWT 签名密钥
type FilerServer struct {
	// 【流量控制】
	inFlightDataSize int64 // 当前正在上传的数据量（原子操作），用于限流
	listenersWaits   int64 // 等待元数据变更通知的监听者数量（原子操作）

	// 【元数据监听者管理】
	// 用于实现元数据变更的实时通知（如 S3 事件通知）
	listenersLock sync.Mutex       // 保护 listenersCond 的锁
	listenersCond *sync.Cond        // 条件变量，用于唤醒等待元数据变更的监听者

	// 【上传流量控制】
	inFlightDataLimitCond *sync.Cond // 条件变量，用于等待上传流量降低

	// 【gRPC 服务实现】
	filer_pb.UnimplementedSeaweedFilerServer // gRPC 服务基类

	// 【核心组件】
	option         *FilerOption      // 配置选项
	secret         security.SigningKey // JWT 签名密钥
	filer          *filer.Filer      // Filer 核心：元数据管理、存储后端、通知系统
	filerGuard     *security.Guard   // Filer 操作的 JWT 认证和权限控制
	volumeGuard    *security.Guard   // Volume 操作的 JWT 认证和权限控制
	grpcDialOption grpc.DialOption   // gRPC 连接选项（TLS 配置）

	// 【Master 集成】
	// metrics 配置从 Master 读取并定期推送
	metricsAddress     string // Metrics 收集服务器地址（如 Prometheus Pushgateway）
	metricsIntervalSec int    // Metrics 推送间隔（秒）

	// 【元数据监听者跟踪】
	// 用于记录已知的元数据监听者，避免重复通知
	knownListenersLock sync.Mutex      // 保护 knownListeners 的锁
	knownListeners     map[int32]int32 // 已知监听者 ID 映射
}

// NewFilerServer 根据传入的 HTTP mux 与配置选项创建完整的 FilerServer
//
// 参数:
//   - defaultMux: 默认 HTTP 路由（读写模式）
//   - readonlyMux: 只读 HTTP 路由（只读模式，用于高可用场景）
//   - option: Filer 配置选项
//
// 返回:
//   - fs: 初始化完成的 FilerServer 实例
//   - err: 初始化错误
//
// 核心步骤:
//   1. 读取 JWT、CORS、目录曝光等配置形成运行选项
//   2. 初始化 Filer、Guard、通知模块以及静态资源路由
//   3. 建立与 Master 的连接并启动指标上报
//   4. 从现有 Filer 节点 bootstrap 元数据（如果是新节点）
//   5. 启动后台任务（metrics 推送、Master 心跳、集群同步）
//
// 配置项说明:
//   - jwt.filer_signing.key：Filer 写操作的 JWT 签名密钥
//   - jwt.filer_signing.read.key：Filer 读操作的 JWT 签名密钥（通常有效期更长）
//   - jwt.signing.key：Volume 写操作的 JWT 签名密钥
//   - jwt.signing.read.key：Volume 读操作的 JWT 签名密钥
//   - cors.allowed_origins.values：CORS 允许的来源域名（逗号分隔）
//   - filer.expose_directory_metadata.enabled：是否暴露目录元数据
//   - filer.options.recursive_delete：是否允许递归删除目录
//   - filer.options.buckets_folder：S3 bucket 根目录（默认 /buckets）
//   - filer.options.max_file_name_length：最大文件名长度（默认 255）
func NewFilerServer(defaultMux, readonlyMux *http.ServeMux, option *FilerOption) (fs *FilerServer, err error) {

	// 【步骤 1：读取 JWT 配置】
	v := util.GetViper()

	// Filer 写操作的 JWT 配置
	signingKey := v.GetString("jwt.filer_signing.key")
	v.SetDefault("jwt.filer_signing.expires_after_seconds", 10)
	expiresAfterSec := v.GetInt("jwt.filer_signing.expires_after_seconds")

	// Filer 读操作的 JWT 配置（有效期通常更长）
	readSigningKey := v.GetString("jwt.filer_signing.read.key")
	v.SetDefault("jwt.filer_signing.read.expires_after_seconds", 60)
	readExpiresAfterSec := v.GetInt("jwt.filer_signing.read.expires_after_seconds")

	// Volume 写操作的 JWT 配置
	volumeSigningKey := v.GetString("jwt.signing.key")
	v.SetDefault("jwt.signing.expires_after_seconds", 10)
	volumeExpiresAfterSec := v.GetInt("jwt.signing.expires_after_seconds")

	// Volume 读操作的 JWT 配置
	volumeReadSigningKey := v.GetString("jwt.signing.read.key")
	v.SetDefault("jwt.signing.read.expires_after_seconds", 60)
	volumeReadExpiresAfterSec := v.GetInt("jwt.signing.read.expires_after_seconds")

	// 【步骤 2：读取 CORS 配置】
	v.SetDefault("cors.allowed_origins.values", "*")
	allowedOrigins := v.GetString("cors.allowed_origins.values")
	domains := strings.Split(allowedOrigins, ",")
	option.AllowedOrigins = domains

	// 【步骤 3：读取目录元数据暴露配置】
	v.SetDefault("filer.expose_directory_metadata.enabled", true)
	returnDirMetadata := v.GetBool("filer.expose_directory_metadata.enabled")
	option.ExposeDirectoryData = returnDirMetadata

	// 【步骤 4：创建 FilerServer 实例】
	fs = &FilerServer{
		option:                option,
		grpcDialOption:        security.LoadClientTLS(util.GetViper(), "grpc.filer"), // 加载 gRPC TLS 配置
		knownListeners:        make(map[int32]int32),                                  // 初始化监听者映射
		inFlightDataLimitCond: sync.NewCond(new(sync.Mutex)),                          // 初始化流量控制条件变量
	}
	fs.listenersCond = sync.NewCond(&fs.listenersLock)

	// 【步骤 5：验证 Master 配置】
	option.Masters.RefreshBySrvIfAvailable() // 尝试通过 DNS SRV 记录刷新 Master 列表
	if len(option.Masters.GetInstances()) == 0 {
		glog.Fatal("master list is required!") // Master 列表为空则退出
	}

	// 【步骤 6：加载存储后端配置】
	if !util.LoadConfiguration("filer", false) {
		// filer.toml 不存在，使用默认的 LevelDB 配置
		v.SetDefault("leveldb2.enabled", true)
		v.SetDefault("leveldb2.dir", option.DefaultLevelDbDir)
		_, err := os.Stat(option.DefaultLevelDbDir)
		if os.IsNotExist(err) {
			os.MkdirAll(option.DefaultLevelDbDir, 0755) // 创建 LevelDB 存储目录
		}
		glog.V(0).Infof("default to create filer store dir in %s", option.DefaultLevelDbDir)
	} else {
		// filer.toml 存在，跳过默认配置
		glog.Warningf("skipping default store dir in %s", option.DefaultLevelDbDir)
	}

	// 【步骤 7：加载通知系统配置】
	util.LoadConfiguration("notification", false)

	// 【步骤 8：创建 Filer 核心实例】
	v.SetDefault("filer.options.max_file_name_length", 255)
	maxFilenameLength := v.GetUint32("filer.options.max_file_name_length")
	glog.V(0).Infof("max_file_name_length %d", maxFilenameLength)

	// 创建 Filer，传入元数据变更回调函数
	fs.filer = filer.NewFiler(*option.Masters, fs.grpcDialOption, option.Host, option.FilerGroup, option.Collection, option.DefaultReplication, option.DataCenter, maxFilenameLength, func() {
		// 元数据变更回调：唤醒等待的监听者
		if atomic.LoadInt64(&fs.listenersWaits) > 0 {
			fs.listenersCond.Broadcast()
		}
	})
	fs.filer.Cipher = option.Cipher

	// 【步骤 9：创建 JWT 认证 Guard】
	whiteList := util.StringSplit(v.GetString("guard.white_list"), ",")
	fs.filerGuard = security.NewGuard(whiteList, signingKey, expiresAfterSec, readSigningKey, readExpiresAfterSec)
	fs.volumeGuard = security.NewGuard([]string{}, volumeSigningKey, volumeExpiresAfterSec, volumeReadSigningKey, volumeReadExpiresAfterSec)

	// 【步骤 10：从 Master 获取配置】
	fs.checkWithMaster() // 获取 metrics 地址和推送间隔

	// 【步骤 11：启动后台任务】
	go stats.LoopPushingMetric("filer", string(fs.option.Host), fs.metricsAddress, fs.metricsIntervalSec) // 定期推送 metrics
	go fs.filer.KeepMasterClientConnected(context.Background())                                            // 保持与 Master 的连接

	// 【步骤 12：加载 Filer 高级配置】
	fs.option.recursiveDelete = v.GetBool("filer.options.recursive_delete")
	v.SetDefault("filer.options.buckets_folder", "/buckets")
	fs.filer.DirBucketsPath = v.GetString("filer.options.buckets_folder")
	// TODO deprecated, will be removed after 2020-12-31
	// replaced by https://github.com/seaweedfs/seaweedfs/wiki/Path-Specific-Configuration
	// fs.filer.FsyncBuckets = v.GetStringSlice("filer.options.buckets_fsync")

	// 【步骤 13：加载存储后端和通知系统配置】
	isFresh := fs.filer.LoadConfiguration(v)        // 加载存储后端配置，返回是否为新节点
	notification.LoadConfiguration(v, "notification.") // 加载通知系统配置

	// 【步骤 14：注册 HTTP 路由】
	handleStaticResources(defaultMux) // 注册静态资源路由（UI、图标等）
	if !option.DisableHttp {
		defaultMux.HandleFunc("/healthz", requestIDMiddleware(fs.filerHealthzHandler)) // 健康检查
		defaultMux.HandleFunc("/", fs.filerGuard.WhiteList(requestIDMiddleware(fs.filerHandler))) // 主处理器（带 JWT 认证）
	}
	if defaultMux != readonlyMux {
		// 只读模式：注册独立的只读路由
		handleStaticResources(readonlyMux)
		readonlyMux.HandleFunc("/healthz", requestIDMiddleware(fs.filerHealthzHandler))
		readonlyMux.HandleFunc("/", fs.filerGuard.WhiteList(requestIDMiddleware(fs.readonlyFilerHandler)))
	}

	// 【步骤 15：集群同步（Bootstrap）】
	existingNodes := fs.filer.ListExistingPeerUpdates(context.Background()) // 获取现有 Filer 节点列表
	startFromTime := time.Now().Add(-filer.LogFlushInterval)
	if isFresh {
		// 新节点：从现有节点 bootstrap 元数据
		glog.V(0).Infof("%s bootstrap from peers %+v", option.Host, existingNodes)
		if err := fs.filer.MaybeBootstrapFromOnePeer(option.Host, existingNodes, startFromTime); err != nil {
			glog.Fatalf("%s bootstrap from %+v: %v", option.Host, existingNodes, err)
		}
	}
	// 聚合其他节点的元数据变更
	fs.filer.AggregateFromPeers(option.Host, existingNodes, startFromTime)

	// 【步骤 16：加载 Filer 配置和远程存储配置】
	fs.filer.LoadFilerConf()                        // 加载路径特定配置（如不同路径使用不同副本策略）
	fs.filer.LoadRemoteStorageConfAndMapping()      // 加载远程存储配置（如 S3、GCS）

	// 【步骤 17：注册信号处理】
	grace.OnReload(fs.Reload)                       // 注册 HUP 信号处理（热重载配置）
	grace.OnInterrupt(func() {
		fs.filer.Shutdown()                         // 注册 INT/TERM 信号处理（优雅关闭）
	})

	// 【步骤 18：设置分布式锁快照回调】
	fs.filer.Dlm.LockRing.SetTakeSnapshotCallback(fs.OnDlmChangeSnapshot)

	return fs, nil
}

// checkWithMaster 从 Master 拉取配置信息
//
// 功能:
//   - 从 Master 获取 metrics 地址和推送间隔
//   - 持续重试直到成功连接到任一 Master
//
// 工作流程:
//   1. 刷新 Master 列表（尝试从 DNS SRV 记录更新）
//   2. 遍历所有 Master 节点
//   3. 调用 GetMasterConfiguration gRPC 方法获取配置
//   4. 如果失败则等待 7 秒后重试
//
// 配置项:
//   - MetricsAddress：Metrics 收集服务器地址（如 Prometheus Pushgateway）
//   - MetricsIntervalSeconds：Metrics 推送间隔（秒）
func (fs *FilerServer) checkWithMaster() {

	isConnected := false
	for !isConnected {
		// 刷新 Master 列表（尝试从 DNS SRV 记录更新）
		fs.option.Masters.RefreshBySrvIfAvailable()

		// 遍历所有 Master 节点，尝试获取配置
		for _, master := range fs.option.Masters.GetInstances() {
			readErr := operation.WithMasterServerClient(false, master, fs.grpcDialOption, func(masterClient master_pb.SeaweedClient) error {
				// 调用 GetMasterConfiguration gRPC 方法
				resp, err := masterClient.GetMasterConfiguration(context.Background(), &master_pb.GetMasterConfigurationRequest{})
				if err != nil {
					return fmt.Errorf("get master %s configuration: %v", master, err)
				}
				// 保存 metrics 配置
				fs.metricsAddress, fs.metricsIntervalSec = resp.MetricsAddress, int(resp.MetricsIntervalSeconds)
				return nil
			})
			if readErr == nil {
				// 成功获取配置，退出循环
				isConnected = true
			} else {
				// 失败则等待 7 秒后重试
				time.Sleep(7 * time.Second)
			}
		}
	}
}

// Reload 实现 util/grace.Reloadable 接口，用于 HUP 信号触发的热重载
//
// 功能:
//   - 重新加载 security.toml 配置文件
//   - 更新 JWT 认证白名单
//
// 使用场景:
//   - 动态更新 IP 白名单，无需重启服务
//   - 更新 JWT 密钥配置
//
// 触发方式:
//   - 发送 HUP 信号：kill -HUP <filer_pid>
//   - 或使用 systemd reload 命令
//
// 注意事项:
//   - 当前仅支持重载安全配置
//   - 其他配置（如存储后端）需要重启服务才能生效
func (fs *FilerServer) Reload() {
	glog.V(0).Infoln("Reload filer server...")

	// 重新加载 security.toml 配置文件
	util.LoadConfiguration("security", false)
	v := util.GetViper()

	// 更新 JWT 认证白名单
	fs.filerGuard.UpdateWhiteList(util.StringSplit(v.GetString("guard.white_list"), ","))
}
