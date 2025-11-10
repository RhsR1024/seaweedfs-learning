package weed_server

import (
	"net/http"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/storage"
)

// VolumeServer 卷服务器结构体
//
// VolumeServer 是 SeaweedFS 架构中的核心组件之一，负责：
// 1. 实际存储文件数据到本地磁盘
// 2. 处理文件的上传、下载、删除请求
// 3. 定期向 Master 服务器发送心跳，报告卷状态
// 4. 管理本地卷的生命周期（创建、压缩、删除等）
// 5. 实现流量控制和并发限制
//
// 架构角色：
// - Master 负责元数据管理和卷分配
// - Volume Server 负责实际的数据存储和读写
// - Filer 负责提供文件系统接口（可选）
type VolumeServer struct {
	// 嵌入未实现的 gRPC 服务器，提供前向兼容性
	volume_server_pb.UnimplementedVolumeServerServer

	// === 流量控制相关字段 ===
	// 这些字段用于实现细粒度的流量控制，防止服务器过载

	// inFlightUploadDataSize 当前正在上传的数据总大小（字节）
	// 实时统计所有进行中的上传请求的数据量
	inFlightUploadDataSize int64

	// inFlightDownloadDataSize 当前正在下载的数据总大小（字节）
	// 实时统计所有进行中的下载请求的数据量
	inFlightDownloadDataSize int64

	// concurrentUploadLimit 并发上传的数据大小限制（字节）
	// 超过此限制的上传请求将被阻塞等待
	concurrentUploadLimit int64

	// concurrentDownloadLimit 并发下载的数据大小限制（字节）
	// 超过此限制的下载请求将被阻塞等待
	concurrentDownloadLimit int64

	// inFlightUploadDataLimitCond 上传流量控制的条件变量
	// 用于在达到上传限制时阻塞请求，空闲时唤醒等待的请求
	inFlightUploadDataLimitCond *sync.Cond

	// inFlightDownloadDataLimitCond 下载流量控制的条件变量
	// 用于在达到下载限制时阻塞请求，空闲时唤醒等待的请求
	inFlightDownloadDataLimitCond *sync.Cond

	// inflightUploadDataTimeout 上传请求等待超时时间
	// 如果等待时间超过此值，请求将被拒绝
	inflightUploadDataTimeout time.Duration

	// inflightDownloadDataTimeout 下载请求等待超时时间
	// 如果等待时间超过此值，请求将被拒绝
	inflightDownloadDataTimeout time.Duration

	// hasSlowRead 是否有慢速读取
	// 用于判断是否启用特殊的读取优化
	hasSlowRead bool

	// readBufferSizeMB 读取缓冲区大小（MB）
	// 控制从磁盘读取数据时的缓冲区大小
	readBufferSizeMB int

	// === Master 节点和集群配置 ===

	// SeedMasterNodes Master 服务器地址列表
	// 用于服务器启动时连接 Master，通常配置多个以实现高可用
	SeedMasterNodes []pb.ServerAddress

	// whiteList IP 白名单列表
	// 只有在白名单中的 IP 才能访问某些敏感操作
	whiteList []string

	// currentMaster 当前连接的 Master 服务器地址
	// 从 SeedMasterNodes 中选择一个可用的 Master
	currentMaster pb.ServerAddress

	// pulseSeconds 心跳间隔（秒）
	// Volume Server 每隔多少秒向 Master 发送一次心跳
	pulseSeconds int

	// dataCenter 数据中心标识
	// 用于数据的地理位置感知和跨数据中心复制
	dataCenter string

	// rack 机架标识
	// 更细粒度的位置标识，用于实现机架感知的副本放置
	rack string

	// === 核心组件 ===

	// store 存储引擎实例
	// 负责管理本地磁盘上的卷和文件数据
	store *storage.Store

	// guard 安全守卫
	// 处理 JWT 认证、IP 白名单等安全相关功能
	guard *security.Guard

	// grpcDialOption gRPC 连接选项
	// 包含 TLS 配置等连接参数
	grpcDialOption grpc.DialOption

	// === 存储配置 ===

	// needleMapKind Needle 索引类型
	// 决定使用何种数据结构存储文件索引（内存/LevelDB/BoltDB等）
	needleMapKind storage.NeedleMapKind

	// ldbTimout LevelDB 操作超时时间（毫秒）
	// 防止数据库操作长时间阻塞
	ldbTimout int64

	// FixJpgOrientation 是否修复 JPEG 图片方向
	// 根据 EXIF 信息自动旋转图片
	FixJpgOrientation bool

	// ReadMode 读取模式
	// 可选值：local（本地读）、proxy（代理读）、redirect（重定向）
	ReadMode string

	// compactionBytePerSecond 压缩速度限制（字节/秒）
	// 限制卷压缩时的磁盘 I/O，避免影响正常服务
	compactionBytePerSecond int64

	// metricsAddress Prometheus metrics 服务地址
	// 用于暴露监控指标
	metricsAddress string

	// metricsIntervalSec metrics 推送间隔（秒）
	metricsIntervalSec int

	// fileSizeLimitBytes 单个文件大小限制（字节）
	// 超过此大小的文件将被拒绝上传
	fileSizeLimitBytes int64

	// === 状态控制 ===

	// isHeartbeating 是否正在发送心跳
	// 用于控制心跳协程的运行
	isHeartbeating bool

	// stopChan 停止信号通道
	// 用于优雅关闭服务器
	stopChan chan bool
}

// NewVolumeServer 创建并初始化一个新的 VolumeServer 实例
//
// 这是 VolumeServer 的构造函数，负责：
// 1. 初始化所有配置参数
// 2. 创建存储引擎实例
// 3. 设置 HTTP 路由处理器
// 4. 启动心跳和监控协程
//
// 参数说明：
//
//	adminMux: 管理端口的 HTTP 路由器，用于暴露管理接口（状态、健康检查等）
//	publicMux: 公共端口的 HTTP 路由器，用于处理文件读写请求
//	ip: 服务器 IP 地址
//	port: HTTP 服务端口
//	grpcPort: gRPC 服务端口
//	publicUrl: 公共访问 URL，用于向 Master 报告和文件重定向
//	folders: 数据存储目录列表，支持多个目录以利用多块磁盘
//	maxCounts: 每个目录的最大卷数量限制
//	minFreeSpaces: 每个目录的最小剩余空间要求
//	diskTypes: 每个目录的磁盘类型（HDD/SSD）
//	idxFolder: 索引文件存储目录
//	needleMapKind: Needle 索引类型（内存/LevelDB/BoltDB等）
//	masterNodes: Master 服务器地址列表
//	pulseSeconds: 心跳间隔（秒）
//	dataCenter: 数据中心标识
//	rack: 机架标识
//	whiteList: IP 白名单
//	fixJpgOrientation: 是否自动修复 JPEG 图片方向
//	readMode: 读取模式（local/proxy/redirect）
//	compactionMBPerSecond: 压缩速度限制（MB/秒）
//	fileSizeLimitMB: 文件大小限制（MB）
//	concurrentUploadLimit: 并发上传数据量限制（字节）
//	concurrentDownloadLimit: 并发下载数据量限制（字节）
//	inflightUploadDataTimeout: 上传请求等待超时时间
//	inflightDownloadDataTimeout: 下载请求等待超时时间
//	hasSlowRead: 是否有慢速读取
//	readBufferSizeMB: 读取缓冲区大小（MB）
//	ldbTimeout: LevelDB 操作超时时间
//
// 返回：
//
//	*VolumeServer: 初始化完成的 VolumeServer 实例
func NewVolumeServer(adminMux, publicMux *http.ServeMux, ip string,
	port int, grpcPort int, publicUrl string,
	folders []string, maxCounts []int32, minFreeSpaces []util.MinFreeSpace, diskTypes []types.DiskType,
	idxFolder string,
	needleMapKind storage.NeedleMapKind,
	masterNodes []pb.ServerAddress, pulseSeconds int,
	dataCenter string, rack string,
	whiteList []string,
	fixJpgOrientation bool,
	readMode string,
	compactionMBPerSecond int,
	fileSizeLimitMB int,
	concurrentUploadLimit int64,
	concurrentDownloadLimit int64,
	inflightUploadDataTimeout time.Duration,
	inflightDownloadDataTimeout time.Duration,
	hasSlowRead bool,
	readBufferSizeMB int,
	ldbTimeout int64,
) *VolumeServer {

	// 获取配置管理器实例
	v := util.GetViper()

	// === JWT 签名配置 ===
	// 用于生成和验证上传请求的 JWT token

	// 获取 JWT 签名密钥，用于写操作（上传、删除）的认证
	signingKey := v.GetString("jwt.signing.key")
	// 设置默认过期时间为 10 秒
	v.SetDefault("jwt.signing.expires_after_seconds", 10)
	expiresAfterSec := v.GetInt("jwt.signing.expires_after_seconds")

	// UI 访问控制：是否允许通过 UI 查看服务器状态
	// 如果没有配置签名密钥，默认允许访问（开发环境）
	enableUiAccess := v.GetBool("access.ui")

	// 只读 JWT 签名配置（用于下载操作）
	readSigningKey := v.GetString("jwt.signing.read.key")
	// 读操作的 token 有效期更长（60 秒），因为读操作更频繁
	v.SetDefault("jwt.signing.read.expires_after_seconds", 60)
	readExpiresAfterSec := v.GetInt("jwt.signing.read.expires_after_seconds")

	// 创建 VolumeServer 实例并初始化基本配置
	vs := &VolumeServer{
		pulseSeconds:      pulseSeconds,
		dataCenter:        dataCenter,
		rack:              rack,
		needleMapKind:     needleMapKind,
		FixJpgOrientation: fixJpgOrientation,
		ReadMode:          readMode,
		// 加载 gRPC 客户端 TLS 配置
		grpcDialOption: security.LoadClientTLS(util.GetViper(), "grpc.volume"),
		// 将 MB/s 转换为 字节/s
		compactionBytePerSecond: int64(compactionMBPerSecond) * 1024 * 1024,
		// 将 MB 转换为字节
		fileSizeLimitBytes: int64(fileSizeLimitMB) * 1024 * 1024,
		// 默认启用心跳
		isHeartbeating: true,
		// 创建停止信号通道
		stopChan: make(chan bool),
		// 初始化流量控制的条件变量
		// sync.NewCond 创建一个条件变量，用于线程间的等待和唤醒
		inFlightUploadDataLimitCond:   sync.NewCond(new(sync.Mutex)),
		inFlightDownloadDataLimitCond: sync.NewCond(new(sync.Mutex)),
		// 设置并发限制
		concurrentUploadLimit:       concurrentUploadLimit,
		concurrentDownloadLimit:     concurrentDownloadLimit,
		inflightUploadDataTimeout:   inflightUploadDataTimeout,
		inflightDownloadDataTimeout: inflightDownloadDataTimeout,
		hasSlowRead:                 hasSlowRead,
		readBufferSizeMB:            readBufferSizeMB,
		ldbTimout:                   ldbTimeout,
		whiteList:                   whiteList,
	}

	// 合并配置文件中的白名单和参数中的白名单
	whiteList = append(whiteList, util.StringSplit(v.GetString("guard.white_list"), ",")...)
	vs.SeedMasterNodes = masterNodes

	// 检查并选择一个可用的 Master 节点
	// 这一步确保在启动时就能连接到 Master
	vs.checkWithMaster()

	// 创建存储引擎实例
	// Store 负责管理本地磁盘上的卷文件
	vs.store = storage.NewStore(vs.grpcDialOption, ip, port, grpcPort, publicUrl, folders, maxCounts, minFreeSpaces, idxFolder, vs.needleMapKind, diskTypes, ldbTimeout)

	// 创建安全守卫实例
	// Guard 负责 JWT 认证和 IP 白名单验证
	vs.guard = security.NewGuard(whiteList, signingKey, expiresAfterSec, readSigningKey, readExpiresAfterSec)

	// === 设置 HTTP 路由 ===

	// 为管理端口设置静态资源处理器（UI 相关的 CSS、JS 等）
	handleStaticResources(adminMux)

	// 注册管理端点，都使用 requestIDMiddleware 中间件为每个请求生成唯一 ID
	// /status 端点：返回服务器状态信息（卷列表、磁盘使用情况等）
	adminMux.HandleFunc("/status", requestIDMiddleware(vs.statusHandler))
	// /healthz 端点：健康检查端点，用于负载均衡器探测
	adminMux.HandleFunc("/healthz", requestIDMiddleware(vs.healthzHandler))

	// 根据安全配置决定是否暴露 UI 界面
	if signingKey == "" || enableUiAccess {
		// 只在安全环境下暴露 UI（没有配置签名密钥或显式允许）
		adminMux.HandleFunc("/ui/index.html", requestIDMiddleware(vs.uiStatusHandler))
		/*
			// 以下统计端点已被注释掉，可能在未来版本启用
			adminMux.HandleFunc("/stats/counter", vs.guard.WhiteList(statsCounterHandler))
			adminMux.HandleFunc("/stats/memory", vs.guard.WhiteList(statsMemoryHandler))
			adminMux.HandleFunc("/stats/disk", vs.guard.WhiteList(vs.statsDiskHandler))
		*/
	}

	// 默认路由处理器：处理所有管理操作（压缩、删除卷等）
	adminMux.HandleFunc("/", requestIDMiddleware(vs.privateStoreHandler))

	// 如果公共端口和管理端口分离（推荐的生产环境配置）
	if publicMux != adminMux {
		// 为公共端口也设置静态资源
		handleStaticResources(publicMux)
		// 公共端口只提供只读访问（下载文件）
		publicMux.HandleFunc("/", requestIDMiddleware(vs.publicReadOnlyHandler))
	}

	// 将并发限制配置暴露给 Prometheus 监控
	// 这样可以在监控面板中看到配置的限流阈值
	stats.VolumeServerConcurrentDownloadLimit.Set(float64(vs.concurrentDownloadLimit))
	stats.VolumeServerConcurrentUploadLimit.Set(float64(vs.concurrentUploadLimit))

	// 启动后台协程

	// 心跳协程：定期向 Master 报告服务器状态
	go vs.heartbeat()

	// 监控指标推送协程：定期推送 Prometheus metrics
	go stats.LoopPushingMetric("volumeServer", util.JoinHostPort(ip, port), vs.metricsAddress, vs.metricsIntervalSec)

	return vs
}

// SetStopping 设置服务器为停止状态
//
// 这是优雅关闭流程的第一步，调用后：
// 1. 不再接受新的写入请求
// 2. 允许正在处理的请求完成
// 3. 停止创建新的卷
//
// 典型调用场景：
// - 收到 SIGTERM 或 SIGINT 信号时
// - 服务器升级或维护前
// - 迁移数据到其他服务器前
//
// 注意：调用此方法后，应该等待一段时间让请求处理完成，
// 然后再调用 Shutdown() 进行最终清理
func (vs *VolumeServer) SetStopping() {
	glog.V(0).Infoln("Stopping volume server...")
	// 通知存储引擎进入停止状态
	vs.store.SetStopping()
}

// LoadNewVolumes 重新扫描并加载新的卷
//
// 用途：
// 1. 动态发现新添加的卷文件（不重启服务器）
// 2. 恢复因错误而未加载的卷
// 3. 在数据恢复或迁移后重新加载卷
//
// 使用场景：
// - 手动复制卷文件到数据目录后
// - 从备份恢复数据后
// - 运维工具触发的卷重新加载
//
// 注意：此操作会扫描磁盘，可能耗时较长
func (vs *VolumeServer) LoadNewVolumes() {
	glog.V(0).Infoln(" Loading new volume ids ...")
	vs.store.LoadNewVolumes()
}

// Shutdown 关闭 VolumeServer 并释放所有资源
//
// 这是优雅关闭流程的最后一步，执行：
// 1. 关闭所有打开的卷文件句柄
// 2. 刷新缓冲区到磁盘
// 3. 关闭数据库连接（LevelDB/BoltDB）
// 4. 释放内存资源
//
// 调用顺序建议：
//  1. SetStopping()  - 停止接受新请求
//  2. 等待若干秒     - 让进行中的请求完成
//  3. Shutdown()     - 最终清理
//
// 注意：此方法调用后，VolumeServer 实例不可再使用
func (vs *VolumeServer) Shutdown() {
	glog.V(0).Infoln("Shutting down volume server...")
	// 关闭存储引擎，刷新所有缓冲区并关闭文件
	vs.store.Close()
	glog.V(0).Infoln("Shut down successfully!")
}

// Reload 重新加载配置
//
// 支持在不重启服务器的情况下更新某些配置项，目前支持：
// 1. 安全配置（security.toml）
// 2. IP 白名单
//
// 使用场景：
// - 动态更新访问控制策略
// - 添加或移除白名单 IP
// - 调整安全策略
//
// 注意：
// - 只能重新加载部分配置，不是所有配置都支持热重载
// - 某些配置（如端口、数据目录）需要重启服务器才能生效
func (vs *VolumeServer) Reload() {
	glog.V(0).Infoln("Reload volume server...")

	// 重新加载安全配置文件
	util.LoadConfiguration("security", false)
	v := util.GetViper()
	// 更新 IP 白名单：合并原有白名单和新加载的白名单
	vs.guard.UpdateWhiteList(append(vs.whiteList, util.StringSplit(v.GetString("guard.white_list"), ",")...))
}
