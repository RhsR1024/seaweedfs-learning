// Package command 包含 Volume Server 的启动命令实现
// Volume Server 是 SeaweedFS 的存储节点，负责：
// 1. 实际存储文件数据到磁盘
// 2. 响应文件的读写请求
// 3. 向 Master Server 定期发送心跳和状态报告
// 4. 执行数据压缩、垃圾回收等维护任务
package command

import (
	"fmt"
	"net/http"
	httppprof "net/http/pprof" // HTTP pprof 性能分析
	"os"
	"runtime/pprof" // Runtime pprof 性能分析
	"strconv"
	"strings"
	"time"

	"github.com/spf13/viper"           // 配置管理
	"google.golang.org/grpc"           // gRPC 服务器
	"google.golang.org/grpc/reflection" // gRPC 反射

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	weed_server "github.com/seaweedfs/seaweedfs/weed/server"
	"github.com/seaweedfs/seaweedfs/weed/server/constants"
	stats_collect "github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/grace"
	"github.com/seaweedfs/seaweedfs/weed/util/httpdown"
	"github.com/seaweedfs/seaweedfs/weed/util/version"
)

var (
	// v 全局的 Volume Server 配置选项
	v VolumeServerOptions
)

// VolumeServerOptions Volume Server 的配置选项
// 包含网络、存储目录、索引、性能限制等各方面的配置
type VolumeServerOptions struct {
	port                      *int      // HTTP 服务端口，默认 8080
	portGrpc                  *int      // gRPC 服务端口，默认 HTTP端口+10000
	publicPort                *int      // 对外公开的端口（用于 NAT 环境）
	folders                   []string  // 存储数据的目录列表
	folderMaxLimits           []int32   // 每个目录的卷数量限制
	idxFolder                 *string   // .idx 索引文件的存储目录
	ip                        *string   // Volume Server 的 IP 地址
	publicUrl                 *string   // 对外公开的 URL（用于 NAT 环境）
	bindIp                    *string   // 绑定的 IP 地址
	mastersString             *string   // Master Server 地址列表（逗号分隔）
	mserverString             *string   // deprecated, for backward compatibility  // 已废弃
	masters                   []pb.ServerAddress  // Master Server 地址列表（解析后）
	idleConnectionTimeout     *int                // 连接空闲超时时间（秒）
	dataCenter                *string             // 数据中心名称
	rack                      *string             // 机架名称
	whiteList                 []string            // IP 白名单
	indexType                 *string             // 索引类型：memory|leveldb|leveldbMedium|leveldbLarge
	diskType                  *string             // 磁盘类型：hdd|ssd|<tag>
	fixJpgOrientation         *bool               // 是否自动修正 JPG 方向
	readMode                  *string             // 读取模式：local|proxy|redirect
	cpuProfile                *string             // CPU 性能分析输出文件
	memProfile                *string             // 内存性能分析输出文件
	compactionMBPerSecond     *int                // 压缩速度限制（MB/s）
	fileSizeLimitMB           *int                // 单文件大小限制（MB）
	concurrentUploadLimitMB   *int                // 并发上传大小限制（MB）
	concurrentDownloadLimitMB *int                // 并发下载大小限制（MB）
	pprof                     *bool               // 是否启用 pprof HTTP 接口
	preStopSeconds            *int                // 停止前等待时间（秒）
	metricsHttpPort           *int                // Prometheus 指标端口
	metricsHttpIp             *string             // Prometheus 指标 IP
	// pulseSeconds          *int                 // 心跳间隔（已废弃）
	inflightUploadDataTimeout   *time.Duration  // 飞行中上传数据的超时时间
	inflightDownloadDataTimeout *time.Duration  // 飞行中下载数据的超时时间
	hasSlowRead                 *bool           // 是否启用慢读优化（实验性）
	readBufferSizeMB            *int            // 读缓冲区大小（MB）
	ldbTimeout                  *int64          // LevelDB 超时时间（小时）
}

// init 初始化 Volume Server 命令的参数
// 定义了所有命令行参数的默认值和说明
func init() {
	cmdVolume.Run = runVolume // break init cycle  // 设置命令的执行函数
	v.port = cmdVolume.Flag.Int("port", 8080, "http listen port")
	v.portGrpc = cmdVolume.Flag.Int("port.grpc", 0, "grpc listen port")
	v.publicPort = cmdVolume.Flag.Int("port.public", 0, "port opened to public")
	v.ip = cmdVolume.Flag.String("ip", util.DetectedHostAddress(), "ip or server name, also used as identifier")
	v.publicUrl = cmdVolume.Flag.String("publicUrl", "", "Publicly accessible address")
	v.bindIp = cmdVolume.Flag.String("ip.bind", "", "ip address to bind to. If empty, default to same as -ip option.")
	v.mastersString = cmdVolume.Flag.String("master", "localhost:9333", "comma-separated master servers")
	v.mserverString = cmdVolume.Flag.String("mserver", "", "comma-separated master servers (deprecated, use -master instead)")
	v.preStopSeconds = cmdVolume.Flag.Int("preStopSeconds", 10, "number of seconds between stop send heartbeats and stop volume server")
	// v.pulseSeconds = cmdVolume.Flag.Int("pulseSeconds", 5, "number of seconds between heartbeats, must be smaller than or equal to the master's setting")
	v.idleConnectionTimeout = cmdVolume.Flag.Int("idleTimeout", 30, "connection idle seconds")
	v.dataCenter = cmdVolume.Flag.String("dataCenter", "", "current volume server's data center name")
	v.rack = cmdVolume.Flag.String("rack", "", "current volume server's rack name")
	// 索引类型说明：
	// - memory: 全部加载到内存，速度最快但内存占用大
	// - leveldb: 使用 LevelDB 存储索引，节省内存
	// - leveldbMedium/leveldbLarge: 不同的缓存大小配置
	v.indexType = cmdVolume.Flag.String("index", "memory", "Choose [memory|leveldb|leveldbMedium|leveldbLarge] mode for memory~performance balance.")
	v.diskType = cmdVolume.Flag.String("disk", "", "[hdd|ssd|<tag>] hard drive or solid state drive or any tag")
	v.fixJpgOrientation = cmdVolume.Flag.Bool("images.fix.orientation", false, "Adjust jpg orientation when uploading.")
	// 读取模式说明：
	// - local: 只读本地卷，非本地返回 404
	// - proxy: 代理到拥有数据的 Volume Server
	// - redirect: 重定向到拥有数据的 Volume Server
	v.readMode = cmdVolume.Flag.String("readMode", "proxy", "[local|proxy|redirect] how to deal with non-local volume: 'not found|proxy to remote node|redirect volume location'.")
	v.cpuProfile = cmdVolume.Flag.String("cpuprofile", "", "cpu profile output file")
	v.memProfile = cmdVolume.Flag.String("memprofile", "", "memory profile output file")
	v.compactionMBPerSecond = cmdVolume.Flag.Int("compactionMBps", 0, "limit background compaction or copying speed in mega bytes per second")
	v.fileSizeLimitMB = cmdVolume.Flag.Int("fileSizeLimitMB", 256, "limit file size to avoid out of memory")
	v.ldbTimeout = cmdVolume.Flag.Int64("index.leveldbTimeout", 0, "alive time for leveldb (default to 0). If leveldb of volume is not accessed in ldbTimeout hours, it will be off loaded to reduce opened files and memory consumption.")
	v.concurrentUploadLimitMB = cmdVolume.Flag.Int("concurrentUploadLimitMB", 256, "limit total concurrent upload size")
	v.concurrentDownloadLimitMB = cmdVolume.Flag.Int("concurrentDownloadLimitMB", 256, "limit total concurrent download size")
	v.pprof = cmdVolume.Flag.Bool("pprof", false, "enable pprof http handlers. precludes --memprofile and --cpuprofile")
	v.metricsHttpPort = cmdVolume.Flag.Int("metricsPort", 0, "Prometheus metrics listen port")
	v.metricsHttpIp = cmdVolume.Flag.String("metricsIp", "", "metrics listen ip. If empty, default to same as -ip.bind option.")
	v.idxFolder = cmdVolume.Flag.String("dir.idx", "", "directory to store .idx files")
	v.inflightUploadDataTimeout = cmdVolume.Flag.Duration("inflightUploadDataTimeout", 60*time.Second, "inflight upload data wait timeout of volume servers")
	v.inflightDownloadDataTimeout = cmdVolume.Flag.Duration("inflightDownloadDataTimeout", 60*time.Second, "inflight download data wait timeout of volume servers")
	v.hasSlowRead = cmdVolume.Flag.Bool("hasSlowRead", true, "<experimental> if true, this prevents slow reads from blocking other requests, but large file read P99 latency will increase.")
	v.readBufferSizeMB = cmdVolume.Flag.Int("readBufferSizeMB", 4, "<experimental> larger values can optimize query performance but will increase some memory usage,Use with hasSlowRead normally.")
}

// cmdVolume Volume Server 命令的定义
var cmdVolume = &Command{
	UsageLine: "volume -port=8080 -dir=/tmp -max=5 -ip=server_name -master=localhost:9333",
	Short:     "start a volume server",
	Long: `start a volume server to provide storage spaces

  `,
}

var (
	// volumeFolders 存储数据的目录列表（逗号分隔）
	volumeFolders         = cmdVolume.Flag.String("dir", os.TempDir(), "directories to store data files. dir[,dir]...")
	// maxVolumeCounts 每个目录的最大卷数量（逗号分隔，与 dir 对应）
	// 如果设为 0，会根据磁盘空间自动计算
	maxVolumeCounts       = cmdVolume.Flag.String("max", "8", "maximum numbers of volumes, count[,count]... If set to zero, the limit will be auto configured as free disk space divided by volume size.")
	// volumeWhiteListOption IP 白名单
	volumeWhiteListOption = cmdVolume.Flag.String("whiteList", "", "comma separated Ip addresses having write permission. No limit if empty.")
	// minFreeSpacePercent 最小空闲空间百分比（已废弃，使用 minFreeSpace）
	minFreeSpacePercent   = cmdVolume.Flag.String("minFreeSpacePercent", "1", "minimum free disk space (default to 1%). Low disk space will mark all volumes as ReadOnly (deprecated, use minFreeSpace instead).")
	// minFreeSpace 最小空闲空间
	// 值<=100 表示百分比，>100 表示字节数（支持 GiB 等单位）
	minFreeSpace          = cmdVolume.Flag.String("minFreeSpace", "", "min free disk space (value<=100 as percentage like 1, other as human readable bytes, like 10GiB). Low disk space will mark all volumes as ReadOnly.")
)

// runVolume Volume Server 的启动入口函数
//
// 这是 Volume Server 启动的主流程，完整的启动步骤包括：
//
// 1. 安全配置加载
//    - 加载 security.toml 配置文件
//    - 配置 JWT 签名密钥、TLS 证书等
//
// 2. 性能分析配置
//    - 设置 CPU 和内存 profiling（如果启用）
//    - 用于性能调优和问题诊断
//
// 3. Prometheus 监控服务启动
//    - 启动独立的 metrics HTTP 服务器
//    - 暴露监控指标供 Prometheus 采集
//
// 4. Master 节点配置解析
//    - 解析 Master Server 地址列表
//    - 支持多个 Master 实现高可用
//
// 5. Volume Server 初始化和启动
//    - 创建 VolumeServer 实例
//    - 加载现有的 Volume 和索引
//    - 启动 HTTP 和 gRPC 服务
//    - 开始向 Master 发送心跳
//
// 参数：
//   cmd: 命令对象（包含所有命令行标志）
//   args: 额外的命令行参数（通常为空）
//
// 返回：
//   bool: 总是返回 true，表示命令已执行
func runVolume(cmd *Command, args []string) bool {

	// === 第一步：加载安全配置 ===
	// 加载 security.toml 文件，配置 JWT、TLS 等安全相关参数
	// 这必须在其他初始化之前完成，因为后续的网络连接需要这些配置
	util.LoadSecurityConfiguration()

	// === 第二步：设置性能分析 ===
	// 如果用户没有启用 --pprof HTTP 接口，则使用传统的 profiling 方式
	// --pprof 提供运行时的 profiling 端点，而 --cpuprofile/--memprofile 输出到文件
	if !*v.pprof {
		grace.SetupProfiling(*v.cpuProfile, *v.memProfile)
	}

	// === 第三步：确定 Metrics 服务的监听地址 ===
	// 按优先级顺序：metricsHttpIp > bindIp > ip
	// 如果没有专门指定，则使用绑定 IP 或服务器 IP
	switch {
	case *v.metricsHttpIp != "":
		// 使用专门指定的 metrics IP
	case *v.bindIp != "":
		// 回退到绑定 IP
		*v.metricsHttpIp = *v.bindIp
	case *v.ip != "":
		// 回退到服务器 IP
		*v.metricsHttpIp = *v.ip
	}

	// === 第四步：启动 Prometheus Metrics 服务器 ===
	// 在独立的 goroutine 中运行，不阻塞主流程
	// 这个服务器暴露 /metrics 端点供 Prometheus 抓取指标
	go stats_collect.StartMetricsServer(*v.metricsHttpIp, *v.metricsHttpPort)

	// === 第五步：解析 Master Server 地址 ===
	// 向后兼容：如果使用了废弃的 -mserver 参数，将其值复制到 -master
	if *v.mserverString != "" {
		*v.mastersString = *v.mserverString
	}

	// === 第六步：解析配置参数 ===
	// 解析最小空闲空间配置（支持百分比和绝对值两种方式）
	minFreeSpaces := util.MustParseMinFreeSpace(*minFreeSpace, *minFreeSpacePercent)
	// 解析 Master Server 地址字符串为结构化的地址列表
	// 格式: "host1:port1,host2:port2,host3:port3"
	v.masters = pb.ServerAddresses(*v.mastersString).ToAddresses()

	// === 第七步：启动 Volume Server ===
	// 这是最核心的步骤，包含：
	// - 配置验证和解析
	// - 创建 VolumeServer 实例
	// - 初始化存储引擎（Store）
	// - 加载已有的 Volume 和索引文件
	// - 启动 HTTP 和 gRPC 服务器
	// - 开始心跳循环
	v.startVolumeServer(*volumeFolders, *maxVolumeCounts, *volumeWhiteListOption, minFreeSpaces)

	return true
}

// startVolumeServer 启动 Volume Server 的核心方法
//
// 这是 Volume Server 初始化的最核心部分，执行以下操作：
//
// 【阶段 1：配置验证和解析】
// 1. 解析和验证存储目录配置
// 2. 解析卷数量限制
// 3. 解析磁盘空间限制
// 4. 解析磁盘类型配置
// 5. 配置网络地址和端口
//
// 【阶段 2：创建服务组件】
// 6. 创建 HTTP 路由器（管理端口和公共端口）
// 7. 选择 Needle 索引类型（内存或 LevelDB）
// 8. 创建 VolumeServer 实例
//
// 【阶段 3：启动服务】
// 9. 启动 gRPC 服务器（用于节点间通信）
// 10. 启动公共 HTTP 服务器（用于文件访问）
// 11. 启动集群 HTTP 服务器（用于管理操作）
//
// 【阶段 4：生命周期管理】
// 12. 注册热重载回调（支持动态加载新卷）
// 13. 注册优雅关闭回调（确保数据安全）
// 14. 阻塞主线程，等待关闭信号
//
// 参数：
//   volumeFolders: 逗号分隔的存储目录列表，如 "/data1,/data2,/data3"
//   maxVolumeCounts: 逗号分隔的卷数量限制，如 "100,100,100"
//   volumeWhiteListOption: 逗号分隔的 IP 白名单
//   minFreeSpaces: 每个目录的最小空闲空间配置
func (v VolumeServerOptions) startVolumeServer(volumeFolders, maxVolumeCounts, volumeWhiteListOption string, minFreeSpaces []util.MinFreeSpace) {

	// ============================================================
	// 【阶段 1：配置验证和解析】
	// ============================================================

	// === 步骤 1.1：解析和验证存储目录 ===
	// 将逗号分隔的目录字符串解析为数组
	// 例如: "/data1,/data2" -> ["/data1", "/data2"]
	v.folders = strings.Split(volumeFolders, ",")

	// 验证每个目录的可写性
	// 确保 Volume Server 有权限在这些目录中创建和修改文件
	// 如果任何一个目录不可写，程序将立即退出
	for _, folder := range v.folders {
		if err := util.TestFolderWritable(util.ResolvePath(folder)); err != nil {
			glog.Fatalf("Check Data Folder(-dir) Writable %s : %s", folder, err)
		}
	}

	// === 步骤 1.2：解析每个目录的卷数量限制 ===
	// 每个目录可以配置不同的最大卷数量
	// 例如: "100,200,300" 表示三个目录分别可存储 100、200、300 个卷
	maxCountStrings := strings.Split(maxVolumeCounts, ",")
	for _, maxString := range maxCountStrings {
		if max, e := strconv.ParseInt(maxString, 10, 64); e == nil {
			v.folderMaxLimits = append(v.folderMaxLimits, int32(max))
		} else {
			glog.Fatalf("The max specified in -max not a valid number %s", maxString)
		}
	}

	// 如果只指定了一个限制值但有多个目录，将该值应用到所有目录
	// 例如: 目录 "/data1,/data2,/data3"，限制 "100" -> [100, 100, 100]
	if len(v.folderMaxLimits) == 1 && len(v.folders) > 1 {
		for i := 0; i < len(v.folders)-1; i++ {
			v.folderMaxLimits = append(v.folderMaxLimits, v.folderMaxLimits[0])
		}
	}

	// 验证目录数量和限制数量必须匹配
	if len(v.folders) != len(v.folderMaxLimits) {
		glog.Fatalf("%d directories by -dir, but only %d max is set by -max", len(v.folders), len(v.folderMaxLimits))
	}

	// === 步骤 1.3：解析最小空闲空间配置 ===
	// 每个目录都需要保持一定的空闲空间
	// 当空闲空间低于阈值时，该目录的所有卷将变为只读
	if len(minFreeSpaces) == 1 && len(v.folders) > 1 {
		for i := 0; i < len(v.folders)-1; i++ {
			minFreeSpaces = append(minFreeSpaces, minFreeSpaces[0])
		}
	}
	if len(v.folders) != len(minFreeSpaces) {
		glog.Fatalf("%d directories by -dir, but only %d minFreeSpacePercent is set by -minFreeSpacePercent", len(v.folders), len(minFreeSpaces))
	}

	// === 步骤 1.4：解析磁盘类型配置 ===
	// 支持为不同目录指定不同的磁盘类型（HDD、SSD 等）
	// Master Server 在分配卷时会考虑磁盘类型
	// 例如: "hdd,ssd,ssd" 表示第一个目录是 HDD，后两个是 SSD
	var diskTypes []types.DiskType
	diskTypeStrings := strings.Split(*v.diskType, ",")
	for _, diskTypeString := range diskTypeStrings {
		diskTypes = append(diskTypes, types.ToDiskType(diskTypeString))
	}

	// 如果只指定了一个类型但有多个目录，将该类型应用到所有目录
	if len(diskTypes) == 1 && len(v.folders) > 1 {
		for i := 0; i < len(v.folders)-1; i++ {
			diskTypes = append(diskTypes, diskTypes[0])
		}
	}
	if len(v.folders) != len(diskTypes) {
		glog.Fatalf("%d directories by -dir, but only %d disk types is set by -disk", len(v.folders), len(diskTypes))
	}

	// === 步骤 1.5：解析 IP 白名单配置 ===
	// 只有白名单中的 IP 才能执行某些敏感操作（如删除卷）
	v.whiteList = util.StringSplit(volumeWhiteListOption, ",")

	// === 步骤 1.6：确定服务器的 IP 地址 ===
	// 如果没有显式指定 IP，则自动检测主机地址
	// DetectedHostAddress 会选择第一个非环回的网络接口地址
	if *v.ip == "" {
		*v.ip = util.DetectedHostAddress()
		glog.V(0).Infof("detected volume server ip address: %v", *v.ip)
	}

	// 如果没有指定绑定 IP，则使用服务器 IP
	// bindIp 用于绑定网络接口，通常在多网卡环境下需要指定
	if *v.bindIp == "" {
		*v.bindIp = *v.ip
	}

	// === 步骤 1.7：配置端口号 ===
	// publicPort: 对外公开的端口（用于 NAT 环境下的端口映射）
	// 如果没有指定，则使用实际监听端口
	if *v.publicPort == 0 {
		*v.publicPort = *v.port
	}

	// grpcPort: gRPC 服务端口，默认为 HTTP 端口 + 10000
	// gRPC 用于 Volume Server 之间的高效通信（如副本复制、EC 分片传输）
	if *v.portGrpc == 0 {
		*v.portGrpc = 10000 + *v.port
	}

	// publicUrl: 对外公开的访问地址
	// Master Server 会将这个地址返回给客户端用于文件下载
	if *v.publicUrl == "" {
		*v.publicUrl = util.JoinHostPort(*v.ip, *v.publicPort)
	}

	// ============================================================
	// 【阶段 2：创建服务组件】
	// ============================================================

	// === 步骤 2.1：创建 HTTP 路由器 ===
	// volumeMux: 管理端口的路由器，处理管理操作（压缩、删除卷等）
	volumeMux := http.NewServeMux()
	publicVolumeMux := volumeMux

	// 如果配置了独立的公共端口，则创建单独的路由器
	// 这是推荐的生产环境配置，可以隔离公共访问和管理操作
	// 优势：
	// - 安全性：管理端口可以配置防火墙限制访问
	// - 性能：公共流量和管理流量分离，互不影响
	if v.isSeparatedPublicPort() {
		publicVolumeMux = http.NewServeMux()
	}

	// === 步骤 2.2：配置 pprof 性能分析端点 ===
	// 如果启用了 pprof，在管理端口上注册性能分析处理器
	// 可以通过 go tool pprof 访问这些端点进行性能调优
	if *v.pprof {
		volumeMux.HandleFunc("/debug/pprof/", httppprof.Index)
		volumeMux.HandleFunc("/debug/pprof/cmdline", httppprof.Cmdline)
		volumeMux.HandleFunc("/debug/pprof/profile", httppprof.Profile)
		volumeMux.HandleFunc("/debug/pprof/symbol", httppprof.Symbol)
		volumeMux.HandleFunc("/debug/pprof/trace", httppprof.Trace)
	}

	// === 步骤 2.3：选择 Needle 索引类型 ===
	// Needle 索引用于快速查找文件在卷中的位置
	// 不同的索引类型在内存占用和性能之间有不同的权衡：
	//
	// - NeedleMapInMemory (memory):
	//   * 全部索引加载到内存，查询速度最快
	//   * 内存占用大：每百万个文件约需 32MB 内存
	//   * 适用于小规模部署或内存充足的环境
	//
	// - NeedleMapLevelDb (leveldb):
	//   * 使用 LevelDB 存储索引，内存占用小
	//   * 查询需要磁盘 I/O，稍慢
	//   * 适用于大规模部署，文件数量超过千万级
	//
	// - NeedleMapLevelDbMedium/Large:
	//   * LevelDB 的不同缓存大小配置
	//   * Medium: 8MB 缓存，Large: 32MB 缓存
	//   * 在内存和性能之间取得平衡
	volumeNeedleMapKind := storage.NeedleMapInMemory
	switch *v.indexType {
	case "leveldb":
		volumeNeedleMapKind = storage.NeedleMapLevelDb
	case "leveldbMedium":
		volumeNeedleMapKind = storage.NeedleMapLevelDbMedium
	case "leveldbLarge":
		volumeNeedleMapKind = storage.NeedleMapLevelDbLarge
	}

	// === 步骤 2.4：创建 VolumeServer 实例 ===
	// 这是最关键的一步，NewVolumeServer 会执行：
	// 1. 创建 Store 实例（存储引擎）
	// 2. 扫描数据目录，加载所有现有的 Volume
	// 3. 为每个 Volume 加载或创建索引（Needle Map）
	// 4. 启动心跳协程，向 Master 报告状态
	// 5. 注册 HTTP 路由处理器
	//
	// Volume 加载过程（在 Store.NewStore 中执行）：
	// - 遍历每个数据目录
	// - 识别 .dat 和 .idx 文件对
	// - 验证文件完整性（检查 .note 文件）
	// - 加载 SuperBlock（卷的元数据）
	// - 根据索引类型加载 Needle 索引
	// - 对于 EC Volume，加载分片信息
	//
	// Needle 索引加载：
	// - 如果使用内存索引：读取 .idx 文件到内存 map
	// - 如果使用 LevelDB：打开 LevelDB 数据库
	// - 索引结构：NeedleId -> (Offset, Size, Flags)
	volumeServer := weed_server.NewVolumeServer(volumeMux, publicVolumeMux,
		*v.ip, *v.port, *v.portGrpc, *v.publicUrl,
		v.folders, v.folderMaxLimits, minFreeSpaces, diskTypes,
		*v.idxFolder,
		volumeNeedleMapKind,
		v.masters, constants.VolumePulseSeconds, *v.dataCenter, *v.rack,
		v.whiteList,
		*v.fixJpgOrientation, *v.readMode,
		*v.compactionMBPerSecond,
		*v.fileSizeLimitMB,
		int64(*v.concurrentUploadLimitMB)*1024*1024,
		int64(*v.concurrentDownloadLimitMB)*1024*1024,
		*v.inflightUploadDataTimeout,
		*v.inflightDownloadDataTimeout,
		*v.hasSlowRead,
		*v.readBufferSizeMB,
		*v.ldbTimeout,
	)

	// ============================================================
	// 【阶段 3：启动服务】
	// ============================================================

	// === 步骤 3.1：启动 gRPC 服务器 ===
	// gRPC 用于 Volume Server 之间的高效通信，主要用途：
	// - 副本数据同步（写入时同步到其他副本）
	// - EC 分片传输（纠删码分片的传输）
	// - Volume 迁移和复制
	// - 集群内部管理命令
	grpcS := v.startGrpcService(volumeServer)

	// === 步骤 3.2：启动公共 HTTP 服务器（如果配置了独立端口）===
	// 公共端口专门处理文件的读写请求，与管理端口隔离
	var publicHttpDown httpdown.Server
	if v.isSeparatedPublicPort() {
		publicHttpDown = v.startPublicHttpService(publicVolumeMux)
		if nil == publicHttpDown {
			glog.Fatalf("start public http service failed")
		}
	}

	// === 步骤 3.3：启动集群 HTTP 服务器 ===
	// 集群端口处理管理操作和内部 API：
	// - /status: 服务器状态查询
	// - /admin/*: 管理操作（压缩、删除卷等）
	// - /vol/*: 卷管理操作
	clusterHttpServer := v.startClusterHttpService(volumeMux)

	// ============================================================
	// 【阶段 4：生命周期管理】
	// ============================================================

	// === 步骤 4.1：注册热重载回调 ===
	// 当收到 SIGHUP 信号时，重新加载：
	// - 新添加的 Volume 文件（LoadNewVolumes）
	// - 配置文件（Reload）
	// 这允许在不重启服务器的情况下添加新卷或更新配置
	grace.OnReload(volumeServer.LoadNewVolumes)
	grace.OnReload(volumeServer.Reload)

	// === 步骤 4.2：注册优雅关闭回调 ===
	// 当收到 SIGTERM 或 SIGINT 信号时执行：
	stopChan := make(chan bool)
	grace.OnInterrupt(func() {
		fmt.Println("volume server has been killed")

		// 第一步：停止心跳
		// 通知 Master 本服务器即将下线，Master 会停止分配新请求
		if !volumeServer.StopHeartbeat() {
			volumeServer.SetStopping()
			// 等待一段时间，让正在处理的请求完成
			// 这个时间窗口允许客户端的请求正常完成，避免中断
			glog.V(0).Infof("stop send heartbeat and wait %d seconds until shutdown ...", *v.preStopSeconds)
			time.Sleep(time.Duration(*v.preStopSeconds) * time.Second)
		}

		// 第二步：执行完整关闭
		// 按顺序关闭各个服务组件，确保数据安全
		shutdown(publicHttpDown, clusterHttpServer, grpcS, volumeServer)
		stopChan <- true
	})

	// === 步骤 4.3：阻塞主线程 ===
	// 等待关闭信号，保持服务器运行
	select {
	case <-stopChan:
	}

}

// shutdown 优雅关闭 Volume Server 的所有服务组件
//
// 关闭顺序非常重要，遵循以下原则：
// 1. 先关闭接收新请求的入口（公共 HTTP 服务器）
// 2. 再关闭集群内部通信（集群 HTTP 服务器）
// 3. 然后关闭 gRPC 服务器
// 4. 最后关闭存储引擎，刷新数据到磁盘
//
// 这个顺序确保：
// - 不会接收新的用户请求
// - 正在处理的请求能够完成
// - 所有数据都安全写入磁盘
// - 资源得到正确释放
//
// 参数：
//   publicHttpDown: 公共 HTTP 服务器（可能为 nil）
//   clusterHttpServer: 集群 HTTP 服务器
//   grpcS: gRPC 服务器
//   volumeServer: Volume Server 实例
func shutdown(publicHttpDown httpdown.Server, clusterHttpServer httpdown.Server, grpcS *grpc.Server, volumeServer *weed_server.VolumeServer) {

	// === 第一步：关闭公共 HTTP 服务器 ===
	// 停止接收新的用户请求（文件上传/下载）
	// 这是最先关闭的，因为它面向外部用户
	if nil != publicHttpDown {
		glog.V(0).Infof("stop public http server ... ")
		if err := publicHttpDown.Stop(); err != nil {
			glog.Warningf("stop the public http server failed, %v", err)
		}
	}

	// === 第二步：关闭集群 HTTP 服务器 ===
	// 停止接收管理命令和集群内部的 HTTP 请求
	glog.V(0).Infof("graceful stop cluster http server ... ")
	if err := clusterHttpServer.Stop(); err != nil {
		glog.Warningf("stop the cluster http server failed, %v", err)
	}

	// === 第三步：优雅关闭 gRPC 服务器 ===
	// GracefulStop 会等待所有正在处理的 gRPC 请求完成
	// 包括副本同步、EC 分片传输等重要操作
	glog.V(0).Infof("graceful stop gRPC ...")
	grpcS.GracefulStop()

	// === 第四步：关闭存储引擎 ===
	// 这是最后一步，执行：
	// - 关闭所有 Volume 文件句柄
	// - 刷新所有缓冲区到磁盘
	// - 关闭索引数据库（LevelDB）
	// - 释放内存资源
	volumeServer.Shutdown()

	// === 第五步：停止 CPU profiling ===
	// 如果启用了 CPU profiling，停止并写入数据
	pprof.StopCPUProfile()

}

// isSeparatedPublicPort 检查是否配置了独立的公共端口
//
// 返回：
//   bool: true 表示公共端口和管理端口分离
//
// 使用场景：
// - 生产环境建议分离端口，增强安全性
// - 可以对公共端口和管理端口应用不同的防火墙规则
// - 可以独立监控和限流两类流量
func (v VolumeServerOptions) isSeparatedPublicPort() bool {
	return *v.publicPort != *v.port
}

// startGrpcService 启动 gRPC 服务器
//
// gRPC 服务器处理 Volume Server 之间的高效二进制通信，主要功能：
// 1. 副本同步：写入时实时同步数据到其他副本节点
// 2. EC 分片传输：纠删码分片在节点间的传输
// 3. Volume 复制：整个卷的复制和迁移
// 4. 批量操作：批量删除、查询等高效操作
//
// gRPC 相比 HTTP 的优势：
// - 二进制协议，性能更高
// - 支持流式传输，适合大文件传输
// - 内置负载均衡和重试机制
// - 强类型接口，更安全
//
// 参数：
//   vs: VolumeServer 实例，实现了 gRPC 服务接口
//
// 返回：
//   *grpc.Server: gRPC 服务器实例，用于优雅关闭
func (v VolumeServerOptions) startGrpcService(vs volume_server_pb.VolumeServerServer) *grpc.Server {
	grpcPort := *v.portGrpc

	// 创建 gRPC 监听器
	// 使用 util.NewListener 而不是 net.Listen，支持优雅重启
	grpcL, err := util.NewListener(util.JoinHostPort(*v.bindIp, grpcPort), 0)
	if err != nil {
		glog.Fatalf("failed to listen on grpc port %d: %v", grpcPort, err)
	}

	// 创建 gRPC 服务器，加载 TLS 配置
	// security.LoadServerTLS 从配置文件读取证书和密钥
	grpcS := pb.NewGrpcServer(security.LoadServerTLS(util.GetViper(), "grpc.volume"))

	// 注册 VolumeServer 服务
	// 这会注册所有 gRPC 方法（写入、读取、复制等）
	volume_server_pb.RegisterVolumeServerServer(grpcS, vs)

	// 注册反射服务
	// 允许使用 grpcurl 等工具调试 gRPC 接口
	reflection.Register(grpcS)

	// 在独立的 goroutine 中启动 gRPC 服务器
	go func() {
		if err := grpcS.Serve(grpcL); err != nil {
			glog.Fatalf("start gRPC service failed, %s", err)
		}
	}()

	return grpcS
}

// startPublicHttpService 启动公共 HTTP 服务器
//
// 公共 HTTP 服务器专门处理面向用户的文件操作：
// - GET /{volumeId}/{fileId}: 下载文件
// - POST /{volumeId}/{fileId}: 上传文件
// - DELETE /{volumeId}/{fileId}: 删除文件
//
// 为什么要分离公共端口：
// 1. 安全性：管理端口可以限制只允许内网访问
// 2. 性能：可以对两类流量应用不同的限流策略
// 3. 监控：可以分别监控用户流量和管理流量
// 4. 负载均衡：可以使用不同的负载均衡策略
//
// 参数：
//   handler: HTTP 处理器（路由器）
//
// 返回：
//   httpdown.Server: HTTP 服务器实例，支持优雅关闭
func (v VolumeServerOptions) startPublicHttpService(handler http.Handler) httpdown.Server {
	publicListeningAddress := util.JoinHostPort(*v.bindIp, *v.publicPort)
	glog.V(0).Infoln("Start Seaweed volume server", version.Version(), "public at", publicListeningAddress)

	// 创建 HTTP 监听器，配置空闲连接超时
	publicListener, e := util.NewListener(publicListeningAddress, time.Duration(*v.idleConnectionTimeout)*time.Second)
	if e != nil {
		glog.Fatalf("Volume server listener error:%v", e)
	}

	// 配置优雅关闭参数
	// StopTimeout: 停止接收新请求后，等待现有请求完成的时间
	// KillTimeout: 强制关闭所有连接的时间
	pubHttp := httpdown.HTTP{StopTimeout: 5 * time.Minute, KillTimeout: 5 * time.Minute}

	// 启动 HTTP 服务器
	publicHttpDown := pubHttp.Serve(&http.Server{Handler: handler}, publicListener)

	// 在独立的 goroutine 中等待服务器关闭
	go func() {
		if err := publicHttpDown.Wait(); err != nil {
			glog.Errorf("public http down wait failed, %v", err)
		}
	}()

	return publicHttpDown
}

// startClusterHttpService 启动集群 HTTP 服务器
//
// 集群 HTTP 服务器处理管理操作和集群内部通信：
// - /status: 查询服务器状态（卷列表、磁盘使用情况等）
// - /admin/assign_volume: 分配新卷
// - /admin/delete_collection: 删除集合
// - /admin/vacuum: 垃圾回收
// - /vol/compact: 压缩卷
// - /vol/mount: 挂载卷
// - /vol/unmount: 卸载卷
//
// 支持 TLS：
// - 如果配置了证书，则使用 HTTPS
// - 支持客户端证书认证（mTLS）
//
// 参数：
//   handler: HTTP 处理器（路由器）
//
// 返回：
//   httpdown.Server: HTTP 服务器实例，支持优雅关闭
func (v VolumeServerOptions) startClusterHttpService(handler http.Handler) httpdown.Server {
	var (
		certFile, keyFile string
	)

	// 检查是否配置了 TLS 证书
	if viper.GetString("https.volume.key") != "" {
		certFile = viper.GetString("https.volume.cert")
		keyFile = viper.GetString("https.volume.key")
	}

	listeningAddress := util.JoinHostPort(*v.bindIp, *v.port)
	glog.V(0).Infof("Start Seaweed volume server %s at %s", version.Version(), listeningAddress)

	// 创建监听器，配置空闲连接超时
	listener, e := util.NewListener(listeningAddress, time.Duration(*v.idleConnectionTimeout)*time.Second)
	if e != nil {
		glog.Fatalf("Volume server listener error:%v", e)
	}

	// 配置优雅关闭参数
	// 集群服务器的超时时间比公共服务器短
	// 因为集群内部操作通常更快完成
	httpDown := httpdown.HTTP{
		KillTimeout: time.Minute,
		StopTimeout: 30 * time.Second,
		CertFile:    certFile,
		KeyFile:     keyFile}
	httpS := &http.Server{Handler: handler}

	// 配置客户端证书认证（mTLS）
	// 如果配置了 CA 证书，则要求客户端提供证书
	if viper.GetString("https.volume.ca") != "" {
		clientCertFile := viper.GetString("https.volume.ca")
		httpS.TLSConfig = security.LoadClientTLSHTTP(clientCertFile)
		security.FixTlsConfig(util.GetViper(), httpS.TLSConfig)
	}

	// 启动 HTTP 服务器
	clusterHttpServer := httpDown.Serve(httpS, listener)

	// 在独立的 goroutine 中等待服务器关闭
	go func() {
		if e := clusterHttpServer.Wait(); e != nil {
			glog.Fatalf("Volume server fail to serve: %v", e)
		}
	}()

	return clusterHttpServer
}
