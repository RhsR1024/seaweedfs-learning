// Package stats 提供了 SeaweedFS 系统的 Prometheus 指标收集和导出功能
// 它定义了所有组件(Master、VolumeServer、Filer、S3)的性能指标和监控数据
package stats

import (
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/push"
	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// Readonly volume types - 只读 Volume 的类型定义
const (
	// Namespace Prometheus 指标的命名空间,所有指标以此为前缀
	Namespace = "SeaweedFS"

	// IsReadOnly Volume 完全只读,不允许任何写入和删除操作
	IsReadOnly = "IsReadOnly"

	// NoWriteOrDelete Volume 禁止写入和删除,通常用于归档场景
	NoWriteOrDelete = "noWriteOrDelete"

	// NoWriteCanDelete Volume 禁止写入但允许删除,用于清理旧数据
	NoWriteCanDelete = "noWriteCanDelete"

	// IsDiskSpaceLow Volume 所在磁盘空间不足,临时设为只读保护
	IsDiskSpaceLow = "isDiskSpaceLow"

	// bucketAtiveTTL S3 bucket 活跃时间 TTL,超时后清理其指标
	// 避免不活跃的 bucket 指标占用过多内存
	bucketAtiveTTL = 10 * time.Minute
)

// readOnlyVolumeTypes 所有只读 Volume 类型的数组,用于枚举和遍历
var readOnlyVolumeTypes = [4]string{IsReadOnly, NoWriteOrDelete, NoWriteCanDelete, IsDiskSpaceLow}

// bucketLastActiveTsNs 记录每个 S3 bucket 最后活跃时间戳(纳秒)
// key: bucket 名称, value: 最后活跃时间的 Unix 纳秒时间戳
var bucketLastActiveTsNs map[string]int64 = map[string]int64{}

// bucketLastActiveLock 保护 bucketLastActiveTsNs map 的并发访问锁
var bucketLastActiveLock sync.Mutex

var (
	// Gather Prometheus 注册表,用于注册和收集所有指标
	// 所有自定义指标都需要先注册到这个 Registry
	Gather = prometheus.NewRegistry()

	// ============================================================
	// Master 相关指标
	// ============================================================

	// MasterClientConnectCounter Master 客户端连接更新计数器
	// 记录 wdclient 与 Master Leader 的连接更新次数
	// labels: type - 更新类型(connect/disconnect/switch)
	MasterClientConnectCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "wdclient",
			Name:      "connect_updates",
			Help:      "Counter of master client leader updates.",
		}, []string{"type"})

	// MasterRaftIsleader Master 节点是否为 Raft Leader
	// 1 表示当前节点是 Leader, 0 表示 Follower
	MasterRaftIsleader = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "master",
			Name:      "is_leader",
			Help:      "is leader",
		})

	// MasterAdminLock Master 管理锁状态
	// 记录哪个客户端持有管理锁,用于维护操作的互斥
	// labels: client - 持有锁的客户端标识
	MasterAdminLock = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "master",
			Name:      "admin_lock",
			Help:      "admin lock",
		}, []string{"client"})

	// MasterReceivedHeartbeatCounter Master 接收心跳计数器
	// 记录从 Volume Server 或其他节点接收到的心跳次数
	// labels: type - 心跳类型(volume/dataNode)
	MasterReceivedHeartbeatCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "master",
			Name:      "received_heartbeats",
			Help:      "Counter of master received heartbeat.",
		}, []string{"type"})

	// MasterReplicaPlacementMismatch 副本放置策略不匹配
	// 当 Volume 的实际副本分布不符合预期策略时记录
	// labels: collection - 集合名, id - Volume ID
	MasterReplicaPlacementMismatch = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "master",
			Name:      "replica_placement_mismatch",
			Help:      "replica placement mismatch",
		}, []string{"collection", "id"})

	// MasterVolumeLayoutWritable 可写 Volume 数量
	// 记录每个 collection 下可接受写入的 Volume 数量
	// labels: collection, disk(磁盘类型), rp(副本策略), ttl(生存时间)
	MasterVolumeLayoutWritable = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "master",
			Name:      "volume_layout_writable",
			Help:      "Number of writable volumes in volume layouts",
		}, []string{"collection", "disk", "rp", "ttl"})

	// MasterVolumeLayoutCrowded 拥挤 Volume 数量
	// Volume 使用率过高被标记为"拥挤",优先分配到其他 Volume
	// labels: collection, disk, rp, ttl
	MasterVolumeLayoutCrowded = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "master",
			Name:      "volume_layout_crowded",
			Help:      "Number of crowded volumes in volume layouts",
		}, []string{"collection", "disk", "rp", "ttl"})

	// MasterPickForWriteErrorCounter 选择写入 Volume 失败计数器
	// 当无法为写入请求找到合适的 Volume 时递增
	MasterPickForWriteErrorCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "master",
			Name:      "pick_for_write_error",
			Help:      "Counter of master pick for write error",
		})

	// MasterBroadcastToFullErrorCounter 广播到满消息通道错误计数器
	// 当消息通道满载无法接收新消息时递增
	MasterBroadcastToFullErrorCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "master",
			Name:      "broadcast_to_full",
			Help:      "Counter of master broadcast send to full message channel err",
		})

	// MasterLeaderChangeCounter Master Leader 变更计数器
	// 记录 Raft Leader 选举和切换的次数
	// labels: type - 变更类型
	MasterLeaderChangeCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "master",
			Name:      "leader_changes",
			Help:      "Counter of master leader changes.",
		}, []string{"type"})

	// ============================================================
	// Filer 相关指标
	// Filer 提供文件系统接口,处理文件元数据和分块管理
	// ============================================================

	// FilerRequestCounter Filer 请求计数器
	// 记录各类 HTTP 请求的数量和响应码
	// labels: type - 请求类型(GET/POST/DELETE等), code - HTTP 状态码
	FilerRequestCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "filer",
			Name:      "request_total",
			Help:      "Counter of filer requests.",
		}, []string{"type", "code"})

	// FilerHandlerCounter Filer 处理器计数器
	// 记录不同业务处理器的调用次数
	// labels: type - 处理器类型
	FilerHandlerCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "filer",
			Name:      "handler_total",
			Help:      "Counter of filer handlers.",
		}, []string{"type"})

	// FilerRequestHistogram Filer 请求耗时直方图
	// 记录请求处理时间的分布情况,用于性能分析
	// buckets: 从 0.1ms 到约 1677 秒,24 个指数级桶
	// labels: type - 请求类型
	FilerRequestHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: "filer",
			Name:      "request_seconds",
			Help:      "Bucketed histogram of filer request processing time.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 24),
		}, []string{"type"})

	// FilerInFlightRequestsGauge Filer 正在处理的请求数量
	// 实时显示当前并发处理的请求数,用于监控负载
	// labels: type - 请求类型
	FilerInFlightRequestsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "filer",
			Name:      "in_flight_requests",
			Help:      "Current number of in-flight requests being handled by filer.",
		}, []string{"type"})

	// FilerServerLastSendTsOfSubscribeGauge Filer 订阅最后发送时间戳
	// 记录 Filer 间元数据同步订阅的最后发送时间,用于监控同步延迟
	// labels: sourceFiler - 源 Filer, clientName - 客户端名, path - 订阅路径
	FilerServerLastSendTsOfSubscribeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "filer",
			Name:      "last_send_timestamp_of_subscribe",
			Help:      "The last send timestamp of the filer subscription.",
		}, []string{"sourceFiler", "clientName", "path"})

	// FilerStoreCounter Filer 存储后端请求计数器
	// 记录对底层元数据存储(MySQL/Redis/LevelDB等)的操作次数
	// labels: store - 存储类型, type - 操作类型(Insert/Update/Delete/Find)
	FilerStoreCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "filerStore",
			Name:      "request_total",
			Help:      "Counter of filer store requests.",
		}, []string{"store", "type"})

	// FilerStoreHistogram Filer 存储后端请求耗时直方图
	// 记录对元数据存储操作的耗时分布
	// labels: store, type
	FilerStoreHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: "filerStore",
			Name:      "request_seconds",
			Help:      "Bucketed histogram of filer store request processing time.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 24),
		}, []string{"store", "type"})

	// FilerSyncOffsetGauge Filer 同步偏移量
	// 记录 Filer 间元数据同步的当前偏移量,用于监控同步进度
	// labels: sourceFiler, targetFiler, clientName, path
	FilerSyncOffsetGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "filerSync",
			Name:      "sync_offset",
			Help:      "The offset of the filer synchronization service.",
		}, []string{"sourceFiler", "targetFiler", "clientName", "path"})

	// ============================================================
	// Volume Server 相关指标
	// Volume Server 存储实际的数据文件,处理文件的读写和存储管理
	// ============================================================

	// VolumeServerRequestCounter Volume Server 请求计数器
	// 记录各类 HTTP 请求的数量和响应码
	// labels: type - 请求类型(GET/POST/DELETE等), code - HTTP 状态码
	VolumeServerRequestCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "volumeServer",
			Name:      "request_total",
			Help:      "Counter of volume server requests.",
		}, []string{"type", "code"})

	// VolumeServerHandlerCounter Volume Server 处理器计数器
	// 记录不同业务处理器的调用次数
	// labels: type - 处理器类型
	VolumeServerHandlerCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "volumeServer",
			Name:      "handler_total",
			Help:      "Counter of volume server handlers.",
		}, []string{"type"})

	// VolumeServerVacuumingCompactCounter Volume 压缩操作计数器
	// 记录 Volume 垃圾回收(vacuuming)的压缩阶段执行次数
	// 压缩操作将删除标记的 needle 清理掉,回收磁盘空间
	// labels: success - 是否成功(true/false)
	VolumeServerVacuumingCompactCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "volumeServer",
			Name:      "vacuuming_compact_count",
			Help:      "Counter of volume vacuuming Compact counter",
		}, []string{"success"})

	// VolumeServerVacuumingCommitCounter Volume 提交操作计数器
	// 记录 Volume 垃圾回收的提交阶段执行次数
	// 提交操作将压缩后的新 Volume 正式替换旧的 Volume
	// labels: success - 是否成功(true/false)
	VolumeServerVacuumingCommitCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "volumeServer",
			Name:      "vacuuming_commit_count",
			Help:      "Counter of volume vacuuming commit counter",
		}, []string{"success"})

	// VolumeServerVacuumingHistogram Volume 垃圾回收耗时直方图
	// 记录垃圾回收操作的耗时分布,用于性能分析
	// labels: type - 操作类型(compact/commit)
	VolumeServerVacuumingHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: "volumeServer",
			Name:      "vacuuming_seconds",
			Help:      "Bucketed histogram of volume server vacuuming processing time.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 24),
		}, []string{"type"})

	// VolumeServerRequestHistogram Volume Server 请求耗时直方图
	// 记录请求处理时间的分布情况
	// labels: type - 请求类型
	VolumeServerRequestHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: "volumeServer",
			Name:      "request_seconds",
			Help:      "Bucketed histogram of volume server request processing time.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 24),
		}, []string{"type"})

	// VolumeServerInFlightRequestsGauge Volume Server 正在处理的请求数量
	// 实时显示当前并发处理的请求数
	// labels: type - 请求类型
	VolumeServerInFlightRequestsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "volumeServer",
			Name:      "in_flight_requests",
			Help:      "Current number of in-flight requests being handled by volume server.",
		}, []string{"type"})

	// VolumeServerVolumeGauge Volume 数量
	// 记录当前 Volume Server 上的 Volume 或 Shard 数量
	// labels: collection - 集合名, type - Volume 类型(如 hdd/ssd)
	VolumeServerVolumeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "volumeServer",
			Name:      "volumes",
			Help:      "Number of volumes or shards.",
		}, []string{"collection", "type"})

	// VolumeServerReadOnlyVolumeGauge 只读 Volume 数量
	// 记录只读状态的 Volume 数量,包括各种只读类型
	// labels: collection, type - 只读类型(IsReadOnly/NoWriteOrDelete等)
	VolumeServerReadOnlyVolumeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "volumeServer",
			Name:      "read_only_volumes",
			Help:      "Number of read only volumes.",
		}, []string{"collection", "type"})

	// VolumeServerMaxVolumeCounter 最大 Volume 数量
	// 记录该 Volume Server 配置的最大 Volume 数量限制
	VolumeServerMaxVolumeCounter = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "volumeServer",
			Name:      "max_volumes",
			Help:      "Maximum number of volumes.",
		})

	// VolumeServerDiskSizeGauge Volume 占用磁盘大小
	// 记录 Volume 实际占用的磁盘空间大小(字节)
	// labels: collection, type
	VolumeServerDiskSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "volumeServer",
			Name:      "total_disk_size",
			Help:      "Actual disk size used by volumes.",
		}, []string{"collection", "type"})

	// VolumeServerResourceGauge Volume Server 资源使用情况
	// 记录各类资源的使用情况(CPU、内存、磁盘等)
	// labels: name - 资源名称, type - 资源类型
	VolumeServerResourceGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "volumeServer",
			Name:      "resource",
			Help:      "Resource usage",
		}, []string{"name", "type"})

	// VolumeServerConcurrentDownloadLimit 并发下载限制
	// 设置的并发下载总大小限制(字节),用于流量控制
	VolumeServerConcurrentDownloadLimit = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "volumeServer",
			Name:      "concurrent_download_limit",
			Help:      "Limit total concurrent download size.",
		})

	// VolumeServerConcurrentUploadLimit 并发上传限制
	// 设置的并发上传总大小限制(字节),用于流量控制
	VolumeServerConcurrentUploadLimit = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "volumeServer",
			Name:      "concurrent_upload_limit",
			Help:      "Limit total concurrent upload size.",
		})

	// VolumeServerInFlightDownloadSize 正在进行的下载大小
	// 当前正在传输的下载数据总大小(字节)
	VolumeServerInFlightDownloadSize = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "volumeServer",
			Name:      "in_flight_download_size",
			Help:      "In flight total download size.",
		})

	// VolumeServerInFlightUploadSize 正在进行的上传大小
	// 当前正在传输的上传数据总大小(字节)
	VolumeServerInFlightUploadSize = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "volumeServer",
			Name:      "in_flight_upload_size",
			Help:      "In flight total upload size.",
		})

	// ============================================================
	// S3 相关指标
	// S3 模块提供 Amazon S3 兼容的对象存储接口
	// ============================================================

	// S3RequestCounter S3 请求计数器
	// 记录各类 S3 API 请求的数量和响应码
	// labels: type - 请求类型(PutObject/GetObject等), code - HTTP状态码, bucket - bucket名称
	S3RequestCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "s3",
			Name:      "request_total",
			Help:      "Counter of s3 requests.",
		}, []string{"type", "code", "bucket"})

	// S3HandlerCounter S3 处理器计数器
	// 记录不同 S3 API 处理器的调用次数
	// labels: type - 处理器类型
	S3HandlerCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "s3",
			Name:      "handler_total",
			Help:      "Counter of s3 server handlers.",
		}, []string{"type"})

	// S3RequestHistogram S3 请求耗时直方图
	// 记录 S3 API 请求处理时间的分布情况
	// labels: type - 请求类型, bucket - bucket名称
	S3RequestHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: "s3",
			Name:      "request_seconds",
			Help:      "Bucketed histogram of s3 request processing time.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 24),
		}, []string{"type", "bucket"})

	// S3TimeToFirstByteHistogram S3 首字节响应时间直方图
	// 记录从接收请求到返回第一个字节的时间(毫秒)
	// 这是衡量响应速度的重要指标,特别是对于大文件下载
	// labels: type - 请求类型, bucket - bucket名称
	S3TimeToFirstByteHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: "s3",
			Name:      "time_to_first_byte_millisecond",
			Help:      "Bucketed histogram of s3 time to first byte request processing time.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 27),
		}, []string{"type", "bucket"})

	// S3InFlightRequestsGauge S3 正在处理的请求数量
	// 实时显示当前并发处理的 S3 请求数
	// labels: type - 请求类型
	S3InFlightRequestsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "s3",
			Name:      "in_flight_requests",
			Help:      "Current number of in-flight requests being handled by s3.",
		}, []string{"type"})

	// S3BucketTrafficReceivedBytesCounter S3 bucket 接收流量计数器
	// 记录 bucket 从客户端接收的总字节数(上传流量)
	// labels: bucket - bucket名称
	S3BucketTrafficReceivedBytesCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "s3",
			Name:      "bucket_traffic_received_bytes_total",
			Help:      "Total number of bytes received by an S3 bucket from clients.",
		}, []string{"bucket"})

	// S3BucketTrafficSentBytesCounter S3 bucket 发送流量计数器
	// 记录 bucket 发送给客户端的总字节数(下载流量)
	// labels: bucket - bucket名称
	S3BucketTrafficSentBytesCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "s3",
			Name:      "bucket_traffic_sent_bytes_total",
			Help:      "Total number of bytes sent from an S3 bucket to clients.",
		}, []string{"bucket"})

	// S3DeletedObjectsCounter S3 删除对象计数器
	// 记录每个 bucket 中删除的对象数量
	// labels: bucket - bucket名称
	S3DeletedObjectsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "s3",
			Name:      "deleted_objects",
			Help:      "Number of objects deleted in each bucket.",
		}, []string{"bucket"})

	// S3UploadedObjectsCounter S3 上传对象计数器
	// 记录每个 bucket 中上传的对象数量
	// labels: bucket - bucket名称
	S3UploadedObjectsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "s3",
			Name:      "uploaded_objects",
			Help:      "Number of objects uploaded in each bucket.",
		}, []string{"bucket"})
)

// init 初始化函数,在包被导入时自动执行
// 负责将所有定义的 Prometheus 指标注册到 Gather 注册表中
// 同时启动 bucket 指标的 TTL 清理 goroutine
func init() {
	// 注册 Master 相关指标
	Gather.MustRegister(MasterClientConnectCounter)
	Gather.MustRegister(MasterRaftIsleader)
	Gather.MustRegister(MasterAdminLock)
	Gather.MustRegister(MasterReceivedHeartbeatCounter)
	Gather.MustRegister(MasterLeaderChangeCounter)
	Gather.MustRegister(MasterReplicaPlacementMismatch)
	Gather.MustRegister(MasterVolumeLayoutWritable)
	Gather.MustRegister(MasterVolumeLayoutCrowded)
	Gather.MustRegister(MasterPickForWriteErrorCounter)
	Gather.MustRegister(MasterBroadcastToFullErrorCounter)

	// 注册 Filer 相关指标
	Gather.MustRegister(FilerRequestCounter)
	Gather.MustRegister(FilerHandlerCounter)
	Gather.MustRegister(FilerRequestHistogram)
	Gather.MustRegister(FilerInFlightRequestsGauge)
	Gather.MustRegister(FilerStoreCounter)
	Gather.MustRegister(FilerStoreHistogram)
	Gather.MustRegister(FilerSyncOffsetGauge)
	Gather.MustRegister(FilerServerLastSendTsOfSubscribeGauge)
	// 注册 Go runtime 和进程级别的指标收集器
	Gather.MustRegister(collectors.NewGoCollector())
	Gather.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	// 注册 Volume Server 相关指标
	Gather.MustRegister(VolumeServerRequestCounter)
	Gather.MustRegister(VolumeServerHandlerCounter)
	Gather.MustRegister(VolumeServerRequestHistogram)
	Gather.MustRegister(VolumeServerInFlightRequestsGauge)
	Gather.MustRegister(VolumeServerVacuumingCompactCounter)
	Gather.MustRegister(VolumeServerVacuumingCommitCounter)
	Gather.MustRegister(VolumeServerVacuumingHistogram)
	Gather.MustRegister(VolumeServerVolumeGauge)
	Gather.MustRegister(VolumeServerMaxVolumeCounter)
	Gather.MustRegister(VolumeServerReadOnlyVolumeGauge)
	Gather.MustRegister(VolumeServerDiskSizeGauge)
	Gather.MustRegister(VolumeServerResourceGauge)
	Gather.MustRegister(VolumeServerConcurrentDownloadLimit)
	Gather.MustRegister(VolumeServerConcurrentUploadLimit)
	Gather.MustRegister(VolumeServerInFlightDownloadSize)
	Gather.MustRegister(VolumeServerInFlightUploadSize)

	// 注册 S3 相关指标
	Gather.MustRegister(S3RequestCounter)
	Gather.MustRegister(S3HandlerCounter)
	Gather.MustRegister(S3RequestHistogram)
	Gather.MustRegister(S3InFlightRequestsGauge)
	Gather.MustRegister(S3TimeToFirstByteHistogram)
	Gather.MustRegister(S3BucketTrafficReceivedBytesCounter)
	Gather.MustRegister(S3BucketTrafficSentBytesCounter)
	Gather.MustRegister(S3DeletedObjectsCounter)
	Gather.MustRegister(S3UploadedObjectsCounter)

	// 启动后台 goroutine,定期清理不活跃 bucket 的指标
	go bucketMetricTTLControl()
}

// LoopPushingMetric 循环推送指标到 Prometheus Push Gateway
// 适用于短期任务或无法被 Prometheus 主动抓取的服务
//
// 参数:
//   - name: 服务名称,作为 job 名称
//   - instance: 实例标识,通常是 hostname:port
//   - addr: Push Gateway 的地址
//   - intervalSeconds: 推送间隔(秒),0 表示禁用推送
//
// 注意: 该函数会启动一个无限循环,通常在 goroutine 中调用
func LoopPushingMetric(name, instance, addr string, intervalSeconds int) {
	if addr == "" || intervalSeconds == 0 {
		return
	}

	glog.V(0).Infof("%s server sends metrics to %s every %d seconds", name, addr, intervalSeconds)

	pusher := push.New(addr, name).Gatherer(Gather).Grouping("instance", instance)

	for {
		err := pusher.Push()
		if err != nil && !strings.HasPrefix(err.Error(), "unexpected status code 200") {
			glog.V(0).Infof("could not push metrics to prometheus push gateway %s: %v", addr, err)
		}
		if intervalSeconds <= 0 {
			intervalSeconds = 15
		}
		time.Sleep(time.Duration(intervalSeconds) * time.Second)
	}
}

// JoinHostPort 将主机和端口组合成地址字符串
// 对于 IPv6 地址会进行特殊处理,已经包含方括号的不会重复添加
//
// 参数:
//   - host: 主机名或 IP 地址,IPv6 可能包含 []
//   - port: 端口号
//
// 返回: "host:port" 格式的地址字符串
func JoinHostPort(host string, port int) string {
	portStr := strconv.Itoa(port)
	if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		return host + ":" + portStr
	}
	return net.JoinHostPort(host, portStr)
}

// StartMetricsServer 启动 HTTP 服务器暴露 Prometheus 指标
// 在 /metrics 路径提供指标数据,供 Prometheus 抓取
//
// 参数:
//   - ip: 监听的 IP 地址,空字符串表示监听所有接口
//   - port: 监听端口,0 表示禁用指标服务器
//
// 注意: 该函数会阻塞,通常在 goroutine 中调用
func StartMetricsServer(ip string, port int) {
	if port == 0 {
		return
	}
	http.Handle("/metrics", promhttp.HandlerFor(Gather, promhttp.HandlerOpts{}))
	glog.Fatal(http.ListenAndServe(JoinHostPort(ip, port), nil))
}

// SourceName 生成源名称标识
// 通常用于标识指标的来源实例
//
// 参数:
//   - port: 服务端口号
//
// 返回: "hostname:port" 格式的源名称,无法获取 hostname 时返回 "unknown"
func SourceName(port uint32) string {
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return net.JoinHostPort(hostname, strconv.Itoa(int(port)))
}

// RecordBucketActiveTime 记录 S3 bucket 的活跃时间
// 每次 bucket 有活动时调用,用于 TTL 清理机制
//
// 参数:
//   - bucket: bucket 名称
func RecordBucketActiveTime(bucket string) {
	bucketLastActiveLock.Lock()
	bucketLastActiveTsNs[bucket] = time.Now().UnixNano()
	bucketLastActiveLock.Unlock()
}

// DeleteCollectionMetrics 删除指定 collection 的所有相关指标
// 当 collection 被删除时调用,避免指标累积占用内存
//
// 参数:
//   - collection: 要删除指标的 collection 名称
func DeleteCollectionMetrics(collection string) {
	labels := prometheus.Labels{"collection": collection}
	c := MasterReplicaPlacementMismatch.DeletePartialMatch(labels)
	c += MasterVolumeLayoutWritable.DeletePartialMatch(labels)
	c += MasterVolumeLayoutCrowded.DeletePartialMatch(labels)
	c += VolumeServerDiskSizeGauge.DeletePartialMatch(labels)
	c += VolumeServerVolumeGauge.DeletePartialMatch(labels)
	c += VolumeServerReadOnlyVolumeGauge.DeletePartialMatch(labels)

	glog.V(0).Infof("delete collection metrics, %s: %d", collection, c)
}

// bucketMetricTTLControl S3 bucket 指标的 TTL 控制循环
// 定期检查并删除不活跃 bucket 的指标,避免内存泄漏
// 在 init() 中作为 goroutine 启动,持续运行
func bucketMetricTTLControl() {
	ttlNs := bucketAtiveTTL.Nanoseconds()
	for {
		now := time.Now().UnixNano()

		bucketLastActiveLock.Lock()
		for bucket, ts := range bucketLastActiveTsNs {
			// 检查 bucket 是否超过 TTL(10分钟)未活跃
			if (now - ts) > ttlNs {
				delete(bucketLastActiveTsNs, bucket)

				// 删除该 bucket 的所有相关指标
				labels := prometheus.Labels{"bucket": bucket}
				c := S3RequestCounter.DeletePartialMatch(labels)
				c += S3RequestHistogram.DeletePartialMatch(labels)
				c += S3TimeToFirstByteHistogram.DeletePartialMatch(labels)
				c += S3BucketTrafficReceivedBytesCounter.DeletePartialMatch(labels)
				c += S3BucketTrafficSentBytesCounter.DeletePartialMatch(labels)
				c += S3DeletedObjectsCounter.DeletePartialMatch(labels)
				c += S3UploadedObjectsCounter.DeletePartialMatch(labels)
				glog.V(0).Infof("delete inactive bucket metrics, %s: %d", bucket, c)
			}
		}

		bucketLastActiveLock.Unlock()
		time.Sleep(bucketAtiveTTL)
	}

}
