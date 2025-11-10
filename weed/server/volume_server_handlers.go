// Package weed_server 实现 SeaweedFS 的服务器端处理逻辑
// 本文件包含 Volume Server 的 HTTP 请求处理器，负责请求路由、并发控制和安全验证
//
// 主要功能:
//  1. HTTP 请求路由（GET/POST/DELETE/OPTIONS）
//  2. 并发上传/下载限制和流量控制
//  3. JWT 授权验证
//  4. 公共只读端口支持
//  5. 副本代理转发
//
// 安全模型:
//   Volume Server 可以配置分离的公共端口，公共端口更加"安全"
//   - 公共端口当前仅支持读取操作
//   - 写操作可以有 3 种安全设置：
//     1. 不安全（无验证）
//     2. 通过白名单保护
//     3. 通过 JWT (Json Web Token) 保护
package weed_server

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/version"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/stats"
)

/*
Volume Server 公共端口说明：

如果 Volume Server 启动时配置了独立的公共端口，该公共端口将更加"安全"。

公共端口当前仅支持读取操作。

未来在公共端口上的写操作可以采用以下 3 种安全设置之一：
1. 不安全（无验证）
2. 通过白名单保护
3. 通过 JWT (Json Web Token) 保护
*/

// checkDownloadLimit 处理下载并发限制，支持超时和副本代理回退
//
// 这是一个复杂的流量控制函数，用于防止 Volume Server 被过多的并发下载请求压垮
//
// 返回值:
//   - true:  请求应继续正常处理（未超限，或成功等待到可用容量）
//   - false: 请求已被此函数处理（代理到副本、超时返回 429、取消返回 499 或错误响应）
//            调用者不应继续处理
//
// 控制流程:
//   - 未配置限制 → 返回 true（正常继续）
//   - 在限制内 → 返回 true（正常继续）
//   - 超过限制 + 有副本 → 代理到副本，返回 false（已处理）
//   - 超过限制 + 无副本 → 等待超时：
//     - 超时 → 发送 429 响应，返回 false（已处理）
//     - 取消 → 发送 499 响应，返回 false（已处理）
//     - 有容量可用 → 返回 true（正常继续）
//
// 工作原理:
//  1. 检查当前正在进行的下载数据大小
//  2. 如果超过限制，尝试代理到副本服务器
//  3. 如果无法代理，进入等待队列
//  4. 等待期间处理超时和取消
func (vs *VolumeServer) checkDownloadLimit(w http.ResponseWriter, r *http.Request) bool {
	// 获取当前正在进行的下载数据大小（原子操作，并发安全）
	inFlightDownloadSize := atomic.LoadInt64(&vs.inFlightDownloadDataSize)
	// 更新 Prometheus 监控指标
	stats.VolumeServerInFlightDownloadSize.Set(float64(inFlightDownloadSize))

	// 步骤 1: 检查是否超过下载限制
	// 如果未配置限制（为 0）或在限制内，直接允许处理
	if vs.concurrentDownloadLimit == 0 || inFlightDownloadSize <= vs.concurrentDownloadLimit {
		return true // no limit configured or within limit - proceed normally
	}

	// 步骤 2: 超过限制，记录监控指标
	stats.VolumeServerHandlerCounter.WithLabelValues(stats.DownloadLimitCond).Inc()
	glog.V(4).Infof("request %s wait because inflight download data %d > %d",
		r.URL.Path, inFlightDownloadSize, vs.concurrentDownloadLimit)

	// 步骤 3: 尝试代理到副本服务器（负载均衡）
	// Try to proxy to replica if available
	if vs.tryProxyToReplica(w, r) {
		return false // handled by proxy - 已通过代理处理
	}

	// 步骤 4: 无副本可用，进入等待队列
	// Wait with timeout
	return vs.waitForDownloadSlot(w, r)
}

// tryProxyToReplica 尝试将请求代理到副本服务器（如果 Volume 有副本）
// 用于负载均衡和故障转移
//
// 返回值:
//   - true:  请求已被处理（成功代理或失败并返回错误响应）
//   - false: 无可用代理（Volume 无副本或请求已被代理过）
//
// 工作原理:
//  1. 解析 URL 中的 volumeId
//  2. 获取 Volume 对象
//  3. 检查 Volume 是否有副本配置
//  4. 检查请求是否已经被代理过（通过查询参数 reqIsProxied）
//  5. 如果满足条件，调用 proxyReqToTargetServer 代理请求
//
// 防止无限代理循环:
//   通过检查 reqIsProxied 参数，确保同一个请求不会被多次代理
func (vs *VolumeServer) tryProxyToReplica(w http.ResponseWriter, r *http.Request) bool {
	// 步骤 1: 解析 URL 路径，提取 volumeId
	vid, _, _, _, _ := parseURLPath(r.URL.Path)
	volumeId, err := needle.NewVolumeId(vid)
	if err != nil {
		glog.V(1).Infof("parsing vid %s: %v", r.URL.Path, err)
		w.WriteHeader(http.StatusBadRequest)
		return true // handled (with error) - 已处理（带错误）
	}

	// 步骤 2: 获取 Volume 并检查副本配置
	volume := vs.store.GetVolume(volumeId)
	// 代理条件：
	// 1. Volume 存在
	// 2. 有副本配置
	// 3. 副本配置为多副本（HasReplication）
	// 4. 请求未被代理过（reqIsProxied != "true"）
	if volume != nil && volume.ReplicaPlacement != nil && volume.ReplicaPlacement.HasReplication() && r.URL.Query().Get(reqIsProxied) != "true" {
		// 步骤 3: 代理请求到副本服务器
		vs.proxyReqToTargetServer(w, r)
		return true // handled by proxy - 已通过代理处理
	}
	// 无可用副本或已被代理过
	return false // no proxy available
}

// waitForDownloadSlot waits for available download capacity with timeout.
//
// This function implements a blocking wait mechanism with timeout for download capacity.
// It continuously checks if download capacity becomes available and handles timeout
// and cancellation scenarios appropriately.
//
// Returns:
//   - true:  Download capacity became available, request should proceed
//   - false: Request failed (timeout or cancellation), error response already sent
//
// HTTP Status Codes:
//   - 429 Too Many Requests: Wait timeout exceeded
//   - 499 Client Closed Request: Request cancelled by client
func (vs *VolumeServer) waitForDownloadSlot(w http.ResponseWriter, r *http.Request) bool {
	timerDownload := time.NewTimer(vs.inflightDownloadDataTimeout)
	defer timerDownload.Stop()

	inFlightDownloadSize := atomic.LoadInt64(&vs.inFlightDownloadDataSize)
	for inFlightDownloadSize > vs.concurrentDownloadLimit {
		switch util.WaitWithTimeout(r.Context(), vs.inFlightDownloadDataLimitCond, timerDownload) {
		case http.StatusTooManyRequests:
			err := fmt.Errorf("request %s because inflight download data %d > %d, and wait timeout",
				r.URL.Path, inFlightDownloadSize, vs.concurrentDownloadLimit)
			glog.V(1).Infof("too many requests: %v", err)
			writeJsonError(w, r, http.StatusTooManyRequests, err)
			return false
		case util.HttpStatusCancelled:
			glog.V(1).Infof("request %s cancelled from %s: %v", r.URL.Path, r.RemoteAddr, r.Context().Err())
			w.WriteHeader(util.HttpStatusCancelled)
			return false
		}
		inFlightDownloadSize = atomic.LoadInt64(&vs.inFlightDownloadDataSize)
		stats.VolumeServerInFlightDownloadSize.Set(float64(inFlightDownloadSize))
	}
	return true
}

// checkUploadLimit handles upload concurrency limiting with timeout.
//
// This function implements upload throttling to prevent overwhelming the volume server
// with too many concurrent uploads. It excludes replication traffic from limits.
//
// Returns:
//   - true:  Request should proceed with upload processing (no limit, within limit,
//     or successfully waited for capacity)
//   - false: Request failed (timeout or cancellation), error response already sent
//
// Special Handling:
//   - Replication requests (type=replicate) bypass upload limits
//   - No upload limit configured (concurrentUploadLimit=0) allows all uploads
func (vs *VolumeServer) checkUploadLimit(w http.ResponseWriter, r *http.Request) bool {
	// exclude the replication from the concurrentUploadLimitMB
	if vs.concurrentUploadLimit == 0 || r.URL.Query().Get("type") == "replicate" {
		return true
	}

	inFlightUploadDataSize := atomic.LoadInt64(&vs.inFlightUploadDataSize)
	stats.VolumeServerInFlightUploadSize.Set(float64(inFlightUploadDataSize))

	if inFlightUploadDataSize <= vs.concurrentUploadLimit {
		return true
	}

	return vs.waitForUploadSlot(w, r)
}

// waitForUploadSlot waits for available upload capacity with timeout.
//
// Returns:
//   - true:  Upload capacity became available, request should proceed
//   - false: Request failed (timeout or cancellation), error response already sent
//
// HTTP Status Codes:
//   - 429 Too Many Requests: Wait timeout exceeded
//   - 499 Client Closed Request: Request cancelled by client
func (vs *VolumeServer) waitForUploadSlot(w http.ResponseWriter, r *http.Request) bool {
	var timerUpload *time.Timer
	inFlightUploadDataSize := atomic.LoadInt64(&vs.inFlightUploadDataSize)

	for inFlightUploadDataSize > vs.concurrentUploadLimit {
		if timerUpload == nil {
			timerUpload = time.NewTimer(vs.inflightUploadDataTimeout)
			defer timerUpload.Stop()
		}

		glog.V(4).Infof("wait because inflight upload data %d > %d", inFlightUploadDataSize, vs.concurrentUploadLimit)
		stats.VolumeServerHandlerCounter.WithLabelValues(stats.UploadLimitCond).Inc()

		switch util.WaitWithTimeout(r.Context(), vs.inFlightUploadDataLimitCond, timerUpload) {
		case http.StatusTooManyRequests:
			err := fmt.Errorf("reject because inflight upload data %d > %d, and wait timeout",
				inFlightUploadDataSize, vs.concurrentUploadLimit)
			glog.V(1).Infof("too many requests: %v", err)
			writeJsonError(w, r, http.StatusTooManyRequests, err)
			return false
		case util.HttpStatusCancelled:
			glog.V(1).Infof("request cancelled from %s: %v", r.RemoteAddr, r.Context().Err())
			writeJsonError(w, r, util.HttpStatusCancelled, r.Context().Err())
			return false
		}

		inFlightUploadDataSize = atomic.LoadInt64(&vs.inFlightUploadDataSize)
		stats.VolumeServerInFlightUploadSize.Set(float64(inFlightUploadDataSize))
	}
	return true
}

// handleGetRequest processes GET/HEAD requests with download limiting.
//
// This function orchestrates the complete GET/HEAD request handling workflow:
// 1. Records read request statistics
// 2. Applies download concurrency limits with proxy fallback
// 3. Delegates to GetOrHeadHandler for actual file serving (if limits allow)
//
// The download limiting logic may handle the request completely (via proxy,
// timeout, or error), in which case normal file serving is skipped.
func (vs *VolumeServer) handleGetRequest(w http.ResponseWriter, r *http.Request) {
	stats.ReadRequest()
	if vs.checkDownloadLimit(w, r) {
		vs.GetOrHeadHandler(w, r)
	}
}

// handleUploadRequest processes PUT/POST requests with upload limiting.
//
// This function manages the complete upload request workflow:
// 1. Extracts content length from request headers
// 2. Applies upload concurrency limits with timeout handling
// 3. Tracks in-flight upload data size for monitoring
// 4. Delegates to PostHandler for actual file processing
// 5. Ensures proper cleanup of in-flight counters
//
// The upload limiting logic may reject the request with appropriate HTTP
// status codes (429 for timeout, 499 for cancellation).
func (vs *VolumeServer) handleUploadRequest(w http.ResponseWriter, r *http.Request) {
	contentLength := getContentLength(r)

	if !vs.checkUploadLimit(w, r) {
		return
	}

	atomic.AddInt64(&vs.inFlightUploadDataSize, contentLength)
	defer func() {
		atomic.AddInt64(&vs.inFlightUploadDataSize, -contentLength)
		if vs.concurrentUploadLimit != 0 {
			vs.inFlightUploadDataLimitCond.Broadcast()
		}
	}()

	// processes uploads
	stats.WriteRequest()
	vs.guard.WhiteList(vs.PostHandler)(w, r)
}

// privateStoreHandler 私有存储处理器
// 处理所有到私有端口的 HTTP 请求，支持完整的 CRUD 操作
//
// 支持的操作:
//   - GET/HEAD: 读取文件
//   - POST/PUT: 上传文件
//   - DELETE: 删除文件
//   - OPTIONS: CORS 预检请求
//
// 功能:
//  1. 记录请求指标（并发数、延迟、状态码）
//  2. 设置 CORS 头（如果需要）
//  3. 根据 HTTP 方法路由到相应的处理器
//  4. 应用白名单安全检查（对写操作和删除操作）
//
// 监控:
//   - 记录并发请求数（in-flight requests）
//   - 记录请求计数器（按方法和状态码分组）
//   - 记录请求延迟直方图
func (vs *VolumeServer) privateStoreHandler(w http.ResponseWriter, r *http.Request) {
	inFlightGauge := stats.VolumeServerInFlightRequestsGauge.WithLabelValues(r.Method)
	inFlightGauge.Inc()
	defer inFlightGauge.Dec()

	statusRecorder := stats.NewStatusResponseWriter(w)
	w = statusRecorder
	w.Header().Set("Server", "SeaweedFS Volume "+version.VERSION)
	if r.Header.Get("Origin") != "" {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Credentials", "true")
	}

	start := time.Now()
	requestMethod := r.Method
	defer func(start time.Time, method *string, statusRecorder *stats.StatusRecorder) {
		stats.VolumeServerRequestCounter.WithLabelValues(*method, strconv.Itoa(statusRecorder.Status)).Inc()
		stats.VolumeServerRequestHistogram.WithLabelValues(*method).Observe(time.Since(start).Seconds())
	}(start, &requestMethod, statusRecorder)

	switch r.Method {
	case http.MethodGet, http.MethodHead:
		vs.handleGetRequest(w, r)
	case http.MethodDelete:
		stats.DeleteRequest()
		vs.guard.WhiteList(vs.DeleteHandler)(w, r)
	case http.MethodPut, http.MethodPost:
		vs.handleUploadRequest(w, r)
	case http.MethodOptions:
		stats.ReadRequest()
		w.Header().Add("Access-Control-Allow-Methods", "PUT, POST, GET, DELETE, OPTIONS")
		w.Header().Add("Access-Control-Allow-Headers", "*")
	default:
		requestMethod = "INVALID"
		writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("unsupported method %s", r.Method))
	}
}

func getContentLength(r *http.Request) int64 {
	contentLength := r.Header.Get("Content-Length")
	if contentLength != "" {
		length, err := strconv.ParseInt(contentLength, 10, 64)
		if err != nil {
			return 0
		}
		return length
	}
	return 0
}

// publicReadOnlyHandler 公共只读处理器
// 处理到公共端口的 HTTP 请求，仅支持读取操作
//
// 安全特性:
//   这是一个更安全的端口，仅允许 GET/HEAD/OPTIONS 操作
//   不允许任何写入或删除操作
//
// 支持的操作:
//   - GET/HEAD: 读取文件（带下载限制）
//   - OPTIONS: CORS 预检请求
//
// 用途:
//   - 向公众提供文件下载服务
//   - CDN 回源
//   - 不信任的客户端访问
func (vs *VolumeServer) publicReadOnlyHandler(w http.ResponseWriter, r *http.Request) {
	// 步骤 1: 包装 ResponseWriter 以记录状态码
	statusRecorder := stats.NewStatusResponseWriter(w)
	w = statusRecorder

	// 步骤 2: 设置响应头
	w.Header().Set("Server", "SeaweedFS Volume "+version.VERSION)
	// CORS 支持
	if r.Header.Get("Origin") != "" {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Credentials", "true")
	}

	// 步骤 3: 记录请求指标
	start := time.Now()
	requestMethod := r.Method
	defer func(start time.Time, method *string, statusRecorder *stats.StatusRecorder) {
		stats.VolumeServerRequestCounter.WithLabelValues(*method, strconv.Itoa(statusRecorder.Status)).Inc()
		stats.VolumeServerRequestHistogram.WithLabelValues(*method).Observe(time.Since(start).Seconds())
	}(start, &requestMethod, statusRecorder)

	// 步骤 4: 仅允许读取操作
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		// 读取操作
		vs.handleGetRequest(w, r)
	case http.MethodOptions:
		// CORS 预检请求
		stats.ReadRequest()
		w.Header().Add("Access-Control-Allow-Methods", "GET, OPTIONS") // 注意：不包含 PUT/POST/DELETE
		w.Header().Add("Access-Control-Allow-Headers", "*")
	}
	// 其他 HTTP 方法被忽略（不返回错误，直接忽略）
}

// maybeCheckJwtAuthorization 检查 JWT 授权（如果启用）
// 用于验证客户端是否有权限访问特定的文件
//
// 参数:
//   - r: HTTP 请求对象
//   - vid: Volume ID 字符串
//   - fid: File ID 字符串
//   - isWrite: 是否是写操作（true: 写/删除, false: 读）
//
// 返回值:
//   - bool: true 表示授权通过，false 表示授权失败
//
// 工作原理:
//  1. 根据操作类型（读/写）选择对应的签名密钥
//  2. 如果未配置密钥，允许所有请求（开放模式）
//  3. 从请求中提取 JWT Token
//  4. 验证 Token 的签名和有效性
//  5. 验证 Token 中的 fileId 是否匹配
//
// JWT Claims 格式:
//   - Fid: "{volumeId},{fileId}" 例如 "3,01637037d6"
//
// 安全性:
//   - 读操作和写操作使用不同的签名密钥
//   - Token 必须包含正确的 volumeId 和 fileId
//   - 支持文件 ID 的变体（去除 _suffix）
func (vs *VolumeServer) maybeCheckJwtAuthorization(r *http.Request, vid, fid string, isWrite bool) bool {

	// 步骤 1: 根据操作类型选择签名密钥
	var signingKey security.SigningKey

	if isWrite {
		// 写操作（上传/删除）
		if len(vs.guard.SigningKey) == 0 {
			return true // 未配置密钥，允许所有写操作
		} else {
			signingKey = vs.guard.SigningKey
		}
	} else {
		// 读操作
		if len(vs.guard.ReadSigningKey) == 0 {
			return true // 未配置密钥，允许所有读操作
		} else {
			signingKey = vs.guard.ReadSigningKey
		}
	}

	// 步骤 2: 从请求中提取 JWT Token
	tokenStr := security.GetJwt(r)
	if tokenStr == "" {
		glog.V(1).Infof("missing jwt from %s", r.RemoteAddr)
		return false
	}

	// 步骤 3: 解码并验证 JWT Token
	token, err := security.DecodeJwt(signingKey, tokenStr, &security.SeaweedFileIdClaims{})
	if err != nil {
		glog.V(1).Infof("jwt verification error from %s: %v", r.RemoteAddr, err)
		return false
	}
	if !token.Valid {
		glog.V(1).Infof("jwt invalid from %s: %v", r.RemoteAddr, tokenStr)
		return false
	}

	// 步骤 4: 验证 Token 中的 fileId 是否匹配
	if sc, ok := token.Claims.(*security.SeaweedFileIdClaims); ok {
		// 处理文件 ID 变体：去除 _suffix（如果有）
		// 例如: "01637037d6_1" -> "01637037d6"
		if sepIndex := strings.LastIndex(fid, "_"); sepIndex > 0 {
			fid = fid[:sepIndex]
		}
		// 验证 Token 中的 Fid 是否匹配 "volumeId,fileId"
		return sc.Fid == vid+","+fid
	}
	glog.V(1).Infof("unexpected jwt from %s: %v", r.RemoteAddr, tokenStr)
	return false
}
