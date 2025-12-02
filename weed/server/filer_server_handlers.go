// Package weed_server 中的 filer_server_handlers.go 负责 Filer HTTP API 的请求入口
// 在此统一实现鉴权、CORS、统计上报等横切关注点。
package weed_server

import (
	"context"
	"errors"
	"github.com/seaweedfs/seaweedfs/weed/util/version"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/stats"
)

// filerHandler 是 Filer HTTP API 的统一入口处理器
//
// 【核心职责】
//   - 处理所有 HTTP 方法（GET/POST/PUT/DELETE/HEAD/OPTIONS）
//   - 实现跨域资源共享（CORS）支持
//   - 执行 JWT 身份验证与授权
//   - 提供 Volume Proxy 功能（直接代理到 Volume Server）
//   - 记录请求统计指标（延迟、并发量、成功率等）
//   - 流量控制（限制并发上传大小）
//
// 【处理流程】
//   1. 记录请求开始时间，更新并发请求计数器
//   2. 检查并处理 CORS 预检（Origin、OPTIONS）
//   3. 处理 Volume Proxy 请求（/?proxyChunkId=xxx）
//   4. 执行 JWT 授权校验（读/写权限分离）
//   5. 根据 HTTP 方法分发到具体处理器
//   6. 对于 POST/PUT，实施并发上传流量控制
//   7. 记录请求完成指标（状态码、响应时间）
//
// 【参数说明】
//   - w: HTTP 响应写入器，会被包装为 statusRecorder 以记录状态码
//   - r: HTTP 请求对象，包含方法、路径、头部、Body 等
//
// 【CORS 支持】
//   - 如果配置了 AllowedOrigins，会验证 Origin 是否在白名单中
//   - AllowedOrigins 为空或包含 "*" 时，允许所有来源
//   - 设置必要的 CORS 响应头（Access-Control-Allow-*）
//
// 【Volume Proxy 机制】
//   - 请求格式: /?proxyChunkId=<fileId>
//   - 直接将请求代理到存储该 fileId 的 Volume Server
//   - 适用于需要直接访问 chunk 数据的场景
//
// 【流量控制】
//   - POST/PUT 请求会占用 inFlightDataSize 配额
//   - 当并发上传数据超过 ConcurrentUploadLimit 时，新请求会阻塞等待
//   - 使用条件变量（inFlightDataLimitCond）实现阻塞唤醒
//   - 请求完成后释放配额并唤醒等待者
func (fs *FilerServer) filerHandler(w http.ResponseWriter, r *http.Request) {
	// 记录请求开始时间，用于计算响应延迟
	start := time.Now()

	// 更新并发请求计数器（按 HTTP 方法分类）
	// Prometheus Gauge 指标，实时反映当前处理中的请求数
	inFlightGauge := stats.FilerInFlightRequestsGauge.WithLabelValues(r.Method)
	inFlightGauge.Inc()
	defer inFlightGauge.Dec()

	// 包装 ResponseWriter 以捕获 HTTP 状态码，用于统计
	statusRecorder := stats.NewStatusResponseWriter(w)
	w = statusRecorder

	// 【步骤 1：处理 CORS 预检】
	// 检查请求头中的 Origin，决定是否设置 CORS 响应头
	origin := r.Header.Get("Origin")
	if origin != "" {
		// 判断来源是否被允许
		// 允许策略：
		//   1. AllowedOrigins 未配置或为空：默认允许所有
		//   2. AllowedOrigins[0] == "*"：明确配置为允许所有
		//   3. Origin 在 AllowedOrigins 白名单中
		if fs.option.AllowedOrigins == nil || len(fs.option.AllowedOrigins) == 0 || fs.option.AllowedOrigins[0] == "*" {
			origin = "*"
		} else {
			// 遍历白名单，查找匹配的来源
			originFound := false
			for _, allowedOrigin := range fs.option.AllowedOrigins {
				if origin == allowedOrigin {
					originFound = true
				}
			}
			// 来源不在白名单中，拒绝请求
			if !originFound {
				writeJsonError(w, r, http.StatusForbidden, errors.New("origin not allowed"))
				return
			}
		}

		// 设置 CORS 响应头，允许跨域访问
		// Access-Control-Allow-Origin: 允许的来源域名
		w.Header().Set("Access-Control-Allow-Origin", origin)
		// Access-Control-Expose-Headers: 允许 JavaScript 访问的响应头
		w.Header().Set("Access-Control-Expose-Headers", "*")
		// Access-Control-Allow-Headers: 允许客户端发送的请求头
		w.Header().Set("Access-Control-Allow-Headers", "*")
		// Access-Control-Allow-Credentials: 允许携带凭证（Cookie、Authorization）
		w.Header().Set("Access-Control-Allow-Credentials", "true")
		// Access-Control-Allow-Methods: 允许的 HTTP 方法
		w.Header().Set("Access-Control-Allow-Methods", "PUT, POST, GET, DELETE, OPTIONS")
	}

	// 【步骤 2：处理 OPTIONS 预检请求】
	// CORS 预检请求不需要实际业务处理，直接返回允许的方法和头部
	if r.Method == http.MethodOptions {
		OptionsHandler(w, r, false)
		return
	}

	// 【步骤 3：处理 Volume Proxy 请求】
	// Volume Proxy 允许客户端绕过 Filer 直接访问 Volume Server 上的 chunk
	// 请求格式: /?proxyChunkId=3,01234567890abcdef
	// 使用场景:
	//   - 需要直接读取某个 chunk 的原始数据
	//   - 绕过 Filer 层减少延迟
	//   - 客户端已经知道 fileId，无需查询文件元数据
	var fileId string
	if strings.HasPrefix(r.RequestURI, "/?proxyChunkId=") {
		// 提取 fileId 参数（去掉前缀）
		fileId = r.RequestURI[len("/?proxyChunkId="):]
	}
	if fileId != "" {
		// 代理请求到对应的 Volume Server
		fs.proxyToVolumeServer(w, r, fileId)
		// 记录 Proxy 统计指标
		stats.FilerHandlerCounter.WithLabelValues(stats.ChunkProxy).Inc()
		stats.FilerRequestHistogram.WithLabelValues(stats.ChunkProxy).Observe(time.Since(start).Seconds())
		return
	}

	// 【步骤 4：设置统计指标延迟记录】
	// 使用指针传递 requestMethod，允许后续修改（如遇到非法方法时标记为 INVALID）
	requestMethod := r.Method
	defer func(method *string) {
		// 记录请求计数（按方法和状态码分类）
		stats.FilerRequestCounter.WithLabelValues(*method, strconv.Itoa(statusRecorder.Status)).Inc()
		// 记录请求延迟（直方图统计，用于 P50/P99 分析）
		stats.FilerRequestHistogram.WithLabelValues(*method).Observe(time.Since(start).Seconds())
	}(&requestMethod)

	// 【步骤 5：JWT 授权校验】
	// 根据请求类型（读/写）选择不同的签名密钥进行验证
	// GET/HEAD 请求属于读操作，使用 ReadSigningKey
	// POST/PUT/DELETE 请求属于写操作，使用 SigningKey
	isReadHttpCall := r.Method == http.MethodGet || r.Method == http.MethodHead
	if !fs.maybeCheckJwtAuthorization(r, !isReadHttpCall) {
		writeJsonError(w, r, http.StatusUnauthorized, errors.New("wrong jwt"))
		return
	}

	// 设置服务器版本响应头，便于客户端识别 SeaweedFS 版本
	w.Header().Set("Server", "SeaweedFS "+version.VERSION)

	// 【步骤 6：根据 HTTP 方法分发到具体处理器】
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		// 读取文件或目录内容
		// HEAD 请求只返回元数据，不返回文件内容
		fs.GetOrHeadHandler(w, r)

	case http.MethodDelete:
		// 删除文件或目录
		// 支持两种删除操作：
		//   1. 删除对象标签（?tagging）
		//   2. 删除文件/目录本身
		if _, ok := r.URL.Query()["tagging"]; ok {
			fs.DeleteTaggingHandler(w, r)
		} else {
			fs.DeleteHandler(w, r)
		}

	case http.MethodPost, http.MethodPut:
		// 【步骤 6.1：获取请求内容长度】
		// 用于流量控制和内存分配
		contentLength := getContentLength(r)

		// 【步骤 6.2：并发上传流量控制】
		// 限制同时上传的数据总量，防止内存耗尽
		// 工作原理：
		//   1. 获取互斥锁，检查当前并发上传数据量
		//   2. 如果超过限制，阻塞等待（条件变量）
		//   3. 直到其他请求完成并释放配额
		//   4. 占用配额后释放锁，继续处理
		fs.inFlightDataLimitCond.L.Lock()
		inFlightDataSize := atomic.LoadInt64(&fs.inFlightDataSize)
		for fs.option.ConcurrentUploadLimit != 0 && inFlightDataSize > fs.option.ConcurrentUploadLimit {
			glog.V(4).Infof("wait because inflight data %d > %d", inFlightDataSize, fs.option.ConcurrentUploadLimit)
			// 阻塞等待，直到其他请求完成并调用 Signal()
			fs.inFlightDataLimitCond.Wait()
			// 被唤醒后重新检查配额
			inFlightDataSize = atomic.LoadInt64(&fs.inFlightDataSize)
		}
		fs.inFlightDataLimitCond.L.Unlock()

		// 【步骤 6.3：占用上传配额】
		// 原子操作增加当前并发数据量
		atomic.AddInt64(&fs.inFlightDataSize, contentLength)
		// 请求完成后释放配额并唤醒等待者
		defer func() {
			// 原子操作减少并发数据量
			atomic.AddInt64(&fs.inFlightDataSize, -contentLength)
			// 唤醒一个等待的请求
			fs.inFlightDataLimitCond.Signal()
		}()

		// 【步骤 6.4：处理上传或标签操作】
		if r.Method == http.MethodPut {
			// PUT 请求可能是上传文件或设置标签
			if _, ok := r.URL.Query()["tagging"]; ok {
				// 设置对象标签（S3 兼容 API）
				fs.PutTaggingHandler(w, r)
			} else {
				// 上传文件内容
				fs.PostHandler(w, r, contentLength)
			}
		} else { // method == "POST"
			// POST 请求用于上传文件
			fs.PostHandler(w, r, contentLength)
		}

	default:
		// 不支持的 HTTP 方法
		requestMethod = "INVALID"
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// readonlyFilerHandler 是只读模式的 Filer HTTP API 入口
//
// 【核心特点】
//   - 仅允许 GET/HEAD/OPTIONS 请求，拒绝所有写操作
//   - 通常绑定到单独的端口（-filer.port.readonly）
//   - 适用于只读副本、备份节点、公开访问节点等场景
//
// 【与 filerHandler 的区别】
//   - CORS 响应头限制为 "GET, OPTIONS"（不包含 POST/PUT/DELETE）
//   - 不支持上传、删除、修改等写操作
//   - JWT 验证始终使用读权限密钥（ReadSigningKey）
//   - 没有并发上传流量控制（因为没有上传）
//
// 【适用场景】
//   - 只读副本节点，提供查询服务
//   - 公开访问节点，防止误操作
//   - 监控/调试节点，避免影响数据
func (fs *FilerServer) readonlyFilerHandler(w http.ResponseWriter, r *http.Request) {

	start := time.Now()
	statusRecorder := stats.NewStatusResponseWriter(w)
	w = statusRecorder

	// 调试用：输出请求信息到标准输出
	// 注意：生产环境中可能会产生大量日志
	os.Stdout.WriteString("Request: " + r.Method + " " + r.URL.String() + "\n")

	// 处理 CORS 预检，验证来源是否被允许
	origin := r.Header.Get("Origin")
	if origin != "" {
		if fs.option.AllowedOrigins == nil || len(fs.option.AllowedOrigins) == 0 || fs.option.AllowedOrigins[0] == "*" {
			origin = "*"
		} else {
			originFound := false
			for _, allowedOrigin := range fs.option.AllowedOrigins {
				if origin == allowedOrigin {
					originFound = true
				}
			}
			if !originFound {
				writeJsonError(w, r, http.StatusForbidden, errors.New("origin not allowed"))
				return
			}
		}

		// 设置 CORS 响应头，只允许读操作
		// 注意：这里只允许 GET/HEAD，不包含 POST/PUT/DELETE
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Headers", "OPTIONS, GET, HEAD")
		w.Header().Set("Access-Control-Allow-Credentials", "true")
	}

	requestMethod := r.Method
	defer func(method *string) {
		stats.FilerRequestCounter.WithLabelValues(*method, strconv.Itoa(statusRecorder.Status)).Inc()
		stats.FilerRequestHistogram.WithLabelValues(*method).Observe(time.Since(start).Seconds())
	}(&requestMethod)

	// OPTIONS 请求不需要身份验证，直接处理
	// 这符合 CORS 规范，预检请求不携带凭证
	if r.Method == http.MethodOptions {
		OptionsHandler(w, r, true) // true 表示只读模式
		return
	}

	// JWT 授权校验，始终使用读权限密钥（false 参数）
	// 因为只读处理器不支持写操作，所以不需要写权限
	if !fs.maybeCheckJwtAuthorization(r, false) {
		writeJsonError(w, r, http.StatusUnauthorized, errors.New("wrong jwt"))
		return
	}

	w.Header().Set("Server", "SeaweedFS "+version.VERSION)

	// 只处理 GET/HEAD 请求，其他方法一律拒绝
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		fs.GetOrHeadHandler(w, r)
	default:
		// 拒绝所有写操作（POST/PUT/DELETE 等）
		requestMethod = "INVALID"
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// OptionsHandler 处理 HTTP OPTIONS 预检请求（CORS Preflight）
//
// 【功能说明】
//   - 响应浏览器发起的跨域预检请求
//   - 告知客户端允许的 HTTP 方法、头部和凭证策略
//   - 根据 isReadOnly 参数限制允许的方法
//
// 【参数说明】
//   - w: HTTP 响应写入器
//   - r: HTTP 请求对象（通常方法为 OPTIONS）
//   - isReadOnly: 是否为只读模式
//     * true: 只允许 GET/OPTIONS（只读节点）
//     * false: 允许所有方法 PUT/POST/GET/DELETE/OPTIONS（读写节点）
//
// 【CORS 响应头说明】
//   - Access-Control-Allow-Methods: 允许的 HTTP 方法列表
//   - Access-Control-Allow-Headers: 允许客户端发送的请求头（*表示所有）
//   - Access-Control-Allow-Credentials: 是否允许携带凭证（Cookie/Authorization）
//   - Access-Control-Expose-Headers: 允许 JavaScript 访问的响应头（仅读写模式）
func OptionsHandler(w http.ResponseWriter, r *http.Request, isReadOnly bool) {
	if isReadOnly {
		// 只读模式：仅允许 GET 和 OPTIONS
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
	} else {
		// 读写模式：允许所有 RESTful 方法
		w.Header().Set("Access-Control-Allow-Methods", "PUT, POST, GET, DELETE, OPTIONS")
		// 允许 JavaScript 访问所有响应头（如 ETag、Content-MD5 等）
		w.Header().Set("Access-Control-Expose-Headers", "*")
	}
	// 允许客户端发送任意请求头
	w.Header().Set("Access-Control-Allow-Headers", "*")
	// 允许携带凭证（Cookie、Authorization 等）
	w.Header().Set("Access-Control-Allow-Credentials", "true")
}

// maybeCheckJwtAuthorization 校验请求中的 JWT 令牌是否有效
//
// 【返回值】
//   - true: JWT 有效或未启用验证，允许访问
//   - false: JWT 无效或缺失，拒绝访问
//
// 【参数说明】
//   - r: HTTP 请求对象，从中提取 JWT 令牌
//   - isWrite: 是否为写操作
//     * true: 使用写权限签名密钥（SigningKey）验证
//     * false: 使用读权限签名密钥（ReadSigningKey）验证
//
// 【JWT 权限分离机制】
//   - 读操作（GET/HEAD）: 使用 ReadSigningKey 验证
//     * 如果 ReadSigningKey 未配置，允许无需验证访问（公开读）
//     * 适用于公开读取、匿名下载等场景
//   - 写操作（POST/PUT/DELETE）: 使用 SigningKey 验证
//     * 如果 SigningKey 未配置，允许无需验证访问（开放写入）
//     * 生产环境建议始终配置写权限密钥
//
// 【JWT 提取位置】
//   1. Authorization 请求头: "Bearer <token>"
//   2. URL 查询参数: ?jwt=<token>
//
// 【验证流程】
//   1. 选择对应的签名密钥（读/写）
//   2. 如果密钥未配置，直接允许访问
//   3. 从请求中提取 JWT 令牌
//   4. 使用签名密钥解码并验证令牌
//   5. 检查令牌的有效性（签名、过期时间等）
func (fs *FilerServer) maybeCheckJwtAuthorization(r *http.Request, isWrite bool) bool {

	var signingKey security.SigningKey

	// 根据操作类型选择签名密钥
	if isWrite {
		// 写操作：使用写权限密钥
		if len(fs.filerGuard.SigningKey) == 0 {
			// 未配置写权限密钥，允许所有写操作（不推荐生产环境）
			return true
		} else {
			signingKey = fs.filerGuard.SigningKey
		}
	} else {
		// 读操作：使用读权限密钥
		if len(fs.filerGuard.ReadSigningKey) == 0 {
			// 未配置读权限密钥，允许所有读操作（适用于公开读场景）
			return true
		} else {
			signingKey = fs.filerGuard.ReadSigningKey
		}
	}

	// 从请求中提取 JWT 令牌
	// 支持从 Authorization 头部或 URL 查询参数获取
	tokenStr := security.GetJwt(r)
	if tokenStr == "" {
		glog.V(1).Infof("missing jwt from %s", r.RemoteAddr)
		return false
	}

	// 解码并验证 JWT 令牌
	// 使用 SeaweedFilerClaims 结构体解析自定义声明
	token, err := security.DecodeJwt(signingKey, tokenStr, &security.SeaweedFilerClaims{})
	if err != nil {
		glog.V(1).Infof("jwt verification error from %s: %v", r.RemoteAddr, err)
		return false
	}

	// 检查令牌的有效性
	// token.Valid 会验证签名、过期时间等标准声明
	if !token.Valid {
		glog.V(1).Infof("jwt invalid from %s: %v", r.RemoteAddr, tokenStr)
		return false
	} else {
		return true
	}
}

// filerHealthzHandler 提供 /healthz 健康检查端点
//
// 【功能说明】
//   - 响应 Kubernetes、负载均衡器等的健康探测请求
//   - 检查 Filer 的基本可用性（能否访问元数据存储）
//   - 返回 HTTP 状态码表示健康状态
//
// 【健康检查逻辑】
//   - 尝试查找 TopicsDir 目录（.topics）
//   - 如果查找成功或目录不存在（ErrNotFound）：返回 200 OK
//   - 如果查找失败（连接错误、超时等）：返回 503 Service Unavailable
//
// 【返回状态码】
//   - 200 OK: Filer 正常工作，可以接受请求
//   - 503 Service Unavailable: Filer 不健康，无法访问元数据存储
//
// 【响应头】
//   - Server: SeaweedFS 版本号，便于监控识别
//
// 【使用场景】
//   - Kubernetes Liveness Probe: 检测进程是否存活
//   - Kubernetes Readiness Probe: 检测是否可以接受流量
//   - 负载均衡器健康检查: 决定是否将流量转发到此节点
func (fs *FilerServer) filerHealthzHandler(w http.ResponseWriter, r *http.Request) {
	// 设置服务器版本响应头
	w.Header().Set("Server", "SeaweedFS "+version.VERSION)

	// 尝试查找 TopicsDir 目录，检查元数据存储是否可用
	// TopicsDir = ".topics"，用于消息队列功能
	// 这里只是借用它来测试 Store 的可用性
	if _, err := fs.filer.Store.FindEntry(context.Background(), filer.TopicsDir); err != nil && err != filer_pb.ErrNotFound {
		// 查找失败且不是"不存在"错误，说明元数据存储不可用
		glog.Warningf("filerHealthzHandler FindEntry: %+v", err)
		w.WriteHeader(http.StatusServiceUnavailable) // 503
	} else {
		// 查找成功或目录不存在（ErrNotFound），Filer 正常工作
		w.WriteHeader(http.StatusOK) // 200
	}
}
