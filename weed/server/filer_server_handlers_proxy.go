// Package weed_server 中的 filer_server_handlers_proxy.go 负责 Filer 对 Volume 的反向代理逻辑
// 该文件确保 HTTP 请求能够透传到正确的 Volume Server 并注入符合要求的 JWT。
package weed_server

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/security"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
	"github.com/seaweedfs/seaweedfs/weed/util/mem"
	"github.com/seaweedfs/seaweedfs/weed/util/request_id"

	"io"
	"math/rand/v2"
	"net/http"
)

// maybeAddVolumeJwtAuthorization 为代理请求添加 Volume 级别的 JWT 认证
// 根据 isWrite 选择读写不同的签名密钥，确保 Volume 端严格鉴权
//
// JWT 认证的目的:
//   - 防止未授权的客户端直接访问 Volume Server
//   - 确保只有通过 Filer 或 Master 授权的请求才能访问 Volume
//   - 读写使用不同的密钥，提供更细粒度的权限控制
//
// 参数:
//   - r: HTTP 请求对象（会被修改，添加 Authorization 头）
//   - fileId: 文件 ID（格式: volumeId,fileKey[_cookie]）
//   - isWrite: true 表示写操作，false 表示读操作
func (fs *FilerServer) maybeAddVolumeJwtAuthorization(r *http.Request, fileId string, isWrite bool) {
	// 【步骤 1: 生成 JWT 令牌】
	// 根据操作类型（读/写）生成相应的 JWT
	encodedJwt := fs.maybeGetVolumeJwtAuthorizationToken(fileId, isWrite)

	// 【步骤 2: 检查是否需要添加认证】
	// 如果返回空字符串，说明没有配置 JWT 认证
	// 直接返回，不添加 Authorization 头
	if encodedJwt == "" {
		return
	}

	// 【步骤 3: 添加 Authorization 头】
	// 使用 BEARER 认证方案（OAuth 2.0 标准）
	// Volume Server 会验证这个 JWT 令牌
	r.Header.Set("Authorization", "BEARER "+string(encodedJwt))
}

// maybeGetVolumeJwtAuthorizationToken 生成 Volume 访问的 JWT 令牌
// 返回空字符串表示无需鉴权，例如 volumeGuard 未设置
//
// JWT 内容:
//   - fileId: 被访问的文件 ID
//   - 过期时间: 根据配置的过期时间生成
//   - 签名: 使用配置的密钥签名
//
// 参数:
//   - fileId: 文件 ID
//   - isWrite: true 使用写密钥，false 使用读密钥
//
// 返回:
//   - string: JWT 令牌字符串，空字符串表示无需认证
func (fs *FilerServer) maybeGetVolumeJwtAuthorizationToken(fileId string, isWrite bool) string {
	var encodedJwt security.EncodedJwt

	// 【步骤 1: 根据操作类型选择密钥和过期时间】
	if isWrite {
		// 【写操作】使用写密钥和写过期时间
		// 写密钥通常更严格，过期时间更短
		encodedJwt = security.GenJwtForVolumeServer(fs.volumeGuard.SigningKey, fs.volumeGuard.ExpiresAfterSec, fileId)
	} else {
		// 【读操作】使用读密钥和读过期时间
		// 读密钥可以配置较长的过期时间，因为读操作风险较低
		encodedJwt = security.GenJwtForVolumeServer(fs.volumeGuard.ReadSigningKey, fs.volumeGuard.ReadExpiresAfterSec, fileId)
	}

	// 【步骤 2: 返回编码后的 JWT】
	// GenJwtForVolumeServer 会返回 Base64 编码的 JWT 字符串
	// 如果 volumeGuard 未配置（SigningKey 为空），会返回空字符串
	return string(encodedJwt)
}

// proxyToVolumeServer 将请求反向代理到 Volume Server
// 会从 master 缓存中选取一个地址并透传请求头，保持幂等行为
//
// 使用场景:
//   - 客户端不知道文件存储在哪个 Volume Server
//   - 通过 Filer 作为统一入口访问文件
//   - Filer 负责查询文件位置并转发请求
//
// 工作流程:
//   1. 通过 Master 查询 fileId 对应的 Volume Server 地址
//   2. 从多个副本中随机选择一个
//   3. 构造代理请求并透传原始请求头
//   4. 将 Volume Server 的响应转发回客户端
//
// 参数:
//   - w: HTTP 响应写入器
//   - r: 原始 HTTP 请求
//   - fileId: 文件 ID（格式: volumeId,fileKey[_cookie]）
func (fs *FilerServer) proxyToVolumeServer(w http.ResponseWriter, r *http.Request, fileId string) {
	// 【步骤 1: 查询文件位置】
	ctx := r.Context()

	// 通过 Master Client 查询 fileId 对应的 Volume Server 地址
	// GetLookupFileIdFunction 返回一个查询函数，可能会使用缓存
	// urlStrings 是 Volume Server 的 URL 列表（多个副本）
	urlStrings, err := fs.filer.MasterClient.GetLookupFileIdFunction()(ctx, fileId)
	if err != nil {
		// 查询失败，可能是 Master 不可用或 fileId 格式错误
		glog.ErrorfCtx(ctx, "locate %s: %v", fileId, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// 【步骤 2: 检查是否找到 Volume】
	if len(urlStrings) == 0 {
		// 没有找到任何 Volume Server
		// 可能的原因:
		//   1. Volume 已被删除
		//   2. Volume 正在迁移
		//   3. 所有副本都不可用
		w.WriteHeader(http.StatusNotFound)
		return
	}

	// 【步骤 3: 构造代理请求】
	// 从多个副本中随机选择一个 URL
	// 使用随机选择实现简单的负载均衡
	targetUrl := urlStrings[rand.IntN(len(urlStrings))]

	// 创建新的 HTTP 请求
	// 保持与原请求相同的方法（GET/POST/PUT/DELETE 等）
	// 使用原请求的 Body（对于流式请求很重要）
	proxyReq, err := http.NewRequest(r.Method, targetUrl, r.Body)
	if err != nil {
		glog.ErrorfCtx(ctx, "NewRequest %s: %v", urlStrings[0], err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// 【步骤 4: 设置代理相关的请求头】
	// Host: 保持原始请求的 Host 头
	proxyReq.Header.Set("Host", r.Host)

	// X-Forwarded-For: 记录原始客户端的 IP 地址
	// 这样 Volume Server 可以知道真实的客户端 IP
	proxyReq.Header.Set("X-Forwarded-For", r.RemoteAddr)

	// 注入请求 ID，用于追踪和调试
	// 整个请求链路（客户端 -> Filer -> Volume）会使用同一个请求 ID
	request_id.InjectToRequest(ctx, proxyReq)

	// 【步骤 5: 复制所有原始请求头】
	// 透传客户端的所有请求头到 Volume Server
	// 例如: Authorization, Content-Type, Range 等
	for header, values := range r.Header {
		for _, value := range values {
			proxyReq.Header.Add(header, value)
		}
	}

	// 【步骤 6: 发送代理请求】
	// 使用全局 HTTP 客户端发送请求到 Volume Server
	// 全局客户端会复用连接，提高性能
	proxyResponse, postErr := util_http.GetGlobalHttpClient().Do(proxyReq)

	if postErr != nil {
		// 请求失败，可能是 Volume Server 不可用
		glog.ErrorfCtx(ctx, "post to filer: %v", postErr)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer util_http.CloseResponse(proxyResponse)

	// 【步骤 7: 复制响应头】
	// 将 Volume Server 的所有响应头转发给客户端
	// 包括: Content-Type, Content-Length, ETag, Last-Modified 等
	for k, v := range proxyResponse.Header {
		w.Header()[k] = v
	}

	// 【步骤 8: 设置响应状态码】
	// 保持与 Volume Server 相同的状态码
	w.WriteHeader(proxyResponse.StatusCode)

	// 【步骤 9: 流式复制响应体】
	// 分配 128KB 的缓冲区用于数据传输
	buf := mem.Allocate(128 * 1024)
	defer mem.Free(buf)

	// 使用 CopyBuffer 流式复制响应体
	// 不会将整个响应加载到内存，适合大文件
	io.CopyBuffer(w, proxyResponse.Body, buf)

}
