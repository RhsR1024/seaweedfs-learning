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

// maybeAddVolumeJwtAuthorization 为代理请求添加 Volume 级别的 JWT
// 根据 isWrite 选择读写不同的签名密钥，确保 Volume 端严格鉴权
func (fs *FilerServer) maybeAddVolumeJwtAuthorization(r *http.Request, fileId string, isWrite bool) {
	encodedJwt := fs.maybeGetVolumeJwtAuthorizationToken(fileId, isWrite)

	if encodedJwt == "" {
		return
	}

	r.Header.Set("Authorization", "BEARER "+string(encodedJwt))
}

// maybeGetVolumeJwtAuthorizationToken 封装 JWT 生成逻辑
// 返回空字符串表示无需鉴权，例如 volumeGuard 未设置
func (fs *FilerServer) maybeGetVolumeJwtAuthorizationToken(fileId string, isWrite bool) string {
	var encodedJwt security.EncodedJwt
	if isWrite {
		encodedJwt = security.GenJwtForVolumeServer(fs.volumeGuard.SigningKey, fs.volumeGuard.ExpiresAfterSec, fileId)
	} else {
		encodedJwt = security.GenJwtForVolumeServer(fs.volumeGuard.ReadSigningKey, fs.volumeGuard.ReadExpiresAfterSec, fileId)
	}
	return string(encodedJwt)
}

// proxyToVolumeServer 将请求转发给持有 fileId 的 Volume Server
// 会从 master 缓存中选取一个地址并透传请求头，保持幂等行为
func (fs *FilerServer) proxyToVolumeServer(w http.ResponseWriter, r *http.Request, fileId string) {
	ctx := r.Context()
	urlStrings, err := fs.filer.MasterClient.GetLookupFileIdFunction()(ctx, fileId)
	if err != nil {
		glog.ErrorfCtx(ctx, "locate %s: %v", fileId, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if len(urlStrings) == 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	proxyReq, err := http.NewRequest(r.Method, urlStrings[rand.IntN(len(urlStrings))], r.Body)
	if err != nil {
		glog.ErrorfCtx(ctx, "NewRequest %s: %v", urlStrings[0], err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	proxyReq.Header.Set("Host", r.Host)
	proxyReq.Header.Set("X-Forwarded-For", r.RemoteAddr)
	request_id.InjectToRequest(ctx, proxyReq)

	for header, values := range r.Header {
		for _, value := range values {
			proxyReq.Header.Add(header, value)
		}
	}

	proxyResponse, postErr := util_http.GetGlobalHttpClient().Do(proxyReq)

	if postErr != nil {
		glog.ErrorfCtx(ctx, "post to filer: %v", postErr)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer util_http.CloseResponse(proxyResponse)

	for k, v := range proxyResponse.Header {
		w.Header()[k] = v
	}
	w.WriteHeader(proxyResponse.StatusCode)

	buf := mem.Allocate(128 * 1024)
	defer mem.Free(buf)
	io.CopyBuffer(w, proxyResponse.Body, buf)

}
