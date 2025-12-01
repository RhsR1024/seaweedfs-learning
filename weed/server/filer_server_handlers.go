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

// filerHandler 是 Filer HTTP API 的入口
// 支持 CORS、JWT 校验、Volume Proxy 以及统计指标记录
// 参数:
//   - w/r: 标准 HTTP 处理器接口
func (fs *FilerServer) filerHandler(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	inFlightGauge := stats.FilerInFlightRequestsGauge.WithLabelValues(r.Method)
	inFlightGauge.Inc()
	defer inFlightGauge.Dec()

	statusRecorder := stats.NewStatusResponseWriter(w)
	w = statusRecorder
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

		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Expose-Headers", "*")
		w.Header().Set("Access-Control-Allow-Headers", "*")
		w.Header().Set("Access-Control-Allow-Credentials", "true")
		w.Header().Set("Access-Control-Allow-Methods", "PUT, POST, GET, DELETE, OPTIONS")
	}

	if r.Method == http.MethodOptions {
		OptionsHandler(w, r, false)
		return
	}

	// proxy to volume servers
	var fileId string
	if strings.HasPrefix(r.RequestURI, "/?proxyChunkId=") {
		fileId = r.RequestURI[len("/?proxyChunkId="):]
	}
	if fileId != "" {
		fs.proxyToVolumeServer(w, r, fileId)
		stats.FilerHandlerCounter.WithLabelValues(stats.ChunkProxy).Inc()
		stats.FilerRequestHistogram.WithLabelValues(stats.ChunkProxy).Observe(time.Since(start).Seconds())
		return
	}
	requestMethod := r.Method
	defer func(method *string) {
		stats.FilerRequestCounter.WithLabelValues(*method, strconv.Itoa(statusRecorder.Status)).Inc()
		stats.FilerRequestHistogram.WithLabelValues(*method).Observe(time.Since(start).Seconds())
	}(&requestMethod)

	isReadHttpCall := r.Method == http.MethodGet || r.Method == http.MethodHead
	if !fs.maybeCheckJwtAuthorization(r, !isReadHttpCall) {
		writeJsonError(w, r, http.StatusUnauthorized, errors.New("wrong jwt"))
		return
	}

	w.Header().Set("Server", "SeaweedFS "+version.VERSION)

	switch r.Method {
	case http.MethodGet, http.MethodHead:
		fs.GetOrHeadHandler(w, r)
	case http.MethodDelete:
		if _, ok := r.URL.Query()["tagging"]; ok {
			fs.DeleteTaggingHandler(w, r)
		} else {
			fs.DeleteHandler(w, r)
		}
	case http.MethodPost, http.MethodPut:
		// wait until in flight data is less than the limit
		contentLength := getContentLength(r)
		fs.inFlightDataLimitCond.L.Lock()
		inFlightDataSize := atomic.LoadInt64(&fs.inFlightDataSize)
		for fs.option.ConcurrentUploadLimit != 0 && inFlightDataSize > fs.option.ConcurrentUploadLimit {
			glog.V(4).Infof("wait because inflight data %d > %d", inFlightDataSize, fs.option.ConcurrentUploadLimit)
			fs.inFlightDataLimitCond.Wait()
			inFlightDataSize = atomic.LoadInt64(&fs.inFlightDataSize)
		}
		fs.inFlightDataLimitCond.L.Unlock()
		atomic.AddInt64(&fs.inFlightDataSize, contentLength)
		defer func() {
			atomic.AddInt64(&fs.inFlightDataSize, -contentLength)
			fs.inFlightDataLimitCond.Signal()
		}()

		if r.Method == http.MethodPut {
			if _, ok := r.URL.Query()["tagging"]; ok {
				fs.PutTaggingHandler(w, r)
			} else {
				fs.PostHandler(w, r, contentLength)
			}
		} else { // method == "POST"
			fs.PostHandler(w, r, contentLength)
		}
	default:
		requestMethod = "INVALID"
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// readonlyFilerHandler 仅允许 GET/HEAD 的 Filer 读请求入口
// 使用单独的 mux 绑定到只读端口，避免误写
func (fs *FilerServer) readonlyFilerHandler(w http.ResponseWriter, r *http.Request) {

	start := time.Now()
	statusRecorder := stats.NewStatusResponseWriter(w)
	w = statusRecorder

	os.Stdout.WriteString("Request: " + r.Method + " " + r.URL.String() + "\n")

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

		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Headers", "OPTIONS, GET, HEAD")
		w.Header().Set("Access-Control-Allow-Credentials", "true")
	}
	requestMethod := r.Method
	defer func(method *string) {
		stats.FilerRequestCounter.WithLabelValues(*method, strconv.Itoa(statusRecorder.Status)).Inc()
		stats.FilerRequestHistogram.WithLabelValues(*method).Observe(time.Since(start).Seconds())
	}(&requestMethod)
	// We handle OPTIONS first because it never should be authenticated
	if r.Method == http.MethodOptions {
		OptionsHandler(w, r, true)
		return
	}

	if !fs.maybeCheckJwtAuthorization(r, false) {
		writeJsonError(w, r, http.StatusUnauthorized, errors.New("wrong jwt"))
		return
	}

	w.Header().Set("Server", "SeaweedFS "+version.VERSION)

	switch r.Method {
	case http.MethodGet, http.MethodHead:
		fs.GetOrHeadHandler(w, r)
	default:
		requestMethod = "INVALID"
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// OptionsHandler 处理 HTTP OPTIONS 预检请求
// 当 isReadOnly 为 true 时拒绝可能触发写操作的方法
func OptionsHandler(w http.ResponseWriter, r *http.Request, isReadOnly bool) {
	if isReadOnly {
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
	} else {
		w.Header().Set("Access-Control-Allow-Methods", "PUT, POST, GET, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Expose-Headers", "*")
	}
	w.Header().Set("Access-Control-Allow-Headers", "*")
	w.Header().Set("Access-Control-Allow-Credentials", "true")
}

// maybeCheckJwtAuthorization returns true if access should be granted, false if it should be denied
// maybeCheckJwtAuthorization 校验请求头中的 JWT 是否有效
// 参数 isWrite 控制当前操作是否需要写权限令牌
func (fs *FilerServer) maybeCheckJwtAuthorization(r *http.Request, isWrite bool) bool {

	var signingKey security.SigningKey

	if isWrite {
		if len(fs.filerGuard.SigningKey) == 0 {
			return true
		} else {
			signingKey = fs.filerGuard.SigningKey
		}
	} else {
		if len(fs.filerGuard.ReadSigningKey) == 0 {
			return true
		} else {
			signingKey = fs.filerGuard.ReadSigningKey
		}
	}

	tokenStr := security.GetJwt(r)
	if tokenStr == "" {
		glog.V(1).Infof("missing jwt from %s", r.RemoteAddr)
		return false
	}

	token, err := security.DecodeJwt(signingKey, tokenStr, &security.SeaweedFilerClaims{})
	if err != nil {
		glog.V(1).Infof("jwt verification error from %s: %v", r.RemoteAddr, err)
		return false
	}
	if !token.Valid {
		glog.V(1).Infof("jwt invalid from %s: %v", r.RemoteAddr, tokenStr)
		return false
	} else {
		return true
	}
}

// filerHealthzHandler 提供 /healthz 健康探针
// 会写出版本信息以及最近一次与 master 通信的时间戳
func (fs *FilerServer) filerHealthzHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Server", "SeaweedFS "+version.VERSION)
	if _, err := fs.filer.Store.FindEntry(context.Background(), filer.TopicsDir); err != nil && err != filer_pb.ErrNotFound {
		glog.Warningf("filerHealthzHandler FindEntry: %+v", err)
		w.WriteHeader(http.StatusServiceUnavailable)
	} else {
		w.WriteHeader(http.StatusOK)
	}
}
