// Package weed_server 包含 Volume Server 的 HTTP 处理函数
// 本文件主要处理文件读取（下载）请求
package weed_server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
	"github.com/seaweedfs/seaweedfs/weed/util/mem"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/images"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const reqIsProxied = "proxied" // 标记请求是否已被代理（避免循环代理）

var fileNameEscaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`) // 文件名转义器

// NotFound 返回 404 Not Found 响应
func NotFound(w http.ResponseWriter) {
	stats.VolumeServerHandlerCounter.WithLabelValues(stats.ErrorGetNotFound).Inc()
	w.WriteHeader(http.StatusNotFound)
}

// InternalError 返回 500 Internal Server Error 响应
func InternalError(w http.ResponseWriter) {
	stats.VolumeServerHandlerCounter.WithLabelValues(stats.ErrorGetInternal).Inc()
	w.WriteHeader(http.StatusInternalServerError)
}

// proxyReqToTargetServer 当本地没有对应的 Volume 时，将请求代理或重定向到拥有该 Volume 的 Volume Server
// 流程：
// 1. 解析 URL 路径获取 vid 和 fid
// 2. 向 Master 查询该 Volume 所在的 Volume Server 位置
// 3. 根据 ReadMode 配置决定是代理（proxy）还是重定向（redirect）
func (vs *VolumeServer) proxyReqToTargetServer(w http.ResponseWriter, r *http.Request) {
	// 从 URL 路径中解析出 volumeId 和 fileId
	vid, fid, _, _, _ := parseURLPath(r.URL.Path)
	volumeId, err := needle.NewVolumeId(vid)
	if err != nil {
		glog.V(2).Infof("parsing vid %s: %v", r.URL.Path, err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// 向 Master 节点查询该 Volume 所在的 Volume Server 位置列表
	lookupResult, err := operation.LookupVolumeId(vs.GetMaster, vs.grpcDialOption, volumeId.String())
	if err != nil || len(lookupResult.Locations) <= 0 {
		glog.V(0).Infoln("lookup error:", err, r.URL.Path)
		NotFound(w)
		return
	}

	// 从查询结果中找到一个不是本机的 Volume Server 作为目标
	var tragetUrl *url.URL
	location := fmt.Sprintf("%s:%d", vs.store.Ip, vs.store.Port)
	for _, loc := range lookupResult.Locations {
		if !strings.Contains(loc.Url, location) {
			rawURL, _ := util_http.NormalizeUrl(loc.Url)
			tragetUrl, _ = url.Parse(rawURL)
			break
		}
	}
	if tragetUrl == nil {
		stats.VolumeServerHandlerCounter.WithLabelValues(stats.EmptyReadProxyLoc).Inc()
		glog.Errorf("failed lookup target host is empty locations: %+v, %s", lookupResult.Locations, location)
		NotFound(w)
		return
	}

	// 根据 ReadMode 配置处理请求
	if vs.ReadMode == "proxy" {
		// 代理模式：由本 Volume Server 代理转发请求，获取数据后再返回给客户端
		stats.VolumeServerHandlerCounter.WithLabelValues(stats.ReadProxyReq).Inc()
		// 构建到目标 Volume Server 的请求
		r.URL.Host = tragetUrl.Host
		r.URL.Scheme = tragetUrl.Scheme
		r.URL.Query().Add(reqIsProxied, "true") // 标记已被代理，防止循环代理
		request, err := http.NewRequest(http.MethodGet, r.URL.String(), nil)
		if err != nil {
			glog.V(0).Infof("failed to instance http request of url %s: %v", r.URL.String(), err)
			InternalError(w)
			return
		}
		// 复制原始请求的所有 HTTP 头
		for k, vv := range r.Header {
			for _, v := range vv {
				request.Header.Add(k, v)
			}
		}

		// 向目标 Volume Server 发起请求
		response, err := util_http.GetGlobalHttpClient().Do(request)
		if err != nil {
			stats.VolumeServerHandlerCounter.WithLabelValues(stats.FailedReadProxyReq).Inc()
			glog.V(0).Infof("request remote url %s: %v", r.URL.String(), err)
			InternalError(w)
			return
		}
		defer util_http.CloseResponse(response)

		// 将目标服务器的响应转发给客户端
		for k, vv := range response.Header {
			if k == "Server" {
				continue
			}
			for _, v := range vv {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(response.StatusCode)
		// 使用 128KB 缓冲区复制响应体数据
		buf := mem.Allocate(128 * 1024)
		defer mem.Free(buf)
		io.CopyBuffer(w, response.Body, buf)
		return
	} else {
		// 重定向模式：返回 301 重定向响应，让客户端直接访问拥有数据的 Volume Server
		stats.VolumeServerHandlerCounter.WithLabelValues(stats.ReadRedirectReq).Inc()
		tragetUrl.Path = fmt.Sprintf("%s/%s,%s", tragetUrl.Path, vid, fid)
		arg := url.Values{}
		if c := r.FormValue("collection"); c != "" {
			arg.Set("collection", c)
		}
		arg.Set(reqIsProxied, "true")
		tragetUrl.RawQuery = arg.Encode()
		http.Redirect(w, r, tragetUrl.String(), http.StatusMovedPermanently)
		return
	}
}

// GetOrHeadHandler 处理 GET 和 HEAD 请求，这是通过 fid 下载文件的核心处理函数
// 执行 curl "http://127.0.0.1:8080/<fid>" 时会调用此函数
//
// 主要流程：
// 1. 解析 URL 路径，提取 volumeId、fileId、filename、ext 等信息
// 2. JWT 授权验证（如果启用）
// 3. 检查本地是否有该 Volume（普通卷或 EC 卷）
// 4. 如果本地没有，根据 ReadMode 进行代理或重定向
// 5. 从存储中读取 Needle（文件数据块）
// 6. 处理 HTTP 缓存头（Last-Modified、ETag、If-Modified-Since、If-None-Match）
// 7. 处理分块文件（Chunked Manifest）
// 8. 处理图片裁剪和缩放
// 9. 处理数据压缩（gzip）
// 10. 返回文件数据给客户端（支持 Range 请求）
func (vs *VolumeServer) GetOrHeadHandler(w http.ResponseWriter, r *http.Request) {
	// 创建一个 Needle 对象用于存储读取的文件数据
	n := new(needle.Needle)
	// 从 URL 路径解析出 volumeId、fileId、filename、ext 等信息
	// 例如：/3,01637037d6 解析为 vid=3, fid=01637037d6
	vid, fid, filename, ext, _ := parseURLPath(r.URL.Path)

	// JWT 授权验证（如果启用了 JWT 安全机制）
	if !vs.maybeCheckJwtAuthorization(r, vid, fid, false) {
		writeJsonError(w, r, http.StatusUnauthorized, errors.New("wrong jwt"))
		return
	}

	// 将字符串形式的 volumeId 转换为 VolumeId 类型
	volumeId, err := needle.NewVolumeId(vid)
	if err != nil {
		glog.V(2).Infof("parsing vid %s: %v", r.URL.Path, err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	// 解析 fileId，提取 key、cookie 等信息
	err = n.ParsePath(fid)
	if err != nil {
		glog.V(2).Infof("parsing fid %s: %v", r.URL.Path, err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// glog.V(4).Infoln("volume", volumeId, "reading", n)
	// 检查本地是否有该普通 Volume
	hasVolume := vs.store.HasVolume(volumeId)
	// 检查本地是否有该 EC（Erasure Coding，纠删码）Volume
	_, hasEcVolume := vs.store.FindEcVolume(volumeId)
	if !hasVolume && !hasEcVolume {
		// 本地没有该 Volume，根据 ReadMode 配置处理
		if vs.ReadMode == "local" {
			// local 模式：只读取本地数据，不代理或重定向
			glog.V(0).Infoln("volume is not local:", err, r.URL.Path)
			NotFound(w)
			return
		}
		// 代理或重定向到拥有该 Volume 的 Volume Server
		vs.proxyReqToTargetServer(w, r)
		return
	}
	// 保存原始 cookie 用于后续验证
	cookie := n.Cookie

	// 构建读取选项
	readOption := &storage.ReadOption{
		ReadDeleted:    r.FormValue("readDeleted") == "true", // 是否读取已删除的文件
		HasSlowRead:    vs.hasSlowRead,                       // 是否有慢速读取
		ReadBufferSize: vs.readBufferSizeMB * 1024 * 1024,    // 读取缓冲区大小
	}

	var count int
	var memoryCost types.Size
	// 判断是否可以使用流式写入（仅读取元数据，数据部分边读边写）
	readOption.AttemptMetaOnly, readOption.MustMetaOnly = shouldAttemptStreamWrite(hasVolume, ext, r)
	// 内存使用计数回调函数，用于限流控制
	onReadSizeFn := func(size types.Size) {
		memoryCost = size
		atomic.AddInt64(&vs.inFlightDownloadDataSize, int64(memoryCost))
	}

	// 从存储中读取 Needle 数据
	if hasVolume {
		// 从普通 Volume 读取
		count, err = vs.store.ReadVolumeNeedle(volumeId, n, readOption, onReadSizeFn)
	} else if hasEcVolume {
		// 从 EC Volume 读取（纠删码卷，用于数据冗余和恢复）
		count, err = vs.store.ReadEcShardNeedle(volumeId, n, onReadSizeFn)
	}

	// 延迟执行：释放内存计数，触发限流条件变量
	defer func() {
		atomic.AddInt64(&vs.inFlightDownloadDataSize, -int64(memoryCost))
		if vs.concurrentDownloadLimit != 0 {
			vs.inFlightDownloadDataLimitCond.Broadcast()
		}
	}()

	// 如果读取出错且不是删除错误，且本地有普通 Volume，可以尝试从其他副本修复
	if err != nil && err != storage.ErrorDeleted && hasVolume {
		glog.V(4).Infof("read needle: %v", err)
		// start to fix it from other replicas, if not deleted and hasVolume and is not a replicated request
	}
	// glog.V(4).Infoln("read bytes", count, "error", err)
	// 处理读取错误
	if err != nil || count < 0 {
		glog.V(3).Infof("read %s isNormalVolume %v error: %v", r.URL.Path, hasVolume, err)
		if err == storage.ErrorNotFound || err == storage.ErrorDeleted || errors.Is(err, erasure_coding.NotFoundError) {
			NotFound(w)
		} else {
			InternalError(w)
		}
		return
	}
	// Cookie 验证：防止未授权访问
	if n.Cookie != cookie {
		glog.V(0).Infof("request %s with cookie:%x expected:%x from %s agent %s", r.URL.Path, cookie, n.Cookie, r.RemoteAddr, r.UserAgent())
		NotFound(w)
		return
	}

	// 处理 Last-Modified 和 If-Modified-Since HTTP 缓存头
	if n.LastModified != 0 {
		w.Header().Set("Last-Modified", time.Unix(int64(n.LastModified), 0).UTC().Format(http.TimeFormat))
		if r.Header.Get("If-Modified-Since") != "" {
			if t, parseError := time.Parse(http.TimeFormat, r.Header.Get("If-Modified-Since")); parseError == nil {
				if t.Unix() >= int64(n.LastModified) {
					// 文件未修改，返回 304 Not Modified
					w.WriteHeader(http.StatusNotModified)
					return
				}
			}
		}
	}
	// 处理 ETag 和 If-None-Match HTTP 缓存头
	if inm := r.Header.Get("If-None-Match"); inm == "\""+n.Etag()+"\"" {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	SetEtag(w, n.Etag())

	// 处理自定义 HTTP 头（从 Needle 的 Pairs 字段读取）
	if n.HasPairs() {
		pairMap := make(map[string]string)
		err = json.Unmarshal(n.Pairs, &pairMap)
		if err != nil {
			glog.V(0).Infoln("Unmarshal pairs error:", err)
		}
		for k, v := range pairMap {
			w.Header().Set(k, v)
		}
	}

	// 处理分块文件（Chunked File）：大文件被分成多个 chunk 存储
	if vs.tryHandleChunkedFile(n, filename, ext, w, r) {
		return
	}

	// 如果 URL 中没有指定 filename，从 Needle 中读取
	if n.NameSize > 0 && filename == "" {
		filename = string(n.Name)
		if ext == "" {
			ext = filepath.Ext(filename)
		}
	}
	// 读取 MIME 类型
	mtype := ""
	if n.MimeSize > 0 {
		mt := string(n.Mime)
		if !strings.HasPrefix(mt, "application/octet-stream") {
			mtype = mt
		}
	}

	// 处理压缩数据（gzip）
	if n.IsCompressed() {
		_, _, _, shouldResize := shouldResizeImages(ext, r)
		_, _, _, _, shouldCrop := shouldCropImages(ext, r)
		if shouldResize || shouldCrop {
			// 如果需要图片处理，先解压
			if n.Data, err = util.DecompressData(n.Data); err != nil {
				glog.V(0).Infoln("ungzip error:", err, r.URL.Path)
			}
			// } else if strings.Contains(r.Header.Get("Accept-Encoding"), "zstd") && util.IsZstdContent(n.Data) {
			//	w.Header().Set("Content-Encoding", "zstd")
		} else if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") && util.IsGzippedContent(n.Data) {
			// 客户端支持 gzip，直接返回压缩数据
			w.Header().Set("Content-Encoding", "gzip")
		} else {
			// 客户端不支持 gzip，解压后返回
			if n.Data, err = util.DecompressData(n.Data); err != nil {
				glog.V(0).Infoln("uncompress error:", err, r.URL.Path)
			}
		}
	}

	// 写入响应数据
	if !readOption.IsMetaOnly {
		// 普通模式：数据已经全部加载到内存
		// 处理图片裁剪
		rs := conditionallyCropImages(bytes.NewReader(n.Data), ext, r)
		// 处理图片缩放
		rs = conditionallyResizeImages(rs, ext, r)
		// 写入响应（支持 Range 请求）
		if e := writeResponseContent(filename, mtype, rs, w, r); e != nil {
			glog.V(2).Infoln("response write error:", e)
		}
	} else {
		// 流式模式：仅读取了元数据，数据部分边读边写（适用于大文件）
		vs.streamWriteResponseContent(filename, mtype, volumeId, n, w, r, readOption)
	}
}

// shouldAttemptStreamWrite 判断是否应该尝试流式写入
// 流式写入模式下，只读取文件的元数据（metadata），数据部分在需要时才从磁盘读取并直接写入响应
// 这样可以减少内存占用，适合大文件下载
//
// 返回值：
// - shouldAttempt: 是否应该尝试流式写入
// - mustMetaOnly: 是否必须只读取元数据（HEAD 请求）
func shouldAttemptStreamWrite(hasLocalVolume bool, ext string, r *http.Request) (shouldAttempt bool, mustMetaOnly bool) {
	// 如果本地没有 Volume，无法流式读取
	if !hasLocalVolume {
		return false, false
	}
	if len(ext) > 0 {
		ext = strings.ToLower(ext)
	}
	// HEAD 请求只需要元数据，不需要实际数据
	if r.Method == http.MethodHead {
		return true, true
	}
	// 检查是否需要图片处理（缩放或裁剪）
	_, _, _, shouldResize := shouldResizeImages(ext, r)
	_, _, _, _, shouldCrop := shouldCropImages(ext, r)
	if shouldResize || shouldCrop {
		// 图片处理需要完整数据在内存中，不能使用流式写入
		return false, false
	}
	// 可以尝试流式写入，但不强制只读元数据
	return true, false
}

// tryHandleChunkedFile 尝试处理分块文件（Chunked File）
// 大文件（超过一定大小阈值）会被 SeaweedFS 分割成多个小块（chunks）分别存储
// 第一个 Needle 存储的是 Chunk Manifest（分块清单），记录了各个分块的位置信息
//
// 参数：
// - n: 当前 Needle（可能包含 Chunk Manifest）
// - fileName: 文件名
// - ext: 文件扩展名
// - w: HTTP 响应写入器
// - r: HTTP 请求
//
// 返回值：
// - processed: 如果是分块文件并已处理，返回 true；否则返回 false
func (vs *VolumeServer) tryHandleChunkedFile(n *needle.Needle, fileName string, ext string, w http.ResponseWriter, r *http.Request) (processed bool) {
	// 检查是否为分块清单，以及是否禁用了分块处理（cm=false）
	if !n.IsChunkedManifest() || r.URL.Query().Get("cm") == "false" {
		return false
	}

	// 从 Needle 数据中加载分块清单
	chunkManifest, e := operation.LoadChunkManifest(n.Data, n.IsCompressed())
	if e != nil {
		glog.V(0).Infof("load chunked manifest (%s) error: %v", r.URL.Path, e)
		return false
	}
	// 如果 URL 没有指定文件名，使用清单中的文件名
	if fileName == "" && chunkManifest.Name != "" {
		fileName = chunkManifest.Name
	}

	if ext == "" {
		ext = filepath.Ext(fileName)
	}

	// 获取 MIME 类型
	mType := ""
	if chunkManifest.Mime != "" {
		mt := chunkManifest.Mime
		if !strings.HasPrefix(mt, "application/octet-stream") {
			mType = mt
		}
	}

	// 设置响应头，标识这是一个分块存储的文件
	w.Header().Set("X-File-Store", "chunked")

	// 创建分块文件读取器，它会自动从各个 chunk 所在的 Volume Server 读取数据并拼接
	chunkedFileReader := operation.NewChunkedFileReader(chunkManifest.Chunks, vs.GetMaster(context.Background()), vs.grpcDialOption)
	defer chunkedFileReader.Close()

	// 处理图片裁剪
	rs := conditionallyCropImages(chunkedFileReader, ext, r)
	// 处理图片缩放
	rs = conditionallyResizeImages(rs, ext, r)

	// 写入响应数据
	if e := writeResponseContent(fileName, mType, rs, w, r); e != nil {
		glog.V(2).Infoln("response write error:", e)
	}
	return true
}

// conditionallyResizeImages 根据请求参数有条件地缩放图片
// 支持通过 URL 参数 width、height、mode 来调整图片尺寸
//
// 参数：
// - originalDataReaderSeeker: 原始图片数据的 ReadSeeker
// - ext: 文件扩展名（用于判断是否为图片）
// - r: HTTP 请求（包含缩放参数）
//
// 返回值：
// - 如果需要缩放，返回缩放后的图片数据；否则返回原始数据
func conditionallyResizeImages(originalDataReaderSeeker io.ReadSeeker, ext string, r *http.Request) io.ReadSeeker {
	rs := originalDataReaderSeeker
	if len(ext) > 0 {
		ext = strings.ToLower(ext)
	}
	// 检查是否需要缩放
	width, height, mode, shouldResize := shouldResizeImages(ext, r)
	if shouldResize {
		// 执行图片缩放
		rs, _, _ = images.Resized(ext, originalDataReaderSeeker, width, height, mode)
	}
	return rs
}

// shouldResizeImages 检查是否应该缩放图片
// 通过 URL 查询参数指定：?width=100&height=100&mode=fit
//
// 参数：
// - ext: 文件扩展名
// - r: HTTP 请求
//
// 返回值：
// - width: 目标宽度
// - height: 目标高度
// - mode: 缩放模式（fit、fill 等）
// - shouldResize: 是否需要缩放
func shouldResizeImages(ext string, r *http.Request) (width, height int, mode string, shouldResize bool) {
	// 只对图片格式进行缩放
	if ext == ".png" || ext == ".jpg" || ext == ".jpeg" || ext == ".gif" || ext == ".webp" {
		if r.FormValue("width") != "" {
			width, _ = strconv.Atoi(r.FormValue("width"))
		}
		if r.FormValue("height") != "" {
			height, _ = strconv.Atoi(r.FormValue("height"))
		}
	}
	mode = r.FormValue("mode")
	shouldResize = width > 0 || height > 0
	return
}

// conditionallyCropImages 根据请求参数有条件地裁剪图片
// 支持通过 URL 参数指定裁剪区域
//
// 参数：
// - originalDataReaderSeeker: 原始图片数据的 ReadSeeker
// - ext: 文件扩展名（用于判断是否为图片）
// - r: HTTP 请求（包含裁剪参数）
//
// 返回值：
// - 如果需要裁剪，返回裁剪后的图片数据；否则返回原始数据
func conditionallyCropImages(originalDataReaderSeeker io.ReadSeeker, ext string, r *http.Request) io.ReadSeeker {
	rs := originalDataReaderSeeker
	if len(ext) > 0 {
		ext = strings.ToLower(ext)
	}
	// 检查是否需要裁剪
	x1, y1, x2, y2, shouldCrop := shouldCropImages(ext, r)
	if shouldCrop {
		var err error
		// 执行图片裁剪
		rs, err = images.Cropped(ext, rs, x1, y1, x2, y2)
		if err != nil {
			glog.Errorf("Cropping images error: %s", err)
		}
	}
	return rs
}

// shouldCropImages 检查是否应该裁剪图片
// 通过 URL 查询参数指定裁剪区域：?crop_x1=0&crop_y1=0&crop_x2=100&crop_y2=100
//
// 参数：
// - ext: 文件扩展名
// - r: HTTP 请求
//
// 返回值：
// - x1, y1: 裁剪区域左上角坐标
// - x2, y2: 裁剪区域右下角坐标
// - shouldCrop: 是否需要裁剪
func shouldCropImages(ext string, r *http.Request) (x1, y1, x2, y2 int, shouldCrop bool) {
	// 只对图片格式进行裁剪
	if ext == ".png" || ext == ".jpg" || ext == ".jpeg" || ext == ".gif" {
		if r.FormValue("crop_x1") != "" {
			x1, _ = strconv.Atoi(r.FormValue("crop_x1"))
		}
		if r.FormValue("crop_y1") != "" {
			y1, _ = strconv.Atoi(r.FormValue("crop_y1"))
		}
		if r.FormValue("crop_x2") != "" {
			x2, _ = strconv.Atoi(r.FormValue("crop_x2"))
		}
		if r.FormValue("crop_y2") != "" {
			y2, _ = strconv.Atoi(r.FormValue("crop_y2"))
		}
	}
	// 验证裁剪区域的有效性
	shouldCrop = x1 >= 0 && y1 >= 0 && x2 > x1 && y2 > y1
	return
}

// writeResponseContent 将文件内容写入 HTTP 响应
// 支持 HTTP Range 请求（断点续传、分段下载）
//
// 参数：
// - filename: 文件名（用于 Content-Disposition 头）
// - mimeType: MIME 类型（用于 Content-Type 头）
// - rs: 文件数据的 ReadSeeker
// - w: HTTP 响应写入器
// - r: HTTP 请求（可能包含 Range 头）
//
// 返回值：
// - error: 如果写入失败，返回错误
func writeResponseContent(filename, mimeType string, rs io.ReadSeeker, w http.ResponseWriter, r *http.Request) error {
	// 定位到文件末尾以获取总大小
	totalSize, e := rs.Seek(0, 2)
	// 如果没有指定 MIME 类型，根据文件扩展名推断
	if mimeType == "" {
		if ext := filepath.Ext(filename); ext != "" {
			mimeType = mime.TypeByExtension(ext)
		}
	}
	if mimeType != "" {
		w.Header().Set("Content-Type", mimeType)
	}
	// 支持 Range 请求（用于断点续传）
	w.Header().Set("Accept-Ranges", "bytes")

	// 调整透传的 HTTP 头（如 Content-Disposition 等）
	AdjustPassthroughHeaders(w, r, filename)

	// HEAD 请求只返回头部信息，不返回实际内容
	if r.Method == http.MethodHead {
		w.Header().Set("Content-Length", strconv.FormatInt(totalSize, 10))
		return nil
	}

	// 处理 Range 请求（支持分段下载）
	return ProcessRangeRequest(r, w, totalSize, mimeType, func(offset int64, size int64) (filer.DoStreamContent, error) {
		return func(writer io.Writer) error {
			// 定位到请求的起始位置
			if _, e = rs.Seek(offset, 0); e != nil {
				return e
			}
			// 复制指定大小的数据到响应
			_, e = io.CopyN(writer, rs, size)
			return e
		}, nil
	})
}

// streamWriteResponseContent 以流式方式将文件内容写入 HTTP 响应
// 与 writeResponseContent 不同，此函数不会将整个文件加载到内存
// 而是边从磁盘读取边写入响应，适合大文件下载
//
// 参数：
// - filename: 文件名
// - mimeType: MIME 类型
// - volumeId: Volume ID
// - n: Needle（仅包含元数据，不含实际数据）
// - w: HTTP 响应写入器
// - r: HTTP 请求
// - readOption: 读取选项
func (vs *VolumeServer) streamWriteResponseContent(filename string, mimeType string, volumeId needle.VolumeId, n *needle.Needle, w http.ResponseWriter, r *http.Request, readOption *storage.ReadOption) {
	// 从 Needle 元数据中获取文件大小
	totalSize := int64(n.DataSize)
	// 如果没有指定 MIME 类型，根据文件扩展名推断
	if mimeType == "" {
		if ext := filepath.Ext(filename); ext != "" {
			mimeType = mime.TypeByExtension(ext)
		}
	}
	if mimeType != "" {
		w.Header().Set("Content-Type", mimeType)
	}
	// 支持 Range 请求
	w.Header().Set("Accept-Ranges", "bytes")
	// 调整透传的 HTTP 头
	AdjustPassthroughHeaders(w, r, filename)

	// HEAD 请求只返回头部信息
	if r.Method == http.MethodHead {
		w.Header().Set("Content-Length", strconv.FormatInt(totalSize, 10))
		return
	}

	// 处理 Range 请求，支持流式读取指定范围的数据
	ProcessRangeRequest(r, w, totalSize, mimeType, func(offset int64, size int64) (filer.DoStreamContent, error) {
		return func(writer io.Writer) error {
			// 直接从 Volume 读取指定范围的数据并写入响应
			// 这样避免了将整个文件加载到内存
			return vs.store.ReadVolumeNeedleDataInto(volumeId, n, readOption, writer, offset, size)
		}, nil
	})

}
