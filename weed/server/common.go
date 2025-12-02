// Package weed_server 提供 Master、Volume、Filer 等 HTTP/gRPC 服务的通用辅助函数
// 本文件包含 HTTP 工具、静态资源挂载、Range 处理、请求统计等基础能力。
package weed_server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"mime/multipart"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/util/request_id"
	"github.com/seaweedfs/seaweedfs/weed/util/version"
	"google.golang.org/grpc/metadata"

	"github.com/seaweedfs/seaweedfs/weed/filer"

	"google.golang.org/grpc"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// serverStats 全局服务器统计信息
// 用于收集和记录服务器的运行统计数据（如请求数、错误数等）
var serverStats *stats.ServerStats

// startTime 服务器启动时间
// 用于计算服务器运行时长
var startTime = time.Now()

// writePool 写入缓冲池
// 对象池，用于复用 bufio.Writer 对象，避免频繁的内存分配
// 缓冲区大小：128KB
//
// 使用场景：
//   - HTTP 响应写入
//   - Range 请求的多部分响应
//
// 优势：
//   - 减少内存分配次数
//   - 降低 GC 压力
//   - 提高并发性能
var writePool = sync.Pool{New: func() interface{} {
	return bufio.NewWriterSize(nil, 128*1024)
},
}

// init 初始化函数
// 在包导入时自动执行
//
// 工作内容:
//  1. 创建全局服务器统计对象
//  2. 启动统计数据收集协程
func init() {
	serverStats = stats.NewServerStats()
	go serverStats.Start()
}

// bodyAllowedForStatus 判断 HTTP 状态码是否允许有响应体
// 这是 http.bodyAllowedForStatus 的复制版本（原版是内部函数）
//
// 参数:
//   - status: HTTP 状态码
//
// 返回值:
//   - bool: true 表示允许有响应体，false 表示不允许
//
// 规则:
//   - 1xx (100-199): 信息性响应，不允许有响应体
//   - 204 No Content: 明确表示无内容，不允许有响应体
//   - 304 Not Modified: 缓存相关，不允许有响应体
//   - 其他状态码: 允许有响应体
//
// HTTP 规范参考：RFC 7230, Section 3.3
func bodyAllowedForStatus(status int) bool {
	switch {
	case status >= 100 && status <= 199:
		return false
	case status == http.StatusNoContent:
		return false
	case status == http.StatusNotModified:
		return false
	}
	return true
}

// writeJson 将对象序列化为 JSON 并写入 HTTP 响应
// 支持普通 JSON 响应和 JSONP 回调
//
// 参数:
//   - w: HTTP 响应写入器
//   - r: HTTP 请求（用于读取 pretty 和 callback 参数）
//   - httpStatus: HTTP 状态码
//   - obj: 要序列化的对象
//
// 返回值:
//   - error: 序列化或写入错误
//
// 支持的 URL 参数:
//   - pretty: 如果存在，输出格式化的 JSON（带缩进）
//   - callback: JSONP 回调函数名（如果存在，输出 JSONP 格式）
//
// 响应格式:
//   - 普通 JSON: {"key": "value"}
//   - 格式化 JSON: {
//     "key": "value"
//     }
//   - JSONP: callback({"key": "value"})
//
// 特殊处理:
//   - 状态码 >= 400 时，记录详细日志
//   - 304 Not Modified 或其他不允许响应体的状态码，只发送头部
//
// 使用示例:
//
//	writeJson(w, r, http.StatusOK, map[string]string{"status": "success"})
//	// 普通 JSON: ?pretty=1 输出格式化 JSON
//	// JSONP: ?callback=myFunc 输出 myFunc({"status":"success"})
func writeJson(w http.ResponseWriter, r *http.Request, httpStatus int, obj interface{}) (err error) {
	// 【步骤 1：检查状态码是否允许响应体】
	// 某些 HTTP 状态码不允许有响应体（如 1xx, 204, 304）
	// 如果不允许，直接返回，不写入任何内容
	if !bodyAllowedForStatus(httpStatus) {
		return
	}

	// 【步骤 2：序列化对象为 JSON】
	var bytes []byte
	if obj != nil {
		// 检查是否需要格式化输出（美化 JSON）
		// URL 参数 ?pretty=1 会触发格式化输出
		if r.FormValue("pretty") != "" {
			// 使用缩进格式化，每级缩进 2 个空格
			// 输出格式：
			// {
			//   "key": "value"
			// }
			bytes, err = json.MarshalIndent(obj, "", "  ")
		} else {
			// 紧凑格式（默认）
			// 输出格式：{"key":"value"}
			bytes, err = json.Marshal(obj)
		}
	}
	if err != nil {
		return
	}

	// 【步骤 3：错误响应日志】
	// 当返回错误状态码（4xx 或 5xx）时，记录详细日志用于排查问题
	if httpStatus >= 400 {
		glog.V(0).Infof("response method:%s URL:%s with httpStatus:%d and JSON:%s",
			r.Method, r.URL.String(), httpStatus, string(bytes))
	}

	// 【步骤 4：输出 JSON 或 JSONP】
	// 检查是否需要 JSONP 格式（用于跨域请求）
	callback := r.FormValue("callback")
	if callback == "" {
		// 普通 JSON 响应
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(httpStatus)
		// 304 Not Modified 不需要响应体（浏览器使用缓存）
		if httpStatus == http.StatusNotModified {
			return
		}
		_, err = w.Write(bytes)
	} else {
		// JSONP 响应（跨域支持）
		// 格式：callbackName({"key":"value"})
		w.Header().Set("Content-Type", "application/javascript")
		w.WriteHeader(httpStatus)
		if httpStatus == http.StatusNotModified {
			return
		}
		// 写入回调函数名
		if _, err = w.Write([]uint8(callback)); err != nil {
			return
		}
		// 写入左括号
		if _, err = w.Write([]uint8("(")); err != nil {
			return
		}
		// 写入 JSON 数据
		fmt.Fprint(w, string(bytes))
		// 写入右括号，完成 JSONP 格式
		if _, err = w.Write([]uint8(")")); err != nil {
			return
		}
	}

	return
}

// writeJsonQuiet writeJson 的包装函数，自动记录错误
// 如果写入 JSON 失败，记录错误日志但不返回错误
//
// 参数:
//   - w: HTTP 响应写入器
//   - r: HTTP 请求
//   - httpStatus: HTTP 状态码
//   - obj: 要序列化的对象
//
// 使用场景:
//   - 不需要处理写入错误的场景
//   - 错误已经在其他地方处理的场景
func writeJsonQuiet(w http.ResponseWriter, r *http.Request, httpStatus int, obj interface{}) {
	if err := writeJson(w, r, httpStatus, obj); err != nil {
		glog.V(0).Infof("error writing JSON status %s %d: %v", r.URL, httpStatus, err)
		glog.V(1).Infof("JSON content: %+v", obj)
	}
}

// writeJsonError 输出错误信息的 JSON 响应
// 将 error 对象转换为 JSON 格式 {"error": "错误信息"}
//
// 参数:
//   - w: HTTP 响应写入器
//   - r: HTTP 请求
//   - httpStatus: HTTP 状态码（通常是 4xx 或 5xx）
//   - err: 错误对象
//
// 输出格式:
//
//	{
//	  "error": "具体的错误信息"
//	}
//
// 使用示例:
//
//	if err := processFile(); err != nil {
//	    writeJsonError(w, r, http.StatusBadRequest, err)
//	    return
//	}
func writeJsonError(w http.ResponseWriter, r *http.Request, httpStatus int, err error) {
	m := make(map[string]interface{})
	m["error"] = err.Error()
	glog.V(1).Infof("error JSON response status %d: %s", httpStatus, m["error"])
	writeJsonQuiet(w, r, httpStatus, m)
}

// debug 调试日志输出函数
// 使用 glog 的 V(4) 级别（最详细的日志级别）
//
// 参数:
//   - params: 任意类型的参数列表
//
// 使用方法:
//   - 启动时添加参数 -v=4 或更高级别才会输出
//   - 用于调试复杂问题时的详细日志
//
// 示例:
//
//	debug("processing file", filename, "size:", fileSize)
func debug(params ...interface{}) {
	glog.V(4).Infoln(params...)
}

// submitForClientHandler 处理客户端的文件上传请求
// 这是 /submit API 的核心处理函数
//
// 功能流程：
// 1. 解析上传的文件（ParseUpload）
// 2. 向 Master 请求分配文件 ID（Assign）
// 3. 将文件上传到 Volume Server（UploadData）
// 4. 返回文件的 fid 和 URL
//
// 参数：
//
//	w: HTTP 响应
//	r: HTTP 请求（包含上传的文件）
//	masterFn: 获取 Master Server 地址的函数
//	grpcDialOption: gRPC 连接选项
//
// 请求示例：
//
//	curl -F file=@/etc/hosts "http://127.0.0.1:9333/submit"
//
// 响应示例：
//
//	{
//	  "fileName": "hosts",
//	  "fid": "3,01e3b0756f",
//	  "fileUrl": "http://localhost:8080/3,01e3b0756f",
//	  "size": 1024
//	}
func submitForClientHandler(w http.ResponseWriter, r *http.Request, masterFn operation.GetMasterFn, grpcDialOption grpc.DialOption) {
	ctx := r.Context()
	m := make(map[string]interface{})

	// 只接受 POST 请求
	if r.Method != http.MethodPost {
		writeJsonError(w, r, http.StatusMethodNotAllowed, errors.New("Only submit via POST!"))
		return
	}

	debug("parsing upload file...")
	// 从对象池中获取缓冲区（避免频繁分配内存）
	// 避免频繁的内存分配和回收
	// 减少垃圾回收压力
	// 提高性能，特别是在高并发场景下
	// 缓冲区可能已经有一定容量，减少重新分配的次数
	bytesBuffer := bufPool.Get().(*bytes.Buffer)
	defer bufPool.Put(bytesBuffer)

	// 解析上传的文件
	// 256MB 是单个文件的最大大小限制
	// pu (ParsedUpload) 包含文件名、数据、MIME 类型等信息
	pu, pe := needle.ParseUpload(r, 256*1024*1024, bytesBuffer)
	if pe != nil {
		writeJsonError(w, r, http.StatusBadRequest, pe)
		return
	}

	debug("assigning file id for", pu.FileName)
	r.ParseForm()

	// 解析要分配的文件 ID 数量（默认为 1）
	// 如果需要批量上传，可以指定 count 参数
	count := uint64(1)
	if r.FormValue("count") != "" {
		count, pe = strconv.ParseUint(r.FormValue("count"), 10, 32)
		if pe != nil {
			writeJsonError(w, r, http.StatusBadRequest, pe)
			return
		}
	}

	// 构建卷分配请求
	// 包含数据中心、复制策略、TTL 等信息
	ar := &operation.VolumeAssignRequest{
		Count:       count,                      // 要分配的文件 ID 数量
		DataCenter:  r.FormValue("dataCenter"),  // 数据中心（可选）
		Rack:        r.FormValue("rack"),        // 机架（可选）
		Replication: r.FormValue("replication"), // 复制策略（如 "001" 表示 1 个副本）
		Collection:  r.FormValue("collection"),  // Collection 名称
		Ttl:         r.FormValue("ttl"),         // Time To Live（可选）
		DiskType:    r.FormValue("disk"),        // 磁盘类型（hdd/ssd）
	}

	// 向 Master Server 请求分配文件 ID
	// assignResult 包含：fid（文件ID）、url（Volume Server 地址）、publicUrl 等
	assignResult, ae := operation.Assign(ctx, masterFn, grpcDialOption, ar)
	if ae != nil {
		writeJsonError(w, r, http.StatusInternalServerError, ae)
		return
	}

	// 构建上传 URL
	// 格式：http://<volume_server>/<fid>
	url := "http://" + assignResult.Url + "/" + assignResult.Fid
	if pu.ModifiedTime != 0 {
		// 如果有修改时间，添加到 URL 中
		url = url + "?ts=" + strconv.FormatUint(pu.ModifiedTime, 10)
	}

	debug("upload file to store", url)

	// 构建上传选项
	uploadOption := &operation.UploadOption{
		UploadUrl:         url,               // Volume Server 的 URL
		Filename:          pu.FileName,       // 文件名
		Cipher:            false,             // 是否加密
		IsInputCompressed: pu.IsGzipped,      // 是否已压缩
		MimeType:          pu.MimeType,       // MIME 类型
		PairMap:           pu.PairMap,        // 自定义键值对
		Jwt:               assignResult.Auth, // JWT 认证令牌
	}

	// 创建上传器
	uploader, err := operation.NewUploader()
	if err != nil {
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}

	// 上传数据到 Volume Server
	// 这是实际的文件存储步骤
	uploadResult, err := uploader.UploadData(ctx, pu.Data, uploadOption)
	if err != nil {
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}

	// 构建响应 JSON
	m["fileName"] = pu.FileName
	m["fid"] = assignResult.Fid                                    // 文件 ID，用于后续访问
	m["fileUrl"] = assignResult.PublicUrl + "/" + assignResult.Fid // 公开访问 URL
	m["size"] = pu.OriginalDataSize                                // 文件大小
	m["eTag"] = uploadResult.ETag                                  // ETag（用于缓存验证）

	// 返回 201 Created 状态码和结果
	writeJsonQuiet(w, r, http.StatusCreated, m)
	return
}

// parseURLPath 解析 URL 路径，提取卷 ID (vid)、文件 ID (fid)、文件名和扩展名
// SeaweedFS 的文件 ID 格式：<volume_id>,<file_key>[.ext]
// 例如：3,01e3b0756f 或 3,01e3b0756f.jpg
//
// 支持的 URL 格式：
//
//	/<vid>/<fid>/<filename>       - 如：/3/01e3b0756f/photo.jpg
//	/<vid>/<fid>                  - 如：/3/01e3b0756f  或 /3/01e3b0756f.jpg
//	/<vid>,<fid>                  - 如：/3,01e3b0756f 或 /3,01e3b0756f.jpg
//
// 参数：
//
//	path: URL 路径（如 "/3,01e3b0756f.jpg"）
//
// 返回值：
//
//	vid: 卷 ID（如 "3"）
//	fid: 文件 ID（如 "01e3b0756f"）
//	filename: 文件名（可选）
//	ext: 扩展名（如 ".jpg"）
//	isVolumeIdOnly: 是否只有卷 ID（用于查询卷信息）
func parseURLPath(path string) (vid, fid, filename, ext string, isVolumeIdOnly bool) {
	// 【根据路径中的斜杠数量判断 URL 格式】
	// SeaweedFS 支持三种 URL 格式，通过统计斜杠数量来区分
	switch strings.Count(path, "/") {
	case 3:
		// 【格式 1：/<vid>/<fid>/<filename>】
		// 三段式路径，包含明确的文件名
		// 例如：/3/01e3b0756f/photo.jpg
		//   - vid: "3"
		//   - fid: "01e3b0756f"
		//   - filename: "photo.jpg"
		//   - ext: ".jpg"
		parts := strings.Split(path, "/")
		vid, fid, filename = parts[1], parts[2], parts[3]
		// 提取扩展名（如 ".jpg"）
		ext = filepath.Ext(filename)

	case 2:
		// 【格式 2：/<vid>/<fid>[.ext]】
		// 两段式路径，文件名可选
		// 例如：/3/01e3b0756f 或 /3/01e3b0756f.jpg
		//   - vid: "3"
		//   - fid: "01e3b0756f"
		//   - ext: ".jpg"（如果有）
		parts := strings.Split(path, "/")
		vid, fid = parts[1], parts[2]

		// 检查 fid 是否包含扩展名
		dotIndex := strings.LastIndex(fid, ".")
		if dotIndex > 0 {
			// 分离扩展名和文件 ID
			ext = fid[dotIndex:]          // 扩展名：".jpg"
			fid = fid[0:dotIndex]         // 纯文件 ID："01e3b0756f"
		}

	default:
		// 【格式 3：/<vid>,<fid>[.ext]（最常用的格式）】
		// 使用逗号分隔 vid 和 fid
		// 例如：/3,01e3b0756f 或 /3,01e3b0756f.jpg
		//   - vid: "3"
		//   - fid: "01e3b0756f"
		//   - ext: ".jpg"（如果有）

		// 找到最后一个斜杠的位置
		sepIndex := strings.LastIndex(path, "/")
		// 在斜杠之后查找逗号位置
		commaIndex := strings.LastIndex(path[sepIndex:], ",")

		if commaIndex <= 0 {
			// 【特殊情况：只有卷 ID，没有逗号】
			// 用于查询卷信息的 API（如 /dir/lookup?volumeId=3）
			// 例如：/3
			//   - vid: "3"
			//   - isVolumeIdOnly: true
			vid, isVolumeIdOnly = path[sepIndex+1:], true
			return
		}

		// 在斜杠之后查找点号位置（扩展名）
		dotIndex := strings.LastIndex(path[sepIndex:], ".")

		// 提取卷 ID（逗号之前的部分）
		vid = path[sepIndex+1 : commaIndex]

		// 提取文件 ID（逗号之后的部分）
		fid = path[commaIndex+1:]
		ext = ""

		if dotIndex > 0 {
			// 如果有扩展名，分离出来
			// 例如：/3,01e3b0756f.jpg
			//   - fid: "01e3b0756f"
			//   - ext: ".jpg"
			fid = path[commaIndex+1 : dotIndex]
			ext = path[dotIndex:]
		}
	}
	return
}

// statsHealthHandler 健康检查接口处理函数
// 用于负载均衡器或监控系统检查服务是否存活
//
// 路径: /stats/health 或类似
//
// 响应格式:
//
//	{
//	  "Version": "3.54"
//	}
//
// 使用场景:
//   - Kubernetes liveness probe
//   - 负载均衡器健康检查
//   - 监控系统状态检测
func statsHealthHandler(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	m["Version"] = version.Version()
	writeJsonQuiet(w, r, http.StatusOK, m)
}

// statsCounterHandler 统计计数器接口处理函数
// 返回服务器的运行统计信息（如请求数、成功数、失败数等）
//
// 路径: /stats/counter 或类似
//
// 响应格式:
//
//	{
//	  "Version": "3.54",
//	  "Counters": {
//	    "requests": 12345,
//	    "successes": 12300,
//	    "failures": 45,
//	    ...
//	  }
//	}
//
// 使用场景:
//   - 监控系统采集指标
//   - 性能分析
//   - 运维监控
func statsCounterHandler(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	m["Version"] = version.Version()
	m["Counters"] = serverStats
	writeJsonQuiet(w, r, http.StatusOK, m)
}

// statsMemoryHandler 内存统计接口处理函数
// 返回服务器的内存使用情况
//
// 路径: /stats/memory 或类似
//
// 响应格式:
//
//	{
//	  "Version": "3.54",
//	  "Memory": {
//	    "Alloc": 12345678,      // 当前分配的内存
//	    "TotalAlloc": 98765432,  // 累计分配的内存
//	    "Sys": 23456789,         // 从系统获取的内存
//	    "NumGC": 42,             // GC 次数
//	    ...
//	  }
//	}
//
// 使用场景:
//   - 内存泄漏排查
//   - 性能调优
//   - 容量规划
func statsMemoryHandler(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	m["Version"] = version.Version()
	m["Memory"] = stats.MemStat()
	writeJsonQuiet(w, r, http.StatusOK, m)
}

// StaticFS 静态资源文件系统
// 嵌入的静态资源（如 favicon.ico、CSS、JS 等）
var StaticFS fs.FS

// handleStaticResources 为 http.ServeMux 注册静态资源处理器
// 用于 Master Server 和 Volume Server 的 Web UI
//
// 参数:
//   - defaultMux: http.ServeMux 路由器
//
// 注册的路由:
//   - /favicon.ico: 网站图标
//   - /seaweedfsstatic/*: 其他静态资源（CSS、JS、图片等）
//
// 使用场景:
//   - Master Server 的 Web UI
//   - Volume Server 的管理界面
func handleStaticResources(defaultMux *http.ServeMux) {
	defaultMux.Handle("/favicon.ico", http.FileServer(http.FS(StaticFS)))
	defaultMux.Handle("/seaweedfsstatic/", http.StripPrefix("/seaweedfsstatic", http.FileServer(http.FS(StaticFS))))
}

// handleStaticResources2 为 mux.Router 注册静态资源处理器
// 与 handleStaticResources 功能相同，但使用 gorilla/mux 路由器
//
// 参数:
//   - r: mux.Router 路由器
//
// 注册的路由:
//   - /favicon.ico: 网站图标
//   - /seaweedfsstatic/*: 其他静态资源（CSS、JS、图片等）
//
// 使用场景:
//   - Filer Server 的 Web UI
//   - S3 Gateway 的管理界面
func handleStaticResources2(r *mux.Router) {
	r.Handle("/favicon.ico", http.FileServer(http.FS(StaticFS)))
	r.PathPrefix("/seaweedfsstatic/").Handler(http.StripPrefix("/seaweedfsstatic", http.FileServer(http.FS(StaticFS))))
}

// AdjustPassthroughHeaders 应用 S3 兼容的透传响应头
// 支持通过 URL 参数覆盖响应头，兼容 AWS S3 API
//
// 参数:
//   - w: HTTP 响应写入器
//   - r: HTTP 请求（包含 URL 参数）
//   - filename: 文件名（用于设置 Content-Disposition）
//
// 支持的 URL 参数（S3 兼容）:
//   - response-cache-control: 覆盖 Cache-Control 头
//   - response-content-disposition: 覆盖 Content-Disposition 头
//   - response-content-encoding: 覆盖 Content-Encoding 头
//   - response-content-language: 覆盖 Content-Language 头
//   - response-content-type: 覆盖 Content-Type 头
//   - response-expires: 覆盖 Expires 头
//
// 使用示例:
//   GET /3,01e3b0756f?response-content-type=application/json
//   返回的响应头将包含 Content-Type: application/json
//
// 应用场景:
//   - S3 API 兼容性
//   - 动态控制缓存策略
//   - 强制下载或在线预览
func AdjustPassthroughHeaders(w http.ResponseWriter, r *http.Request, filename string) {
	// Apply S3 passthrough headers from query parameters
	// AWS S3 supports overriding response headers via query parameters like:
	// ?response-cache-control=no-cache&response-content-type=application/json
	for queryParam, headerValue := range r.URL.Query() {
		if normalizedHeader, ok := s3_constants.PassThroughHeaders[strings.ToLower(queryParam)]; ok && len(headerValue) > 0 {
			w.Header().Set(normalizedHeader, headerValue[0])
		}
	}
	adjustHeaderContentDisposition(w, r, filename)
}

// adjustHeaderContentDisposition 设置 Content-Disposition 响应头
// 控制浏览器是在线显示还是下载文件
//
// 参数:
//   - w: HTTP 响应写入器
//   - r: HTTP 请求（读取 dl 参数）
//   - filename: 文件名
//
// Content-Disposition 类型:
//   - inline: 浏览器尝试在线显示（默认）
//   - attachment: 浏览器下载文件（当 dl=true 时）
//
// URL 参数:
//   - dl=true: 强制下载（设置为 attachment）
//   - dl=false 或不设置: 在线显示（设置为 inline）
//
// 响应头示例:
//   Content-Disposition: inline; filename="photo.jpg"
//   Content-Disposition: attachment; filename="document.pdf"
//
// 注意:
//   - 如果 Content-Disposition 已存在，不会覆盖
//   - 文件名会进行 URL 编码，处理特殊字符
func adjustHeaderContentDisposition(w http.ResponseWriter, r *http.Request, filename string) {
	// 【步骤 1：检查是否已设置 Content-Disposition】
	// 如果响应头已经包含 Content-Disposition，不再覆盖
	// 避免与其他中间件或处理函数冲突
	if contentDisposition := w.Header().Get("Content-Disposition"); contentDisposition != "" {
		return
	}

	// 【步骤 2：设置 Content-Disposition 头】
	if filename != "" {
		// 对文件名进行 URL 编码，处理特殊字符
		// 例如："我的文件.jpg" -> "%E6%88%91%E7%9A%84%E6%96%87%E4%BB%B6.jpg"
		filename = url.QueryEscape(filename)

		// 默认为 "inline"（在线显示）
		// 浏览器会尝试在浏览器窗口中直接打开文件
		// 适用于图片、PDF、视频等可预览的文件类型
		contentDisposition := "inline"

		// 【检查是否强制下载】
		// URL 参数 ?dl=true 会强制浏览器下载文件
		// 而不是在线预览
		if r.FormValue("dl") != "" {
			if dl, _ := strconv.ParseBool(r.FormValue("dl")); dl {
				// 设置为 "attachment"（附件下载）
				// 浏览器会弹出下载对话框
				contentDisposition = "attachment"
			}
		}

		// 【设置响应头】
		// 格式：Content-Disposition: inline; filename="photo.jpg"
		//      或 Content-Disposition: attachment; filename="document.pdf"
		// fileNameEscaper.Replace 进一步处理文件名中的特殊字符
		w.Header().Set("Content-Disposition", contentDisposition+`; filename="`+fileNameEscaper.Replace(filename)+`"`)
	}
}

// ProcessRangeRequest 处理 HTTP Range 请求
// 支持部分内容读取，用于视频拖拽、断点续传等场景
//
// 参数:
//   - r: HTTP 请求（包含 Range 头）
//   - w: HTTP 响应写入器
//   - totalSize: 文件的完整大小（字节）
//   - mimeType: MIME 类型（如 "video/mp4"）
//   - prepareWriteFn: 准备数据写入的回调函数
//   - offset: 要读取的起始位置
//   - size: 要读取的长度
//   - 返回: 实际写入数据的函数
//
// 返回值:
//   - error: 处理错误
//
// 工作流程:
//  1. 解析 Range 头（格式：bytes=start-end）
//  2. 验证 Range 的有效性
//  3. 单范围请求：返回 206 Partial Content + Content-Range
//  4. 多范围请求：返回 206 + multipart/byteranges 响应
//  5. 无 Range 头：返回 200 OK + 完整内容
//
// Range 请求示例:
//   - Range: bytes=0-1023       (前 1KB)
//   - Range: bytes=1024-2047    (第二个 1KB)
//   - Range: bytes=1024-        (从 1KB 到末尾)
//   - Range: bytes=-1024        (最后 1KB)
//   - Range: bytes=0-1023,2048-3071  (多个范围)
//
// 响应示例:
//   单范围:
//     HTTP/1.1 206 Partial Content
//     Content-Range: bytes 0-1023/10240
//     Content-Length: 1024
//
//   多范围:
//     HTTP/1.1 206 Partial Content
//     Content-Type: multipart/byteranges; boundary=...
//
// 应用场景:
//   - 视频播放器拖拽
//   - 断点续传
//   - 并行下载（多线程下载不同部分）
//
// 错误处理:
//   - 416 Range Not Satisfiable: 范围无效或超出文件大小
//   - 500 Internal Server Error: 数据读取失败
func ProcessRangeRequest(r *http.Request, w http.ResponseWriter, totalSize int64, mimeType string, prepareWriteFn func(offset int64, size int64) (filer.DoStreamContent, error)) error {
	// 【步骤 1：获取 Range 请求头】
	// Range 头格式：Range: bytes=start-end
	// 例如：Range: bytes=0-1023 表示请求前 1KB
	rangeReq := r.Header.Get("Range")

	// 【步骤 2：从对象池获取缓冲写入器】
	// 使用缓冲写入器提高性能，避免频繁的系统调用
	bufferedWriter := writePool.Get().(*bufio.Writer)
	bufferedWriter.Reset(w)
	defer func() {
		// 确保数据被刷新到响应中
		bufferedWriter.Flush()
		// 归还缓冲写入器到对象池，供下次复用
		writePool.Put(bufferedWriter)
	}()

	// 【步骤 3：处理完整内容请求（无 Range 头）】
	if rangeReq == "" {
		// 没有 Range 头，返回完整文件内容
		// 设置 Content-Length 为文件总大小
		w.Header().Set("Content-Length", strconv.FormatInt(totalSize, 10))

		// 准备写入完整文件（从偏移 0 开始，长度为 totalSize）
		writeFn, err := prepareWriteFn(0, totalSize)
		if err != nil {
			glog.Errorf("ProcessRangeRequest: %v", err)
			// 发生错误时删除 Content-Length 头，避免误导客户端
			w.Header().Del("Content-Length")
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return fmt.Errorf("ProcessRangeRequest: %w", err)
		}

		// 执行实际的数据写入
		if err = writeFn(bufferedWriter); err != nil {
			glog.Errorf("ProcessRangeRequest: %v", err)
			w.Header().Del("Content-Length")
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return fmt.Errorf("ProcessRangeRequest: %w", err)
		}
		return nil
	}

	// 【步骤 4：处理部分内容请求（有 Range 头）】
	// 以下代码主要参考自 Go 标准库 src/pkg/net/http/fs.go
	// 解析 Range 头，提取请求的字节范围
	// 例如："bytes=0-1023,2048-3071" -> [{0, 1024}, {2048, 1024}]
	ranges, err := parseRange(rangeReq, totalSize)
	if err != nil {
		glog.Errorf("ProcessRangeRequest headers: %+v err: %v", w.Header(), err)
		// 416 Range Not Satisfiable：范围无效或超出文件大小
		http.Error(w, err.Error(), http.StatusRequestedRangeNotSatisfiable)
		return fmt.Errorf("ProcessRangeRequest header: %w", err)
	}

	// 【步骤 5：安全性检查】
	// 验证所有范围的总大小不超过文件大小
	if sumRangesSize(ranges) > totalSize {
		// 所有范围的总字节数大于文件本身的大小
		// 这可能是攻击行为（试图耗尽服务器资源）
		// 或者是错误的客户端实现
		// 忽略此 Range 请求，不处理
		return nil
	}

	// 空范围列表，直接返回
	if len(ranges) == 0 {
		return nil
	}
	// 【步骤 6：处理单范围请求】
	if len(ranges) == 1 {
		// 【RFC 2616, Section 14.16 规范要求】
		// 当 HTTP 消息包含单个范围的内容时（例如对单个范围的请求响应，
		// 或对一组重叠范围的请求响应），该内容必须：
		//   1. 使用 Content-Range 头传输
		//   2. 使用 Content-Length 头显示实际传输的字节数
		//   3. 不得使用 multipart/byteranges 媒体类型
		//
		// 单范围响应示例：
		//   HTTP/1.1 206 Partial Content
		//   Content-Range: bytes 0-1023/10240
		//   Content-Length: 1024
		//   [1024 字节的数据]

		ra := ranges[0]

		// 设置响应头
		// Content-Length：实际返回的字节数（范围长度）
		w.Header().Set("Content-Length", strconv.FormatInt(ra.length, 10))
		// Content-Range：范围信息，格式 "bytes start-end/total"
		// 例如："bytes 0-1023/10240" 表示返回 0-1023 字节，总共 10240 字节
		w.Header().Set("Content-Range", ra.contentRange(totalSize))

		// 准备写入指定范围的数据
		writeFn, err := prepareWriteFn(ra.start, ra.length)
		if err != nil {
			glog.Errorf("ProcessRangeRequest range[0]: %+v err: %v", w.Header(), err)
			w.Header().Del("Content-Length")
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return fmt.Errorf("ProcessRangeRequest: %w", err)
		}

		// 发送 206 Partial Content 状态码
		w.WriteHeader(http.StatusPartialContent)

		// 执行数据写入
		err = writeFn(bufferedWriter)
		if err != nil {
			glog.Errorf("ProcessRangeRequest range[0]: %+v err: %v", w.Header(), err)
			// 注意：此时无法调用 http.Error()，因为 WriteHeader 已被调用
			// HTTP 响应头已发送，只能返回错误
			return fmt.Errorf("ProcessRangeRequest range[0]: %w", err)
		}
		return nil
	}

	// 【步骤 7：处理多范围请求】
	// 当请求包含多个范围时（如 Range: bytes=0-1023,2048-3071）
	// 必须使用 multipart/byteranges 格式返回
	//
	// 多范围响应示例：
	//   HTTP/1.1 206 Partial Content
	//   Content-Type: multipart/byteranges; boundary=BOUNDARY_STRING
	//
	//   --BOUNDARY_STRING
	//   Content-Type: video/mp4
	//   Content-Range: bytes 0-1023/10240
	//
	//   [第一个范围的 1024 字节数据]
	//   --BOUNDARY_STRING
	//   Content-Type: video/mp4
	//   Content-Range: bytes 2048-3071/10240
	//
	//   [第二个范围的 1024 字节数据]
	//   --BOUNDARY_STRING--

	// 创建写入函数映射表
	// 键：范围索引，值：对应的数据写入函数
	writeFnByRange := make(map[int](func(writer io.Writer) error))

	// 【步骤 7.1：验证范围并准备写入函数】
	for i, ra := range ranges {
		// 验证范围起始位置不超过文件大小
		if ra.start > totalSize {
			http.Error(w, "Out of Range", http.StatusRequestedRangeNotSatisfiable)
			return fmt.Errorf("out of range: %w", err)
		}

		// 为每个范围准备写入函数
		writeFn, err := prepareWriteFn(ra.start, ra.length)
		if err != nil {
			glog.Errorf("ProcessRangeRequest range[%d] err: %v", i, err)
			http.Error(w, "Internal Error", http.StatusInternalServerError)
			return fmt.Errorf("ProcessRangeRequest range[%d] err: %v", i, err)
		}
		// 保存到映射表中
		writeFnByRange[i] = writeFn
	}

	// 【步骤 7.2：计算 multipart 响应的总大小】
	// 包括所有范围数据 + MIME 边界 + 头部信息
	sendSize := rangesMIMESize(ranges, mimeType, totalSize)

	// 【步骤 7.3：创建管道用于流式传输】
	// pr (PipeReader): 读取端，用于向 HTTP 响应写入
	// pw (PipeWriter): 写入端，用于 multipart writer 写入
	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)

	// 设置 Content-Type 为 multipart/byteranges
	// boundary 参数用于分隔不同的范围
	w.Header().Set("Content-Type", "multipart/byteranges; boundary="+mw.Boundary())
	sendContent := pr
	defer pr.Close() // 确保管道关闭，如果 CopyN 未完成，导致写入协程退出

	// 【步骤 7.4：启动后台协程写入多部分数据】
	go func() {
		// 遍历所有范围，依次写入
		for i, ra := range ranges {
			// 创建新的 multipart 部分
			// 包含该范围的 Content-Type 和 Content-Range 头
			part, e := mw.CreatePart(ra.mimeHeader(mimeType, totalSize))
			if e != nil {
				pw.CloseWithError(e)
				return
			}

			// 获取该范围的写入函数
			writeFn := writeFnByRange[i]
			if writeFn == nil {
				pw.CloseWithError(e)
				return
			}

			// 执行数据写入到当前 part
			if e = writeFn(part); e != nil {
				pw.CloseWithError(e)
				return
			}
		}

		// 关闭 multipart writer（写入最终的边界标记）
		mw.Close()
		// 关闭管道写入端，通知读取端数据已完成
		pw.Close()
	}()

	// 【步骤 7.5：设置响应头并发送数据】
	// 如果没有内容编码，设置 Content-Length
	if w.Header().Get("Content-Encoding") == "" {
		w.Header().Set("Content-Length", strconv.FormatInt(sendSize, 10))
	}

	// 发送 206 Partial Content 状态码
	w.WriteHeader(http.StatusPartialContent)

	// 从管道读取数据并写入 HTTP 响应
	// CopyN 确保只复制 sendSize 字节
	if _, err := io.CopyN(bufferedWriter, sendContent, sendSize); err != nil {
		glog.Errorf("ProcessRangeRequest err: %v", err)
		// 注意：此时无法调用 http.Error()，因为 WriteHeader 已被调用
		return fmt.Errorf("ProcessRangeRequest err: %w", err)
	}
	return nil
}

// requestIDMiddleware HTTP 请求 ID 中间件
// 为每个请求分配唯一 ID，用于请求追踪和日志关联
//
// 参数:
//   - h: 要包装的处理函数
//
// 返回值:
//   - http.HandlerFunc: 包装后的处理函数
//
// 工作流程:
//  1. 检查请求头是否已有 Request ID
//  2. 如果没有，生成新的 UUID 作为 Request ID
//  3. 将 Request ID 存入请求上下文（Context）
//  4. 将 Request ID 存入 gRPC 元数据（用于 gRPC 调用传递）
//  5. 将 Request ID 写入响应头
//  6. 调用实际的处理函数
//
// Request ID 的用途:
//   - 分布式追踪：关联多个服务之间的调用
//   - 日志聚合：根据 Request ID 查询完整的请求日志
//   - 问题排查：追踪请求在系统中的完整路径
//
// HTTP 头:
//   - 请求头: X-Amz-Request-Id（如果客户端提供）
//   - 响应头: X-Amz-Request-Id（返回给客户端）
//
// 使用示例:
//
//	http.HandleFunc("/upload", requestIDMiddleware(uploadHandler))
//
// S3 兼容性:
//   - 使用 X-Amz-Request-Id 头，兼容 AWS S3 API
func requestIDMiddleware(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		reqID := r.Header.Get(request_id.AmzRequestIDHeader)
		if reqID == "" {
			reqID = uuid.New().String()
		}

		ctx := context.WithValue(r.Context(), request_id.AmzRequestIDHeader, reqID)
		ctx = metadata.NewOutgoingContext(ctx,
			metadata.New(map[string]string{
				request_id.AmzRequestIDHeader: reqID,
			}))

		w.Header().Set(request_id.AmzRequestIDHeader, reqID)
		h(w, r.WithContext(ctx))
	}
}
