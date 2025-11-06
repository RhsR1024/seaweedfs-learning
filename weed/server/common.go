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

var serverStats *stats.ServerStats
var startTime = time.Now()
var writePool = sync.Pool{New: func() interface{} {
	return bufio.NewWriterSize(nil, 128*1024)
},
}

func init() {
	serverStats = stats.NewServerStats()
	go serverStats.Start()
}

// bodyAllowedForStatus is a copy of http.bodyAllowedForStatus non-exported function.
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

func writeJson(w http.ResponseWriter, r *http.Request, httpStatus int, obj interface{}) (err error) {
	if !bodyAllowedForStatus(httpStatus) {
		return
	}

	var bytes []byte
	if obj != nil {
		if r.FormValue("pretty") != "" {
			bytes, err = json.MarshalIndent(obj, "", "  ")
		} else {
			bytes, err = json.Marshal(obj)
		}
	}
	if err != nil {
		return
	}

	if httpStatus >= 400 {
		glog.V(0).Infof("response method:%s URL:%s with httpStatus:%d and JSON:%s",
			r.Method, r.URL.String(), httpStatus, string(bytes))
	}

	callback := r.FormValue("callback")
	if callback == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(httpStatus)
		if httpStatus == http.StatusNotModified {
			return
		}
		_, err = w.Write(bytes)
	} else {
		w.Header().Set("Content-Type", "application/javascript")
		w.WriteHeader(httpStatus)
		if httpStatus == http.StatusNotModified {
			return
		}
		if _, err = w.Write([]uint8(callback)); err != nil {
			return
		}
		if _, err = w.Write([]uint8("(")); err != nil {
			return
		}
		fmt.Fprint(w, string(bytes))
		if _, err = w.Write([]uint8(")")); err != nil {
			return
		}
	}

	return
}

// wrapper for writeJson - just logs errors
func writeJsonQuiet(w http.ResponseWriter, r *http.Request, httpStatus int, obj interface{}) {
	if err := writeJson(w, r, httpStatus, obj); err != nil {
		glog.V(0).Infof("error writing JSON status %s %d: %v", r.URL, httpStatus, err)
		glog.V(1).Infof("JSON content: %+v", obj)
	}
}
func writeJsonError(w http.ResponseWriter, r *http.Request, httpStatus int, err error) {
	m := make(map[string]interface{})
	m["error"] = err.Error()
	glog.V(1).Infof("error JSON response status %d: %s", httpStatus, m["error"])
	writeJsonQuiet(w, r, httpStatus, m)
}

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
//   w: HTTP 响应
//   r: HTTP 请求（包含上传的文件）
//   masterFn: 获取 Master Server 地址的函数
//   grpcDialOption: gRPC 连接选项
//
// 请求示例：
//   curl -F file=@/etc/hosts "http://127.0.0.1:9333/submit"
//
// 响应示例：
//   {
//     "fileName": "hosts",
//     "fid": "3,01e3b0756f",
//     "fileUrl": "http://localhost:8080/3,01e3b0756f",
//     "size": 1024
//   }
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
		UploadUrl:         url,             // Volume Server 的 URL
		Filename:          pu.FileName,     // 文件名
		Cipher:            false,           // 是否加密
		IsInputCompressed: pu.IsGzipped,    // 是否已压缩
		MimeType:          pu.MimeType,     // MIME 类型
		PairMap:           pu.PairMap,      // 自定义键值对
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
	m["fid"] = assignResult.Fid // 文件 ID，用于后续访问
	m["fileUrl"] = assignResult.PublicUrl + "/" + assignResult.Fid // 公开访问 URL
	m["size"] = pu.OriginalDataSize // 文件大小
	m["eTag"] = uploadResult.ETag   // ETag（用于缓存验证）

	// 返回 201 Created 状态码和结果
	writeJsonQuiet(w, r, http.StatusCreated, m)
	return
}

// parseURLPath 解析 URL 路径，提取卷 ID (vid)、文件 ID (fid)、文件名和扩展名
// SeaweedFS 的文件 ID 格式：<volume_id>,<file_key>[.ext]
// 例如：3,01e3b0756f 或 3,01e3b0756f.jpg
//
// 支持的 URL 格式：
//   /<vid>/<fid>/<filename>       - 如：/3/01e3b0756f/photo.jpg
//   /<vid>/<fid>                  - 如：/3/01e3b0756f  或 /3/01e3b0756f.jpg
//   /<vid>,<fid>                  - 如：/3,01e3b0756f 或 /3,01e3b0756f.jpg
//
// 参数：
//   path: URL 路径（如 "/3,01e3b0756f.jpg"）
//
// 返回值：
//   vid: 卷 ID（如 "3"）
//   fid: 文件 ID（如 "01e3b0756f"）
//   filename: 文件名（可选）
//   ext: 扩展名（如 ".jpg"）
//   isVolumeIdOnly: 是否只有卷 ID（用于查询卷信息）
func parseURLPath(path string) (vid, fid, filename, ext string, isVolumeIdOnly bool) {
	switch strings.Count(path, "/") {
	case 3:
		// 格式：/<vid>/<fid>/<filename>
		// 例如：/3/01e3b0756f/photo.jpg
		parts := strings.Split(path, "/")
		vid, fid, filename = parts[1], parts[2], parts[3]
		ext = filepath.Ext(filename)
	case 2:
		// 格式：/<vid>/<fid>[.ext]
		// 例如：/3/01e3b0756f 或 /3/01e3b0756f.jpg
		parts := strings.Split(path, "/")
		vid, fid = parts[1], parts[2]
		dotIndex := strings.LastIndex(fid, ".")
		if dotIndex > 0 {
			ext = fid[dotIndex:]
			fid = fid[0:dotIndex]
		}
	default:
		// 格式：/<vid>,<fid>[.ext]（最常用）
		// 例如：/3,01e3b0756f 或 /3,01e3b0756f.jpg
		sepIndex := strings.LastIndex(path, "/")
		commaIndex := strings.LastIndex(path[sepIndex:], ",")
		if commaIndex <= 0 {
			// 只有卷 ID，没有逗号（查询卷信息时使用）
			vid, isVolumeIdOnly = path[sepIndex+1:], true
			return
		}
		dotIndex := strings.LastIndex(path[sepIndex:], ".")
		vid = path[sepIndex+1 : commaIndex]
		fid = path[commaIndex+1:]
		ext = ""
		if dotIndex > 0 {
			fid = path[commaIndex+1 : dotIndex]
			ext = path[dotIndex:]
		}
	}
	return
}

func statsHealthHandler(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	m["Version"] = version.Version()
	writeJsonQuiet(w, r, http.StatusOK, m)
}
func statsCounterHandler(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	m["Version"] = version.Version()
	m["Counters"] = serverStats
	writeJsonQuiet(w, r, http.StatusOK, m)
}

func statsMemoryHandler(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	m["Version"] = version.Version()
	m["Memory"] = stats.MemStat()
	writeJsonQuiet(w, r, http.StatusOK, m)
}

var StaticFS fs.FS

func handleStaticResources(defaultMux *http.ServeMux) {
	defaultMux.Handle("/favicon.ico", http.FileServer(http.FS(StaticFS)))
	defaultMux.Handle("/seaweedfsstatic/", http.StripPrefix("/seaweedfsstatic", http.FileServer(http.FS(StaticFS))))
}

func handleStaticResources2(r *mux.Router) {
	r.Handle("/favicon.ico", http.FileServer(http.FS(StaticFS)))
	r.PathPrefix("/seaweedfsstatic/").Handler(http.StripPrefix("/seaweedfsstatic", http.FileServer(http.FS(StaticFS))))
}

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
func adjustHeaderContentDisposition(w http.ResponseWriter, r *http.Request, filename string) {
	if contentDisposition := w.Header().Get("Content-Disposition"); contentDisposition != "" {
		return
	}
	if filename != "" {
		filename = url.QueryEscape(filename)
		contentDisposition := "inline"
		if r.FormValue("dl") != "" {
			if dl, _ := strconv.ParseBool(r.FormValue("dl")); dl {
				contentDisposition = "attachment"
			}
		}
		w.Header().Set("Content-Disposition", contentDisposition+`; filename="`+fileNameEscaper.Replace(filename)+`"`)
	}
}

func ProcessRangeRequest(r *http.Request, w http.ResponseWriter, totalSize int64, mimeType string, prepareWriteFn func(offset int64, size int64) (filer.DoStreamContent, error)) error {
	rangeReq := r.Header.Get("Range")
	bufferedWriter := writePool.Get().(*bufio.Writer)
	bufferedWriter.Reset(w)
	defer func() {
		bufferedWriter.Flush()
		writePool.Put(bufferedWriter)
	}()

	if rangeReq == "" {
		w.Header().Set("Content-Length", strconv.FormatInt(totalSize, 10))
		writeFn, err := prepareWriteFn(0, totalSize)
		if err != nil {
			glog.Errorf("ProcessRangeRequest: %v", err)
			w.Header().Del("Content-Length")
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return fmt.Errorf("ProcessRangeRequest: %w", err)
		}
		if err = writeFn(bufferedWriter); err != nil {
			glog.Errorf("ProcessRangeRequest: %v", err)
			w.Header().Del("Content-Length")
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return fmt.Errorf("ProcessRangeRequest: %w", err)
		}
		return nil
	}

	//the rest is dealing with partial content request
	//mostly copy from src/pkg/net/http/fs.go
	ranges, err := parseRange(rangeReq, totalSize)
	if err != nil {
		glog.Errorf("ProcessRangeRequest headers: %+v err: %v", w.Header(), err)
		http.Error(w, err.Error(), http.StatusRequestedRangeNotSatisfiable)
		return fmt.Errorf("ProcessRangeRequest header: %w", err)
	}
	if sumRangesSize(ranges) > totalSize {
		// The total number of bytes in all the ranges
		// is larger than the size of the file by
		// itself, so this is probably an attack, or a
		// dumb client.  Ignore the range request.
		return nil
	}
	if len(ranges) == 0 {
		return nil
	}
	if len(ranges) == 1 {
		// RFC 2616, Section 14.16:
		// "When an HTTP message includes the content of a single
		// range (for example, a response to a request for a
		// single range, or to a request for a set of ranges
		// that overlap without any holes), this content is
		// transmitted with a Content-Range header, and a
		// Content-Length header showing the number of bytes
		// actually transferred.
		// ...
		// A response to a request for a single range MUST NOT
		// be sent using the multipart/byteranges media type."
		ra := ranges[0]
		w.Header().Set("Content-Length", strconv.FormatInt(ra.length, 10))
		w.Header().Set("Content-Range", ra.contentRange(totalSize))

		writeFn, err := prepareWriteFn(ra.start, ra.length)
		if err != nil {
			glog.Errorf("ProcessRangeRequest range[0]: %+v err: %v", w.Header(), err)
			w.Header().Del("Content-Length")
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return fmt.Errorf("ProcessRangeRequest: %w", err)
		}
		w.WriteHeader(http.StatusPartialContent)
		err = writeFn(bufferedWriter)
		if err != nil {
			glog.Errorf("ProcessRangeRequest range[0]: %+v err: %v", w.Header(), err)
			// Cannot call http.Error() here because WriteHeader was already called
			return fmt.Errorf("ProcessRangeRequest range[0]: %w", err)
		}
		return nil
	}

	// process multiple ranges
	writeFnByRange := make(map[int](func(writer io.Writer) error))

	for i, ra := range ranges {
		if ra.start > totalSize {
			http.Error(w, "Out of Range", http.StatusRequestedRangeNotSatisfiable)
			return fmt.Errorf("out of range: %w", err)
		}
		writeFn, err := prepareWriteFn(ra.start, ra.length)
		if err != nil {
			glog.Errorf("ProcessRangeRequest range[%d] err: %v", i, err)
			http.Error(w, "Internal Error", http.StatusInternalServerError)
			return fmt.Errorf("ProcessRangeRequest range[%d] err: %v", i, err)
		}
		writeFnByRange[i] = writeFn
	}
	sendSize := rangesMIMESize(ranges, mimeType, totalSize)
	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)
	w.Header().Set("Content-Type", "multipart/byteranges; boundary="+mw.Boundary())
	sendContent := pr
	defer pr.Close() // cause writing goroutine to fail and exit if CopyN doesn't finish.
	go func() {
		for i, ra := range ranges {
			part, e := mw.CreatePart(ra.mimeHeader(mimeType, totalSize))
			if e != nil {
				pw.CloseWithError(e)
				return
			}
			writeFn := writeFnByRange[i]
			if writeFn == nil {
				pw.CloseWithError(e)
				return
			}
			if e = writeFn(part); e != nil {
				pw.CloseWithError(e)
				return
			}
		}
		mw.Close()
		pw.Close()
	}()
	if w.Header().Get("Content-Encoding") == "" {
		w.Header().Set("Content-Length", strconv.FormatInt(sendSize, 10))
	}
	w.WriteHeader(http.StatusPartialContent)
	if _, err := io.CopyN(bufferedWriter, sendContent, sendSize); err != nil {
		glog.Errorf("ProcessRangeRequest err: %v", err)
		// Cannot call http.Error() here because WriteHeader was already called
		return fmt.Errorf("ProcessRangeRequest err: %w", err)
	}
	return nil
}

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
