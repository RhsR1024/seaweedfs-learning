// Package needle 实现了 SeaweedFS 的核心数据结构 Needle
// Needle 是 SeaweedFS 中存储文件数据的最小单元,包含文件内容、元数据等信息
// 本文件负责解析 HTTP 文件上传请求,提取文件数据和元数据
package needle

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"
	"mime"
	"net/http"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// ParsedUpload 解析后的上传文件信息
// 包含了文件的所有元数据和内容,用于后续存储到 Needle 中
type ParsedUpload struct {
	FileName    string            // 文件名
	Data        []byte            // 文件数据(可能已压缩)
	bytesBuffer *bytes.Buffer     // 字节缓冲区,用于复用内存
	MimeType    string            // MIME 类型(如 image/jpeg, text/plain)
	PairMap     map[string]string // 自定义键值对映射(通过 HTTP Header 传递)
	IsGzipped   bool              // 数据是否已 Gzip 压缩
	// IsZstd           bool       // 数据是否已 Zstd 压缩(已注释,暂未实现)
	OriginalDataSize int    // 原始未压缩数据大小
	ModifiedTime     uint64 // 文件修改时间(Unix 时间戳)
	Ttl              *TTL   // 文件生存时间(Time To Live)
	IsChunkedFile    bool   // 是否为分块文件(大文件分块上传)
	UncompressedData []byte // 未压缩的原始数据
	ContentMd5       string // 内容 MD5 校验值(Base64 编码)
}

// ParseUpload 解析 HTTP 上传请求,提取文件数据和元数据
// 这是文件上传的核心解析函数,支持多种上传方式:
//   1. multipart/form-data (浏览器表单上传)
//   2. 直接 PUT/POST 二进制数据
//   3. 支持 Gzip 压缩传输和自动压缩
// 参数:
//   r: HTTP 请求对象
//   sizeLimit: 文件大小限制(字节)
//   bytesBuffer: 复用的字节缓冲区,减少内存分配
// 返回:
//   pu: 解析后的上传信息
//   e: 错误信息
func ParseUpload(r *http.Request, sizeLimit int64, bytesBuffer *bytes.Buffer) (pu *ParsedUpload, e error) {
	bytesBuffer.Reset() // 重置缓冲区
	pu = &ParsedUpload{bytesBuffer: bytesBuffer}
	pu.PairMap = make(map[string]string)
	// 提取自定义键值对(通过 HTTP Header 传递,前缀为 PairNamePrefix)
	for k, v := range r.Header {
		if len(v) > 0 && strings.HasPrefix(k, PairNamePrefix) {
			pu.PairMap[k] = v[0]
		}
	}

	// 解析上传的文件数据
	e = parseUpload(r, sizeLimit, pu)

	if e != nil {
		return
	}

	// 解析时间戳和 TTL 参数
	pu.ModifiedTime, _ = strconv.ParseUint(r.FormValue("ts"), 10, 64)
	pu.Ttl, _ = ReadTTL(r.FormValue("ttl"))

	// 记录原始数据大小
	pu.OriginalDataSize = len(pu.Data)
	pu.UncompressedData = pu.Data
	// println("received data", len(pu.Data), "isGzipped", pu.IsGzipped, "mime", pu.MimeType, "name", pu.FileName)

	// 如果数据已经 Gzip 压缩,解压以获取原始数据
	if pu.IsGzipped {
		if unzipped, e := util.DecompressData(pu.Data); e == nil {
			pu.OriginalDataSize = len(unzipped)
			pu.UncompressedData = unzipped
			// println("ungzipped data size", len(unzipped))
		}
	} else {
		// 如果数据未压缩,尝试自动压缩以节省存储空间
		ext := filepath.Base(pu.FileName)
		mimeType := pu.MimeType
		if mimeType == "" {
			// 如果没有 MIME 类型,通过数据内容自动检测
			mimeType = http.DetectContentType(pu.Data)
		}
		// println("detected mimetype to", pu.MimeType)
		// 忽略通用的 application/octet-stream 类型
		if mimeType == "application/octet-stream" {
			mimeType = ""
		}
		// 判断文件类型是否适合压缩(如文本文件、JSON、XML 等)
		if shouldBeCompressed, iAmSure := util.IsCompressableFileType(ext, mimeType); shouldBeCompressed && iAmSure {
			// println("ext", ext, "iAmSure", iAmSure, "shouldBeCompressed", shouldBeCompressed, "mimeType", pu.MimeType)
			if compressedData, err := util.GzipData(pu.Data); err == nil {
				// 只有在压缩后体积减小 10% 以上时才使用压缩数据
				// 压缩率 = 压缩后大小 / 原始大小 < 0.9
				if len(compressedData)*10 < len(pu.Data)*9 {
					pu.Data = compressedData
					pu.IsGzipped = true
				}
				// println("gzipped data size", len(compressedData))
			}
		}
	}

	// 计算 MD5 校验值(对未压缩数据计算)
	h := md5.New()
	h.Write(pu.UncompressedData)
	pu.ContentMd5 = base64.StdEncoding.EncodeToString(h.Sum(nil))
	// 验证客户端提供的 MD5 校验值
	if expectedChecksum := r.Header.Get("Content-MD5"); expectedChecksum != "" {
		if expectedChecksum != pu.ContentMd5 {
			e = fmt.Errorf("Content-MD5 did not match md5 of file data expected [%s] received [%s] size %d", expectedChecksum, pu.ContentMd5, len(pu.UncompressedData))
			return
		}
	}

	return
}

// parseUpload 内部函数,负责实际的文件数据解析
// 支持两种上传方式:
//   1. POST multipart/form-data: 浏览器表单上传,多部分数据
//   2. PUT/POST 直接二进制: 直接上传文件内容,通过 Content-Disposition 获取文件名
// 参数:
//   r: HTTP 请求对象
//   sizeLimit: 文件大小限制(字节)
//   pu: 待填充的 ParsedUpload 对象
// 返回:
//   e: 错误信息
func parseUpload(r *http.Request, sizeLimit int64, pu *ParsedUpload) (e error) {

	// 延迟处理:如果发生错误,清空并关闭请求体,释放资源
	defer func() {
		if e != nil && r.Body != nil {
			io.Copy(io.Discard, r.Body) // 丢弃剩余数据
			r.Body.Close()
		}
	}()

	contentType := r.Header.Get("Content-Type")
	var dataSize int64

	// 处理 multipart/form-data 上传(浏览器表单上传)
	if r.Method == http.MethodPost && (contentType == "" || strings.Contains(contentType, "form-data")) {
		form, fe := r.MultipartReader()

		if fe != nil {
			glog.V(0).Infoln("MultipartReader [ERROR]", fe)
			e = fe
			return
		}

		// 读取第一个 multi-part 项(通常是文件)
		part, fe := form.NextPart()
		if fe != nil {
			glog.V(0).Infoln("Reading Multi part [ERROR]", fe)
			e = fe
			return
		}

		pu.FileName = part.FileName()
		if pu.FileName != "" {
			pu.FileName = path.Base(pu.FileName) // 只保留文件名,去除路径
		}

		// 读取文件数据,限制大小为 sizeLimit+1 以检测超限
		dataSize, e = pu.bytesBuffer.ReadFrom(io.LimitReader(part, sizeLimit+1))
		if e != nil {
			glog.V(0).Infoln("Reading Content [ERROR]", e)
			return
		}
		if dataSize == sizeLimit+1 {
			e = fmt.Errorf("file over the limited %d bytes", sizeLimit)
			return
		}
		pu.Data = pu.bytesBuffer.Bytes()

		contentType = part.Header.Get("Content-Type")

		// 如果第一个 part 没有文件名,继续搜索后续 part
		// 某些客户端可能先发送其他表单字段,文件在后面
		for pu.FileName == "" {
			part2, fe := form.NextPart()
			if fe != nil {
				break // 没有更多数据或出错,安全退出
			}

			fName := part2.FileName()

			// 找到第一个有文件名的 multi-part
			if fName != "" {
				pu.bytesBuffer.Reset()
				dataSize2, fe2 := pu.bytesBuffer.ReadFrom(io.LimitReader(part2, sizeLimit+1))
				if fe2 != nil {
					glog.V(0).Infoln("Reading Content [ERROR]", fe2)
					e = fe2
					return
				}
				if dataSize2 == sizeLimit+1 {
					e = fmt.Errorf("file over the limited %d bytes", sizeLimit)
					return
				}

				// 更新为找到的文件数据
				pu.Data = pu.bytesBuffer.Bytes()
				pu.FileName = path.Base(fName)
				contentType = part.Header.Get("Content-Type")
				part = part2
				break
			}
		}

		// 检查是否已 Gzip 压缩
		pu.IsGzipped = part.Header.Get("Content-Encoding") == "gzip"
		// pu.IsZstd = part.Header.Get("Content-Encoding") == "zstd"

	} else {
		// 处理直接二进制上传(PUT/POST)
		disposition := r.Header.Get("Content-Disposition")

		// 从 Content-Disposition 头中提取文件名
		if strings.Contains(disposition, "name=") {

			// 确保 disposition 符合标准格式
			if !strings.HasPrefix(disposition, "inline") && !strings.HasPrefix(disposition, "attachment") {
				disposition = "attachment; " + disposition
			}

			_, mediaTypeParams, err := mime.ParseMediaType(disposition)

			if err == nil {
				dpFilename, hasFilename := mediaTypeParams["filename"]
				dpName, hasName := mediaTypeParams["name"]

				if hasFilename {
					pu.FileName = dpFilename
				} else if hasName {
					pu.FileName = dpName
				}

			}

		} else {
			pu.FileName = ""
		}

		// 如果 Content-Disposition 中没有文件名,尝试从 URL 路径提取
		if pu.FileName != "" {
			pu.FileName = path.Base(pu.FileName)
		} else {
			pu.FileName = path.Base(r.URL.Path)
		}

		// 读取请求体数据
		dataSize, e = pu.bytesBuffer.ReadFrom(io.LimitReader(r.Body, sizeLimit+1))

		if e != nil {
			glog.V(0).Infoln("Reading Content [ERROR]", e)
			return
		}
		if dataSize == sizeLimit+1 {
			e = fmt.Errorf("file over the limited %d bytes", sizeLimit)
			return
		}

		pu.Data = pu.bytesBuffer.Bytes()
		pu.MimeType = contentType
		pu.IsGzipped = r.Header.Get("Content-Encoding") == "gzip"
		// pu.IsZstd = r.Header.Get("Content-Encoding") == "zstd"
	}

	// 解析是否为分块文件(大文件分块上传标志)
	pu.IsChunkedFile, _ = strconv.ParseBool(r.FormValue("cm"))

	// 如果不是分块文件,处理 MIME 类型
	if !pu.IsChunkedFile {

		dotIndex := strings.LastIndex(pu.FileName, ".")
		ext, mtype := "", ""

		// 从文件扩展名推断 MIME 类型
		if dotIndex > 0 {
			ext = strings.ToLower(pu.FileName[dotIndex:])
			mtype = mime.TypeByExtension(ext)
		}

		// MIME 类型优先级:
		// 1. 如果有明确的 contentType(非 octet-stream)且与扩展名不匹配,使用 contentType
		// 2. 否则如果扩展名能推断出 MIME 类型,使用推断的类型
		if contentType != "" && contentType != "application/octet-stream" && mtype != contentType {
			pu.MimeType = contentType // 只有在无法推断时才返回 MIME 类型
		} else if mtype != "" && pu.MimeType == "" && mtype != "application/octet-stream" {
			pu.MimeType = mtype
		}

	}

	return
}
