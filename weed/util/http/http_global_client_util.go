// Package http 提供全局 HTTP 客户端的实用工具函数
// 包括 GET、POST、DELETE 等常用 HTTP 操作,支持 JWT 认证、gzip 压缩、加密数据读取等
package http

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/mem"
	"github.com/seaweedfs/seaweedfs/weed/util/request_id"

	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"

	"github.com/seaweedfs/seaweedfs/weed/security"
)

// ErrNotFound 404 错误常量
// 用于标识资源未找到的情况
var ErrNotFound = fmt.Errorf("not found")

var (
	// jwtSigningReadKey JWT 签名密钥(用于读取操作)
	jwtSigningReadKey        security.SigningKey

	// jwtSigningReadKeyExpires JWT 令牌过期时间(秒)
	jwtSigningReadKeyExpires int

	// loadJwtConfigOnce 确保 JWT 配置只加载一次
	loadJwtConfigOnce        sync.Once
)

// loadJwtConfig 从配置文件加载 JWT 配置
// 读取 JWT 签名密钥和过期时间
// 使用 sync.Once 确保只加载一次
func loadJwtConfig() {
	v := util.GetViper()
	jwtSigningReadKey = security.SigningKey(v.GetString("jwt.signing.read.key"))
	jwtSigningReadKeyExpires = v.GetInt("jwt.signing.read.expires_after_seconds")
}

// Post 发送 POST 表单请求
// 使用全局 HTTP 客户端发送表单数据
//
// 参数:
//   url: 请求的 URL
//   values: 表单数据(键值对)
//
// 返回:
//   []byte: 响应体数据
//   error: 错误信息(包含 HTTP 状态码)
func Post(url string, values url.Values) ([]byte, error) {
	r, err := GetGlobalHttpClient().PostForm(url, values)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	b, err := io.ReadAll(r.Body)
	if r.StatusCode >= 400 {
		if err != nil {
			return nil, fmt.Errorf("%s: %d - %s", url, r.StatusCode, string(b))
		} else {
			return nil, fmt.Errorf("%s: %s", url, r.Status)
		}
	}
	if err != nil {
		return nil, err
	}
	return b, nil
}

// Get 发送 GET 请求(不带认证)
// 这是 GetAuthenticated 的简化版本
//
// 参数:
//   url: 请求的 URL
//
// 返回:
//   []byte: 响应体数据
//   bool: 是否可重试(5xx 错误为 true)
//   error: 错误信息
//
// 注意:
//   github.com/seaweedfs/seaweedfs/unmaintained/repeated_vacuum/repeated_vacuum.go
//   可能需要增加 http.Client.Timeout
func Get(url string) ([]byte, bool, error) {
	return GetAuthenticated(url, "")
}

// GetAuthenticated 发送带认证的 GET 请求
// 支持 gzip 压缩响应,自动解压
//
// 参数:
//   url: 请求的 URL
//   jwt: JWT 认证令牌(空字符串表示不使用认证)
//
// 返回:
//   []byte: 响应体数据(已解压)
//   bool: 是否可重试(5xx 错误为 true)
//   error: 错误信息
func GetAuthenticated(url, jwt string) ([]byte, bool, error) {
	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, true, err
	}
	maybeAddAuth(request, jwt)
	request.Header.Add("Accept-Encoding", "gzip")  // 请求 gzip 压缩

	response, err := GetGlobalHttpClient().Do(request)
	if err != nil {
		return nil, true, err
	}
	defer CloseResponse(response)

	var reader io.ReadCloser
	// 根据响应头判断是否需要解压
	switch response.Header.Get("Content-Encoding") {
	case "gzip":
		reader, err = gzip.NewReader(response.Body)
		if err != nil {
			return nil, true, err
		}
		defer reader.Close()
	default:
		reader = response.Body
	}

	b, err := io.ReadAll(reader)
	if response.StatusCode >= 400 {
		retryable := response.StatusCode >= 500  // 5xx 错误可重试
		return nil, retryable, fmt.Errorf("%s: %s", url, response.Status)
	}
	if err != nil {
		return nil, false, err
	}
	return b, false, nil
}

// Head 发送 HEAD 请求
// HEAD 请求只获取响应头,不获取响应体
//
// 参数:
//   url: 请求的 URL
//
// 返回:
//   http.Header: 响应头
//   error: 错误信息
func Head(url string) (http.Header, error) {
	r, err := GetGlobalHttpClient().Head(url)
	if err != nil {
		return nil, err
	}
	defer CloseResponse(r)
	if r.StatusCode >= 400 {
		return nil, fmt.Errorf("%s: %s", url, r.Status)
	}
	return r.Header, nil
}

// maybeAddAuth 如果提供了 JWT,则添加认证头
// 内部辅助函数
//
// 参数:
//   req: HTTP 请求对象
//   jwt: JWT 令牌(空字符串表示不添加认证)
func maybeAddAuth(req *http.Request, jwt string) {
	if jwt != "" {
		req.Header.Set("Authorization", "BEARER "+string(jwt))
	}
}

// Delete 发送 DELETE 请求
// 支持 JWT 认证,处理各种响应状态码
//
// 参数:
//   url: 请求的 URL
//   jwt: JWT 认证令牌
//
// 返回:
//   error: 错误信息(成功返回 nil)
//
// 成功的状态码:
//   - 200 OK
//   - 202 Accepted
//   - 404 Not Found (资源不存在也视为成功)
func Delete(url string, jwt string) error {
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	maybeAddAuth(req, jwt)
	if err != nil {
		return err
	}
	resp, e := GetGlobalHttpClient().Do(req)
	if e != nil {
		return e
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	// 这些状态码都视为成功
	switch resp.StatusCode {
	case http.StatusNotFound, http.StatusAccepted, http.StatusOK:
		return nil
	}
	// 尝试从响应体解析错误信息
	m := make(map[string]interface{})
	if e := json.Unmarshal(body, &m); e == nil {
		if s, ok := m["error"].(string); ok {
			return errors.New(s)
		}
	}
	return errors.New(string(body))
}

// DeleteProxied 发送 DELETE 请求并返回完整响应
// 与 Delete 不同,这个函数返回响应体和状态码,用于代理场景
//
// 参数:
//   url: 请求的 URL
//   jwt: JWT 认证令牌
//
// 返回:
//   body: 响应体数据
//   httpStatus: HTTP 状态码
//   err: 错误信息
func DeleteProxied(url string, jwt string) (body []byte, httpStatus int, err error) {
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	maybeAddAuth(req, jwt)
	if err != nil {
		return
	}
	resp, err := GetGlobalHttpClient().Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	body, err = io.ReadAll(resp.Body)
	if err != nil {
		return
	}
	httpStatus = resp.StatusCode
	return
}

// GetBufferStream 流式读取 POST 响应
// 使用预分配的缓冲区循环读取响应体,每次读取后调用回调函数
//
// 参数:
//   url: 请求的 URL
//   values: 表单数据
//   allocatedBytes: 预分配的缓冲区(避免频繁分配内存)
//   eachBuffer: 每次读取到数据后的回调函数
//
// 返回:
//   error: 错误信息(读取完成返回 nil)
func GetBufferStream(url string, values url.Values, allocatedBytes []byte, eachBuffer func([]byte)) error {
	r, err := GetGlobalHttpClient().PostForm(url, values)
	if err != nil {
		return err
	}
	defer CloseResponse(r)
	if r.StatusCode != 200 {
		return fmt.Errorf("%s: %s", url, r.Status)
	}
	for {
		n, err := r.Body.Read(allocatedBytes)
		if n > 0 {
			eachBuffer(allocatedBytes[:n])  // 只传递有效数据部分
		}
		if err != nil {
			if err == io.EOF {
				return nil  // 正常结束
			}
			return err
		}
	}
}

// GetUrlStream 流式读取 POST 响应(使用自定义读取函数)
// 与 GetBufferStream 不同,这个函数允许自定义读取逻辑
//
// 参数:
//   url: 请求的 URL
//   values: 表单数据
//   readFn: 自定义读取函数(接收 io.Reader)
//
// 返回:
//   error: 错误信息
//
// 示例:
//   GetUrlStream(url, values, func(reader io.Reader) error {
//       return json.NewDecoder(reader).Decode(&result)
//   })
func GetUrlStream(url string, values url.Values, readFn func(io.Reader) error) error {
	r, err := GetGlobalHttpClient().PostForm(url, values)
	if err != nil {
		return err
	}
	defer CloseResponse(r)
	if r.StatusCode != 200 {
		return fmt.Errorf("%s: %s", url, r.Status)
	}
	return readFn(r.Body)
}

// DownloadFile 下载文件
// 从 URL 下载文件,支持 JWT 认证,提取文件名
//
// 参数:
//   fileUrl: 文件下载 URL
//   jwt: JWT 认证令牌
//
// 返回:
//   filename: 从 Content-Disposition 头提取的文件名
//   header: 响应头
//   resp: HTTP 响应对象(调用者负责关闭)
//   e: 错误信息
func DownloadFile(fileUrl string, jwt string) (filename string, header http.Header, resp *http.Response, e error) {
	req, err := http.NewRequest(http.MethodGet, fileUrl, nil)
	if err != nil {
		return "", nil, nil, err
	}

	maybeAddAuth(req, jwt)

	response, err := GetGlobalHttpClient().Do(req)
	if err != nil {
		return "", nil, nil, err
	}
	header = response.Header
	// 从 Content-Disposition 头提取文件名
	// 格式: Content-Disposition: attachment; filename="example.txt"
	contentDisposition := response.Header["Content-Disposition"]
	if len(contentDisposition) > 0 {
		idx := strings.Index(contentDisposition[0], "filename=")
		if idx != -1 {
			filename = contentDisposition[0][idx+len("filename="):]
			filename = strings.Trim(filename, "\"")  // 移除引号
		}
	}
	resp = response
	return
}

// Do 使用全局 HTTP 客户端执行请求
// 直接暴露底层客户端的 Do 方法
//
// 参数:
//   req: HTTP 请求对象
//
// 返回:
//   resp: HTTP 响应对象
//   err: 错误信息
func Do(req *http.Request) (resp *http.Response, err error) {
	return GetGlobalHttpClient().Do(req)
}

// NormalizeUrl 规范化 URL 的协议方案
// 根据全局客户端配置自动添加或转换 http/https 协议
//
// 参数:
//   url: 原始 URL
//
// 返回:
//   string: 规范化后的 URL
//   error: 解析错误
func NormalizeUrl(url string) (string, error) {
	return GetGlobalHttpClient().NormalizeHttpScheme(url)
}

// ReadUrl 从 URL 读取数据到缓冲区
// 支持加密数据、压缩数据和范围请求
//
// 参数:
//   ctx: 上下文
//   fileUrl: 文件 URL
//   cipherKey: 加密密钥(nil 表示未加密)
//   isContentCompressed: 内容是否被压缩
//   isFullChunk: 是否读取完整块(false 时使用 Range 请求)
//   offset: 读取偏移量
//   size: 读取大小
//   buf: 目标缓冲区
//
// 返回:
//   int64: 实际读取的字节数
//   error: 错误信息
func ReadUrl(ctx context.Context, fileUrl string, cipherKey []byte, isContentCompressed bool, isFullChunk bool, offset int64, size int, buf []byte) (int64, error) {

	// 如果数据被加密,使用专门的加密读取函数
	if cipherKey != nil {
		var n int
		_, err := readEncryptedUrl(ctx, fileUrl, "", cipherKey, isContentCompressed, isFullChunk, offset, size, func(data []byte) {
			n = copy(buf, data)
		})
		return int64(n), err
	}

	req, err := http.NewRequest(http.MethodGet, fileUrl, nil)
	if err != nil {
		return 0, err
	}
	if !isFullChunk {
		// 使用 Range 请求读取部分数据
		req.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+int64(size)-1))
	} else {
		// 读取完整块,请求 gzip 压缩
		req.Header.Set("Accept-Encoding", "gzip")
	}

	r, err := GetGlobalHttpClient().Do(req)
	if err != nil {
		return 0, err
	}
	defer CloseResponse(r)

	if r.StatusCode >= 400 {
		return 0, fmt.Errorf("%s: %s", fileUrl, r.Status)
	}

	var reader io.ReadCloser
	contentEncoding := r.Header.Get("Content-Encoding")
	switch contentEncoding {
	case "gzip":
		// 自动解压 gzip 数据
		reader, err = gzip.NewReader(r.Body)
		if err != nil {
			return 0, err
		}
		defer reader.Close()
	default:
		reader = r.Body
	}

	var (
		i, m int
		n    int64
	)

	// 参考 Go 标准库 bytes.Buffer 的实现
	// commit id c170b14c2c1cfb2fd853a37add92a82fd6eb4318
	for {
		m, err = reader.Read(buf[i:])
		i += m
		n += int64(m)
		if err == io.EOF {
			return n, nil
		}
		if err != nil {
			return n, err
		}
		if n == int64(len(buf)) {
			break
		}
	}
	// 排空响应体,避免内存泄漏
	data, _ := io.ReadAll(reader)
	if len(data) != 0 {
		glog.V(1).InfofCtx(ctx, "%s reader has remaining %d bytes", contentEncoding, len(data))
	}
	return n, err
}

// ReadUrlAsStream 流式读取 URL 数据
// 使用回调函数逐块处理数据,支持加密、压缩、范围请求和上下文取消
//
// 参数:
//   ctx: 上下文(支持取消)
//   fileUrl: 文件 URL
//   jwt: JWT 认证令牌
//   cipherKey: 加密密钥(nil 表示未加密)
//   isContentGzipped: 内容是否被 gzip 压缩
//   isFullChunk: 是否读取完整块
//   offset: 读取偏移量
//   size: 读取大小
//   fn: 数据处理回调函数(每读取一块数据就调用一次)
//
// 返回:
//   retryable: 是否可重试(5xx 错误为 true)
//   err: 错误信息
func ReadUrlAsStream(ctx context.Context, fileUrl, jwt string, cipherKey []byte, isContentGzipped bool, isFullChunk bool, offset int64, size int, fn func(data []byte)) (retryable bool, err error) {
	// 如果数据被加密,使用专门的加密读取函数
	if cipherKey != nil {
		return readEncryptedUrl(ctx, fileUrl, jwt, cipherKey, isContentGzipped, isFullChunk, offset, size, fn)
	}

	req, err := http.NewRequest(http.MethodGet, fileUrl, nil)
	maybeAddAuth(req, jwt)
	if err != nil {
		return false, err
	}

	if isFullChunk {
		req.Header.Add("Accept-Encoding", "gzip")
	} else {
		// 使用 Range 请求读取部分数据
		req.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+int64(size)-1))
	}
	request_id.InjectToRequest(ctx, req)  // 注入请求 ID,用于追踪

	r, err := GetGlobalHttpClient().Do(req)
	if err != nil {
		return true, err
	}
	defer CloseResponse(r)
	if r.StatusCode >= 400 {
		if r.StatusCode == http.StatusNotFound {
			return true, fmt.Errorf("%s: %s: %w", fileUrl, r.Status, ErrNotFound)
		}
		retryable = r.StatusCode >= 499  // 5xx 错误可重试
		return retryable, fmt.Errorf("%s: %s", fileUrl, r.Status)
	}

	var reader io.ReadCloser
	contentEncoding := r.Header.Get("Content-Encoding")
	switch contentEncoding {
	case "gzip":
		// 自动解压 gzip 数据
		reader, err = gzip.NewReader(r.Body)
		defer reader.Close()
	default:
		reader = r.Body
	}

	var (
		m int
	)
	buf := mem.Allocate(64 * 1024)  // 分配 64KB 缓冲区
	defer mem.Free(buf)

	for {
		// 在每次读取前检查上下文是否被取消
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		default:
		}

		m, err = reader.Read(buf)
		if m > 0 {
			fn(buf[:m])  // 调用回调函数处理数据
		}
		if err == io.EOF {
			return false, nil
		}
		if err != nil {
			return true, err
		}
	}

}

// readEncryptedUrl 读取加密的 URL 数据
// 下载完整数据,解密后调用回调函数
//
// 参数:
//   ctx: 上下文
//   fileUrl: 文件 URL
//   jwt: JWT 认证令牌
//   cipherKey: 加密密钥
//   isContentCompressed: 内容是否被压缩(在加密之前)
//   isFullChunk: 是否完整块
//   offset: 读取偏移量
//   size: 读取大小
//   fn: 数据处理回调函数
//
// 返回:
//   bool: 是否可重试
//   error: 错误信息
func readEncryptedUrl(ctx context.Context, fileUrl, jwt string, cipherKey []byte, isContentCompressed bool, isFullChunk bool, offset int64, size int, fn func(data []byte)) (bool, error) {
	// 下载加密数据
	encryptedData, retryable, err := GetAuthenticated(fileUrl, jwt)
	if err != nil {
		return retryable, fmt.Errorf("fetch %s: %v", fileUrl, err)
	}
	// 解密数据
	decryptedData, err := util.Decrypt(encryptedData, util.CipherKey(cipherKey))
	if err != nil {
		return false, fmt.Errorf("decrypt %s: %v", fileUrl, err)
	}
	// 如果数据被压缩,解压
	if isContentCompressed {
		decryptedData, err = util.DecompressData(decryptedData)
		if err != nil {
			glog.V(0).InfofCtx(ctx, "unzip decrypt %s: %v", fileUrl, err)
		}
	}
	// 检查数据大小是否足够
	if len(decryptedData) < int(offset)+size {
		return false, fmt.Errorf("read decrypted %s size %d [%d, %d)", fileUrl, len(decryptedData), offset, int(offset)+size)
	}
	// 调用回调函数处理数据
	if isFullChunk {
		fn(decryptedData)
	} else {
		sliceEnd := int(offset) + size
		fn(decryptedData[int(offset):sliceEnd])
	}
	return false, nil
}

// ReadUrlAsReaderCloser 以 ReadCloser 形式读取 URL
// 返回响应对象和数据读取器,调用者负责关闭
//
// 参数:
//   fileUrl: 文件 URL
//   jwt: JWT 认证令牌
//   rangeHeader: Range 请求头值(如 "bytes=0-1023",空字符串表示完整请求)
//
// 返回:
//   *http.Response: HTTP 响应对象
//   io.ReadCloser: 数据读取器(可能已解压 gzip)
//   error: 错误信息
func ReadUrlAsReaderCloser(fileUrl string, jwt string, rangeHeader string) (*http.Response, io.ReadCloser, error) {

	req, err := http.NewRequest(http.MethodGet, fileUrl, nil)
	if err != nil {
		return nil, nil, err
	}
	if rangeHeader != "" {
		req.Header.Add("Range", rangeHeader)
	} else {
		// 如果不是 Range 请求,请求 gzip 压缩
		req.Header.Add("Accept-Encoding", "gzip")
	}

	maybeAddAuth(req, jwt)

	r, err := GetGlobalHttpClient().Do(req)
	if err != nil {
		return nil, nil, err
	}
	if r.StatusCode >= 400 {
		CloseResponse(r)
		return nil, nil, fmt.Errorf("%s: %s", fileUrl, r.Status)
	}

	var reader io.ReadCloser
	contentEncoding := r.Header.Get("Content-Encoding")
	switch contentEncoding {
	case "gzip":
		// 自动解压 gzip 数据
		reader, err = gzip.NewReader(r.Body)
		if err != nil {
			return nil, nil, err
		}
	default:
		reader = r.Body
	}

	return r, reader, nil
}

// CloseResponse 正确关闭 HTTP 响应
// 读取并丢弃剩余数据,避免连接泄漏
//
// 参数:
//   resp: HTTP 响应对象
//
// 注意:
//   - 这个函数很重要,直接关闭 Body 可能导致连接无法复用
//   - 读取剩余数据确保连接可以被连接池复用
func CloseResponse(resp *http.Response) {
	if resp == nil || resp.Body == nil {
		return
	}
	reader := &CountingReader{reader: resp.Body}
	io.Copy(io.Discard, reader)  // 排空响应体
	resp.Body.Close()
	if reader.BytesRead > 0 {
		glog.V(1).Infof("response leftover %d bytes", reader.BytesRead)
	}
}

// CloseRequest 正确关闭 HTTP 请求体
// 读取并丢弃剩余数据
//
// 参数:
//   req: HTTP 请求对象
func CloseRequest(req *http.Request) {
	reader := &CountingReader{reader: req.Body}
	io.Copy(io.Discard, reader)  // 排空请求体
	req.Body.Close()
	if reader.BytesRead > 0 {
		glog.V(1).Infof("request leftover %d bytes", reader.BytesRead)
	}
}

// CountingReader 计数读取器
// 包装 io.Reader,统计读取的字节数
type CountingReader struct {
	reader    io.Reader
	BytesRead int  // 已读取的字节数
}

// Read 实现 io.Reader 接口
// 读取数据并累计字节数
func (r *CountingReader) Read(p []byte) (n int, err error) {
	n, err = r.reader.Read(p)
	r.BytesRead += n
	return n, err
}

// RetriedFetchChunkData 带重试的数据块获取
// 从多个 URL 尝试获取数据块,支持加密、压缩、重试和上下文取消
// 这是 SeaweedFS 读取文件块的核心函数,用于容错和高可用
//
// 参数:
//   ctx: 上下文(支持取消)
//   buffer: 目标缓冲区
//   urlStrings: URL 列表(可能有多个副本)
//   cipherKey: 加密密钥(nil 表示未加密)
//   isGzipped: 数据是否被 gzip 压缩
//   isFullChunk: 是否读取完整块
//   offset: 读取偏移量
//   fileId: 文件 ID(用于生成 JWT)
//
// 返回:
//   n: 实际读取的字节数
//   err: 错误信息
//
// 重试策略:
//   - 尝试所有 URL
//   - 如果所有 URL 都失败且可重试,等待后重试
//   - 等待时间指数增长:1s, 1.5s, 2.25s, ...
//   - 最多等待到 util.RetryWaitTime
//   - 支持上下文取消
func RetriedFetchChunkData(ctx context.Context, buffer []byte, urlStrings []string, cipherKey []byte, isGzipped bool, isFullChunk bool, offset int64, fileId string) (n int, err error) {

	// 加载 JWT 配置(只加载一次)
	loadJwtConfigOnce.Do(loadJwtConfig)
	var jwt security.EncodedJwt
	// 如果配置了 JWT 密钥,生成令牌
	if len(jwtSigningReadKey) > 0 {
		jwt = security.GenJwtForVolumeServer(
			jwtSigningReadKey,
			jwtSigningReadKeyExpires,
			fileId,
		)
	}

	var shouldRetry bool

	// 重试循环,等待时间指数增长
	for waitTime := time.Second; waitTime < util.RetryWaitTime; waitTime += waitTime / 2 {
		// 在重试前检查上下文是否被取消
		select {
		case <-ctx.Done():
			return n, ctx.Err()
		default:
		}

		// 尝试所有 URL
		for _, urlString := range urlStrings {
			// 在每个 Volume Server 请求前检查上下文
			select {
			case <-ctx.Done():
				return n, ctx.Err()
			default:
			}

			n = 0
			// URL 编码处理(如果包含特殊字符)
			if strings.Contains(urlString, "%") {
				urlString = url.PathEscape(urlString)
			}
			// 流式读取数据
			shouldRetry, err = ReadUrlAsStream(ctx, urlString+"?readDeleted=true", string(jwt), cipherKey, isGzipped, isFullChunk, offset, len(buffer), func(data []byte) {
				// 在数据处理时检查上下文取消
				select {
				case <-ctx.Done():
					// 上下文取消时停止处理数据
					return
				default:
				}

				// 将数据复制到缓冲区
				if n < len(buffer) {
					x := copy(buffer[n:], data)
					n += x
				}
			})
			if !shouldRetry {
				break  // 成功或不可重试的错误
			}
			if err != nil {
				glog.V(0).InfofCtx(ctx, "read %s failed, err: %v", urlString, err)
			} else {
				break  // 成功读取
			}
		}
		// 如果有错误且可重试,等待后重试
		if err != nil && shouldRetry {
			glog.V(0).InfofCtx(ctx, "retry reading in %v", waitTime)
			// 使用 Timer 进行可取消的等待
			timer := time.NewTimer(waitTime)
			select {
			case <-ctx.Done():
				timer.Stop()
				return n, ctx.Err()
			case <-timer.C:
				// 继续重试
			}
		} else {
			break  // 成功或不可重试的错误
		}
	}

	return n, err

}
