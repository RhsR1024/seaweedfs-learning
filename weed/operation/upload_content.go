// Package operation 提供文件上传相关的核心功能
// 包括上传内容到 Volume Server、处理压缩、加密和重试逻辑
package operation

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util/request_id"
	"github.com/valyala/bytebufferpool"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
	util_http_client "github.com/seaweedfs/seaweedfs/weed/util/http/client"
)

// UploadOption 上传选项配置
// 定义了上传文件时的各种参数和选项
type UploadOption struct {
	UploadUrl         string              // 上传目标 URL (Volume Server 地址)
	Filename          string              // 文件名
	Cipher            bool                // 是否启用加密
	IsInputCompressed bool                // 输入数据是否已压缩
	MimeType          string              // MIME 类型
	PairMap           map[string]string   // 自定义 HTTP 头键值对
	Jwt               security.EncodedJwt // JWT 认证令牌
	RetryForever      bool                // 是否无限重试
	Md5               string              // MD5 校验值
	BytesBuffer       *bytes.Buffer       // 可重用的字节缓冲区(避免频繁分配内存)
}

// UploadResult 上传结果
// 包含上传完成后的元数据和状态信息
type UploadResult struct {
	Name       string `json:"name,omitempty"`       // 文件名
	Size       uint32 `json:"size,omitempty"`       // 原始文件大小(未压缩)
	Error      string `json:"error,omitempty"`      // 错误信息
	ETag       string `json:"eTag,omitempty"`       // HTTP ETag
	CipherKey  []byte `json:"cipherKey,omitempty"`  // 加密密钥(如果启用了加密)
	Mime       string `json:"mime,omitempty"`       // MIME 类型
	Gzip       uint32 `json:"gzip,omitempty"`       // Gzip 压缩标志(0=未压缩, 1=已压缩)
	ContentMd5 string `json:"contentMd5,omitempty"` // 内容 MD5 值
	RetryCount int    `json:"-"`                    // 重试次数(不序列化到 JSON)
}

// ToPbFileChunk 将上传结果转换为 FileChunk protobuf 对象
// 用于在 Filer 中记录文件块的元数据
// 参数:
//
//	fileId: 文件 ID (格式: volumeId,needleId,cookie)
//	offset: 该文件块在完整文件中的偏移量
//	tsNs: 修改时间戳(纳秒)
func (uploadResult *UploadResult) ToPbFileChunk(fileId string, offset int64, tsNs int64) *filer_pb.FileChunk {
	fid, _ := filer_pb.ToFileIdObject(fileId)
	return &filer_pb.FileChunk{
		FileId:       fileId,                    // 文件 ID 字符串
		Offset:       offset,                    // 文件块偏移量
		Size:         uint64(uploadResult.Size), // 文件块大小
		ModifiedTsNs: tsNs,                      // 修改时间戳
		ETag:         uploadResult.ContentMd5,   // ETag (用于缓存和完整性校验)
		CipherKey:    uploadResult.CipherKey,    // 加密密钥
		IsCompressed: uploadResult.Gzip > 0,     // 是否压缩
		Fid:          fid,                       // 文件 ID 对象(已解析)
	}
}

// ToPbFileChunkWithSSE 创建带有 SSE (Server-Side Encryption) 元数据的 FileChunk
// 在服务器端加密场景中使用,包含加密类型和元数据
// 参数:
//
//	fileId: 文件 ID
//	offset: 文件块偏移量
//	tsNs: 修改时间戳(纳秒)
//	sseType: SSE 加密类型 (如 AES256, KMS 等)
//	sseMetadata: SSE 元数据(加密相关的配置信息)
func (uploadResult *UploadResult) ToPbFileChunkWithSSE(fileId string, offset int64, tsNs int64, sseType filer_pb.SSEType, sseMetadata []byte) *filer_pb.FileChunk {
	fid, _ := filer_pb.ToFileIdObject(fileId)
	chunk := &filer_pb.FileChunk{
		FileId:       fileId,
		Offset:       offset,
		Size:         uint64(uploadResult.Size),
		ModifiedTsNs: tsNs,
		ETag:         uploadResult.ContentMd5,
		CipherKey:    uploadResult.CipherKey,
		IsCompressed: uploadResult.Gzip > 0,
		Fid:          fid,
	}

	// 添加 SSE 元数据(如果提供)
	chunk.SseType = sseType
	if len(sseMetadata) > 0 {
		chunk.SseMetadata = sseMetadata
	}

	return chunk
}

var (
	// fileNameEscaper 文件名转义器
	// 用于转义文件名中的特殊字符:反斜杠、双引号和换行符
	// 确保文件名在 HTTP 头中的安全传输
	fileNameEscaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", "")

	// uploader 全局上传器单例
	uploader *Uploader

	// uploaderErr 上传器初始化错误
	uploaderErr error

	// once 确保上传器只初始化一次
	once sync.Once
)

// HTTPClient HTTP 客户端接口,用于测试时的 mock
type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

// Uploader 上传器结构
// 封装了 HTTP 客户端,用于向 Volume Server 上传文件
type Uploader struct {
	httpClient HTTPClient // HTTP 客户端(可能是标准客户端或测试 mock)
}

// NewUploader 创建新的上传器单例
// 使用 sync.Once 确保全局只初始化一次
// 返回全局上传器实例和可能的初始化错误
func NewUploader() (*Uploader, error) {
	once.Do(func() {
		// 创建带有 Dial context 的 HTTP 客户端
		// 这允许更好的连接管理和超时控制
		var httpClient *util_http_client.HTTPClient
		httpClient, uploaderErr = util_http.NewGlobalHttpClient(util_http_client.AddDialContext)
		if uploaderErr != nil {
			uploaderErr = fmt.Errorf("error initializing the loader: %s", uploaderErr)
		}
		if httpClient != nil {
			uploader = newUploader(httpClient)
		}
	})
	return uploader, uploaderErr
}

// newUploader 内部构造函数,创建上传器实例
// 参数:
//
//	httpClient: HTTP 客户端接口
func newUploader(httpClient HTTPClient) *Uploader {
	return &Uploader{
		httpClient: httpClient,
	}
}

// UploadWithRetry 带重试的上传操作
// 同时重试分配 Volume 和上传内容两个步骤
// 这是一个完整的上传流程:先分配存储位置,然后上传数据
//
// 参数:
//
//	filerClient: Filer 客户端,用于请求分配 Volume
//	assignRequest: 分配 Volume 的请求参数
//	uploadOption: 上传选项(不需要指定 UploadUrl 和 Jwt,会从分配结果中获取)
//	genFileUrlFn: 生成文件 URL 的函数(根据 host 和 fileId 生成完整 URL)
//	reader: 要上传的数据流
//
// 返回:
//
//	fileId: 分配的文件 ID
//	uploadResult: 上传结果
//	err: 错误信息
//	data: 读取的数据(用于可能的重试)
func (uploader *Uploader) UploadWithRetry(filerClient filer_pb.FilerClient, assignRequest *filer_pb.AssignVolumeRequest, uploadOption *UploadOption, genFileUrlFn func(host, fileId string) string, reader io.Reader) (fileId string, uploadResult *UploadResult, err error, data []byte) {
	// 定义上传执行函数,用于重试
	doUploadFunc := func() error {

		var host string
		var auth security.EncodedJwt

		// 通过 gRPC 向 Filer 请求分配 Volume
		if grpcAssignErr := filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
			resp, assignErr := client.AssignVolume(context.Background(), assignRequest)
			if assignErr != nil {
				glog.V(0).Infof("assign volume failure %v: %v", assignRequest, assignErr)
				return assignErr
			}
			if resp.Error != "" {
				return fmt.Errorf("assign volume failure %v: %v", assignRequest, resp.Error)
			}

			// 获取分配结果:文件 ID、认证令牌和存储位置
			fileId, auth = resp.FileId, security.EncodedJwt(resp.Auth)
			loc := resp.Location
			host = filerClient.AdjustedUrl(loc) // 调整 URL(可能处理代理、负载均衡等)

			return nil
		}); grpcAssignErr != nil {
			return fmt.Errorf("filerGrpcAddress assign volume: %w", grpcAssignErr)
		}

		// 根据分配结果设置上传 URL 和认证信息
		uploadOption.UploadUrl = genFileUrlFn(host, fileId)
		uploadOption.Jwt = auth

		// 执行实际的上传操作
		var uploadErr error
		uploadResult, uploadErr, data = uploader.doUpload(context.Background(), reader, uploadOption)
		return uploadErr
	}

	// 根据配置选择重试策略
	if uploadOption.RetryForever {
		// 无限重试模式:一直重试直到成功
		util.RetryUntil("uploadWithRetryForever", doUploadFunc, func(err error) (shouldContinue bool) {
			glog.V(0).Infof("upload content: %v", err)
			return true // 总是继续重试
		})
	} else {
		// 有限重试模式:只重试特定类型的错误
		uploadErrList := []string{"transport", "is read only"} // 可重试的错误类型
		err = util.MultiRetry("uploadWithRetry", uploadErrList, doUploadFunc)
	}

	return
}

// UploadData 上传字节数据
// 发送 POST 请求到 Volume Server,支持可调节的压缩级别
// 适用于已经在内存中的数据
//
// 参数:
//
//	ctx: 上下文,用于超时和取消控制
//	data: 要上传的字节数据
//	option: 上传选项
//
// 返回:
//
//	uploadResult: 上传结果
//	err: 错误信息
func (uploader *Uploader) UploadData(ctx context.Context, data []byte, option *UploadOption) (uploadResult *UploadResult, err error) {
	uploadResult, err = uploader.retriedUploadData(ctx, data, option)
	return
}

// Upload 上传数据流
// 发送 POST 请求到 Volume Server,使用快速压缩
// 适用于流式数据或未知大小的数据
//
// 参数:
//
//	ctx: 上下文
//	reader: 数据读取器
//	option: 上传选项
//
// 返回:
//
//	uploadResult: 上传结果
//	err: 错误信息
//	data: 读取的完整数据(可能用于重试)
func (uploader *Uploader) Upload(ctx context.Context, reader io.Reader, option *UploadOption) (uploadResult *UploadResult, err error, data []byte) {
	uploadResult, err, data = uploader.doUpload(ctx, reader, option)
	return
}

// doUpload 内部上传方法
// 处理从 reader 读取数据,然后调用 retriedUploadData 进行实际上传
//
// 参数:
//
//	ctx: 上下文
//	reader: 数据读取器
//	option: 上传选项
//
// 返回:
//
//	uploadResult: 上传结果
//	err: 错误信息
//	data: 读取的完整数据
func (uploader *Uploader) doUpload(ctx context.Context, reader io.Reader, option *UploadOption) (uploadResult *UploadResult, err error, data []byte) {
	// 优化:如果 reader 已经是 BytesReader,直接获取底层字节数组,避免重复复制
	bytesReader, ok := reader.(*util.BytesReader)
	if ok {
		data = bytesReader.Bytes
	} else {
		// 从 reader 读取所有数据到内存
		data, err = io.ReadAll(reader)
		if err != nil {
			err = fmt.Errorf("read input: %w", err)
			return
		}
	}
	// 带重试地上传数据
	uploadResult, uploadErr := uploader.retriedUploadData(ctx, data, option)
	return uploadResult, uploadErr, data
}

// retriedUploadData 带重试的数据上传
// 最多重试 3 次,每次失败后等待递增的时间(237ms * (i+1))
//
// 参数:
//
//	ctx: 上下文
//	data: 要上传的字节数据
//	option: 上传选项
//
// 返回:
//
//	uploadResult: 上传结果(包含重试次数)
//	err: 最后一次的错误信息(如果所有重试都失败)
func (uploader *Uploader) retriedUploadData(ctx context.Context, data []byte, option *UploadOption) (uploadResult *UploadResult, err error) {
	for i := 0; i < 3; i++ {
		if i > 0 {
			// 第一次之后的尝试需要等待:237ms, 474ms, 711ms
			time.Sleep(time.Millisecond * time.Duration(237*(i+1)))
		}
		uploadResult, err = uploader.doUploadData(ctx, data, option)
		if err == nil {
			// 上传成功,记录重试次数
			uploadResult.RetryCount = i
			return
		}
		// 记录失败日志,继续重试
		glog.WarningfCtx(ctx, "uploading %d to %s: %v", i, option.UploadUrl, err)
	}
	return
}

// doUploadData 执行数据上传的核心逻辑
// 处理压缩、加密、并通过 HTTP 上传到 Volume Server
// 这是整个上传流程中最复杂的方法,包含以下步骤:
// 1. 判断是否需要压缩
// 2. 执行压缩(如果需要)
// 3. 执行加密(如果配置)
// 4. 构造 HTTP 请求并上传
//
// 参数:
//
//	ctx: 上下文
//	data: 要上传的原始字节数据
//	option: 上传选项
//
// 返回:
//
//	uploadResult: 上传结果
//	err: 错误信息
func (uploader *Uploader) doUploadData(ctx context.Context, data []byte, option *UploadOption) (uploadResult *UploadResult, err error) {
	contentIsGzipped := option.IsInputCompressed // 内容是否已压缩
	shouldGzipNow := false                       // 是否需要立即压缩

	// 如果输入数据未压缩,判断是否需要压缩
	if !option.IsInputCompressed {
		// 如果未指定 MIME 类型,自动检测
		if option.MimeType == "" {
			option.MimeType = http.DetectContentType(data)
			// println("detect1 mimetype to", MimeType)
			// 如果检测为通用二进制类型,重置为空(让后续逻辑处理)
			if option.MimeType == "application/octet-stream" {
				option.MimeType = ""
			}
		}

		// 根据文件类型判断是否应该压缩
		if shouldBeCompressed, iAmSure := util.IsCompressableFileType(filepath.Base(option.Filename), option.MimeType); iAmSure && shouldBeCompressed {
			// 明确知道该文件类型应该压缩
			shouldGzipNow = true
		} else if !iAmSure && option.MimeType == "" && len(data) > 16*1024 {
			// 不确定文件类型且数据量较大(>16KB),通过压缩前 128 字节来判断
			var compressed []byte
			compressed, err = util.GzipData(data[0:128])
			if err != nil {
				return
			}
			// 如果压缩率低于 90%,认为值得压缩（取数据的前128字节做测试压缩）
			// len(compressed)*10 < 128*9
			// 等价于：
			// len(compressed)/128 < 9/10
			// 等价于：
			// len(compressed)/128 < 0.9 （即：如果128字节测试数据压缩后的大小不到128字节的90%（也就是压缩率超过10%），就认为整个文件值得压缩）
			shouldGzipNow = len(compressed)*10 < 128*9 // can not compress to less than 90%
		}
	}

	var clearDataLen int // 原始数据长度

	// 执行 gzip 压缩(如果需要)
	// 注意:这可能涉及双重内存复制
	clearDataLen = len(data)
	clearData := data
	if shouldGzipNow {
		// 压缩数据
		compressed, compressErr := util.GzipData(data)
		// fmt.Printf("data is compressed from %d ==> %d\n", len(data), len(compressed))
		if compressErr == nil {
			data = compressed       // 替换为压缩后的数据
			contentIsGzipped = true // 标记为已压缩
		}
	} else if option.IsInputCompressed {
		// 输入已压缩,需要解压以获取原始数据长度
		clearData, err = util.DecompressData(data)
		if err == nil {
			clearDataLen = len(clearData)
		}
	}

	// 处理加密(如果启用)
	if option.Cipher {
		// 加密流程: encrypt(gzip(data))
		// 即先压缩再加密,这样可以减少加密数据量

		// 生成随机加密密钥
		cipherKey := util.GenCipherKey()
		// 加密数据(可能是压缩后的数据)
		encryptedData, encryptionErr := util.Encrypt(data, cipherKey)
		if encryptionErr != nil {
			err = fmt.Errorf("encrypt input: %w", encryptionErr)
			return
		}

		// 上传加密后的数据
		// 注意:加密后上传时,文件名、MIME 类型等元数据会被清空
		// 这些信息会在上传结果中恢复
		uploadResult, err = uploader.upload_content(ctx, func(w io.Writer) (err error) {
			_, err = w.Write(encryptedData)
			return
		}, len(encryptedData), &UploadOption{
			UploadUrl:         option.UploadUrl,
			Filename:          "",         // 加密时不暴露文件名
			Cipher:            false,      // 已经加密,内部调用不再加密
			IsInputCompressed: false,      // 加密数据不标记为压缩
			MimeType:          "",         // 加密时不暴露 MIME 类型
			PairMap:           nil,        // 不传递自定义头
			Jwt:               option.Jwt, // 保留认证令牌
		})
		if uploadResult == nil {
			return
		}
		// 恢复元数据到上传结果
		uploadResult.Name = option.Filename
		uploadResult.Mime = option.MimeType
		uploadResult.CipherKey = cipherKey       // 保存加密密钥
		uploadResult.Size = uint32(clearDataLen) // 原始数据大小
		if contentIsGzipped {
			uploadResult.Gzip = 1 // 标记已压缩
		}
	} else {
		// 不加密,直接上传数据(可能已压缩)
		uploadResult, err = uploader.upload_content(ctx, func(w io.Writer) (err error) {
			_, err = w.Write(data)
			return
		}, len(data), &UploadOption{
			UploadUrl:         option.UploadUrl,
			Filename:          option.Filename,
			Cipher:            false,
			IsInputCompressed: contentIsGzipped, // 传递压缩状态
			MimeType:          option.MimeType,
			PairMap:           option.PairMap,
			Jwt:               option.Jwt,
			Md5:               option.Md5,
			BytesBuffer:       option.BytesBuffer,
		})
		if uploadResult == nil {
			return
		}
		uploadResult.Size = uint32(clearDataLen) // 设置原始数据大小
		if contentIsGzipped {
			uploadResult.Gzip = 1
		}
	}

	return uploadResult, err
}

// upload_content 构造并发送 multipart/form-data HTTP 上传请求
// 这是实际执行 HTTP 上传的底层方法
//
// 参数:
//
//	ctx: 上下文
//	fillBufferFunction: 填充缓冲区的函数(将数据写入 writer)
//	originalDataSize: 原始数据大小(用于日志和错误消息)
//	option: 上传选项
//
// 返回:
//
//	*UploadResult: 上传结果
//	error: 错误信息
func (uploader *Uploader) upload_content(ctx context.Context, fillBufferFunction func(w io.Writer) error, originalDataSize int, option *UploadOption) (*UploadResult, error) {
	var body_writer *multipart.Writer
	var reqReader *bytes.Reader
	var buf *bytebufferpool.ByteBuffer

	// 使用缓冲区池或用户提供的缓冲区,避免频繁分配内存
	if option.BytesBuffer == nil {
		buf = GetBuffer()    // 从池中获取缓冲区
		defer PutBuffer(buf) // 使用完毕后归还到池
		body_writer = multipart.NewWriter(buf)
	} else {
		option.BytesBuffer.Reset() // 重置用户提供的缓冲区
		body_writer = multipart.NewWriter(option.BytesBuffer)
	}

	// 构造 multipart form-data 的头部
	h := make(textproto.MIMEHeader)
	filename := fileNameEscaper.Replace(option.Filename) // 转义文件名中的特殊字符
	h.Set("Content-Disposition", fmt.Sprintf(`form-data; name="file"; filename="%s"`, filename))
	h.Set("Idempotency-Key", option.UploadUrl) // 幂等性键,用于防止重复上传

	// 设置 MIME 类型(如果未提供,从文件扩展名推断)
	if option.MimeType == "" {
		option.MimeType = mime.TypeByExtension(strings.ToLower(filepath.Ext(option.Filename)))
	}
	if option.MimeType != "" {
		h.Set("Content-Type", option.MimeType)
	}

	// 设置压缩编码
	if option.IsInputCompressed {
		h.Set("Content-Encoding", "gzip")
	}

	// 设置 MD5 校验值
	if option.Md5 != "" {
		h.Set("Content-MD5", option.Md5)
	}

	// 创建 multipart 的文件部分
	file_writer, cp_err := body_writer.CreatePart(h)
	if cp_err != nil {
		glog.V(0).InfolnCtx(ctx, "error creating form file", cp_err.Error())
		return nil, cp_err
	}

	// 调用填充函数,将数据写入 multipart
	if err := fillBufferFunction(file_writer); err != nil {
		glog.V(0).InfolnCtx(ctx, "error copying data", err)
		return nil, err
	}

	// 获取 Content-Type (包含 boundary)
	content_type := body_writer.FormDataContentType()
	if err := body_writer.Close(); err != nil {
		glog.V(0).InfolnCtx(ctx, "error closing body", err)
		return nil, err
	}

	// 根据缓冲区类型创建请求读取器
	if option.BytesBuffer == nil {
		reqReader = bytes.NewReader(buf.Bytes())
	} else {
		reqReader = bytes.NewReader(option.BytesBuffer.Bytes())
	}

	// 创建 HTTP POST 请求
	req, postErr := http.NewRequest(http.MethodPost, option.UploadUrl, reqReader)
	if postErr != nil {
		glog.V(1).InfofCtx(ctx, "create upload request %s: %v", option.UploadUrl, postErr)
		return nil, fmt.Errorf("create upload request %s: %v", option.UploadUrl, postErr)
	}

	// 设置请求头
	req.Header.Set("Content-Type", content_type)
	for k, v := range option.PairMap {
		req.Header.Set(k, v) // 设置自定义头
	}
	if option.Jwt != "" {
		req.Header.Set("Authorization", "BEARER "+string(option.Jwt)) // JWT 认证
	}

	// 注入请求 ID (用于追踪和日志关联)
	request_id.InjectToRequest(ctx, req)

	// print("+")
	// 执行 HTTP 请求
	resp, post_err := uploader.httpClient.Do(req)
	defer util_http.CloseResponse(resp)

	// 如果遇到连接重置错误,重试一次
	if post_err != nil {
		if strings.Contains(post_err.Error(), "connection reset by peer") ||
			strings.Contains(post_err.Error(), "use of closed network connection") {
			glog.V(1).InfofCtx(ctx, "repeat error upload request %s: %v", option.UploadUrl, postErr)
			stats.FilerHandlerCounter.WithLabelValues(stats.RepeatErrorUploadContent).Inc()
			resp, post_err = uploader.httpClient.Do(req)
			defer util_http.CloseResponse(resp)
		}
	}

	if post_err != nil {
		return nil, fmt.Errorf("upload %s %d bytes to %v: %v", option.Filename, originalDataSize, option.UploadUrl, post_err)
	}
	// print("-")

	var ret UploadResult
	etag := getEtag(resp)

	// 处理 204 No Content 响应(上传成功但无返回体)
	if resp.StatusCode == http.StatusNoContent {
		ret.ETag = etag
		return &ret, nil
	}

	// 读取响应体
	resp_body, ra_err := io.ReadAll(resp.Body)
	if ra_err != nil {
		return nil, fmt.Errorf("read response body %v: %w", option.UploadUrl, ra_err)
	}

	// 解析 JSON 响应
	unmarshal_err := json.Unmarshal(resp_body, &ret)
	if unmarshal_err != nil {
		glog.ErrorfCtx(ctx, "unmarshal %s: %v", option.UploadUrl, string(resp_body))
		return nil, fmt.Errorf("unmarshal %v: %w", option.UploadUrl, unmarshal_err)
	}

	// 检查响应中的错误信息
	if ret.Error != "" {
		return nil, fmt.Errorf("unmarshalled error %v: %v", option.UploadUrl, ret.Error)
	}

	// 设置 ETag 和 MD5
	ret.ETag = etag
	ret.ContentMd5 = resp.Header.Get("Content-MD5")
	return &ret, nil
}

// getEtag 从 HTTP 响应中提取 ETag 值
// ETag 值可能被双引号包围,此函数会移除引号
//
// 参数:
//
//	r: HTTP 响应对象
//
// 返回:
//
//	etag: 去除引号后的 ETag 字符串
func getEtag(r *http.Response) (etag string) {
	etag = r.Header.Get("ETag")
	// 移除 ETag 两端的双引号(如果存在)
	// 例如: "abc123" -> abc123
	if strings.HasPrefix(etag, "\"") && strings.HasSuffix(etag, "\"") {
		etag = etag[1 : len(etag)-1]
	}
	return
}
