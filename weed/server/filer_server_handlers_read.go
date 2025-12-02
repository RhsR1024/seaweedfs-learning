// Package weed_server 中的 filer_server_handlers_read.go 提供 GET/HEAD 等读取路径的实现
// 负责处理目录展示、Range 读取、预条件校验以及与 Volume 的协同。
package weed_server

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"mime"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/security"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// checkPreconditions 检查 HTTP 条件请求头，实现缓存验证和并发控制
// 支持的条件包括 If-Modified-Since、If-Unmodified-Since、If-Match、If-None-Match
//
// HTTP 条件请求的作用:
//   1. 缓存验证: 避免重复下载未修改的资源（If-Modified-Since, If-None-Match）
//   2. 并发控制: 防止丢失更新问题（If-Match, If-Unmodified-Since）
//   3. 带宽优化: 304 Not Modified 响应不包含实体内容
//
// 评估顺序（RFC 7232 标准）:
//   1. If-Match / If-Unmodified-Since (更新冲突检测，优先级最高)
//   2. If-None-Match / If-Modified-Since (缓存验证，优先级较低)
//
// 参数:
//   - w: HTTP 响应写入器
//   - r: HTTP 请求
//   - entry: 文件元数据
//
// 返回:
//   - bool: true 表示已向客户端写入响应（304/412），调用方应停止处理
func checkPreconditions(w http.ResponseWriter, r *http.Request, entry *filer.Entry) bool {

	// 【步骤 1: 生成 ETag】
	// ETag 是资源的唯一标识，基于内容生成
	// 用于精确的缓存验证和并发控制
	etag := filer.ETagEntry(entry)

	// RFC 7232 说明：
	// 当请求中存在多个条件头时，评估顺序很重要
	// 实践中按以下逻辑顺序处理:
	//   1. "丢失更新"预条件（If-Match/If-Unmodified-Since）要求更严格
	//   2. 缓存验证（If-None-Match/If-Modified-Since）效率更高
	//   3. ETag 通常比日期验证更准确
	// 参考: https://tools.ietf.org/html/rfc7232#section-5

	// 【步骤 2: 检查修改时间是否有效】
	if entry.Attr.Mtime.IsZero() {
		// 如果没有修改时间，无法进行基于时间的条件检查
		return false
	}

	// 【步骤 3: 设置 Last-Modified 响应头】
	// 即使条件不满足，也需要设置此头供客户端缓存使用
	w.Header().Set("Last-Modified", entry.Attr.Mtime.UTC().Format(http.TimeFormat))

	// 【优先级 1: 检查更新冲突预条件】
	// 这些条件用于防止"丢失更新"问题
	// 例如: 两个客户端同时修改同一个文件

	ifMatchETagHeader := r.Header.Get("If-Match")
	ifUnmodifiedSinceHeader := r.Header.Get("If-Unmodified-Since")

	if ifMatchETagHeader != "" {
		// 【If-Match 检查】
		// "仅当资源的 ETag 与指定值匹配时才处理请求"
		// 用于更新操作，确保修改的是最新版本
		if util.CanonicalizeETag(etag) != util.CanonicalizeETag(ifMatchETagHeader) {
			// ETag 不匹配，资源已被其他人修改
			w.WriteHeader(http.StatusPreconditionFailed) // 412
			return true
		}
	} else if ifUnmodifiedSinceHeader != "" {
		// 【If-Unmodified-Since 检查】
		// "仅当资源自指定时间后未被修改时才处理请求"
		// 功能类似 If-Match，但使用时间戳而非 ETag
		if t, parseError := time.Parse(http.TimeFormat, ifUnmodifiedSinceHeader); parseError == nil {
			if t.Before(entry.Attr.Mtime) {
				// 资源在指定时间之后被修改了
				w.WriteHeader(http.StatusPreconditionFailed) // 412
				return true
			}
		}
	}

	// 【优先级 2: 检查缓存验证条件】
	// 这些条件用于缓存验证，避免重复传输未修改的资源

	ifNoneMatchETagHeader := r.Header.Get("If-None-Match")
	ifModifiedSinceHeader := r.Header.Get("If-Modified-Since")

	if ifNoneMatchETagHeader != "" {
		// 【If-None-Match 检查】
		// "仅当资源的 ETag 与指定值不匹配时才返回内容"
		// 用于缓存验证，如果匹配说明客户端的缓存仍然有效
		if util.CanonicalizeETag(etag) == util.CanonicalizeETag(ifNoneMatchETagHeader) {
			// ETag 匹配，客户端缓存仍然有效
			SetEtag(w, etag)
			w.WriteHeader(http.StatusNotModified) // 304
			return true // 不发送响应体
		}
	} else if ifModifiedSinceHeader != "" {
		// 【If-Modified-Since 检查】
		// "仅当资源自指定时间后被修改时才返回内容"
		// 功能类似 If-None-Match，但使用时间戳
		if t, parseError := time.Parse(http.TimeFormat, ifModifiedSinceHeader); parseError == nil {
			if !t.Before(entry.Attr.Mtime) {
				// 资源未在指定时间之后修改，客户端缓存仍然有效
				SetEtag(w, etag)
				w.WriteHeader(http.StatusNotModified) // 304
				return true // 不发送响应体
			}
		}
	}

	// 【步骤 4: 所有条件都满足或不存在】
	// 返回 false 表示应该继续处理请求，返回完整响应
	return false
}

// GetOrHeadHandler 统一处理 GET 和 HEAD 请求
// 逻辑覆盖目录列表、元数据查询、Range 下载以及清单解析
//
// 功能支持:
//   1. 目录列表: 返回目录下的文件列表（HTML 或 JSON）
//   2. 文件下载: 支持完整下载和 Range 请求
//   3. 元数据查询: ?metadata=true 返回文件元数据
//   4. Manifest 解析: ?resolveManifest=true 展开 manifest chunks
//   5. S3 兼容: 支持 S3 API 的各种头部和功能
//   6. SSE 加密: 支持服务端加密文件的访问
//   7. Multipart: 支持 S3 multipart upload 的 part 访问
//
// URL 参数:
//   - metadata: 返回文件元数据而非内容
//   - resolveManifest: 展开 manifest chunks 显示实际 chunks
//
// HTTP 头支持:
//   - Range: 范围请求
//   - If-Modified-Since, If-None-Match: 缓存验证
//   - If-Match, If-Unmodified-Since: 并发控制
//   - SeaweedFSPartNumber: S3 multipart part 访问
func (fs *FilerServer) GetOrHeadHandler(w http.ResponseWriter, r *http.Request) {
	// 【步骤 1: 解析请求路径】
	ctx := r.Context()
	path := r.URL.Path

	// 判断是否为目录请求（以 / 结尾）
	isForDirectory := strings.HasSuffix(path, "/")
	if isForDirectory && len(path) > 1 {
		// 移除末尾的斜杠（除了根目录）
		path = path[:len(path)-1]
	}

	// 【步骤 2: 查找文件或目录的元数据】
	entry, err := fs.filer.FindEntry(ctx, util.FullPath(path))
	if err != nil {
		// 【特殊情况: 根目录不存在】
		// 根目录总是隐式存在的，即使没有元数据
		if path == "/" {
			fs.listDirectoryHandler(w, r)
			return
		}

		// 【处理查找错误】
		if err == filer_pb.ErrNotFound {
			// 文件或目录不存在
			glog.V(2).InfofCtx(ctx, "Not found %s: %v", path, err)
			stats.FilerHandlerCounter.WithLabelValues(stats.ErrorReadNotFound).Inc()
			w.WriteHeader(http.StatusNotFound)
		} else {
			// 内部错误（例如元数据存储不可用）
			glog.ErrorfCtx(ctx, "Internal %s: %v", path, err)
			stats.FilerHandlerCounter.WithLabelValues(stats.ErrorReadInternal).Inc()
			w.WriteHeader(http.StatusInternalServerError)
		}
		return
	}

	// 【步骤 3: 解析查询参数】
	query := r.URL.Query()

	// 【步骤 4: 处理目录请求】
	if entry.IsDirectory() {
		// 【检查 4.1: 目录列表是否被禁用】
		if fs.option.DisableDirListing {
			w.WriteHeader(http.StatusForbidden)
			return
		}

		// 【检查 4.2: 是否请求目录元数据】
		if query.Get("metadata") == "true" {
			// 返回目录的元数据（JSON 格式）
			writeJsonQuiet(w, r, http.StatusOK, entry)
			return
		}

		// 【检查 4.3: 判断目录类型】
		// 区分两种目录:
		//   1. 普通目录: 没有 Mime 类型，或者是非 S3 创建的目录
		//   2. S3 目录键: S3 API 创建的目录对象（Mime = FolderMimeType）
		if entry.Attr.Mime == "" || (entry.Attr.Mime == s3_constants.FolderMimeType && r.Header.Get(s3_constants.AmzIdentityId) == "") {
			// 【普通目录】
			// 检查是否允许暴露目录数据
			if fs.option.ExposeDirectoryData == false {
				writeJsonError(w, r, http.StatusForbidden, errors.New("directory listing is disabled"))
				return
			}
			// 返回目录列表（HTML 或 JSON）
			fs.listDirectoryHandler(w, r)
			return
		}

		// 【S3 目录键】
		// 这是 S3 API 创建的目录对象
		// 设置特殊头部通知 S3 API 这是一个目录键
		w.Header().Set(s3_constants.SeaweedFSIsDirectoryKey, "true")
	}

	// 【步骤 5: 验证路径类型一致性】
	// 如果请求路径以 / 结尾（期望目录），但 entry 不是 S3 目录键
	// 说明请求的是文件而非目录，返回 404
	if isForDirectory && entry.Attr.Mime != s3_constants.FolderMimeType {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	// 【步骤 6: 处理元数据查询请求】
	if query.Get("metadata") == "true" {
		// 【检查是否需要解析 manifest】
		if query.Get("resolveManifest") == "true" {
			// 展开 manifest chunks，显示实际的 data chunks
			// 这对于调试和理解文件结构很有用
			if entry.Chunks, _, err = filer.ResolveChunkManifest(
				ctx,
				fs.filer.MasterClient.GetLookupFileIdFunction(),
				entry.GetChunks(), 0, math.MaxInt64); err != nil {
				err = fmt.Errorf("failed to resolve chunk manifest, err: %s", err.Error())
				writeJsonError(w, r, http.StatusInternalServerError, err)
				return
			}
		}
		// 返回文件元数据（包含 chunks 信息）
		writeJsonQuiet(w, r, http.StatusOK, entry)
		return
	}

	// 【步骤 7: 检查 HTTP 预条件】
	// 处理缓存验证和并发控制头部
	// 如果条件不满足，checkPreconditions 会返回 304 或 412
	if checkPreconditions(w, r, entry) {
		return
	}

	// 【步骤 8: 处理 S3 Multipart Part 请求】
	var etag string
	if partNumber, errNum := strconv.Atoi(r.Header.Get(s3_constants.SeaweedFSPartNumber)); errNum == nil {
		// 【S3 Multipart 特性】
		// 客户端请求访问 multipart upload 的特定 part
		// 每个 part 对应一个 chunk

		// 验证 part number 是否有效
		if len(entry.Chunks) < partNumber {
			stats.FilerHandlerCounter.WithLabelValues(stats.ErrorReadChunk).Inc()
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("InvalidPart"))
			return
		}

		// 设置 part count 头部（S3 API 要求）
		w.Header().Set(s3_constants.AmzMpPartsCount, strconv.Itoa(len(entry.Chunks)))

		// 获取对应的 part chunk
		partChunk := entry.GetChunks()[partNumber-1]

		// 提取 part 的 ETag（从 base64 解码为 hex）
		md5, _ := base64.StdEncoding.DecodeString(partChunk.ETag)
		etag = hex.EncodeToString(md5)

		// 将请求转换为 Range 请求，只读取这个 part 的数据
		r.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", partChunk.Offset, uint64(partChunk.Offset)+partChunk.Size-1))
	} else {
		// 【普通请求】使用整个文件的 ETag
		etag = filer.ETagEntry(entry)
	}

	// 【步骤 9: 设置 Range 支持头部】
	// 告诉客户端支持 Range 请求
	w.Header().Set("Accept-Ranges", "bytes")

	// 【步骤 10: 设置 Content-Type 头部】
	mimeType := entry.Attr.Mime
	if mimeType == "" {
		// 如果元数据中没有 MIME 类型，尝试从文件扩展名推断
		if ext := filepath.Ext(entry.Name()); ext != "" {
			mimeType = mime.TypeByExtension(ext)
		}
	}
	if mimeType != "" {
		w.Header().Set("Content-Type", mimeType)
	} else {
		// 无法推断 MIME 类型，使用通用的二进制类型
		w.Header().Set("Content-Type", "application/octet-stream")
	}

	// 【步骤 11: 设置扩展属性头部】
	// 将文件的扩展属性转换为 HTTP 响应头
	// 例如: 用户自定义的元数据、S3 对象标签等
	for k, v := range entry.Extended {
		if !strings.HasPrefix(k, "xattr-") && !s3_constants.IsSeaweedFSInternalHeader(k) {
			// 过滤规则:
			//   1. 排除 FUSE xattr 属性（以 "xattr-" 开头）
			//   2. 排除 SeaweedFS 内部头部（不应暴露给客户端）
			w.Header().Set(k, string(v))
		}
	}

	// 【步骤 12: 设置 CORS 暴露头部】
	// Seaweed 自定义头部默认对前端 JavaScript 不可见
	// 需要通过 Access-Control-Expose-Headers 明确暴露
	seaweedHeaders := []string{}
	for header := range w.Header() {
		if strings.HasPrefix(header, "Seaweed-") {
			seaweedHeaders = append(seaweedHeaders, header)
		}
	}
	// 同时暴露 Content-Disposition 头（用于指定下载文件名）
	seaweedHeaders = append(seaweedHeaders, "Content-Disposition")
	w.Header().Set("Access-Control-Expose-Headers", strings.Join(seaweedHeaders, ","))

	// 【步骤 13: 设置 S3 对象标签计数】
	// 统计文件有多少个 S3 标签
	tagCount := 0
	for k := range entry.Extended {
		if strings.HasPrefix(k, s3_constants.AmzObjectTagging+"-") {
			tagCount++
		}
	}
	if tagCount > 0 {
		// 设置标签计数头部（S3 API 要求）
		w.Header().Set(s3_constants.AmzTagCount, strconv.Itoa(tagCount))
	}

	// 【步骤 14: 设置 SSE（服务端加密）相关头部】

	// SSE-C: 客户端提供加密密钥
	if sseIV, exists := entry.Extended[s3_constants.SeaweedFSSSEIV]; exists {
		// 将二进制 IV（初始化向量）转换为 base64 用于 HTTP 头部
		ivBase64 := base64.StdEncoding.EncodeToString(sseIV)
		w.Header().Set(s3_constants.SeaweedFSSSEIVHeader, ivBase64)
	}

	// 设置 SSE-C 算法和密钥 MD5 头部（S3 API 响应）
	if sseAlgorithm, exists := entry.Extended[s3_constants.AmzServerSideEncryptionCustomerAlgorithm]; exists {
		w.Header().Set(s3_constants.AmzServerSideEncryptionCustomerAlgorithm, string(sseAlgorithm))
	}
	if sseKeyMD5, exists := entry.Extended[s3_constants.AmzServerSideEncryptionCustomerKeyMD5]; exists {
		w.Header().Set(s3_constants.AmzServerSideEncryptionCustomerKeyMD5, string(sseKeyMD5))
	}

	// SSE-KMS: 使用密钥管理服务
	if sseKMSKey, exists := entry.Extended[s3_constants.SeaweedFSSSEKMSKey]; exists {
		// 将二进制 KMS 元数据转换为 base64
		kmsBase64 := base64.StdEncoding.EncodeToString(sseKMSKey)
		w.Header().Set(s3_constants.SeaweedFSSSEKMSKeyHeader, kmsBase64)
	}

	// SSE-S3: 使用 S3 管理的加密密钥
	if _, exists := entry.Extended[s3_constants.SeaweedFSSSES3Key]; exists {
		// 设置标准 S3 SSE-S3 响应头（不是内部 SeaweedFS 头）
		w.Header().Set(s3_constants.AmzServerSideEncryption, s3_constants.SSEAlgorithmAES256)
	}

	// 【步骤 15: 设置 ETag 头部】
	SetEtag(w, etag)

	// 【步骤 16: 调整透传头部】
	// 处理 Content-Disposition 等头部
	filename := entry.Name()
	AdjustPassthroughHeaders(w, r, filename)

	// 【步骤 17: 计算文件大小】
	// 对于 Range 处理，使用原始内容大小，而不是加密后的大小
	// entry.Size() 返回 max(chunk_sizes, file_size)，其中 chunk_sizes 包含加密开销
	// 对于 SSE 对象，我们需要原始未加密大小以进行正确的 range 验证
	totalSize := int64(entry.FileSize)

	// 【步骤 18: 处理 HEAD 请求】
	if r.Method == http.MethodHead {
		// HEAD 请求只返回头部，不返回内容
		w.Header().Set("Content-Length", strconv.FormatInt(totalSize, 10))
		return
	}

	// 【步骤 19: 处理 GET 请求 - 返回文件内容】
	// ProcessRangeRequest 会处理 Range 请求和完整内容请求
	// 提供一个回调函数来生成内容流
	ProcessRangeRequest(r, w, totalSize, mimeType, func(offset int64, size int64) (filer.DoStreamContent, error) {
		// 【情况 1: 内容直接存储在元数据中（小文件优化）】
		// 对于很小的文件（通常 < 256 字节），内容直接存储在 entry.Content 中
		// 不需要访问 Volume Server
		if offset+size <= int64(len(entry.Content)) {
			return func(writer io.Writer) error {
				// 直接从内存写入请求的范围
				_, err := writer.Write(entry.Content[offset : offset+size])
				if err != nil {
					stats.FilerHandlerCounter.WithLabelValues(stats.ErrorWriteEntry).Inc()
					glog.ErrorfCtx(ctx, "failed to write entry content: %v", err)
				}
				return err
			}, nil
		}

		// 【情况 2: 内容存储在 chunks 中（普通文件）】
		chunks := entry.GetChunks()

		// 【情况 2.1: 远程存储对象（冷数据）】
		// 如果文件标记为 RemoteOnly，说明数据在远程对象存储（S3/GCS 等）
		// 需要先缓存到本地 SeaweedFS 集群
		if entry.IsInRemoteOnly() {
			dir, name := entry.FullPath.DirAndName()

			// 调用缓存接口，将远程对象下载到本地
			if resp, err := fs.CacheRemoteObjectToLocalCluster(ctx, &filer_pb.CacheRemoteObjectToLocalClusterRequest{
				Directory: dir,
				Name:      name,
			}); err != nil {
				stats.FilerHandlerCounter.WithLabelValues(stats.ErrorReadCache).Inc()
				glog.ErrorfCtx(ctx, "CacheRemoteObjectToLocalCluster %s: %v", entry.FullPath, err)
				return nil, fmt.Errorf("cache %s: %v", entry.FullPath, err)
			} else {
				// 使用缓存后的 chunks
				chunks = resp.Entry.GetChunks()
			}
		}

		// 【重要】创建分离的上下文用于流式传输
		// 原因:
		//   1. 客户端断开连接不应中止正在进行的 volume server 操作
		//   2. 保留请求范围的值（如追踪 ID）
		//   3. 匹配 S3 API 行为
		// 上面的元数据操作使用原始 ctx，流式传输使用 streamCtx
		streamCtx, streamCancel := context.WithCancel(context.WithoutCancel(ctx))

		// 【准备流式内容读取】
		// PrepareStreamContentWithThrottler 会:
		//   1. 找到覆盖 [offset, offset+size) 范围的所有 chunks
		//   2. 创建一个函数，从 Volume Server 流式读取这些 chunks
		//   3. 应用下载速率限制（如果配置了 DownloadMaxBytesPs）
		streamFn, err := filer.PrepareStreamContentWithThrottler(
			streamCtx,
			fs.filer.MasterClient,
			fs.maybeGetVolumeReadJwtAuthorizationToken, // JWT 令牌生成函数
			chunks,
			offset,
			size,
			fs.option.DownloadMaxBytesPs, // 下载速率限制（字节/秒）
		)
		if err != nil {
			streamCancel()
			stats.FilerHandlerCounter.WithLabelValues(stats.ErrorReadStream).Inc()
			glog.ErrorfCtx(ctx, "failed to prepare stream content %s: %v", r.URL, err)
			return nil, err
		}

		// 返回流式内容函数
		return func(writer io.Writer) error {
			// 确保在流式传输完成后取消上下文
			defer streamCancel()

			// 执行实际的流式传输
			err := streamFn(writer)
			if err != nil {
				stats.FilerHandlerCounter.WithLabelValues(stats.ErrorReadStream).Inc()
				glog.ErrorfCtx(ctx, "failed to stream content %s: %v", r.URL, err)
			}
			return err
		}, nil
	})
}

// maybeGetVolumeReadJwtAuthorizationToken 生成只读 Volume 访问的 JWT 令牌
// 用于流式读取底层 chunk 时附带到 HTTP 请求头中
//
// 工作方式:
//   - 使用只读签名密钥（ReadSigningKey）
//   - 使用只读过期时间（ReadExpiresAfterSec）
//   - 为每个 fileId 生成独立的令牌
//
// 参数:
//   - fileId: 文件 ID（格式: volumeId,fileKey[_cookie]）
//
// 返回:
//   - string: Base64 编码的 JWT 令牌
func (fs *FilerServer) maybeGetVolumeReadJwtAuthorizationToken(fileId string) string {
	return string(security.GenJwtForVolumeServer(fs.volumeGuard.ReadSigningKey, fs.volumeGuard.ReadExpiresAfterSec, fileId))
}
