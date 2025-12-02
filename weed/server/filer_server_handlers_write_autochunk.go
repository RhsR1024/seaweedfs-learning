// Package weed_server 中的 filer_server_handlers_write_autochunk.go 实现自动分片上传逻辑
// 涵盖多种 HTTP 动作（POST/PUT）、WORM 权限校验以及元数据持久化。
package weed_server

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/constants"
)

// autoChunk 自动决定是否分片上传，并分发到相应的处理函数
//
// 【功能说明】
//   - 根据文件大小和配置决定 chunk 大小
//   - 区分 POST（multipart）和 PUT（单文件）请求
//   - 处理目录创建（POST 请求且路径以 / 结尾）
//   - 统一错误处理和 MD5 响应
//
// 【参数说明】
//   - ctx: 请求上下文
//   - w: HTTP 响应写入器
//   - r: HTTP 请求对象
//   - contentLength: 请求内容长度（从 Content-Length 头部获取，可为 -1）
//   - so: 存储策略选项
//
// 【Chunk 大小决策】
//   - 优先使用 URL 查询参数 ?maxMB=<数值>
//   - 其次使用 Filer 启动参数 -maxMB
//   - 默认值通常为 32MB
//   - chunkSize = maxMB * 1024 * 1024 字节
//
// 【请求类型判断】
//   1. POST + 无 Content-Type + 路径以 / 结尾 → 创建目录
//   2. POST + 有 Content-Type → multipart 表单上传
//   3. PUT → 单文件上传
//
// 【错误码映射】
//   - 403 Forbidden: WORM 保护，操作不允许
//   - 409 Conflict: 路径冲突（目录覆盖文件等）
//   - 400 Bad Request: MD5 校验失败
//   - 499 Client Closed Request: 客户端取消上传
//   - 500 Internal Server Error: 其他错误
//
// 【成功响应】
//   - HTTP 201 Created
//   - Content-MD5 响应头（Base64 编码）
//   - JSON 响应体：{name, size}
func (fs *FilerServer) autoChunk(ctx context.Context, w http.ResponseWriter, r *http.Request, contentLength int64, so *operation.StorageOption) {

	// 【步骤 1：确定 Chunk 大小】
	// Chunk 大小决定了大文件如何被拆分
	// 更大的 chunk：减少元数据数量，但增加单次上传失败的风险
	// 更小的 chunk：增加元数据数量，但提升并发上传效率
	query := r.URL.Query()

	// 尝试从 URL 查询参数获取 maxMB
	parsedMaxMB, _ := strconv.ParseInt(query.Get("maxMB"), 10, 32)
	maxMB := int32(parsedMaxMB)
	// 如果未指定或为 0，使用 Filer 全局配置
	if maxMB <= 0 && fs.option.MaxMB > 0 {
		maxMB = int32(fs.option.MaxMB)
	}

	// 计算实际的 chunk 字节数
	// 示例：maxMB=32 → chunkSize=33554432 字节（32MB）
	chunkSize := 1024 * 1024 * maxMB

	var reply *FilerPostResult
	var err error
	var md5bytes []byte

	// 【步骤 2：根据 HTTP 方法分发】
	if r.Method == http.MethodPost {
		// POST 请求有两种可能：
		// 1. 创建目录（无 Content-Type 且路径以 / 结尾）
		// 2. multipart 表单上传文件
		if r.Header.Get("Content-Type") == "" && strings.HasSuffix(r.URL.Path, "/") {
			// 创建目录
			reply, err = fs.mkdir(ctx, w, r, so)
		} else {
			// multipart 表单上传
			reply, md5bytes, err = fs.doPostAutoChunk(ctx, w, r, chunkSize, contentLength, so)
		}
	} else {
		// PUT 请求：单文件上传
		reply, md5bytes, err = fs.doPutAutoChunk(ctx, w, r, chunkSize, contentLength, so)
	}

	// 【步骤 3：错误处理】
	// 根据错误类型返回不同的 HTTP 状态码
	if err != nil {
		errStr := err.Error()
		switch {
		case errStr == constants.ErrMsgOperationNotPermitted:
			// WORM 保护，拒绝操作
			writeJsonError(w, r, http.StatusForbidden, err)
		case strings.HasPrefix(errStr, "read input:") || errStr == io.ErrUnexpectedEOF.Error():
			// 客户端中断上传（关闭连接）
			// HTTP 499: Client Closed Request（非标准状态码）
			writeJsonError(w, r, util.HttpStatusCancelled, err)
		case strings.HasSuffix(errStr, "is a file") || strings.HasSuffix(errStr, "already exists"):
			// 路径冲突（例如：用文件覆盖目录，或目录已存在）
			writeJsonError(w, r, http.StatusConflict, err)
		case errStr == constants.ErrMsgBadDigest:
			// MD5 校验失败
			writeJsonError(w, r, http.StatusBadRequest, err)
		default:
			// 其他未知错误
			writeJsonError(w, r, http.StatusInternalServerError, err)
		}
	} else if reply != nil {
		// 【步骤 4：成功响应】
		// 设置 Content-MD5 响应头（S3 兼容）
		if len(md5bytes) > 0 {
			md5InBase64 := util.Base64Encode(md5bytes)
			w.Header().Set("Content-MD5", md5InBase64)
		}
		// 返回 201 Created 和 JSON 响应体
		writeJsonQuiet(w, r, http.StatusCreated, reply)
	}
}

// doPostAutoChunk 处理 multipart/form-data 表单上传
//
// 【功能说明】
//   - 解析 multipart 表单数据
//   - 提取第一个文件部分（part）进行上传
//   - 支持小文件内联存储（SaveInside）
//   - 支持大文件自动分片
//   - 验证 MD5 校验和
//
// 【参数说明】
//   - ctx: 请求上下文
//   - w: HTTP 响应写入器
//   - r: HTTP 请求对象（multipart/form-data）
//   - chunkSize: 单个 chunk 的最大字节数
//   - contentLength: 整个请求的内容长度
//   - so: 存储策略选项
//
// 【返回值】
//   - filerResult: 上传结果（文件名、大小、错误）
//   - md5bytes: 文件内容的 MD5 哈希值
//   - replyerr: 错误信息
//
// 【处理流程】
//   1. 解析 multipart 表单，获取第一个文件部分
//   2. 提取文件名和 Content-Type
//   3. 检查权限（WORM 保护等）
//   4. 判断是否内联存储（SaveInside）
//   5. 上传文件内容到 Volume（分片或整体）
//   6. 验证 MD5 校验和
//   7. 保存元数据到 Filer Store
//
// 【小文件内联存储】
//   - 当 SaveInside=true 时，文件内容直接存储到元数据中
//   - 不分配 Volume 空间，不生成 chunk
//   - 适用于极小文件（如配置文件）
//
// 【MD5 校验】
//   - 计算实际上传内容的 MD5
//   - 与请求头 Content-MD5 比对
//   - 支持 Base64 和十六进制两种格式
//   - 校验失败则删除已上传的 chunk
func (fs *FilerServer) doPostAutoChunk(ctx context.Context, w http.ResponseWriter, r *http.Request, chunkSize int32, contentLength int64, so *operation.StorageOption) (filerResult *FilerPostResult, md5bytes []byte, replyerr error) {
	// 【步骤 1：创建 multipart 读取器】
	multipartReader, multipartReaderErr := r.MultipartReader()
	if multipartReaderErr != nil {
		return nil, nil, multipartReaderErr
	}

	// 【步骤 2：读取第一个文件部分】
	// multipart 表单可能包含多个部分（文件、字段等）
	// 这里只处理第一个部分作为上传文件
	part1, part1Err := multipartReader.NextPart()
	if part1Err != nil {
		return nil, nil, part1Err
	}

	// 【步骤 3：提取文件名】
	// FileName() 返回 Content-Disposition 中的 filename
	// path.Base() 提取文件名部分，去除路径
	fileName := part1.FileName()
	if fileName != "" {
		fileName = path.Base(fileName)
	}

	// 【步骤 4：提取 Content-Type】
	// application/octet-stream 是通用二进制类型，重置为空让系统自动检测
	contentType := part1.Header.Get("Content-Type")
	if contentType == "application/octet-stream" {
		contentType = ""
	}

	// 【步骤 5：权限检查】
	// 检查是否允许写入（WORM 保护等）
	if err := fs.checkPermissions(ctx, r, fileName); err != nil {
		return nil, nil, err
	}

	// 【步骤 6：小文件内联存储】
	// 如果启用 SaveInside，文件内容直接存储到元数据中
	// 优点：减少 Volume 查询，适合极小文件
	// 缺点：增加元数据存储大小
	if so.SaveInside {
		buf := bufPool.Get().(*bytes.Buffer)
		buf.Reset()
		// 读取整个文件内容到内存
		buf.ReadFrom(part1)
		// 保存元数据，content 参数包含文件内容
		filerResult, replyerr = fs.saveMetaData(ctx, r, fileName, contentType, so, nil, nil, 0, buf.Bytes())
		bufPool.Put(buf)
		return
	}

	// 【步骤 7：常规上传（可能分片）】
	// uploadRequestToChunks 会：
	//   1. 读取文件内容
	//   2. 根据 chunkSize 决定是否分片
	//   3. 上传到 Volume Server
	//   4. 返回 chunk 列表和 MD5 哈希
	fileChunks, md5Hash, chunkOffset, err, smallContent := fs.uploadRequestToChunks(ctx, w, r, part1, chunkSize, fileName, contentType, contentLength, so)
	if err != nil {
		return nil, nil, err
	}

	// 【步骤 8：计算 MD5 校验和】
	md5bytes = md5Hash.Sum(nil)
	headerMd5 := r.Header.Get("Content-Md5")

	// 【步骤 9：验证 MD5】
	// 如果请求头包含 Content-MD5，验证是否匹配
	// 支持两种格式：
	//   1. Base64 编码（标准格式）
	//   2. 十六进制字符串（兼容格式）
	if headerMd5 != "" && !(util.Base64Encode(md5bytes) == headerMd5 || fmt.Sprintf("%x", md5bytes) == headerMd5) {
		// MD5 不匹配，删除已上传的 chunk
		fs.filer.DeleteUncommittedChunks(ctx, fileChunks)
		return nil, nil, errors.New(constants.ErrMsgBadDigest)
	}

	// 【步骤 10：保存元数据】
	// 将文件信息（名称、大小、chunk 列表等）保存到 Filer Store
	filerResult, replyerr = fs.saveMetaData(ctx, r, fileName, contentType, so, md5bytes, fileChunks, chunkOffset, smallContent)
	if replyerr != nil {
		// 元数据保存失败，删除已上传的 chunk（回滚）
		fs.filer.DeleteUncommittedChunks(ctx, fileChunks)
	}

	return
}

// doPutAutoChunk 处理单个对象的 PUT 上传
//
// 【功能说明】
//   - 处理 HTTP PUT 请求上传文件
//   - 直接从请求 Body 读取内容（不使用 multipart）
//   - 支持自动分片、追加模式、偏移写入
//   - 验证 MD5 校验和
//
// 【参数说明】
//   - ctx: 请求上下文
//   - w: HTTP 响应写入器
//   - r: HTTP 请求对象（PUT 请求）
//   - chunkSize: 单个 chunk 的最大字节数
//   - contentLength: 请求内容长度
//   - so: 存储策略选项
//
// 【返回值】
//   - filerResult: 上传结果（文件名、大小、错误）
//   - md5bytes: 文件内容的 MD5 哈希值
//   - replyerr: 错误信息
//
// 【PUT vs POST】
//   - PUT: 请求 Body 直接是文件内容（application/octet-stream）
//     curl -X PUT http://localhost:8888/path/file.txt --data-binary @file.txt
//   - POST: 使用 multipart/form-data 表单上传
//     curl -X POST http://localhost:8888/path/ -F "file=@file.txt"
//
// 【处理流程】
//   1. 提取文件名和 Content-Type
//   2. 权限检查（WORM 保护等）
//   3. 上传文件内容到 Volume（分片或整体）
//   4. 验证 MD5 校验和
//   5. 保存元数据到 Filer Store
//
// 【支持的 URL 参数】
//   - op=append: 追加模式，在文件末尾追加内容
//   - offset=N: 偏移写入，从指定位置开始写入
//   - mode=0644: 文件权限（八进制）
//   - maxMB=32: 单个 chunk 大小（MB）
func (fs *FilerServer) doPutAutoChunk(ctx context.Context, w http.ResponseWriter, r *http.Request, chunkSize int32, contentLength int64, so *operation.StorageOption) (filerResult *FilerPostResult, md5bytes []byte, replyerr error) {

	// 【步骤 1：提取文件名】
	// path.Base 提取路径的最后一部分作为文件名
	// 示例：/path/to/file.txt → file.txt
	fileName := path.Base(r.URL.Path)

	// 【步骤 2：提取 Content-Type】
	// application/octet-stream 是通用二进制类型，重置为空让系统自动检测
	contentType := r.Header.Get("Content-Type")
	if contentType == "application/octet-stream" {
		contentType = ""
	}

	// 【步骤 3：权限检查】
	// 检查是否允许写入（WORM 保护等）
	if err := fs.checkPermissions(ctx, r, fileName); err != nil {
		return nil, nil, err
	}

	// 【步骤 4：上传文件内容】
	// uploadRequestToChunks 会：
	//   1. 读取请求 Body（r.Body）
	//   2. 根据 chunkSize 决定是否分片
	//   3. 上传到 Volume Server
	//   4. 返回 chunk 列表和 MD5 哈希
	// 参数说明：
	//   - r.Body: 请求 Body 作为 io.Reader
	//   - chunkSize: 单个 chunk 的最大字节数
	//   - fileName: 文件名（用于日志和调试）
	//   - contentType: MIME 类型（用于响应头）
	//   - contentLength: 请求内容长度（用于优化）
	fileChunks, md5Hash, chunkOffset, err, smallContent := fs.uploadRequestToChunks(ctx, w, r, r.Body, chunkSize, fileName, contentType, contentLength, so)

	if err != nil {
		return nil, nil, err
	}

	// 【步骤 5：计算 MD5 校验和】
	md5bytes = md5Hash.Sum(nil)

	// 【步骤 6：验证 MD5】
	// 如果请求头包含 Content-MD5，验证是否匹配
	// 支持两种格式：
	//   1. Base64 编码（标准格式）：1B2M2Y8AsgTpgAmY7PhCfg==
	//   2. 十六进制字符串（兼容格式）：d41d8cd98f00b204e9800998ecf8427e
	headerMd5 := r.Header.Get("Content-Md5")
	if headerMd5 != "" && !(util.Base64Encode(md5bytes) == headerMd5 || fmt.Sprintf("%x", md5bytes) == headerMd5) {
		// MD5 不匹配，删除已上传的 chunk（回滚）
		fs.filer.DeleteUncommittedChunks(ctx, fileChunks)
		return nil, nil, errors.New(constants.ErrMsgBadDigest)
	}

	// 【步骤 7：保存元数据】
	// 将文件信息（名称、大小、chunk 列表等）保存到 Filer Store
	filerResult, replyerr = fs.saveMetaData(ctx, r, fileName, contentType, so, md5bytes, fileChunks, chunkOffset, smallContent)
	if replyerr != nil {
		// 元数据保存失败，删除已上传的 chunk（回滚）
		fs.filer.DeleteUncommittedChunks(ctx, fileChunks)
	}

	return
}

// isAppend 判断请求是否需采用追加模式
//
// 【功能说明】
//   - 检查请求是否要求追加写入（而非覆盖）
//   - 追加模式会保留原文件内容，在末尾追加新数据
//
// 【判定依据】
//   - URL 查询参数：?op=append
//   - 示例：PUT http://localhost:8888/file.txt?op=append
//
// 【追加模式行为】
//   - 保留原有 chunks
//   - 新 chunks 的 offset 加上原文件大小
//   - 文件大小累加
//   - 不支持追加到小文件（Content 不为空）
//
// 【使用场景】
//   - 日志文件追加写入
//   - 增量数据追加
//   - 断点续传
func isAppend(r *http.Request) bool {
	return r.URL.Query().Get("op") == "append"
}

// skipCheckParentDirEntry 判断是否跳过父目录存在性检查
//
// 【功能说明】
//   - 决定保存文件时是否检查父目录是否存在
//   - 跳过检查可以提升性能，但可能导致孤立文件
//
// 【判定依据】
//   - URL 查询参数：?skipCheckParentDir=true
//   - 示例：PUT http://localhost:8888/a/b/c/file.txt?skipCheckParentDir=true
//
// 【使用场景】
//   - S3 协议：对象存储不要求父目录存在
//   - 批量上传：预先知道目录结构，跳过检查提升性能
//   - 临时文件：不关心目录完整性
//
// 【注意事项】
//   - 跳过检查可能导致 /a/b/c/file.txt 存在但 /a/b/c 目录不存在
//   - Filer 会在需要时自动创建父目录
//   - S3 客户端通常设置此参数为 true
func skipCheckParentDirEntry(r *http.Request) bool {
	return r.URL.Query().Get("skipCheckParentDir") == "true"
}

// isS3Request 用于快速判定当前请求是否来自 S3 协议栈
//
// 【功能说明】
//   - 检测请求是否来自 S3 API（而非原生 Filer API）
//   - 用于特殊处理 S3 协议的兼容性逻辑
//
// 【判定依据】
//   - 请求头包含 X-Seaweed-Auth-Type（AWS v4 签名）
//   - 或请求头包含 X-Amz-Date（AWS 时间戳）
//
// 【S3 特殊处理】
//   - 允许父目录即对象（object key 即目录名）
//   - 跳过父目录存在性检查
//   - 兼容 S3 元数据格式
//
// 【示例】
//   S3 请求头：
//     X-Seaweed-Auth-Type: AWS4-HMAC-SHA256
//     X-Amz-Date: 20231201T120000Z
//     Authorization: AWS4-HMAC-SHA256 Credential=...
func isS3Request(r *http.Request) bool {
	return r.Header.Get(s3_constants.AmzAuthType) != "" || r.Header.Get("X-Amz-Date") != ""
}

// checkPermissions 校验访问者是否具有写入/修改指定路径的权限
//
// 【功能说明】
//   - 检查是否允许写入或修改文件
//   - 主要检查 WORM（Write Once Read Many）保护
//   - 未来可扩展其他权限检查（ACL、用户权限等）
//
// 【参数说明】
//   - ctx: 请求上下文
//   - r: HTTP 请求对象（用于构造完整路径）
//   - fileName: 文件名（不包含路径）
//
// 【返回值】
//   - error: 权限错误（WORM 保护等），nil 表示允许操作
//
// 【检查项目】
//   1. WORM 保护：文件是否处于 WORM 保护期
//   2. 未来可扩展：用户权限、ACL、配额等
//
// 【错误类型】
//   - constants.ErrMsgOperationNotPermitted: WORM 保护，禁止操作
func (fs *FilerServer) checkPermissions(ctx context.Context, r *http.Request, fileName string) error {
	// 【步骤 1：构造完整路径】
	// fixFilePath 会处理：
	//   1. 路径以 / 结尾的情况（自动补充文件名）
	//   2. 目录路径的正确拼接
	fullPath := fs.fixFilePath(ctx, r, fileName)

	// 【步骤 2：检查 WORM 保护】
	// wormEnforcedForEntry 会判断文件是否处于 WORM 保护期
	enforced, err := fs.wormEnforcedForEntry(ctx, fullPath)
	if err != nil {
		return err
	} else if enforced {
		// WORM 文件禁止修改或删除
		// 返回标准错误消息，上层会转换为 HTTP 403 Forbidden
		return errors.New(constants.ErrMsgOperationNotPermitted)
	}

	// 【步骤 3：未来扩展点】
	// 这里可以添加其他权限检查：
	//   - 用户 ACL：检查用户是否有权限访问此路径
	//   - 配额检查：检查用户是否超过存储配额
	//   - 路径黑名单：禁止访问某些特殊路径

	return nil
}

// wormEnforcedForEntry 判断路径是否启用了 WORM（Write Once Read Many）策略
//
// 【功能说明】
//   - WORM 是一种数据保护策略，文件一旦写入后禁止修改或删除
//   - 常用于合规性场景（如金融、医疗数据保留）
//   - 支持永久 WORM 和定时 WORM（保留期过后可删除）
//
// 【参数说明】
//   - ctx: 请求上下文
//   - fullPath: 文件完整路径
//
// 【返回值】
//   - enforced: true 表示 WORM 保护生效，禁止修改/删除
//   - err: 错误信息（查询失败等）
//
// 【WORM 配置】
//   在 filer.toml 中配置：
//   [[filer.storage]]
//     location_prefix = "/archive/"
//     worm = true
//     worm_retention_time_seconds = 2592000  # 30 天，0 表示永久
//
// 【WORM 生效逻辑】
//   1. 文件不存在：WORM 不生效（允许新建）
//   2. 文件存在但 WORMEnforcedAtTsNs=0：WORM 未启用（允许修改）
//   3. WORMEnforcedAtTsNs>0 且保留期=0：永久 WORM（禁止修改）
//   4. WORMEnforcedAtTsNs>0 且保留期>0：定时 WORM，过期后允许修改
//
// 【实现细节】
//   - WORMEnforcedAtTsNs: 文件首次写入时的纳秒时间戳
//   - WormRetentionTimeSeconds: 保留期（秒），0 表示永久
//   - 保留期计算：当前时间 - 写入时间 >= 保留期
func (fs *FilerServer) wormEnforcedForEntry(ctx context.Context, fullPath string) (bool, error) {
	// 【步骤 1：匹配存储规则】
	// 根据文件路径匹配 filer.toml 中的存储规则
	// 返回的 rule 包含：
	//   - Worm: 是否启用 WORM
	//   - WormRetentionTimeSeconds: 保留期（0 表示永久）
	rule := fs.filer.FilerConf.MatchStorageRule(fullPath)
	if !rule.Worm {
		// 此路径未启用 WORM，允许修改
		return false, nil
	}

	// 【步骤 2：查找文件元数据】
	// 检查文件是否存在，以及 WORM 时间戳
	entry, err := fs.filer.FindEntry(ctx, util.FullPath(fullPath))
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			// 文件不存在，允许新建
			return false, nil
		}

		// 查询失败，返回错误
		return false, err
	}

	// 【步骤 3：检查 WORM 是否已启用】
	// WORMEnforcedAtTsNs=0 表示 WORM 尚未生效
	// 这种情况下，文件首次写入后会设置此时间戳
	if entry.WORMEnforcedAtTsNs == 0 {
		return false, nil
	}

	// 【步骤 4：检查是否永久 WORM】
	// WormRetentionTimeSeconds=0 表示永久保留，禁止删除
	if rule.WormRetentionTimeSeconds == 0 {
		return true, nil
	}

	// 【步骤 5：计算保留期是否过期】
	// 将纳秒时间戳转换为 time.Time
	enforcedAt := time.Unix(0, entry.WORMEnforcedAtTsNs)

	// 计算已经过去的时间（秒）
	// 如果已过保留期，允许删除
	if time.Now().Sub(enforcedAt).Seconds() >= float64(rule.WormRetentionTimeSeconds) {
		return false, nil
	}

	// 【步骤 6：WORM 保护生效】
	// 文件处于保留期内，禁止修改或删除
	return true, nil
}

// fixFilePath 统一处理路径规范化，包括去重 //、处理 ..、补充 multipart 中的覆盖路径等
//
// 【功能说明】
//   - 规范化文件路径，确保目录和文件名正确拼接
//   - 处理各种路径格式（以 / 结尾、不以 / 结尾）
//   - 智能判断路径是目录还是文件
//
// 【参数说明】
//   - ctx: 请求上下文
//   - r: HTTP 请求对象（包含 URL 路径）
//   - fileName: 文件名（可能为空）
//
// 【返回值】
//   - fullPath: 规范化后的完整路径
//
// 【处理逻辑】
//   1. URL 路径以 / 结尾（明确是目录）：
//      - /bucket/ + file.txt → /bucket/file.txt
//      - /bucket/ + "" → /bucket/
//   2. URL 路径不以 / 结尾（可能是目录或文件）：
//      - /bucket (目录) + file.txt → /bucket/file.txt
//      - /bucket/file.txt (文件) + "" → /bucket/file.txt
//
// 【智能判断】
//   - 如果 URL 路径不以 / 结尾且 fileName 不为空
//   - 查询该路径是否为目录
//   - 如果是目录，拼接文件名；否则使用原路径
//
// 【使用场景】
//   - POST 上传：URL=/bucket/，fileName=file.txt → /bucket/file.txt
//   - PUT 上传：URL=/bucket/file.txt，fileName="" → /bucket/file.txt
//   - 目录创建：URL=/bucket/，fileName="" → /bucket/
func (fs *FilerServer) fixFilePath(ctx context.Context, r *http.Request, fileName string) string {
	// 【步骤 1：获取请求路径】
	fullPath := r.URL.Path

	// 【步骤 2：处理以 / 结尾的路径（明确是目录）】
	if strings.HasSuffix(fullPath, "/") {
		if fileName != "" {
			// 目录路径拼接文件名
			// 示例：/bucket/ + file.txt → /bucket/file.txt
			fullPath += fileName
		}
		// 如果 fileName 为空，保持目录路径不变
	} else {
		// 【步骤 3：处理不以 / 结尾的路径（可能是目录或文件）】
		if fileName != "" {
			// 尝试查询该路径是否为目录
			possibleDirEntry, findDirErr := fs.filer.FindEntry(ctx, util.FullPath(fullPath))
			if findDirErr == nil {
				if possibleDirEntry.IsDirectory() {
					// 确认是目录，拼接文件名
					// 示例：/bucket (目录) + file.txt → /bucket/file.txt
					fullPath += "/" + fileName
				}
				// 如果不是目录，使用原路径（覆盖场景）
			}
			// 如果查询失败，假设是新路径，使用原路径
		}
	}

	return fullPath
}

// saveMetaData 构建并保存文件元数据到 Filer Store
//
// 【功能说明】
//   - 构造 Filer Entry 对象（文件元数据）
//   - 保存文件属性（权限、时间戳、MIME 类型等）
//   - 保存 chunk 列表（文件数据位置）
//   - 处理 S3 扩展元数据（AMZ headers、SSE 加密等）
//   - 支持追加模式（append）和偏移写入
//
// 【参数说明】
//   - ctx: 请求上下文
//   - r: HTTP 请求对象（用于提取请求头）
//   - fileName: 文件名（不包含路径）
//   - contentType: MIME 类型（如 image/jpeg、text/plain）
//   - so: 存储策略选项
//   - md5bytes: 文件内容的 MD5 哈希值
//   - fileChunks: chunk 列表（文件数据在 Volume 上的位置）
//   - chunkOffset: 文件总大小（所有 chunk 的累计大小）
//   - content: 小文件内联内容（SaveInside 模式）
//
// 【返回值】
//   - filerResult: 保存结果（文件名、大小、错误）
//   - replyerr: 错误信息
//
// 【Entry 结构】
//   - FullPath: 文件完整路径
//   - Attr: 文件属性（权限、时间、所有者、大小等）
//   - Chunks: chunk 列表（大文件分片）
//   - Content: 内联内容（小文件）
//   - Extended: 扩展元数据（S3 headers、SSE 加密信息等）
//
// 【追加模式】
//   - 如果 URL 包含 ?op=append，则保留旧 chunks
//   - 新 chunks 的 offset 会加上旧文件大小
//   - 不支持追加到小文件（Content 不为空）
//
// 【偏移写入】
//   - 如果第一个 chunk 的 offset > 0，则保留旧 chunks
//   - 用于支持分段上传和断点续传
//
// 【Chunk 优化】
//   - maybeMergeChunks: 合并小 chunks 减少元数据
//   - MaybeManifestize: 当 chunks 过多时创建 manifest chunk
func (fs *FilerServer) saveMetaData(ctx context.Context, r *http.Request, fileName string, contentType string, so *operation.StorageOption, md5bytes []byte, fileChunks []*filer_pb.FileChunk, chunkOffset int64, content []byte) (filerResult *FilerPostResult, replyerr error) {

	// 【步骤 1：解析文件权限】
	// 从 URL 查询参数获取 mode（Unix 文件权限）
	// 格式：八进制字符串，如 "0755"、"0644"
	modeStr := r.URL.Query().Get("mode")
	if modeStr == "" {
		// 默认权限：0660（所有者和组可读写，其他人无权限）
		modeStr = "0660"
	}
	mode, err := strconv.ParseUint(modeStr, 8, 32)
	if err != nil {
		glog.ErrorfCtx(ctx, "Invalid mode format: %s, use 0660 by default", modeStr)
		mode = 0660
	}

	// 【步骤 2：规范化文件路径】
	// fixFilePath 会处理：
	//   1. 路径以 / 结尾的情况（自动补充文件名）
	//   2. 目录路径的正确拼接
	path := fs.fixFilePath(ctx, r, fileName)

	var entry *filer.Entry
	var newChunks []*filer_pb.FileChunk
	var mergedChunks []*filer_pb.FileChunk

	isAppend := isAppend(r)
	isOffsetWrite := len(fileChunks) > 0 && fileChunks[0].Offset > 0
	// 追加模式需要保留旧 chunk 并在末尾拼接
	if isAppend || isOffsetWrite {
		existingEntry, findErr := fs.filer.FindEntry(ctx, util.FullPath(path))
		if findErr != nil && findErr != filer_pb.ErrNotFound {
			glog.V(0).InfofCtx(ctx, "failing to find %s: %v", path, findErr)
		}
		entry = existingEntry
	}
	if entry != nil {
		entry.Mtime = time.Now()
		entry.Md5 = nil
		// 修正 chunk offset，保证连续
		if isAppend {
			for _, chunk := range fileChunks {
				chunk.Offset += int64(entry.FileSize)
			}
			entry.FileSize += uint64(chunkOffset)
		}
		newChunks = append(entry.GetChunks(), fileChunks...)

		// TODO: 可考虑在这里做更多冲突检测
		if len(entry.Content) > 0 {
			replyerr = fmt.Errorf("append to small file is not supported yet")
			return
		}

	} else {
		glog.V(4).InfolnCtx(ctx, "saving", path)
		newChunks = fileChunks
		entry = &filer.Entry{
			FullPath: util.FullPath(path),
			Attr: filer.Attr{
				Mtime:    time.Now(),
				Crtime:   time.Now(),
				Mode:     os.FileMode(mode),
				Uid:      OS_UID,
				Gid:      OS_GID,
				TtlSec:   so.TtlSeconds,
				Mime:     contentType,
				Md5:      md5bytes,
				FileSize: uint64(chunkOffset),
			},
			Content: content,
		}
	}

	// 若用户开启自动合并，小文件可拼成大 chunk
	mergedChunks, replyerr = fs.maybeMergeChunks(ctx, so, newChunks)
	if replyerr != nil {
		glog.V(0).InfofCtx(ctx, "merge chunks %s: %v", r.RequestURI, replyerr)
		mergedChunks = newChunks
	}

	// 根据策略压缩 Entry 的 chunk 列表
	mergedChunks, replyerr = filer.MaybeManifestize(fs.saveAsChunk(ctx, so), mergedChunks)
	if replyerr != nil {
		glog.V(0).InfofCtx(ctx, "manifestize %s: %v", r.RequestURI, replyerr)
		return
	}
	entry.Chunks = mergedChunks
	if isOffsetWrite {
		entry.Md5 = nil
		entry.FileSize = entry.Size()
	}

	filerResult = &FilerPostResult{
		Name: fileName,
		Size: int64(entry.FileSize),
	}

	entry.Extended = SaveAmzMetaData(r, entry.Extended, false)

	for k, v := range r.Header {
		if len(v) > 0 && len(v[0]) > 0 {
			if strings.HasPrefix(k, needle.PairNamePrefix) || k == "Cache-Control" || k == "Expires" || k == "Content-Disposition" {
				entry.Extended[k] = []byte(v[0])
				// 记录版本号，便于调试
				if k == "Seaweed-X-Amz-Version-Id" {
					glog.V(0).Infof("filer: storing version ID header in Extended: %s=%s for path=%s", k, v[0], path)
				}
			}
			if k == "Response-Content-Disposition" {
				entry.Extended["Content-Disposition"] = []byte(v[0])
			}
		}
	}

	// 解析 S3 发送的 SSE 头部，并写入扩展元数据
	if sseIVHeader := r.Header.Get(s3_constants.SeaweedFSSSEIVHeader); sseIVHeader != "" {
		// 解码 IV 后写入 metadata
		if ivData, err := base64.StdEncoding.DecodeString(sseIVHeader); err == nil {
			entry.Extended[s3_constants.SeaweedFSSSEIV] = ivData
			glog.V(4).Infof("Stored SSE-C IV metadata for %s", entry.FullPath)
		} else {
			glog.Errorf("Failed to decode SSE-C IV header for %s: %v", entry.FullPath, err)
		}
	}

	// 保存 SSE-C 算法与 Key MD5，便于响应头回写
	if sseAlgorithm := r.Header.Get(s3_constants.AmzServerSideEncryptionCustomerAlgorithm); sseAlgorithm != "" {
		entry.Extended[s3_constants.AmzServerSideEncryptionCustomerAlgorithm] = []byte(sseAlgorithm)
		glog.V(4).Infof("Stored SSE-C algorithm metadata for %s", entry.FullPath)
	}
	if sseKeyMD5 := r.Header.Get(s3_constants.AmzServerSideEncryptionCustomerKeyMD5); sseKeyMD5 != "" {
		entry.Extended[s3_constants.AmzServerSideEncryptionCustomerKeyMD5] = []byte(sseKeyMD5)
		glog.V(4).Infof("Stored SSE-C key MD5 metadata for %s", entry.FullPath)
	}

	if sseKMSHeader := r.Header.Get(s3_constants.SeaweedFSSSEKMSKeyHeader); sseKMSHeader != "" {
		// 解码 SSE-KMS 元数据
		if kmsData, err := base64.StdEncoding.DecodeString(sseKMSHeader); err == nil {
			entry.Extended[s3_constants.SeaweedFSSSEKMSKey] = kmsData
			glog.V(4).Infof("Stored SSE-KMS metadata for %s", entry.FullPath)
		} else {
			glog.Errorf("Failed to decode SSE-KMS metadata header for %s: %v", entry.FullPath, err)
		}
	}

	if sseS3Header := r.Header.Get(s3_constants.SeaweedFSSSES3Key); sseS3Header != "" {
		// 解码 SSE-S3 元数据
		if s3Data, err := base64.StdEncoding.DecodeString(sseS3Header); err == nil {
			entry.Extended[s3_constants.SeaweedFSSSES3Key] = s3Data
			glog.V(4).Infof("Stored SSE-S3 metadata for %s", entry.FullPath)
		} else {
			glog.Errorf("Failed to decode SSE-S3 metadata header for %s: %v", entry.FullPath, err)
		}
	}

	dbErr := fs.filer.CreateEntry(ctx, entry, false, false, nil, skipCheckParentDirEntry(r), so.MaxFileNameLength)
	// 某些 S3 测试场景中，object key 即父目录，需要特殊处理
	if dbErr != nil && strings.HasSuffix(dbErr.Error(), " is a file") && isS3Request(r) {
		dbErr = fs.filer.CreateEntry(ctx, entry, false, false, nil, true, so.MaxFileNameLength)
	}
	if dbErr != nil {
		replyerr = dbErr
		filerResult.Error = dbErr.Error()
		glog.V(0).InfofCtx(ctx, "failing to write %s to filer server : %v", path, dbErr)
	}
	return filerResult, replyerr
}

// saveAsChunk 返回一个闭包，用于在 autoChunk 期间将内存数据保存为 Volume chunk
// 闭包内部负责调用 assignNewFileInfo 与实际上传逻辑
func (fs *FilerServer) saveAsChunk(ctx context.Context, so *operation.StorageOption) filer.SaveDataAsChunkFunctionType {

	return func(reader io.Reader, name string, offset int64, tsNs int64) (*filer_pb.FileChunk, error) {
		var fileId string
		var uploadResult *operation.UploadResult

		err := util.Retry("saveAsChunk", func() error {
			// 每个 chunk 分配一个唯一 fid
			assignedFileId, urlLocation, auth, assignErr := fs.assignNewFileInfo(ctx, so)
			if assignErr != nil {
				return assignErr
			}

			fileId = assignedFileId

			// 上传 chunk 到 Volume
			uploadOption := &operation.UploadOption{
				UploadUrl:         urlLocation,
				Filename:          name,
				Cipher:            fs.option.Cipher,
				IsInputCompressed: false,
				MimeType:          "",
				PairMap:           nil,
				Jwt:               auth,
			}

			uploader, uploaderErr := operation.NewUploader()
			if uploaderErr != nil {
				return uploaderErr
			}

			var uploadErr error
			uploadResult, uploadErr, _ = uploader.Upload(ctx, reader, uploadOption)
			if uploadErr != nil {
				return uploadErr
			}
			return nil
		})
		if err != nil {
			return nil, err
		}

		return uploadResult.ToPbFileChunk(fileId, offset, tsNs), nil
	}
}

// mkdir 处理创建目录的请求
//
// 【功能说明】
//   - 创建新目录（不支持递归创建多级目录）
//   - 支持自定义目录权限
//   - 检查目录是否已存在
//
// 【参数说明】
//   - ctx: 请求上下文
//   - w: HTTP 响应写入器
//   - r: HTTP 请求对象（POST 请求）
//   - so: 存储策略选项
//
// 【返回值】
//   - filerResult: 创建结果（目录名、错误）
//   - replyerr: 错误信息
//
// 【触发条件】
//   - POST 请求
//   - URL 路径以 / 结尾
//   - 无 Content-Type 头部
//   - 示例：POST http://localhost:8888/bucket/
//
// 【URL 参数】
//   - mode: 目录权限（八进制），默认 0660
//     示例：POST http://localhost:8888/bucket/?mode=0755
//
// 【目录权限】
//   - 0755: 所有者读写执行，组读执行，其他人读执行
//   - 0660: 所有者和组读写，其他人无权限（默认）
//   - 0700: 仅所有者读写执行，其他人无权限
//
// 【错误情况】
//   - 目录已存在：返回 409 Conflict
//   - 父目录不存在：返回 500 Internal Server Error
//   - 权限格式错误：使用默认权限 0660
func (fs *FilerServer) mkdir(ctx context.Context, w http.ResponseWriter, r *http.Request, so *operation.StorageOption) (filerResult *FilerPostResult, replyerr error) {

	// 【步骤 1：解析目录权限】
	// 从 URL 查询参数获取 mode（Unix 文件权限）
	// 格式：八进制字符串，如 "0755"、"0660"
	modeStr := r.URL.Query().Get("mode")
	if modeStr == "" {
		// 默认权限：0660（所有者和组可读写）
		modeStr = "0660"
	}
	mode, err := strconv.ParseUint(modeStr, 8, 32)
	if err != nil {
		glog.ErrorfCtx(ctx, "Invalid mode format: %s, use 0660 by default", modeStr)
		mode = 0660
	}

	// 【步骤 2：规范化路径】
	// 去除末尾的 /，保持路径一致性
	// 示例：/bucket/ → /bucket
	path := r.URL.Path
	if strings.HasSuffix(path, "/") {
		path = path[:len(path)-1]
	}

	// 【步骤 3：检查目录是否已存在】
	existingEntry, err := fs.filer.FindEntry(ctx, util.FullPath(path))
	if err == nil && existingEntry != nil {
		// 目录已存在，返回错误
		// 上层会转换为 HTTP 409 Conflict
		replyerr = fmt.Errorf("dir %s already exists", path)
		return
	}

	glog.V(4).InfolnCtx(ctx, "mkdir", path)

	// 【步骤 4：构造目录 Entry】
	// Entry 是 Filer 元数据结构，包含：
	//   - FullPath: 目录完整路径
	//   - Attr: 目录属性（权限、时间、所有者等）
	//   - Chunks: 空（目录没有数据）
	entry := &filer.Entry{
		FullPath: util.FullPath(path),
		Attr: filer.Attr{
			Mtime:  time.Now(),  // 修改时间
			Crtime: time.Now(),  // 创建时间
			Mode:   os.FileMode(mode) | os.ModeDir, // 目录权限 | 目录标志
			Uid:    OS_UID,      // 所有者 UID
			Gid:    OS_GID,      // 所有者 GID
			TtlSec: so.TtlSeconds, // TTL（通常目录不设置 TTL）
		},
	}

	// 【步骤 5：构造响应结果】
	filerResult = &FilerPostResult{
		Name: util.FullPath(path).Name(), // 目录名（不包含路径）
	}

	// 【步骤 6：保存目录元数据】
	// CreateEntry 会：
	//   1. 检查父目录是否存在
	//   2. 将 Entry 写入元数据存储（MySQL、LevelDB 等）
	//   3. 更新目录索引
	// 参数说明：
	//   - entry: 目录元数据
	//   - O_EXCL=false: 允许覆盖（虽然前面已检查）
	//   - O_DIRECTORY=false: 通过 Mode 中的 os.ModeDir 标志判断
	//   - signatures=nil: 不使用签名验证
	//   - skipCheckParent=false: 检查父目录存在性
	//   - maxFileNameLength: 文件名长度限制
	if dbErr := fs.filer.CreateEntry(ctx, entry, false, false, nil, false, so.MaxFileNameLength); dbErr != nil {
		replyerr = dbErr
		filerResult.Error = dbErr.Error()
		glog.V(0).InfofCtx(ctx, "failing to create dir %s on filer server : %v", path, dbErr)
	}
	return filerResult, replyerr
}

// SaveAmzMetaData 将请求头中以 Seaweed-/X-Amz-Meta- 开头的字段写入扩展属性
//
// 【功能说明】
//   - 提取 S3 兼容的元数据头部（X-Amz-*）
//   - 提取用户自定义元数据（X-Amz-Meta-*）
//   - 提取 SSE 加密相关头部（X-Amz-Server-Side-Encryption-*）
//   - 提取对象标签（X-Amz-Tagging）
//   - 提取 ACL 权限信息
//
// 【参数说明】
//   - r: HTTP 请求对象（包含请求头）
//   - existing: 现有元数据（用于更新操作）
//   - isReplace: true=替换模式（覆盖旧值），false=合并模式（保留旧值）
//
// 【返回值】
//   - metadata: 元数据映射表（key → value）
//
// 【S3 元数据类型】
//   1. 系统元数据：
//      - X-Amz-Storage-Class: 存储类别（STANDARD、GLACIER 等）
//      - Content-Encoding: 内容编码（gzip、deflate 等）
//
//   2. 用户元数据：
//      - X-Amz-Meta-*: 用户自定义键值对
//      - 示例：X-Amz-Meta-Author: Alice
//
//   3. 对象标签：
//      - X-Amz-Tagging: URL 编码的键值对
//      - 格式：key1=value1&key2=value2
//      - 示例：project=demo&env=prod
//
//   4. SSE 加密：
//      - X-Amz-Server-Side-Encryption-Customer-Algorithm: 加密算法（AES256）
//      - X-Amz-Server-Side-Encryption-Customer-Key-MD5: 密钥 MD5
//
//   5. ACL 权限：
//      - X-Seaweed-Owner: 对象所有者
//      - X-Seaweed-Acl: 访问控制列表
//
// 【存储格式】
//   - 所有元数据以 []byte 形式存储在 Entry.Extended 中
//   - 读取时需要转换回字符串
//
// 【使用场景】
//   - S3 PutObject: 保存对象元数据
//   - S3 CopyObject: 复制对象元数据
//   - S3 PutObjectTagging: 更新对象标签
func SaveAmzMetaData(r *http.Request, existing map[string][]byte, isReplace bool) (metadata map[string][]byte) {

	// 【步骤 1：初始化元数据映射】
	metadata = make(map[string][]byte)

	// 【步骤 2：处理现有元数据】
	// 如果不是替换模式，保留现有元数据
	if !isReplace {
		for k, v := range existing {
			metadata[k] = v
		}
	}

	// 【步骤 3：提取存储类别】
	// X-Amz-Storage-Class: STANDARD、REDUCED_REDUNDANCY、GLACIER 等
	// 用于区分热存储和冷存储
	if sc := r.Header.Get(s3_constants.AmzStorageClass); sc != "" {
		metadata[s3_constants.AmzStorageClass] = []byte(sc)
	}

	// 【步骤 4：提取内容编码】
	// Content-Encoding: gzip、deflate、br 等
	// 用于指示客户端如何解压内容
	if ce := r.Header.Get("Content-Encoding"); ce != "" {
		metadata["Content-Encoding"] = []byte(ce)
	}

	// 【步骤 5：提取对象标签】
	// X-Amz-Tagging: key1=value1&key2=value2
	// S3 标签用于对象分类和成本分配
	if tags := r.Header.Get(s3_constants.AmzObjectTagging); tags != "" {
		// 使用 url.ParseQuery 解析 URL 编码的标签
		// 示例：project%3Ddemo&env%3Dprod → {project: [demo], env: [prod]}
		parsedTags, err := url.ParseQuery(tags)
		if err != nil {
			glog.Errorf("Failed to parse S3 tags '%s': %v", tags, err)
		} else {
			// 遍历所有标签键值对
			for key, values := range parsedTags {
				// S3 规范要求：相同 key 取最后一个值
				// 值可以为空字符串但不能为 nil
				value := ""
				if len(values) > 0 {
					value = values[len(values)-1]
				}
				// 存储格式：X-Amz-Tagging-<key> → <value>
				// 示例：X-Amz-Tagging-project → demo
				metadata[s3_constants.AmzObjectTagging+"-"+key] = []byte(value)
			}
		}
	}

	// 【步骤 6：提取用户自定义元数据】
	// X-Amz-Meta-*: 用户自定义键值对
	// 示例：
	//   - X-Amz-Meta-Author: Alice
	//   - X-Amz-Meta-Department: Engineering
	for header, values := range r.Header {
		if strings.HasPrefix(header, s3_constants.AmzUserMetaPrefix) {
			// 取最后一个值（S3 规范）
			for _, value := range values {
				metadata[header] = []byte(value)
			}
		}
	}

	// 【步骤 7：提取 SSE-C 加密元数据】
	// SSE-C (Server-Side Encryption with Customer-Provided Keys)
	// 客户端提供加密密钥，服务端负责加密/解密
	if algorithm := r.Header.Get(s3_constants.AmzServerSideEncryptionCustomerAlgorithm); algorithm != "" {
		// 加密算法：通常是 AES256
		metadata[s3_constants.AmzServerSideEncryptionCustomerAlgorithm] = []byte(algorithm)
	}
	if keyMD5 := r.Header.Get(s3_constants.AmzServerSideEncryptionCustomerKeyMD5); keyMD5 != "" {
		// 密钥 MD5：用于验证客户端提供的密钥是否正确
		// 直接存储，保持大小写不变
		metadata[s3_constants.AmzServerSideEncryptionCustomerKeyMD5] = []byte(keyMD5)
	}

	// 【步骤 8：提取 ACL 所有者】
	// X-Seaweed-Owner: 对象所有者的 ID
	// 用于访问控制和权限管理
	acpOwner := r.Header.Get(s3_constants.ExtAmzOwnerKey)
	if len(acpOwner) > 0 {
		metadata[s3_constants.ExtAmzOwnerKey] = []byte(acpOwner)
	}

	// 【步骤 9：提取 ACL 授权】
	// X-Seaweed-Acl: 访问控制列表（JSON 格式）
	// 包含授权信息（谁可以读、写、执行等）
	acpGrants := r.Header.Get(s3_constants.ExtAmzAclKey)
	if len(acpOwner) > 0 {
		metadata[s3_constants.ExtAmzAclKey] = []byte(acpGrants)
	}

	return

}
