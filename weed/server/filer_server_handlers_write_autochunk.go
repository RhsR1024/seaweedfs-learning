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

// autoChunk 根据请求内容自动决定切片大小并分发到 POST/PUT 处理函数
// 参数:
//   - ctx: 请求上下文
//   - contentLength: HTTP Header 中的 Content-Length，可为 -1
//   - so: 存储策略，控制副本/TTL 等
func (fs *FilerServer) autoChunk(ctx context.Context, w http.ResponseWriter, r *http.Request, contentLength int64, so *operation.StorageOption) {

	// chunk 大小既可以通过命令行设置，也可以用 query 覆盖
	query := r.URL.Query()

	parsedMaxMB, _ := strconv.ParseInt(query.Get("maxMB"), 10, 32)
	maxMB := int32(parsedMaxMB)
	if maxMB <= 0 && fs.option.MaxMB > 0 {
		maxMB = int32(fs.option.MaxMB)
	}

	chunkSize := 1024 * 1024 * maxMB

	var reply *FilerPostResult
	var err error
	var md5bytes []byte
	if r.Method == http.MethodPost {
		if r.Header.Get("Content-Type") == "" && strings.HasSuffix(r.URL.Path, "/") {
			reply, err = fs.mkdir(ctx, w, r, so)
		} else {
			reply, md5bytes, err = fs.doPostAutoChunk(ctx, w, r, chunkSize, contentLength, so)
		}
	} else {
		reply, md5bytes, err = fs.doPutAutoChunk(ctx, w, r, chunkSize, contentLength, so)
	}
	if err != nil {
		errStr := err.Error()
		switch {
		case errStr == constants.ErrMsgOperationNotPermitted:
			writeJsonError(w, r, http.StatusForbidden, err)
		case strings.HasPrefix(errStr, "read input:") || errStr == io.ErrUnexpectedEOF.Error():
			writeJsonError(w, r, util.HttpStatusCancelled, err)
		case strings.HasSuffix(errStr, "is a file") || strings.HasSuffix(errStr, "already exists"):
			writeJsonError(w, r, http.StatusConflict, err)
		case errStr == constants.ErrMsgBadDigest:
			writeJsonError(w, r, http.StatusBadRequest, err)
		default:
			writeJsonError(w, r, http.StatusInternalServerError, err)
		}
	} else if reply != nil {
		if len(md5bytes) > 0 {
			md5InBase64 := util.Base64Encode(md5bytes)
			w.Header().Set("Content-MD5", md5InBase64)
		}
		writeJsonQuiet(w, r, http.StatusCreated, reply)
	}
}

// doPostAutoChunk 处理 multipart/form-data 上传
// 会遍历各 form part，分别写入目录或文件数据，返回写入结果与 MD5
func (fs *FilerServer) doPostAutoChunk(ctx context.Context, w http.ResponseWriter, r *http.Request, chunkSize int32, contentLength int64, so *operation.StorageOption) (filerResult *FilerPostResult, md5bytes []byte, replyerr error) {
	multipartReader, multipartReaderErr := r.MultipartReader()
	if multipartReaderErr != nil {
		return nil, nil, multipartReaderErr
	}

	part1, part1Err := multipartReader.NextPart()
	if part1Err != nil {
		return nil, nil, part1Err
	}

	fileName := part1.FileName()
	if fileName != "" {
		fileName = path.Base(fileName)
	}
	contentType := part1.Header.Get("Content-Type")
	if contentType == "application/octet-stream" {
		contentType = ""
	}

	if err := fs.checkPermissions(ctx, r, fileName); err != nil {
		return nil, nil, err
	}

	if so.SaveInside {
		buf := bufPool.Get().(*bytes.Buffer)
		buf.Reset()
		buf.ReadFrom(part1)
		filerResult, replyerr = fs.saveMetaData(ctx, r, fileName, contentType, so, nil, nil, 0, buf.Bytes())
		bufPool.Put(buf)
		return
	}

	fileChunks, md5Hash, chunkOffset, err, smallContent := fs.uploadRequestToChunks(ctx, w, r, part1, chunkSize, fileName, contentType, contentLength, so)
	if err != nil {
		return nil, nil, err
	}

	md5bytes = md5Hash.Sum(nil)
	headerMd5 := r.Header.Get("Content-Md5")
	if headerMd5 != "" && !(util.Base64Encode(md5bytes) == headerMd5 || fmt.Sprintf("%x", md5bytes) == headerMd5) {
		fs.filer.DeleteUncommittedChunks(ctx, fileChunks)
		return nil, nil, errors.New(constants.ErrMsgBadDigest)
	}
	filerResult, replyerr = fs.saveMetaData(ctx, r, fileName, contentType, so, md5bytes, fileChunks, chunkOffset, smallContent)
	if replyerr != nil {
		fs.filer.DeleteUncommittedChunks(ctx, fileChunks)
	}

	return
}

// doPutAutoChunk 处理单个对象的 PUT 上传
// 自动支持追加/覆盖、目录创建以及内容长度校验
func (fs *FilerServer) doPutAutoChunk(ctx context.Context, w http.ResponseWriter, r *http.Request, chunkSize int32, contentLength int64, so *operation.StorageOption) (filerResult *FilerPostResult, md5bytes []byte, replyerr error) {

	fileName := path.Base(r.URL.Path)
	contentType := r.Header.Get("Content-Type")
	if contentType == "application/octet-stream" {
		contentType = ""
	}

	if err := fs.checkPermissions(ctx, r, fileName); err != nil {
		return nil, nil, err
	}

	fileChunks, md5Hash, chunkOffset, err, smallContent := fs.uploadRequestToChunks(ctx, w, r, r.Body, chunkSize, fileName, contentType, contentLength, so)

	if err != nil {
		return nil, nil, err
	}

	md5bytes = md5Hash.Sum(nil)
	headerMd5 := r.Header.Get("Content-Md5")
	if headerMd5 != "" && !(util.Base64Encode(md5bytes) == headerMd5 || fmt.Sprintf("%x", md5bytes) == headerMd5) {
		fs.filer.DeleteUncommittedChunks(ctx, fileChunks)
		return nil, nil, errors.New(constants.ErrMsgBadDigest)
	}
	filerResult, replyerr = fs.saveMetaData(ctx, r, fileName, contentType, so, md5bytes, fileChunks, chunkOffset, smallContent)
	if replyerr != nil {
		fs.filer.DeleteUncommittedChunks(ctx, fileChunks)
	}

	return
}

// isAppend 判断请求是否需采用追加模式
// 依据 query 参数 append=true 或 header X-Seaweed-Append
func isAppend(r *http.Request) bool {
	return r.URL.Query().Get("op") == "append"
}

// skipCheckParentDirEntry 判断是否跳过父目录存在性检查
// S3 直写对象时可以携带 skipCheckParent=true 来避免额外查询
func skipCheckParentDirEntry(r *http.Request) bool {
	return r.URL.Query().Get("skipCheckParentDir") == "true"
}

// isS3Request 用于快速判定当前请求是否来自 S3 协议栈
// 依据特定 Header（如 Authorization/AWS v4）进行判定
func isS3Request(r *http.Request) bool {
	return r.Header.Get(s3_constants.AmzAuthType) != "" || r.Header.Get("X-Amz-Date") != ""
}

// checkPermissions 校验访问者是否具有写入/修改指定路径的权限
// 主动检查 WORM 模式、S3 特殊限制等条件
func (fs *FilerServer) checkPermissions(ctx context.Context, r *http.Request, fileName string) error {
	fullPath := fs.fixFilePath(ctx, r, fileName)
	enforced, err := fs.wormEnforcedForEntry(ctx, fullPath)
	if err != nil {
		return err
	} else if enforced {
		// WORM 文件禁止修改或删除
		return errors.New(constants.ErrMsgOperationNotPermitted)
	}

	return nil
}

// wormEnforcedForEntry 判断路径是否启用了 WORM（Write Once Read Many）策略
// 返回 true 表示禁止覆盖/删除
func (fs *FilerServer) wormEnforcedForEntry(ctx context.Context, fullPath string) (bool, error) {
	rule := fs.filer.FilerConf.MatchStorageRule(fullPath)
	if !rule.Worm {
		return false, nil
	}

	entry, err := fs.filer.FindEntry(ctx, util.FullPath(fullPath))
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			return false, nil
		}

		return false, err
	}

	// 尚未真正启用 WORM（时间戳为 0）
	if entry.WORMEnforcedAtTsNs == 0 {
		return false, nil
	}

	// WORM 永久生效，不会过期
	if rule.WormRetentionTimeSeconds == 0 {
		return true, nil
	}

	enforcedAt := time.Unix(0, entry.WORMEnforcedAtTsNs)

	// WORM 已经过期，允许写入
	if time.Now().Sub(enforcedAt).Seconds() >= float64(rule.WormRetentionTimeSeconds) {
		return false, nil
	}

	return true, nil
}

// fixFilePath 统一处理路径规范化，包括去重 //、处理 ..、补充 multipart 中的覆盖路径等
func (fs *FilerServer) fixFilePath(ctx context.Context, r *http.Request, fileName string) string {
	// 修正 path，确保目录和文件名组合正确
	fullPath := r.URL.Path
	if strings.HasSuffix(fullPath, "/") {
		if fileName != "" {
			fullPath += fileName
		}
	} else {
		if fileName != "" {
			if possibleDirEntry, findDirErr := fs.filer.FindEntry(ctx, util.FullPath(fullPath)); findDirErr == nil {
				if possibleDirEntry.IsDirectory() {
					fullPath += "/" + fileName
				}
			}
		}
	}

	return fullPath
}

// saveMetaData 构建并写入 Filer Entry 元数据
// 包括用户自定义 header、ETag、chunk 位置等信息
func (fs *FilerServer) saveMetaData(ctx context.Context, r *http.Request, fileName string, contentType string, so *operation.StorageOption, md5bytes []byte, fileChunks []*filer_pb.FileChunk, chunkOffset int64, content []byte) (filerResult *FilerPostResult, replyerr error) {

	// 检查请求头中是否携带文件权限信息
	modeStr := r.URL.Query().Get("mode")
	if modeStr == "" {
		modeStr = "0660"
	}
	mode, err := strconv.ParseUint(modeStr, 8, 32)
	if err != nil {
		glog.ErrorfCtx(ctx, "Invalid mode format: %s, use 0660 by default", modeStr)
		mode = 0660
	}

	// 再次规范化路径，兼容 multipart 中的覆盖行为
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
// 支持多级创建并复用 saveMetaData 写入目录属性
func (fs *FilerServer) mkdir(ctx context.Context, w http.ResponseWriter, r *http.Request, so *operation.StorageOption) (filerResult *FilerPostResult, replyerr error) {

	// 设置目录权限
	modeStr := r.URL.Query().Get("mode")
	if modeStr == "" {
		modeStr = "0660"
	}
	mode, err := strconv.ParseUint(modeStr, 8, 32)
	if err != nil {
		glog.ErrorfCtx(ctx, "Invalid mode format: %s, use 0660 by default", modeStr)
		mode = 0660
	}

	// 再次修正路径，确保目录尾部以 / 结尾
	path := r.URL.Path
	if strings.HasSuffix(path, "/") {
		path = path[:len(path)-1]
	}

	existingEntry, err := fs.filer.FindEntry(ctx, util.FullPath(path))
	if err == nil && existingEntry != nil {
		replyerr = fmt.Errorf("dir %s already exists", path)
		return
	}

	glog.V(4).InfolnCtx(ctx, "mkdir", path)
	entry := &filer.Entry{
		FullPath: util.FullPath(path),
		Attr: filer.Attr{
			Mtime:  time.Now(),
			Crtime: time.Now(),
			Mode:   os.FileMode(mode) | os.ModeDir,
			Uid:    OS_UID,
			Gid:    OS_GID,
			TtlSec: so.TtlSeconds,
		},
	}

	filerResult = &FilerPostResult{
		Name: util.FullPath(path).Name(),
	}

	if dbErr := fs.filer.CreateEntry(ctx, entry, false, false, nil, false, so.MaxFileNameLength); dbErr != nil {
		replyerr = dbErr
		filerResult.Error = dbErr.Error()
		glog.V(0).InfofCtx(ctx, "failing to create dir %s on filer server : %v", path, dbErr)
	}
	return filerResult, replyerr
}

// SaveAmzMetaData 将请求头中以 Seaweed-/X-Amz-Meta- 开头的字段写入扩展属性
// isReplace 控制是否覆盖旧值，existing 用于在更新时保留原始数据
func SaveAmzMetaData(r *http.Request, existing map[string][]byte, isReplace bool) (metadata map[string][]byte) {

	metadata = make(map[string][]byte)
	if !isReplace {
		for k, v := range existing {
			metadata[k] = v
		}
	}

	if sc := r.Header.Get(s3_constants.AmzStorageClass); sc != "" {
		metadata[s3_constants.AmzStorageClass] = []byte(sc)
	}

	if ce := r.Header.Get("Content-Encoding"); ce != "" {
		metadata["Content-Encoding"] = []byte(ce)
	}

	if tags := r.Header.Get(s3_constants.AmzObjectTagging); tags != "" {
		// 使用 url.ParseQuery 解析并自动反解编码
		parsedTags, err := url.ParseQuery(tags)
		if err != nil {
			glog.Errorf("Failed to parse S3 tags '%s': %v", tags, err)
		} else {
			for key, values := range parsedTags {
				// S3 规范要求相同 key 取最后一个值；值可以为空字符串但不能为 nil
				value := ""
				if len(values) > 0 {
					value = values[len(values)-1]
				}
				metadata[s3_constants.AmzObjectTagging+"-"+key] = []byte(value)
			}
		}
	}

	for header, values := range r.Header {
		if strings.HasPrefix(header, s3_constants.AmzUserMetaPrefix) {
			for _, value := range values {
				metadata[header] = []byte(value)
			}
		}
	}

	// 处理 SSE-C 相关请求头
	if algorithm := r.Header.Get(s3_constants.AmzServerSideEncryptionCustomerAlgorithm); algorithm != "" {
		metadata[s3_constants.AmzServerSideEncryptionCustomerAlgorithm] = []byte(algorithm)
	}
	if keyMD5 := r.Header.Get(s3_constants.AmzServerSideEncryptionCustomerKeyMD5); keyMD5 != "" {
		// 直接存储 SSE-C MD5，保持大小写
		metadata[s3_constants.AmzServerSideEncryptionCustomerKeyMD5] = []byte(keyMD5)
	}

	//acp-owner
	acpOwner := r.Header.Get(s3_constants.ExtAmzOwnerKey)
	if len(acpOwner) > 0 {
		metadata[s3_constants.ExtAmzOwnerKey] = []byte(acpOwner)
	}

	//acp-grants
	acpGrants := r.Header.Get(s3_constants.ExtAmzAclKey)
	if len(acpOwner) > 0 {
		metadata[s3_constants.ExtAmzAclKey] = []byte(acpGrants)
	}

	return

}
