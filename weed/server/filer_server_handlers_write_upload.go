// Package weed_server 中的 filer_server_handlers_write_upload.go 提供上传过程中与 Volume Server 交互的细节实现
// 包括请求读流拆分、chunk 上传、服务端加密以及小文件内联逻辑。
package weed_server

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"hash"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"slices"

	"encoding/json"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// bufPool 复用 bytes.Buffer，避免在大文件上传时重复分配内存
var bufPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

// uploadRequestToChunks 解析 HTTP 请求并将 payload 拆分为 chunk
// 同时支持 offset 指定起始位置、append 模式等高级特性
func (fs *FilerServer) uploadRequestToChunks(ctx context.Context, w http.ResponseWriter, r *http.Request, reader io.Reader, chunkSize int32, fileName, contentType string, contentLength int64, so *operation.StorageOption) (fileChunks []*filer_pb.FileChunk, md5Hash hash.Hash, chunkOffset int64, uploadErr error, smallContent []byte) {
	query := r.URL.Query()

	isAppend := isAppend(r)
	if query.Has("offset") {
		offset := query.Get("offset")
		offsetInt, err := strconv.ParseInt(offset, 10, 64)
		if err != nil || offsetInt < 0 {
			err = fmt.Errorf("invalid 'offset': '%s'", offset)
			return nil, nil, 0, err, nil
		}
		if isAppend && offsetInt > 0 {
			err = fmt.Errorf("cannot set offset when op=append")
			return nil, nil, 0, err, nil
		}
		chunkOffset = offsetInt
	}

	return fs.uploadReaderToChunks(ctx, r, reader, chunkOffset, chunkSize, fileName, contentType, isAppend, so)
}

// uploadReaderToChunks 实际执行读取与并发上传
// 采用带缓冲池的 goroutine 并发模型，将大文件拆分后同步或异步上传
func (fs *FilerServer) uploadReaderToChunks(ctx context.Context, r *http.Request, reader io.Reader, startOffset int64, chunkSize int32, fileName, contentType string, isAppend bool, so *operation.StorageOption) (fileChunks []*filer_pb.FileChunk, md5Hash hash.Hash, chunkOffset int64, uploadErr error, smallContent []byte) {

	md5Hash = md5.New()
	chunkOffset = startOffset
	var partReader = io.NopCloser(io.TeeReader(reader, md5Hash))

	var wg sync.WaitGroup
	var bytesBufferCounter int64 = 4
	bytesBufferLimitChan := make(chan struct{}, bytesBufferCounter)
	var fileChunksLock sync.Mutex
	var uploadErrLock sync.Mutex
	for {

		// 限制同时占用的缓冲区数量，避免占满内存
		bytesBufferLimitChan <- struct{}{}

		// 任一 chunk 上传失败即可提前结束
		// uploadErr 可能被多个协程修改，因此必须加锁
		uploadErrLock.Lock()
		if uploadErr != nil {
			<-bytesBufferLimitChan
			uploadErrLock.Unlock()
			break
		}
		uploadErrLock.Unlock()

		bytesBuffer := bufPool.Get().(*bytes.Buffer)

		limitedReader := io.LimitReader(partReader, int64(chunkSize))

		bytesBuffer.Reset()

		dataSize, err := bytesBuffer.ReadFrom(limitedReader)

		// data, err := io.ReadAll(limitedReader) // 旧实现，保留以便排查
		if err != nil || dataSize == 0 {
			bufPool.Put(bytesBuffer)
			<-bytesBufferLimitChan
			if err != nil {
				uploadErrLock.Lock()
				if uploadErr == nil {
					uploadErr = err
				}
				uploadErrLock.Unlock()
			}
			break
		}
		if chunkOffset == 0 && !isAppend {
			if dataSize < fs.option.SaveToFilerLimit {
				chunkOffset += dataSize
				smallContent = make([]byte, dataSize)
				bytesBuffer.Read(smallContent)
				bufPool.Put(bytesBuffer)
				<-bytesBufferLimitChan
				stats.FilerHandlerCounter.WithLabelValues(stats.ContentSaveToFiler).Inc()
				break
			}
		} else {
			stats.FilerHandlerCounter.WithLabelValues(stats.AutoChunk).Inc()
		}

		wg.Add(1)
		go func(offset int64, buf *bytes.Buffer) {
			defer func() {
				bufPool.Put(buf)
				<-bytesBufferLimitChan
				wg.Done()
			}()

			chunks, toChunkErr := fs.dataToChunkWithSSE(ctx, r, fileName, contentType, buf.Bytes(), offset, so)
			if toChunkErr != nil {
				uploadErrLock.Lock()
				if uploadErr == nil {
					uploadErr = toChunkErr
				}
				uploadErrLock.Unlock()
			}
			if chunks != nil {
				fileChunksLock.Lock()
				fileChunksSize := len(fileChunks) + len(chunks)
				for _, chunk := range chunks {
					fileChunks = append(fileChunks, chunk)
					glog.V(4).InfofCtx(ctx, "uploaded %s chunk %d to %s [%d,%d)", fileName, fileChunksSize, chunk.FileId, offset, offset+int64(chunk.Size))
				}
				fileChunksLock.Unlock()
			}
		}(chunkOffset, bytesBuffer)

		// 重置下一个 chunk 需要用到的偏移
		chunkOffset = chunkOffset + dataSize

		// 如果最后一个 chunk 没有填满 chunkSize，说明已经读到末尾，直接退出
		if dataSize < int64(chunkSize) {
			break
		}
	}

	wg.Wait()

	if uploadErr != nil {
		glog.V(0).InfofCtx(ctx, "upload file %s error: %v", fileName, uploadErr)
		for _, chunk := range fileChunks {
			glog.V(4).InfofCtx(ctx, "purging failed uploaded %s chunk %s [%d,%d)", fileName, chunk.FileId, chunk.Offset, chunk.Offset+int64(chunk.Size))
		}
		fs.filer.DeleteUncommittedChunks(ctx, fileChunks)
		return nil, md5Hash, 0, uploadErr, nil
	}
	slices.SortFunc(fileChunks, func(a, b *filer_pb.FileChunk) int {
		return int(a.Offset - b.Offset)
	})
	return fileChunks, md5Hash, chunkOffset, nil, smallContent
}

// doUpload 调用 operation.UploadResult 完成实际的 HTTP 上传
// 返回上传结果与可能的内联内容（用于极小文件）
func (fs *FilerServer) doUpload(ctx context.Context, urlLocation string, limitedReader io.Reader, fileName string, contentType string, pairMap map[string]string, auth security.EncodedJwt) (*operation.UploadResult, error, []byte) {

	stats.FilerHandlerCounter.WithLabelValues(stats.ChunkUpload).Inc()
	start := time.Now()
	defer func() {
		stats.FilerRequestHistogram.WithLabelValues(stats.ChunkUpload).Observe(time.Since(start).Seconds())
	}()

	uploadOption := &operation.UploadOption{
		UploadUrl:         urlLocation,
		Filename:          fileName,
		Cipher:            fs.option.Cipher,
		IsInputCompressed: false,
		MimeType:          contentType,
		PairMap:           pairMap,
		Jwt:               auth,
	}

	uploader, err := operation.NewUploader()
	if err != nil {
		return nil, err, []byte{}
	}

	uploadResult, err, data := uploader.Upload(ctx, limitedReader, uploadOption)
	if uploadResult != nil && uploadResult.RetryCount > 0 {
		stats.FilerHandlerCounter.WithLabelValues(stats.ChunkUploadRetry).Add(float64(uploadResult.RetryCount))
	}
	return uploadResult, err, data
}

// dataToChunk 直接将内存数据写入 Volume，产生新的 chunk 元信息
// 常用于 append 或内联写入场景
func (fs *FilerServer) dataToChunk(ctx context.Context, fileName, contentType string, data []byte, chunkOffset int64, so *operation.StorageOption) ([]*filer_pb.FileChunk, error) {
	return fs.dataToChunkWithSSE(ctx, nil, fileName, contentType, data, chunkOffset, so)
}

// dataToChunkWithSSE 在 dataToChunk 基础上，增加对 SSE-C 请求头的解析和透传
func (fs *FilerServer) dataToChunkWithSSE(ctx context.Context, r *http.Request, fileName, contentType string, data []byte, chunkOffset int64, so *operation.StorageOption) ([]*filer_pb.FileChunk, error) {
	dataReader := util.NewBytesReader(data)

	// 如果分配文件 ID 失败，重试以获取不同的 file id
	var fileId, urlLocation string
	var auth security.EncodedJwt
	var uploadErr error
	var uploadResult *operation.UploadResult
	var failedFileChunks []*filer_pb.FileChunk

	err := util.Retry("filerDataToChunk", func() error {
		// 每个 chunk 都单独分配 fid，避免并发覆盖
		fileId, urlLocation, auth, uploadErr = fs.assignNewFileInfo(ctx, so)
		if uploadErr != nil {
			glog.V(4).InfofCtx(ctx, "retry later due to assign error: %v", uploadErr)
			stats.FilerHandlerCounter.WithLabelValues(stats.ChunkAssignRetry).Inc()
			return uploadErr
		}
		// 将 chunk 上传至对应的 Volume Server
		uploadResult, uploadErr, _ = fs.doUpload(ctx, urlLocation, dataReader, fileName, contentType, nil, auth)
		if uploadErr != nil {
			glog.V(4).InfofCtx(ctx, "retry later due to upload error: %v", uploadErr)
			stats.FilerHandlerCounter.WithLabelValues(stats.ChunkDoUploadRetry).Inc()
			fid, _ := filer_pb.ToFileIdObject(fileId)
			fileChunk := filer_pb.FileChunk{
				FileId: fileId,
				Offset: chunkOffset,
				Fid:    fid,
			}
			failedFileChunks = append(failedFileChunks, &fileChunk)
			return uploadErr
		}
		return nil
	})
	if err != nil {
		glog.ErrorfCtx(ctx, "upload error: %v", err)
		return failedFileChunks, err
	}

	// 如果最后一个 chunk 恰好读到边界，此时 chunkOffset 需要回退
	if uploadResult.Size == 0 {
		return nil, nil
	}

	// 从请求头中提取 SSE 相关元数据（如算法、Key ID 等）
	var sseType filer_pb.SSEType = filer_pb.SSEType_NONE
	var sseMetadata []byte

	if r != nil {

		// 处理 SSE-KMS: 直接复用请求头信息
		sseKMSHeaderValue := r.Header.Get(s3_constants.SeaweedFSSSEKMSKeyHeader)
		if sseKMSHeaderValue != "" {
			sseType = filer_pb.SSEType_SSE_KMS
			if kmsData, err := base64.StdEncoding.DecodeString(sseKMSHeaderValue); err == nil {
				sseMetadata = kmsData
				glog.V(4).InfofCtx(ctx, "Storing SSE-KMS metadata for chunk %s at offset %d", fileId, chunkOffset)
			} else {
				glog.V(1).InfofCtx(ctx, "Failed to decode SSE-KMS metadata for chunk %s: %v", fileId, err)
			}
		} else if r.Header.Get(s3_constants.AmzServerSideEncryptionCustomerAlgorithm) != "" {
			// SSE-C: 为每个 chunk 构造独立元数据，方便统一处理
			sseType = filer_pb.SSEType_SSE_C

			// 从请求头拿到 SSE-C 的密钥、IV 等信息
			sseIVHeader := r.Header.Get(s3_constants.SeaweedFSSSEIVHeader)
			keyMD5Header := r.Header.Get(s3_constants.AmzServerSideEncryptionCustomerKeyMD5)

			if sseIVHeader != "" && keyMD5Header != "" {
				// Base64 解码 IV
				if ivData, err := base64.StdEncoding.DecodeString(sseIVHeader); err == nil {
					// SSE-C 的偏移必须与 chunkOffset 对齐，以便解密时生成正确 IV
					ssecMetadataStruct := struct {
						Algorithm  string `json:"algorithm"`
						IV         string `json:"iv"`
						KeyMD5     string `json:"keyMD5"`
						PartOffset int64  `json:"partOffset"`
					}{
						Algorithm:  "AES256",
						IV:         base64.StdEncoding.EncodeToString(ivData),
						KeyMD5:     keyMD5Header,
						PartOffset: chunkOffset,
					}
					if ssecMetadata, serErr := json.Marshal(ssecMetadataStruct); serErr == nil {
						sseMetadata = ssecMetadata
					} else {
						glog.V(1).InfofCtx(ctx, "Failed to serialize SSE-C metadata for chunk %s: %v", fileId, serErr)
					}
				} else {
					glog.V(1).InfofCtx(ctx, "Failed to decode SSE-C IV for chunk %s: %v", fileId, err)
				}
			} else {
				glog.V(4).InfofCtx(ctx, "SSE-C chunk %s missing IV or KeyMD5 header", fileId)
			}
		} else if r.Header.Get(s3_constants.SeaweedFSSSES3Key) != "" {
			// SSE-S3: 使用服务器托管密钥
			// 将 chunk 类型标记为 SSE-S3，后续便于统计
			sseType = filer_pb.SSEType_SSE_S3

			// 从请求头提取 SSE-S3 所需的元数据
			sseS3Header := r.Header.Get(s3_constants.SeaweedFSSSES3Key)
			if sseS3Header != "" {
				if s3Data, err := base64.StdEncoding.DecodeString(sseS3Header); err == nil {
					// SSE-S3 也保持 chunk 级别的 metadata，以统一处理逻辑
					glog.V(4).InfofCtx(ctx, "Storing SSE-S3 metadata for chunk %s at offset %d", fileId, chunkOffset)
					sseMetadata = s3Data
				} else {
					glog.V(1).InfofCtx(ctx, "Failed to decode SSE-S3 metadata for chunk %s: %v", fileId, err)
				}
			}
		}
	}

	// 如果存在 SSE 元数据，写入 chunk 结构中
	var chunk *filer_pb.FileChunk
	if sseType != filer_pb.SSEType_NONE {
		chunk = uploadResult.ToPbFileChunkWithSSE(fileId, chunkOffset, time.Now().UnixNano(), sseType, sseMetadata)
	} else {
		chunk = uploadResult.ToPbFileChunk(fileId, chunkOffset, time.Now().UnixNano())
	}

	return []*filer_pb.FileChunk{chunk}, nil
}
