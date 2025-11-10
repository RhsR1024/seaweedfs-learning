// Package weed_server 实现 SeaweedFS 的服务器端处理逻辑
// 本文件包含 Volume Server 的写入操作处理器，负责接收和处理文件上传、删除请求
package weed_server

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/topology"
	"github.com/seaweedfs/seaweedfs/weed/util/buffer_pool"
)

// PostHandler Volume Server 的 HTTP POST 请求处理器
// 这是文件上传到 Volume Server 的主要入口点
//
// 处理流程:
//  1. 解析 HTTP 请求表单数据
//  2. 从 URL 路径中提取 volumeId 和 fileId
//  3. 验证 JWT 授权（如果启用）
//  4. 从 HTTP 请求中创建 Needle 对象（包含文件数据和元数据）
//  5. 调用 ReplicatedWrite 写入本地存储和副本
//  6. 返回上传结果给客户端
//
// URL 格式: POST /{volumeId}/{fileId}
// 例如: POST /3/01637037d6
//
// 请求格式: multipart/form-data
// - file: 文件内容
// - Content-Type: 文件 MIME 类型
// - Content-Encoding: gzip (如果压缩)
//
// 响应格式: JSON
// - name: 文件名
// - size: 文件大小
// - eTag: ETag 值
// - mime: MIME 类型
//
// HTTP 状态码:
// - 201 Created: 上传成功
// - 204 No Content: 文件未改变（幂等上传）
// - 400 Bad Request: 请求参数错误
// - 401 Unauthorized: JWT 认证失败
// - 500 Internal Server Error: 写入失败
func (vs *VolumeServer) PostHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// 步骤 1: 解析 HTTP 表单数据
	// 这会解析 URL 查询参数和 POST 表单参数（但不包括 multipart 数据）
	if e := r.ParseForm(); e != nil {
		glog.V(0).InfolnCtx(ctx, "form parse error:", e)
		writeJsonError(w, r, http.StatusBadRequest, e)
		return
	}

	// 步骤 2: 从 URL 路径中解析 volumeId 和 fileId
	// URL 格式: /{volumeId}/{fileId}[,{cookie}]
	// 例如: /3/01637037d6 或 /3/01637037d6,1a2b3c
	vid, fid, _, _, _ := parseURLPath(r.URL.Path)
	volumeId, ve := needle.NewVolumeId(vid)
	if ve != nil {
		glog.V(0).InfolnCtx(ctx, "NewVolumeId error:", ve)
		writeJsonError(w, r, http.StatusBadRequest, ve)
		return
	}

	// 步骤 3: JWT 授权验证（如果启用安全模式）
	// 验证客户端是否有权限写入指定的 volumeId 和 fileId
	if !vs.maybeCheckJwtAuthorization(r, vid, fid, true) {
		writeJsonError(w, r, http.StatusUnauthorized, errors.New("wrong jwt"))
		return
	}

	// 步骤 4: 从缓冲池获取临时缓冲区，用于解析上传数据
	// 使用对象池避免频繁的内存分配，提高性能
	bytesBuffer := buffer_pool.SyncPoolGetBuffer()
	defer buffer_pool.SyncPoolPutBuffer(bytesBuffer) // 使用完毕后归还缓冲区

	// 步骤 5: 从 HTTP 请求创建 Needle 对象
	// 这会解析 multipart form-data，提取文件内容和元数据
	// reqNeedle: 包含文件数据的 Needle 对象
	// originalSize: 原始文件大小（解压后）
	// contentMd5: 内容的 MD5 校验值
	reqNeedle, originalSize, contentMd5, ne := needle.CreateNeedleFromRequest(r, vs.FixJpgOrientation, vs.fileSizeLimitBytes, bytesBuffer)
	if ne != nil {
		writeJsonError(w, r, http.StatusBadRequest, ne)
		return
	}

	// 步骤 6: 执行复制写入
	// 将数据写入本地 Volume，并同步到所有副本服务器
	// isUnchanged: 如果文件内容未变化（幂等上传），返回 true
	ret := operation.UploadResult{}
	isUnchanged, writeError := topology.ReplicatedWrite(ctx, vs.GetMaster, vs.grpcDialOption, vs.store, volumeId, reqNeedle, r, contentMd5)
	if writeError != nil {
		writeJsonError(w, r, http.StatusInternalServerError, writeError)
		return
	}

	// 步骤 7: 处理幂等上传（文件内容未变化）
	// HTTP 204 No Content 响应不允许包含响应体
	if writeError == nil && isUnchanged {
		SetEtag(w, reqNeedle.Etag()) // 设置 ETag 响应头
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// 步骤 8: 构造并返回成功响应
	httpStatus := http.StatusCreated // 201 Created
	if reqNeedle.HasName() {
		ret.Name = string(reqNeedle.Name) // 文件名
	}
	ret.Size = uint32(originalSize)   // 原始大小（未压缩）
	ret.ETag = reqNeedle.Etag()       // ETag 用于缓存验证
	ret.Mime = string(reqNeedle.Mime) // MIME 类型
	SetEtag(w, ret.ETag)              // 设置 ETag 响应头
	w.Header().Set("Content-MD5", contentMd5) // 设置 MD5 响应头
	writeJsonQuiet(w, r, httpStatus, ret)     // 返回 JSON 响应
}

// DeleteHandler Volume Server 的 HTTP DELETE 请求处理器
// 处理文件删除请求，支持普通 Volume 和 EC (Erasure Coding) Volume
//
// 处理流程:
//  1. 从 URL 路径中提取 volumeId 和 fileId
//  2. 验证 JWT 授权
//  3. 检查文件是否存在以及 Cookie 是否匹配
//  4. 如果是 Chunk Manifest，先删除所有 Chunk
//  5. 调用 ReplicatedDelete 删除本地和副本数据
//  6. 返回删除结果
//
// URL 格式: DELETE /{volumeId}/{fileId}[,{cookie}]
// 查询参数:
//   - ts: 可选，指定删除时间戳
//
// 响应格式: JSON
// - size: 删除的数据大小
//
// HTTP 状态码:
// - 202 Accepted: 删除成功
// - 400 Bad Request: Cookie 不匹配
// - 401 Unauthorized: JWT 认证失败
// - 404 Not Found: 文件不存在
// - 500 Internal Server Error: 删除失败
func (vs *VolumeServer) DeleteHandler(w http.ResponseWriter, r *http.Request) {
	// 步骤 1: 创建 Needle 对象并解析请求参数
	n := new(needle.Needle)
	vid, fid, _, _, _ := parseURLPath(r.URL.Path) // 从 URL 解析 volumeId 和 fileId
	volumeId, _ := needle.NewVolumeId(vid)
	n.ParsePath(fid) // 解析 fileId，提取 needleId 和 cookie

	// 步骤 2: JWT 授权验证
	if !vs.maybeCheckJwtAuthorization(r, vid, fid, true) {
		writeJsonError(w, r, http.StatusUnauthorized, errors.New("wrong jwt"))
		return
	}

	// glog.V(2).Infof("volume %s deleting %s", vid, n)

	// 保存原始 cookie，用于后续验证
	cookie := n.Cookie

	// 步骤 3: 检查是否是 EC (Erasure Coding) Volume
	// EC Volume 使用不同的存储机制，需要特殊处理
	ecVolume, hasEcVolume := vs.store.FindEcVolume(volumeId)

	if hasEcVolume {
		// EC Volume 的删除逻辑
		count, err := vs.store.DeleteEcShardNeedle(ecVolume, n, cookie)
		writeDeleteResult(err, count, w, r)
		return
	}

	// 步骤 4: 验证文件是否存在（普通 Volume）
	// 读取 Needle 以检查其存在性
	_, ok := vs.store.ReadVolumeNeedle(volumeId, n, nil, nil)
	if ok != nil {
		// 文件不存在，返回 404
		m := make(map[string]uint32)
		m["size"] = 0
		writeJsonQuiet(w, r, http.StatusNotFound, m)
		return
	}

	// 步骤 5: 验证 Cookie 是否匹配
	// Cookie 是文件的随机密钥，防止未授权删除
	if n.Cookie != cookie {
		glog.V(0).Infoln("delete", r.URL.Path, "with unmaching cookie from ", r.RemoteAddr, "agent", r.UserAgent())
		writeJsonError(w, r, http.StatusBadRequest, errors.New("File Random Cookie does not match."))
		return
	}

	// 步骤 6: 记录要删除的数据大小
	count := int64(n.Size)

	// 步骤 7: 处理 Chunk Manifest（大文件分块存储）
	// 如果是 Chunk Manifest，需要先删除所有 Chunk，再删除 Manifest
	if n.IsChunkedManifest() {
		// 加载 Chunk Manifest，获取所有 Chunk 的信息
		chunkManifest, e := operation.LoadChunkManifest(n.Data, n.IsCompressed())
		if e != nil {
			writeJsonError(w, r, http.StatusInternalServerError, fmt.Errorf("Load chunks manifest error: %v", e))
			return
		}
		// 删除所有 Chunk，确保数据完整性
		// make sure all chunks had deleted before delete manifest
		if e := chunkManifest.DeleteChunks(vs.GetMaster, false, vs.grpcDialOption); e != nil {
			writeJsonError(w, r, http.StatusInternalServerError, fmt.Errorf("Delete chunks error: %v", e))
			return
		}
		count = chunkManifest.Size // 使用 Manifest 中记录的总大小
	}

	// 步骤 8: 设置删除时间戳
	n.LastModified = uint64(time.Now().Unix())
	// 允许客户端指定删除时间戳（用于数据同步场景）
	if len(r.FormValue("ts")) > 0 {
		modifiedTime, err := strconv.ParseInt(r.FormValue("ts"), 10, 64)
		if err == nil {
			n.LastModified = uint64(modifiedTime)
		}
	}

	// 步骤 9: 执行复制删除
	// 删除本地 Volume 中的数据，并同步到所有副本服务器
	_, err := topology.ReplicatedDelete(vs.GetMaster, vs.grpcDialOption, vs.store, volumeId, n, r)

	// 步骤 10: 返回删除结果
	writeDeleteResult(err, count, w, r)

}

// writeDeleteResult 写入删除操作的结果到 HTTP 响应
// 参数:
//   - err: 删除操作的错误（如果有）
//   - count: 删除的数据大小（字节）
//   - w: HTTP 响应写入器
//   - r: HTTP 请求对象
//
// 响应格式:
//   成功: {"size": 1234} (HTTP 202 Accepted)
//   失败: {"error": "Deletion Failed: ..."} (HTTP 500)
func writeDeleteResult(err error, count int64, w http.ResponseWriter, r *http.Request) {
	if err == nil {
		// 删除成功，返回删除的数据大小
		m := make(map[string]int64)
		m["size"] = count
		writeJsonQuiet(w, r, http.StatusAccepted, m) // 202 Accepted
	} else {
		// 删除失败，返回错误信息
		writeJsonError(w, r, http.StatusInternalServerError, fmt.Errorf("Deletion Failed: %w", err))
	}
}

// SetEtag 设置 HTTP 响应的 ETag 头
// ETag 用于 HTTP 缓存验证和条件请求
//
// 参数:
//   - w: HTTP 响应写入器
//   - etag: ETag 值
//
// 工作原理:
//   如果 ETag 值没有被双引号包围，自动添加双引号
//   这符合 HTTP/1.1 规范，ETag 值必须是带引号的字符串
//
// 示例:
//   SetEtag(w, "abc123")  -> ETag: "abc123"
//   SetEtag(w, "\"abc123\"")  -> ETag: "abc123"
func SetEtag(w http.ResponseWriter, etag string) {
	if etag != "" {
		// 检查 ETag 是否已经有引号
		if strings.HasPrefix(etag, "\"") {
			w.Header().Set("ETag", etag)
		} else {
			// 添加引号，符合 HTTP 规范
			w.Header().Set("ETag", "\""+etag+"\"")
		}
	}
}

// getEtag 从 HTTP 响应中提取 ETag 值
// 自动移除 ETag 两端的双引号
//
// 参数:
//   - resp: HTTP 响应对象
//
// 返回值:
//   - etag: 去除引号后的 ETag 字符串
//
// 示例:
//   响应头 ETag: "abc123" -> 返回 "abc123"
//   响应头 ETag: abc123  -> 返回 "abc123"
func getEtag(resp *http.Response) (etag string) {
	etag = resp.Header.Get("ETag")
	// 移除 ETag 两端的双引号（如果存在）
	if strings.HasPrefix(etag, "\"") && strings.HasSuffix(etag, "\"") {
		return etag[1 : len(etag)-1]
	}
	return
}
