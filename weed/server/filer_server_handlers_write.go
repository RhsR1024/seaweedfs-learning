// Package weed_server 中的 filer_server_handlers_write.go 管理 Filer 写路径相关逻辑
// 包括上传、移动、删除以及存储策略的推导。
package weed_server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/constants"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

var (
	// OS_UID 存储当前进程的用户 ID
	// 用作新创建文件/目录的默认所有者 UID
	// 在 POSIX 系统上通过 os.Getuid() 获取
	OS_UID = uint32(os.Getuid())

	// OS_GID 存储当前进程的用户组 ID
	// 用作新创建文件/目录的默认所有者 GID
	// 在 POSIX 系统上通过 os.Getgid() 获取
	OS_GID = uint32(os.Getgid())

	// ErrReadOnly 表示路径处于只读状态，拒绝写入操作
	// 触发场景：
	//   - FilerConf 中配置了 readOnly: true
	//   - WORM（Write Once Read Many）策略生效
	//   - 路径权限不允许写入
	ErrReadOnly = errors.New("read only")
)

// FilerPostResult 是 POST/PUT 上传请求的统一响应结构
//
// 【响应字段】
//   - Name: 上传成功的文件名（不包含路径）
//   - Size: 文件实际写入的字节数
//   - Error: 错误信息（成功时为空字符串）
//
// 【HTTP 状态码对应关系】
//   - 201 Created: 上传成功，Error 为空
//   - 400/403/500: 上传失败，Error 包含错误原因
type FilerPostResult struct {
	Name  string `json:"name,omitempty"` // 文件名
	Size  int64  `json:"size,omitempty"` // 文件大小（字节）
	Error string `json:"error,omitempty"` // 错误信息
}

// assignNewFileInfo 从 Master 分配一个可写的 File ID 和上传地址
//
// 【功能说明】
//   - 向 Master 请求分配一个新的 Volume + Needle 位置
//   - 根据存储策略（副本、数据中心、TTL 等）选择合适的 Volume Server
//   - 返回 File ID、上传 URL 和 JWT 认证令牌
//
// 【参数说明】
//   - ctx: 请求上下文，用于超时控制和取消
//   - so: 存储选项，包含副本策略、TTL、数据中心等配置
//
// 【返回值】
//   - fileId: 分配的文件 ID，格式: "volumeId,needleId"
//   - urlLocation: 上传 URL，格式: "http://host:port/volumeId,needleId[?fsync=true]"
//   - auth: JWT 认证令牌，用于上传时的身份验证
//   - err: 分配失败时的错误信息
//
// 【分配流程】
//   1. 将 StorageOption 转换为 AssignRequest（主请求和备选请求）
//   2. 调用 Master 的 Assign RPC 分配 File ID
//   3. 从返回的副本列表中选择最优上传地址
//      - 优先选择与 Filer 同数据中心的 Volume Server
//      - 减少跨数据中心传输延迟
//   4. 根据 Fsync 选项添加 URL 参数
//      - fsync=true: 数据写入磁盘后才返回（确保持久性）
//   5. 返回 File ID、上传 URL 和认证令牌
//
// 【性能统计】
//   - 记录 ChunkAssign 计数器（分配次数）
//   - 记录 ChunkAssign 直方图（分配延迟）
func (fs *FilerServer) assignNewFileInfo(ctx context.Context, so *operation.StorageOption) (fileId, urlLocation string, auth security.EncodedJwt, err error) {

	// 记录 chunk 分配次数
	stats.FilerHandlerCounter.WithLabelValues(stats.ChunkAssign).Inc()
	start := time.Now()
	defer func() {
		// 记录 chunk 分配延迟
		stats.FilerRequestHistogram.WithLabelValues(stats.ChunkAssign).Observe(time.Since(start).Seconds())
	}()

	// 【步骤 1：构造分配请求】
	// ToAssignRequests 返回两个请求：
	//   - ar: 主请求（使用配置的副本策略、数据中心等）
	//   - altRequest: 备选请求（放宽限制，用于主请求失败时重试）
	ar, altRequest := so.ToAssignRequests(1)

	// 【步骤 2：调用 Master 分配 File ID】
	// operation.Assign 会：
	//   1. 查找符合条件的可写 Volume
	//   2. 在 Volume 中生成唯一的 Needle ID
	//   3. 返回 File ID 和所有副本的地址列表
	assignResult, ae := operation.Assign(ctx, fs.filer.GetMaster, fs.grpcDialOption, ar, altRequest)
	if ae != nil {
		glog.ErrorfCtx(ctx, "failing to assign a file id: %v", ae)
		err = ae
		return
	}

	// 【步骤 3：提取 File ID 和默认上传地址】
	fileId = assignResult.Fid     // 格式: "volumeId,needleId"
	assignUrl := assignResult.Url // 默认的 Volume Server 地址

	// 【步骤 4：选择最优上传地址】
	// 优先选择与 Filer 同数据中心的 Volume Server
	// 好处：减少跨数据中心网络延迟，提升上传性能
	if fs.option.DataCenter != "" {
		for _, repl := range assignResult.Replicas {
			if repl.DataCenter == fs.option.DataCenter {
				assignUrl = repl.Url
				break
			}
		}
	}

	// 【步骤 5：构造完整的上传 URL】
	urlLocation = "http://" + assignUrl + "/" + assignResult.Fid

	// 【步骤 6：添加 Fsync 参数】
	// fsync=true 确保数据写入磁盘后才返回成功
	// 牺牲性能换取数据安全性，适用于重要文件
	if so.Fsync {
		urlLocation += "?fsync=true"
	}

	// 【步骤 7：返回认证令牌】
	// JWT 令牌用于上传时的身份验证
	auth = assignResult.Auth
	return
}

// PostHandler 处理所有 POST/PUT 写入请求的统一入口
//
// 【核心职责】
//   - 解析 URL 查询参数和请求头，构造存储策略（StorageOption）
//   - 根据特殊参数判断操作类型：上传、移动、复制
//   - 执行 WORM 只读检查、文件名长度验证
//   - 分发到具体的处理函数（autoChunk/move/copy）
//
// 【参数说明】
//   - w: HTTP 响应写入器
//   - r: HTTP 请求对象
//   - contentLength: 请求内容长度（已从 filerHandler 中提取）
//
// 【支持的操作类型】
//   1. 文件上传（默认）: 无特殊参数，调用 autoChunk
//   2. 文件移动: URL 包含 ?mv.from=<源路径>
//   3. 文件复制: URL 包含 ?cp.from=<源路径>
//
// 【存储策略参数（URL Query）】
//   - collection: 文件集合名称，用于逻辑分组
//   - replication: 副本策略，如 "001"（同机架1副本）、"100"（跨数据中心1副本）
//   - ttl: 生存时间，如 "3m"（3分钟）、"2h"（2小时）、"7d"（7天）
//   - disk: 磁盘类型，如 "hdd"、"ssd"、"nvme"
//   - fsync: 是否强制同步写入磁盘，"true" 或 "false"
//   - dataCenter: 指定数据中心
//   - rack: 指定机架
//   - dataNode: 指定数据节点
//   - saveInside: 小文件内联存储，"true" 或 "false"
//
// 【特殊路径处理】
//   - /etc 路径下的文件强制 saveInside=true（配置文件内联存储）
//
// 【错误处理】
//   - ErrReadOnly: 返回 507 Insufficient Storage
//   - 其他错误: 返回 500 Internal Server Error
//   - 文件名过长: 返回 414 Request-URI Too Long
func (fs *FilerServer) PostHandler(w http.ResponseWriter, r *http.Request, contentLength int64) {
	ctx := r.Context()

	// 【步骤 1：确定目标路径】
	// 优先使用 S3 协议的目标路径头部（用于多部分上传的重定向）
	destination := r.RequestURI
	if finalDestination := r.Header.Get(s3_constants.SeaweedStorageDestinationHeader); finalDestination != "" {
		destination = finalDestination
	}

	// 【步骤 2：解析存储策略参数】
	// 从 URL 查询参数中提取所有存储相关配置
	query := r.URL.Query()
	so, err := fs.detectStorageOption0(ctx, destination,
		query.Get("collection"),    // 集合名称
		query.Get("replication"),   // 副本策略
		query.Get("ttl"),           // 生存时间
		query.Get("disk"),          // 磁盘类型
		query.Get("fsync"),         // 是否强制同步
		query.Get("dataCenter"),    // 数据中心
		query.Get("rack"),          // 机架
		query.Get("dataNode"),      // 数据节点
		query.Get("saveInside"),    // 内联存储
	)
	if err != nil {
		// 处理存储策略错误
		if err == ErrReadOnly {
			// 路径为只读，返回 507（存储不足/不可写）
			w.WriteHeader(http.StatusInsufficientStorage)
		} else {
			// 其他错误（如 TTL 解析失败）
			glog.V(1).InfolnCtx(ctx, "post", r.RequestURI, ":", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
		}
		return
	}

	// 【步骤 3：验证文件名长度】
	// 检查文件名是否超过配置的最大长度限制
	// 不同文件系统有不同的限制（ext4=255, NTFS=255, 等）
	if util.FullPath(r.URL.Path).IsLongerFileName(so.MaxFileNameLength) {
		glog.V(1).InfolnCtx(ctx, "post", r.RequestURI, ": ", "entry name too long")
		w.WriteHeader(http.StatusRequestURITooLong)
		return
	}

	// 【步骤 4：设置默认磁盘类型】
	// 如果查询参数未指定磁盘类型，使用 Filer 启动时的默认值
	if so.DiskType == "" {
		so.DiskType = fs.option.DiskType
	}

	// 【步骤 5：特殊路径处理】
	// /etc 路径用于存储配置文件，强制使用内联存储
	// 好处：配置文件通常很小，内联存储避免额外的 Volume 查询
	if strings.HasPrefix(r.URL.Path, "/etc") {
		so.SaveInside = true
	}

	// 【步骤 6：根据操作类型分发】
	if query.Has("mv.from") {
		// 文件移动操作
		// 示例: POST /path/to/dest?mv.from=/path/to/source
		fs.move(ctx, w, r, so)
	} else if query.Has("cp.from") {
		// 文件复制操作
		// 示例: POST /path/to/dest?cp.from=/path/to/source
		fs.copy(ctx, w, r, so)
	} else {
		// 文件上传操作（默认）
		// 支持 multipart/form-data 和单文件上传
		fs.autoChunk(ctx, w, r, contentLength, so)
	}

	// 【步骤 7：清理请求资源】
	// 关闭请求 Body，释放连接资源
	util_http.CloseRequest(r)

}

// move 执行文件或目录的移动操作（重命名）
//
// 【功能说明】
//   - 将源路径的文件/目录移动到目标路径
//   - 支持同目录重命名和跨目录移动
//   - 通过 AtomicRenameEntry 保证原子性（不会出现中间状态）
//   - 执行 WORM 只读检查，防止移动受保护的文件
//
// 【URL 格式】
//   POST /path/to/dest?mv.from=/path/to/source
//
// 【移动规则】
//   1. 源路径必须存在
//   2. 源路径不能是根目录 "/"
//   3. 源路径不能是 WORM 保护的文件/目录
//   4. 不能用目录覆盖非目录
//   5. 目标文件名长度不能超过限制
//
// 【原子性保证】
//   - 使用 AtomicRenameEntry RPC 实现原子移动
//   - 移动过程中其他客户端要么看到旧路径，要么看到新路径
//   - 不会出现文件消失或同时存在于两个路径的情况
//
// 【成功响应】
//   - HTTP 204 No Content（无响应体）
//
// 【错误响应】
//   - 400 Bad Request: 路径格式错误、源不存在、目录覆盖冲突
//   - 403 Forbidden: 源路径被 WORM 保护
//   - 500 Internal Server Error: 元数据存储错误
func (fs *FilerServer) move(ctx context.Context, w http.ResponseWriter, r *http.Request, so *operation.StorageOption) {
	// 【步骤 1：提取源路径和目标路径】
	src := r.URL.Query().Get("mv.from") // 源路径
	dst := r.URL.Path                   // 目标路径

	glog.V(2).InfofCtx(ctx, "FilerServer.move %v to %v", src, dst)

	// 【步骤 2：清理和验证路径格式】
	var err error
	// clearName 会去除路径中的 ".." 等不安全字符
	if src, err = clearName(src); err != nil {
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}
	if dst, err = clearName(dst); err != nil {
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}

	// 【步骤 3：验证源路径有效性】
	// 去除尾部斜杠，规范化路径
	src = strings.TrimRight(src, "/")
	if src == "" {
		// 不允许移动根目录
		err = fmt.Errorf("invalid source '/'")
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}

	// 【步骤 4：验证目标文件名长度】
	srcPath := util.FullPath(src)
	dstPath := util.FullPath(dst)
	if dstPath.IsLongerFileName(so.MaxFileNameLength) {
		err = fmt.Errorf("dst name to long")
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}

	// 【步骤 5：查找源条目】
	// 验证源路径是否存在
	srcEntry, err := fs.filer.FindEntry(ctx, srcPath)
	if err != nil {
		err = fmt.Errorf("failed to get src entry '%s', err: %s", src, err)
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}

	// 【步骤 6：WORM 只读检查】
	// 检查源路径是否被 WORM（Write Once Read Many）策略保护
	wormEnforced, err := fs.wormEnforcedForEntry(ctx, src)
	if err != nil {
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	} else if wormEnforced {
		// WORM 保护的文件/目录不能移动或重命名
		err = fmt.Errorf("cannot move write-once entry from '%s' to '%s': %s", src, dst, constants.ErrMsgOperationNotPermitted)
		writeJsonError(w, r, http.StatusForbidden, err)
		return
	}

	// 【步骤 7：解析源和目标的目录/文件名】
	oldDir, oldName := srcPath.DirAndName() // 源目录和文件名
	newDir, newName := dstPath.DirAndName() // 目标目录和文件名
	// 如果目标路径以 / 结尾（表示目录），则保留原文件名
	newName = util.Nvl(newName, oldName)

	// 【步骤 8：检查目标路径是否存在】
	dstEntry, err := fs.filer.FindEntry(ctx, util.FullPath(strings.TrimRight(dst, "/")))
	if err != nil && err != filer_pb.ErrNotFound {
		// 查询失败（非"不存在"错误）
		err = fmt.Errorf("failed to get dst entry '%s', err: %s", dst, err)
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}

	// 【步骤 9：验证目录覆盖规则】
	// 不能用目录覆盖非目录（文件）
	if err == nil && !dstEntry.IsDirectory() && srcEntry.IsDirectory() {
		err = fmt.Errorf("move: cannot overwrite non-directory '%s' with directory '%s'", dst, src)
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}

	// 【步骤 10：执行原子移动】
	// AtomicRenameEntry 保证移动操作的原子性
	// 底层实现：
	//   1. 在新位置创建元数据
	//   2. 删除旧位置元数据
	//   3. 两步操作在事务中完成
	_, err = fs.AtomicRenameEntry(ctx, &filer_pb.AtomicRenameEntryRequest{
		OldDirectory: oldDir,  // 源目录
		OldName:      oldName, // 源文件名
		NewDirectory: newDir,  // 目标目录
		NewName:      newName, // 目标文件名
	})
	if err != nil {
		err = fmt.Errorf("failed to move entry from '%s' to '%s', err: %s", src, dst, err)
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}

	// 【步骤 11：返回成功响应】
	// HTTP 204 No Content 表示操作成功但无响应体
	w.WriteHeader(http.StatusNoContent)
}

// DeleteHandler 处理文件或目录的删除请求
//
// 【功能说明】
//   - 删除指定路径的文件或目录
//   - 支持递归删除目录及其所有内容
//   - 执行 WORM 只读检查，防止删除受保护的数据
//   - 可选择是否删除 Volume 上的实际数据块（chunk）
//
// 【URL 示例】
//   curl -X DELETE http://localhost:8888/path/to/file
//   curl -X DELETE http://localhost:8888/path/to/dir?recursive=true
//   curl -X DELETE http://localhost:8888/path/to/dir?recursive=true&ignoreRecursiveError=true
//   curl -X DELETE http://localhost:8888/path/to/file?skipChunkDeletion=true
//
// 【URL 查询参数】
//   - recursive: 是否递归删除目录（"true"/"false"）
//     * true: 删除目录及其所有子文件/子目录
//     * false: 仅删除空目录或单个文件
//   - ignoreRecursiveError: 递归删除时是否忽略子项错误（"true"/"false"）
//     * true: 即使某些子项删除失败也继续
//     * false: 任何子项删除失败则停止并返回错误
//   - skipChunkDeletion: 是否跳过 Volume 数据块删除（"true"/"false"）
//     * true: 仅删除元数据，保留 Volume 上的实际数据（节省删除时间）
//     * false: 同时删除元数据和 Volume 数据块（默认）
//
// 【Filer 全局配置】
//   - 如果 Filer 启动时设置了 -recursiveDelete=true
//     则默认启用递归删除（除非显式指定 recursive=false）
//
// 【删除流程】
//   1. 解析查询参数（recursive、ignoreRecursiveError、skipChunkDeletion）
//   2. 规范化路径（去除尾部斜杠）
//   3. WORM 只读检查
//   4. 调用 DeleteEntryMetaAndData 删除元数据和数据
//   5. 返回 HTTP 204 No Content
//
// 【成功响应】
//   - HTTP 204 No Content（无响应体）
//
// 【错误响应】
//   - 403 Forbidden: 路径被 WORM 保护
//   - 404 Not Found: 路径不存在（不会返回错误，视为成功）
//   - 500 Internal Server Error: 删除失败（元数据存储错误）
func (fs *FilerServer) DeleteHandler(w http.ResponseWriter, r *http.Request) {
	// 【步骤 1：解析递归删除参数】
	// 优先使用 URL 查询参数，其次使用 Filer 全局配置
	isRecursive := r.FormValue("recursive") == "true"
	if !isRecursive && fs.option.recursiveDelete {
		// Filer 启动时配置了默认递归删除
		// 但允许通过 recursive=false 显式禁用
		if r.FormValue("recursive") != "false" {
			isRecursive = true
		}
	}

	// 【步骤 2：解析其他删除选项】
	// ignoreRecursiveError: 递归删除时是否忽略子项错误
	ignoreRecursiveError := r.FormValue("ignoreRecursiveError") == "true"
	// skipChunkDeletion: 是否跳过 Volume 数据块删除（仅删除元数据）
	skipChunkDeletion := r.FormValue("skipChunkDeletion") == "true"

	// 【步骤 3：规范化路径】
	// 去除尾部斜杠，确保路径格式一致
	objectPath := r.URL.Path
	if len(r.URL.Path) > 1 && strings.HasSuffix(objectPath, "/") {
		objectPath = objectPath[0 : len(objectPath)-1]
	}

	// 【步骤 4：WORM 只读检查】
	// 检查路径是否被 WORM（Write Once Read Many）策略保护
	wormEnforced, err := fs.wormEnforcedForEntry(context.TODO(), objectPath)
	if err != nil {
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	} else if wormEnforced {
		// WORM 保护的文件/目录不能删除
		writeJsonError(w, r, http.StatusForbidden, errors.New(constants.ErrMsgOperationNotPermitted))
		return
	}

	// 【步骤 5：执行删除操作】
	// DeleteEntryMetaAndData 会：
	//   1. 删除 Filer 元数据（目录树、文件属性等）
	//   2. 如果 !skipChunkDeletion，向 Volume Server 发送删除 chunk 请求
	//   3. 如果 isRecursive，递归删除所有子项
	err = fs.filer.DeleteEntryMetaAndData(
		context.Background(),
		util.FullPath(objectPath),
		isRecursive,           // 是否递归删除
		ignoreRecursiveError,  // 是否忽略递归错误
		!skipChunkDeletion,    // 是否删除 Volume chunk
		false,                 // 是否来自内部递归调用
		nil,                   // 递归删除时的进度通知回调
		0,                     // 递归删除时的起始签名（用于分页）
	)

	// 【步骤 6：处理删除结果】
	// ErrNotFound 不视为错误（幂等删除）
	if err != nil && err != filer_pb.ErrNotFound {
		glog.V(1).Infoln("deleting", objectPath, ":", err.Error())
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}

	// 【步骤 7：返回成功响应】
	// HTTP 204 No Content 表示删除成功（无响应体）
	w.WriteHeader(http.StatusNoContent)
}

// detectStorageOption 推导并构造存储策略配置
//
// 【功能说明】
//   - 根据路径匹配 FilerConf 中的存储规则
//   - 综合考虑查询参数、配置规则、全局默认值
//   - 构造最终的 StorageOption 对象
//   - 用于 gRPC 接口（传入 ttlSeconds）
//
// 【参数说明】
//   - ctx: 请求上下文
//   - requestURI: 请求路径，用于匹配存储规则
//   - qCollection: 查询参数中的集合名称
//   - qReplication: 查询参数中的副本策略
//   - ttlSeconds: TTL 秒数（0 表示使用规则或默认值）
//   - diskType: 磁盘类型（hdd/ssd/nvme）
//   - dataCenter: 数据中心名称
//   - rack: 机架名称
//   - dataNode: 数据节点地址
//
// 【返回值】
//   - StorageOption: 存储策略配置对象
//   - error: ErrReadOnly（路径只读）或 nil
//
// 【优先级规则】（从高到低）
//   1. URL 查询参数（qCollection、qReplication 等）
//   2. FilerConf 路径匹配规则（rule.Collection、rule.Replication 等）
//   3. S3 Bucket 默认集合（针对 /buckets/ 路径）
//   4. Filer 全局默认值（fs.option.DefaultReplication 等）
//
// 【FilerConf 规则匹配】
//   - 使用最长前缀匹配算法
//   - 例如: /data/images/ 规则优先于 /data/ 规则
//   - 规则可配置: collection、replication、ttl、fsync、readOnly 等
//
// 【S3 Bucket 特殊处理】
//   - 如果路径以 /buckets/ 开头，自动使用 bucket 名称作为 collection
//   - 例如: /buckets/my-bucket/file.jpg -> collection = "my-bucket"
func (fs *FilerServer) detectStorageOption(ctx context.Context, requestURI, qCollection, qReplication string, ttlSeconds int32, diskType, dataCenter, rack, dataNode string) (*operation.StorageOption, error) {

	// 【步骤 1：匹配 FilerConf 存储规则】
	// 根据请求路径找到最匹配的规则配置
	rule := fs.filer.FilerConf.MatchStorageRule(requestURI)

	// 【步骤 2：只读检查】
	// 如果规则标记为只读，拒绝写入操作
	if rule.ReadOnly {
		return nil, ErrReadOnly
	}

	// 【步骤 3：设置文件名长度限制】
	// 使用规则配置的值，如果未配置则使用 Filer 全局默认值
	if rule.MaxFileNameLength == 0 {
		rule.MaxFileNameLength = fs.filer.MaxFilenameLength
	}

	// 【步骤 4：S3 Bucket 集合推导】
	// 如果路径以 /buckets/ 开头，使用 bucket 名称作为默认集合
	// 好处：S3 的每个 bucket 自动隔离存储，便于管理和删除
	bucketDefaultCollection := ""
	if strings.HasPrefix(requestURI, fs.filer.DirBucketsPath+"/") {
		bucketDefaultCollection = fs.filer.DetectBucket(util.FullPath(requestURI))
	}

	// 【步骤 5：TTL 解析】
	// 如果传入的 ttlSeconds 为 0，从规则中解析 TTL
	if ttlSeconds == 0 {
		ttl, err := needle.ReadTTL(rule.GetTtl())
		if err != nil {
			glog.ErrorfCtx(ctx, "fail to parse %s ttl setting %s: %v", rule.LocationPrefix, rule.Ttl, err)
		}
		ttlSeconds = int32(ttl.Minutes()) * 60
	}

	// 【步骤 6：构造 StorageOption】
	// 使用 util.Nvl（Non-null value）选择第一个非空值
	// 优先级：查询参数 > 规则配置 > Bucket 默认 > Filer 全局默认
	return &operation.StorageOption{
		// 副本策略：优先使用查询参数，其次规则，最后全局默认
		Replication: util.Nvl(qReplication, rule.Replication, fs.option.DefaultReplication),
		// 集合名称：优先查询参数，其次规则，再次 Bucket 名，最后全局默认
		Collection: util.Nvl(qCollection, rule.Collection, bucketDefaultCollection, fs.option.Collection),
		// 数据中心：优先查询参数，其次规则，最后全局默认
		DataCenter: util.Nvl(dataCenter, rule.DataCenter, fs.option.DataCenter),
		// 机架：优先查询参数，其次规则，最后全局默认
		Rack: util.Nvl(rack, rule.Rack, fs.option.Rack),
		// 数据节点：优先查询参数，其次规则，最后全局默认
		DataNode: util.Nvl(dataNode, rule.DataNode, fs.option.DataNode),
		// TTL 秒数
		TtlSeconds: ttlSeconds,
		// 磁盘类型：优先查询参数，其次规则
		DiskType: util.Nvl(diskType, rule.DiskType),
		// 强制同步写入（从规则继承）
		Fsync: rule.Fsync,
		// Volume 增长数量（从规则继承）
		VolumeGrowthCount: rule.VolumeGrowthCount,
		// 最大文件名长度
		MaxFileNameLength: rule.MaxFileNameLength,
	}, nil
}

// detectStorageOption0 是 HTTP 接口专用的存储策略推导函数
//
// 【功能说明】
//   - 从字符串参数解析 TTL、Fsync、SaveInside
//   - 调用 detectStorageOption 获取基础配置
//   - 覆盖 Fsync 和 SaveInside 的布尔值
//
// 【参数说明】
//   - ctx: 请求上下文
//   - requestURI: 请求路径
//   - qCollection: 查询参数中的集合名称
//   - qReplication: 查询参数中的副本策略
//   - qTtl: 查询参数中的 TTL 字符串（如 "3m"、"2h"、"7d"）
//   - diskType: 磁盘类型
//   - fsync: 是否强制同步（"true"/"false"）
//   - dataCenter: 数据中心
//   - rack: 机架
//   - dataNode: 数据节点
//   - saveInside: 是否内联存储（"true"/"false"）
//
// 【TTL 格式】
//   - "3m": 3分钟
//   - "2h": 2小时
//   - "7d": 7天
//   - "": 永不过期（默认）
//
// 【与 detectStorageOption 的区别】
//   - detectStorageOption: 接收 ttlSeconds 整数，用于 gRPC
//   - detectStorageOption0: 接收 qTtl 字符串，用于 HTTP
func (fs *FilerServer) detectStorageOption0(ctx context.Context, requestURI, qCollection, qReplication string, qTtl string, diskType string, fsync string, dataCenter, rack, dataNode, saveInside string) (*operation.StorageOption, error) {

	// 【步骤 1：解析 TTL 字符串】
	// ReadTTL 将 "3m"、"2h"、"7d" 等字符串转换为 time.Duration
	ttl, err := needle.ReadTTL(qTtl)
	if err != nil {
		glog.ErrorfCtx(ctx, "fail to parse ttl %s: %v", qTtl, err)
	}

	// 【步骤 2：调用基础推导函数】
	// 将 TTL 转换为秒数传入
	so, err := fs.detectStorageOption(ctx, requestURI, qCollection, qReplication, int32(ttl.Minutes())*60, diskType, dataCenter, rack, dataNode)

	// 【步骤 3：覆盖布尔参数】
	if so != nil {
		// Fsync 参数：显式指定时覆盖规则配置
		if fsync == "false" {
			so.Fsync = false
		} else if fsync == "true" {
			so.Fsync = true
		}
		// SaveInside 参数：是否将小文件内联存储到元数据中
		if saveInside == "true" {
			so.SaveInside = true
		} else {
			so.SaveInside = false
		}
	}

	return so, err
}
