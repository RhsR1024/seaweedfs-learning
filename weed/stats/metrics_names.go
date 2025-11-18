// Package stats 定义了 SeaweedFS 系统中所有的指标名称常量
// 这些指标用于监控、统计和报警,涵盖 Volume Server、Master、Filer 和 S3 等各个组件
package stats

// 本文件包含所有错误和操作的指标名称
// 命名约定: ErrorSomeThing = "error.some.thing"
// 这些指标名称会被 Prometheus/Grafana 等监控系统使用
const (
	// ============================================================
	// Volume Server 相关指标
	// Volume Server 是存储实际数据的节点,处理文件的读写请求
	// ============================================================

	// WriteToLocalDisk Volume Server 写入本地磁盘的操作指标
	// 统计将 needle 数据写入本地 .dat 文件的次数
	WriteToLocalDisk = "writeToLocalDisk"

	// WriteToReplicas Volume Server 写入副本的操作指标
	// 统计将数据同步到其他副本节点的次数
	WriteToReplicas = "writeToReplicas"

	// DownloadLimitCond 下载限流条件触发指标
	// 当下载速度超过限制时触发,用于流量控制
	DownloadLimitCond = "downloadLimitCondition"

	// UploadLimitCond 上传限流条件触发指标
	// 当上传速度超过限制时触发,用于流量控制
	UploadLimitCond = "uploadLimitCondition"

	// ReadProxyReq 读代理请求指标
	// 当前 Volume Server 代理读取其他 Volume Server 数据的次数
	ReadProxyReq = "readProxyRequest"

	// ReadRedirectReq 读重定向请求指标
	// 将客户端重定向到其他 Volume Server 读取数据的次数
	ReadRedirectReq = "readRedirectRequest"

	// EmptyReadProxyLoc 空的读代理位置错误指标
	// 无法找到代理读取的目标位置时触发
	EmptyReadProxyLoc = "emptyReadProxyLocaction"

	// FailedReadProxyReq 读代理请求失败指标
	// 代理读取其他 Volume Server 失败的次数
	FailedReadProxyReq = "failedReadProxyRequest"

	// ErrorSizeMismatchOffsetSize 偏移量与大小不匹配错误
	// needle 数据的 offset 和 size 不一致时触发
	ErrorSizeMismatchOffsetSize = "errorSizeMismatchOffsetSize"

	// ErrorSizeMismatch 大小不匹配错误
	// 读取的数据大小与预期不符时触发
	ErrorSizeMismatch = "errorSizeMismatch"

	// ErrorCRC CRC 校验错误指标
	// needle 数据的 CRC32 校验失败时触发,表示数据损坏
	ErrorCRC = "errorCRC"

	// ErrorIndexOutOfRange 索引超出范围错误
	// needle offset 超出索引范围时触发
	ErrorIndexOutOfRange = "errorIndexOutOfRange"

	// ErrorGetNotFound GET 请求未找到错误
	// 请求的 needle ID 不存在时触发 (404)
	ErrorGetNotFound = "errorGetNotFound"

	// ErrorGetInternal GET 请求内部错误
	// 处理 GET 请求时发生内部错误 (500)
	ErrorGetInternal = "errorGetInternal"

	// ============================================================
	// Master Topology 相关指标
	// Master 节点管理集群拓扑和卷的分配
	// ============================================================

	// ErrorWriteToLocalDisk 写入本地磁盘错误指标
	// Master 节点写入元数据到本地磁盘失败
	ErrorWriteToLocalDisk = "errorWriteToLocalDisk"

	// ErrorUnmarshalPairs 反序列化键值对错误
	// 解析 Raft 日志或元数据时反序列化失败
	ErrorUnmarshalPairs = "errorUnmarshalPairs"

	// ErrorWriteToReplicas 写入副本错误指标
	// Master 节点同步元数据到副本失败
	ErrorWriteToReplicas = "errorWriteToReplicas"

	// ============================================================
	// Master Client 相关指标
	// Master Client 是其他组件与 Master 通信的客户端
	// ============================================================

	// FailedToKeepConnected 保持连接失败指标
	// 与 Master 节点的长连接断开或无法保持
	FailedToKeepConnected = "failedToKeepConnected"

	// FailedToSend 发送失败指标
	// 向 Master 发送请求失败
	FailedToSend = "failedToSend"

	// FailedToReceive 接收失败指标
	// 从 Master 接收响应失败
	FailedToReceive = "failedToReceive"

	// RedirectedToLeader 重定向到 Leader 指标
	// 请求被重定向到 Master Leader 节点的次数
	RedirectedToLeader = "redirectedToLeader"

	// OnPeerUpdate 节点更新指标
	// Master 集群节点信息更新的次数
	OnPeerUpdate = "onPeerUpdate"

	// Failed 通用失败指标
	// 未分类的失败操作
	Failed = "failed"

	// ============================================================
	// Filer Handler 相关指标
	// Filer 提供文件系统接口,处理文件的元数据和分块
	// ============================================================

	// DirList 目录列表操作指标
	// 列出目录内容的请求次数
	DirList = "dirList"

	// ContentSaveToFiler 内容保存到 Filer 指标
	// 将文件内容保存到 Filer 元数据存储的次数
	ContentSaveToFiler = "contentSaveToFiler"

	// AutoChunk 自动分块指标
	// 大文件自动分块处理的次数
	AutoChunk = "autoChunk"

	// ChunkProxy 块代理指标
	// Filer 代理读取 chunk 数据的次数
	ChunkProxy = "chunkProxy"

	// ChunkAssign 块分配指标
	// 向 Master 请求分配新 chunk 的次数
	ChunkAssign = "chunkAssign"

	// ChunkUpload 块上传指标
	// 上传 chunk 到 Volume Server 的次数
	ChunkUpload = "chunkUpload"

	// ChunkMerge 块合并指标
	// 合并多个 chunk 的操作次数
	ChunkMerge = "chunkMerge"

	// ChunkDoUploadRetry 块上传执行重试指标
	// chunk 上传实际重试的次数
	ChunkDoUploadRetry = "chunkDoUploadRetry"

	// ChunkUploadRetry 块上传重试指标
	// chunk 上传需要重试的次数
	ChunkUploadRetry = "chunkUploadRetry"

	// ChunkAssignRetry 块分配重试指标
	// chunk 分配需要重试的次数
	ChunkAssignRetry = "chunkAssignRetry"

	// ErrorReadNotFound 读取未找到错误
	// 请求的文件或 chunk 不存在 (404)
	ErrorReadNotFound = "read.notfound"

	// ErrorReadInternal 读取内部错误
	// 处理读取请求时发生内部错误 (500)
	ErrorReadInternal = "read.internal.error"

	// ErrorWriteEntry 写入条目失败错误
	// 写入文件元数据条目失败
	ErrorWriteEntry = "write.entry.failed"

	// RepeatErrorUploadContent 重复上传内容失败错误
	// 多次尝试上传内容后仍然失败
	RepeatErrorUploadContent = "upload.content.repeat.failed"

	// ErrorChunkAssign 块分配失败错误
	// 无法从 Master 获取 chunk 分配
	ErrorChunkAssign = "chunkAssign.failed"

	// ErrorReadChunk 读取块失败错误
	// 从 Volume Server 读取 chunk 失败
	ErrorReadChunk = "read.chunk.failed"

	// ErrorReadCache 读取缓存失败错误
	// 从缓存读取数据失败
	ErrorReadCache = "read.cache.failed"

	// ErrorReadStream 读取流失败错误
	// 流式读取数据失败
	ErrorReadStream = "read.stream.failed"

	// ============================================================
	// S3 Handler 相关指标
	// S3 Handler 实现了 S3 兼容的 API 接口
	// ============================================================

	// ErrorCompletedNoSuchUpload 完成时找不到上传错误
	// 完成多部分上传时找不到对应的 upload ID
	ErrorCompletedNoSuchUpload = "errorCompletedNoSuchUpload"

	// ErrorCompleteEntityTooSmall 完成时实体太小错误
	// 多部分上传的某个部分小于最小大小 (通常是 5MB)
	ErrorCompleteEntityTooSmall = "errorCompleteEntityTooSmall"

	// ErrorCompletedPartEmpty 完成时部分为空错误
	// 多部分上传的部分列表为空
	ErrorCompletedPartEmpty = "errorCompletedPartEmpty"

	// ErrorCompletedPartNumber 完成时部分编号错误
	// 多部分上传的部分编号无效或不连续
	ErrorCompletedPartNumber = "errorCompletedPartNumber"

	// ErrorCompletedPartNotFound 完成时部分未找到错误
	// 多部分上传时某个部分不存在
	ErrorCompletedPartNotFound = "errorCompletedPartNotFound"

	// ErrorCompletedEtagInvalid 完成时 ETag 无效错误
	// 多部分上传时提供的 ETag 格式无效
	ErrorCompletedEtagInvalid = "errorCompletedEtagInvalid"

	// ErrorCompletedEtagMismatch 完成时 ETag 不匹配错误
	// 多部分上传时 ETag 与实际内容不符
	ErrorCompletedEtagMismatch = "errorCompletedEtagMismatch"

	// ErrorCompletedPartEntryMismatch 完成时部分条目不匹配错误
	// 多部分上传时部分的元数据与实际数据不一致
	ErrorCompletedPartEntryMismatch = "errorCompletedPartEntryMismatch"
)
