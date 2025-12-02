// Package weed_server 中的 filer_grpc_server.go 提供 Filer gRPC API 的核心实现
// 负责目录项 CRUD、卷信息查询、分配文件上传位置等功能，所有逻辑均保持与上游一致。
package weed_server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

// LookupDirectoryEntry 根据目录和文件名查询单个目录项
// 参数:
//   - ctx: 请求上下文，承载链路日志信息
//   - req: gRPC 请求，内含 Directory/Name
// 返回:
//   - LookupDirectoryEntryResponse: 命中时带回 Filer Entry
//   - error: 找不到返回 filer_pb.ErrNotFound，发生其他异常时返回错误详情
func (fs *FilerServer) LookupDirectoryEntry(ctx context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {

	glog.V(4).InfofCtx(ctx, "LookupDirectoryEntry %s", filepath.Join(req.Directory, req.Name))

	// 【步骤 1: 从 Filer Store 中查找 Entry】
	// 拼接完整路径并调用 FindEntry 查询
	// FindEntry 会:
	//   1. 先查询本地缓存
	//   2. 如果缓存未命中，从 Store 后端（MySQL/PostgreSQL/LevelDB 等）查询
	//   3. 查询到的 Entry 包含文件元数据和 chunk 列表
	entry, err := fs.filer.FindEntry(ctx, util.JoinPath(req.Directory, req.Name))

	// 【步骤 2: 处理未找到的情况】
	// 如果是 ErrNotFound，返回空响应但保留错误
	// 这允许客户端区分"文件不存在"和"查询失败"
	if err == filer_pb.ErrNotFound {
		return &filer_pb.LookupDirectoryEntryResponse{}, err
	}

	// 【步骤 3: 处理其他错误】
	// 可能的错误：
	//   - 存储后端连接失败
	//   - 数据库查询超时
	//   - 权限错误
	if err != nil {
		glog.V(3).InfofCtx(ctx, "LookupDirectoryEntry %s: %+v, ", filepath.Join(req.Directory, req.Name), err)
		return nil, err
	}

	// 【步骤 4: 成功查询，转换并返回】
	// 将内部 Entry 结构体转换为 protobuf 格式
	return &filer_pb.LookupDirectoryEntryResponse{
		Entry: entry.ToProtoEntry(),
	}, nil
}

// ListEntries 通过流式 gRPC 接口列出目录下的文件
// 参数:
//   - req: 包含目录、分页游标、前缀过滤等信息
//   - stream: Server 端流，用于逐条发送 filer_pb.ListEntriesResponse
// 返回:
//   - error: 发送流失败或底层 Store 异常会返回错误，其余情况返回 nil
// 说明:
//   - 函数会按分页窗口逐段列举，直到达到 limit 或目录被遍历完毕
func (fs *FilerServer) ListEntries(req *filer_pb.ListEntriesRequest, stream filer_pb.SeaweedFiler_ListEntriesServer) (err error) {

	glog.V(4).Infof("ListEntries %v", req)

	// 【步骤 1: 确定总数限制】
	// 如果客户端未指定 limit，使用服务端默认配置
	// 默认值通常为 1000 或配置文件中的 DirListingLimit
	limit := int(req.Limit)
	if limit == 0 {
		limit = fs.option.DirListingLimit
	}

	// 【步骤 2: 确定分页窗口大小】
	// PaginationSize 是每次从 Store 读取的批次大小（通常为 1024）
	// 如果总 limit 更小，则使用 limit 作为窗口大小
	// 这样可以避免读取超过需要的条目
	paginationLimit := filer.PaginationSize
	if limit < paginationLimit {
		paginationLimit = limit
	}

	// 【步骤 3: 初始化分页游标和标志】
	lastFileName := req.StartFromFileName   // 分页游标：从哪个文件名之后开始
	includeLastFile := req.InclusiveStartFrom // 是否包含游标指向的文件本身
	var listErr error

	// 【步骤 4: 分页循环】
	// 持续读取直到达到 limit 或没有更多条目
	for limit > 0 {
		var hasEntries bool // 标记本批次是否有条目返回

		// 【步骤 4.1: 调用 StreamListDirectoryEntries 读取一批条目】
		// 参数说明:
		//   - stream.Context(): gRPC 上下文，用于超时和取消
		//   - req.Directory: 要列出的目录路径
		//   - lastFileName: 分页游标
		//   - includeLastFile: 是否包含游标文件
		//   - paginationLimit: 本批次最多读取多少条目
		//   - req.Prefix: 文件名前缀过滤（例如只列出 "test" 开头的文件）
		//   - "": 不限制 collection
		//   - "": 不限制 replication
		//   - 回调函数: 对每个条目执行
		lastFileName, listErr = fs.filer.StreamListDirectoryEntries(stream.Context(), util.FullPath(req.Directory), lastFileName, includeLastFile, int64(paginationLimit), req.Prefix, "", "", func(entry *filer.Entry) bool {
			hasEntries = true // 标记有条目

			// 【步骤 4.2: 通过流发送条目到客户端】
			// 使用 gRPC stream.Send 逐个发送
			// 如果发送失败（例如客户端断开连接），停止遍历
			if err = stream.Send(&filer_pb.ListEntriesResponse{
				Entry: entry.ToProtoEntry(),
			}); err != nil {
				return false // 停止遍历
			}

			// 【步骤 4.3: 递减剩余限制】
			limit--
			if limit == 0 {
				// 已发送足够数量的条目，停止遍历
				return false
			}
			return true // 继续处理下一个条目
		})

		// 【步骤 4.4: 错误处理】
		// StreamListDirectoryEntries 本身的错误（如 Store 查询失败）
		if listErr != nil {
			return listErr
		}
		// stream.Send 的错误
		if err != nil {
			return err
		}

		// 【步骤 4.5: 检查是否已遍历完毕】
		// 如果本批次没有返回任何条目，说明目录已遍历完
		if !hasEntries {
			return nil
		}

		// 【步骤 4.6: 准备下一轮分页】
		// 第一轮之后，不再包含 lastFileName 对应的条目
		// 避免重复发送同一个文件
		includeLastFile = false

	}

	return nil
}

// LookupVolume 根据卷 ID 列表查询 Volume Server 列表
// 参数:
//   - ctx: 上下文用于透传 trace 和超时
//   - req: 请求中包含待查询的 VolumeIds
// 返回:
//   - LookupVolumeResponse: 以 map 形式返回卷与位置的映射
//   - error: 查询 master 或缓存失败时返回错误，但仍可能包含部分结果
func (fs *FilerServer) LookupVolume(ctx context.Context, req *filer_pb.LookupVolumeRequest) (*filer_pb.LookupVolumeResponse, error) {

	// 【步骤 1: 初始化响应对象】
	// LocationsMap: volumeId -> Locations (包含多个 Volume Server 地址)
	resp := &filer_pb.LookupVolumeResponse{
		LocationsMap: make(map[string]*filer_pb.Locations),
	}

	// 【步骤 2: 使用 Master Client 查询卷位置】
	// LookupVolumeIdsWithFallback 会:
	//   1. 先查询本地缓存
	//   2. 缓存未命中时向 Master 查询
	//   3. 如果主 Master 不可用，尝试备用 Master
	//   4. 返回 map[volumeId] -> []Location
	//
	// 即使部分卷查询失败，也会返回已查询到的结果
	vidLocations, err := fs.filer.MasterClient.LookupVolumeIdsWithFallback(ctx, req.VolumeIds)

	// 【步骤 3: 转换数据格式】
	// 将 wdclient.Location 转换为 filer_pb.Location
	// 即使有错误，也返回部分结果，由客户端决定如何处理
	for vidString, locations := range vidLocations {
		resp.LocationsMap[vidString] = &filer_pb.Locations{
			Locations: wdclientLocationsToPb(locations),
		}
	}

	// 返回响应和可能的错误
	// 错误不为 nil 时表示部分卷查询失败，但 LocationsMap 中包含成功查询的卷
	return resp, err
}

// wdclientLocationsToPb 将 wdclient.Location 转为 gRPC Proto 定义
// 主要用于在 LookupVolume 等接口中复用 master 缓存结果
func wdclientLocationsToPb(locations []wdclient.Location) []*filer_pb.Location {
	locs := make([]*filer_pb.Location, 0, len(locations))
	for _, loc := range locations {
		locs = append(locs, &filer_pb.Location{
			Url:        loc.Url,
			PublicUrl:  loc.PublicUrl,
			GrpcPort:   uint32(loc.GrpcPort),
			DataCenter: loc.DataCenter,
		})
	}
	return locs
}

// lookupFileId 根据 fileId 查找其所在的 Volume 地址列表
// 这是 FilerServer 内部工具函数，供 chunk 清理和 manifest 处理复用
func (fs *FilerServer) lookupFileId(ctx context.Context, fileId string) (targetUrls []string, err error) {
	// 【步骤 1: 解析 file ID】
	// file ID 格式: volumeId,fileKey[_cookie]
	// 例如: "3,01e3b0756f" 或 "3,01e3b0756f_a1b2c3d4"
	fid, err := needle.ParseFileIdFromString(fileId)
	if err != nil {
		return nil, err
	}

	// 【步骤 2: 从 Master Client 缓存中获取 volume 位置】
	// GetLocations 只查询本地缓存，不会向 Master 发起请求
	// 返回该 volume 所在的所有 Volume Server 地址列表（包括副本）
	locations, found := fs.filer.MasterClient.GetLocations(uint32(fid.VolumeId))
	if !found || len(locations) == 0 {
		return nil, fmt.Errorf("not found volume %d in %s", fid.VolumeId, fileId)
	}

	// 【步骤 3: 构建完整的 HTTP URL 列表】
	// 每个 location 对应一个 Volume Server
	// 构造的 URL 格式: http://volumeServer/volumeId,fileKey[_cookie]
	// 客户端可以选择任意一个 URL 进行访问
	for _, loc := range locations {
		targetUrls = append(targetUrls, fmt.Sprintf("http://%s/%s", loc.Url, fileId))
	}
	return
}

// CreateEntry 在指定目录下创建新的元数据记录
// 步骤:
//   1. 调用 cleanupChunks 清理旧块并生成最终 chunk 列表
//   2. 根据路径探测存储策略（TTL、副本等）
//   3. 将 proto Entry 转换为内部 Entry 并写入 filer store
//   4. 成功后异步删除垃圾块
func (fs *FilerServer) CreateEntry(ctx context.Context, req *filer_pb.CreateEntryRequest) (resp *filer_pb.CreateEntryResponse, err error) {

	glog.V(4).InfofCtx(ctx, "CreateEntry %v/%v", req.Directory, req.Entry.Name)

	resp = &filer_pb.CreateEntryResponse{}

	// 【步骤 1: 清理和整理 chunks】
	// cleanupChunks 会:
	//   1. 对比新旧 chunks（此处旧 Entry 为 nil，表示新建文件）
	//   2. 压缩重叠的 chunks
	//   3. 可能将多个小 chunks 合并为 manifest chunk
	//   4. 返回最终的 chunks 和需要删除的垃圾 chunks
	chunks, garbage, err2 := fs.cleanupChunks(ctx, util.Join(req.Directory, req.Entry.Name), nil, req.Entry)
	if err2 != nil {
		return &filer_pb.CreateEntryResponse{}, fmt.Errorf("CreateEntry cleanupChunks %s %s: %v", req.Directory, req.Entry.Name, err2)
	}

	// 【步骤 2: 检测存储选项】
	// 根据文件路径从配置中推导存储策略:
	//   - TTL (Time To Live): 数据自动过期时间
	//   - Replication: 副本策略
	//   - Collection: 集合名称
	//   - DiskType: 磁盘类型（HDD/SSD）
	//   - MaxFileNameLength: 文件名长度限制
	so, err := fs.detectStorageOption(ctx, string(util.NewFullPath(req.Directory, req.Entry.Name)), "", "", 0, "", "", "", "")
	if err != nil {
		return nil, err
	}

	// 【步骤 3: 构建内部 Entry 对象】
	// 将 protobuf Entry 转换为内部 filer.Entry 结构
	newEntry := filer.FromPbEntry(req.Directory, req.Entry)
	newEntry.Chunks = chunks         // 使用清理后的 chunks
	newEntry.TtlSec = so.TtlSeconds  // 应用检测到的 TTL

	// 【步骤 4: 写入 Filer Store】
	// 参数说明:
	//   - req.OExcl: 如果文件已存在是否返回错误（O_EXCL 语义）
	//   - req.IsFromOtherCluster: 是否来自其他集群的同步请求
	//   - req.Signatures: 数字签名（用于跨集群同步验证）
	//   - req.SkipCheckParentDirectory: 是否跳过父目录存在性检查
	//   - so.MaxFileNameLength: 文件名最大长度限制
	createErr := fs.filer.CreateEntry(ctx, newEntry, req.OExcl, req.IsFromOtherCluster, req.Signatures, req.SkipCheckParentDirectory, so.MaxFileNameLength)

	// 【步骤 5: 后处理】
	if createErr == nil {
		// 创建成功，异步删除垃圾 chunks
		// DeleteChunksNotRecursive 会向 Volume Server 发送删除请求
		fs.filer.DeleteChunksNotRecursive(garbage)
	} else {
		// 创建失败，记录错误日志并返回错误信息
		glog.V(3).InfofCtx(ctx, "CreateEntry %s: %v", filepath.Join(req.Directory, req.Entry.Name), createErr)
		resp.Error = createErr.Error()
	}

	return
}

// UpdateEntry 用于替换现有目录项的属性与 Chunk 信息
// 会在更新前读取旧 Entry，计算差异并在成功后触发通知事件
func (fs *FilerServer) UpdateEntry(ctx context.Context, req *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {

	glog.V(4).InfofCtx(ctx, "UpdateEntry %v", req)

	// 【步骤 1: 查找现有 Entry】
	// 必须先加载旧 Entry，用于:
	//   1. 对比新旧 chunks，找出需要删除的垃圾块
	//   2. 检测内容是否真的发生了变化
	fullpath := util.Join(req.Directory, req.Entry.Name)
	entry, err := fs.filer.FindEntry(ctx, util.FullPath(fullpath))
	if err != nil {
		return &filer_pb.UpdateEntryResponse{}, fmt.Errorf("not found %s: %v", fullpath, err)
	}

	// 【步骤 2: 清理和整理 chunks】
	// 与 CreateEntry 不同，这里传入了旧 Entry
	// cleanupChunks 会:
	//   1. 找出旧 Entry 中不再需要的 chunks（垃圾块）
	//   2. 压缩新 chunks 中的重叠部分
	//   3. 可能生成 manifest chunk
	chunks, garbage, err2 := fs.cleanupChunks(ctx, fullpath, entry, req.Entry)
	if err2 != nil {
		return &filer_pb.UpdateEntryResponse{}, fmt.Errorf("UpdateEntry cleanupChunks %s: %v", fullpath, err2)
	}

	// 【步骤 3: 构建新 Entry】
	newEntry := filer.FromPbEntry(req.Directory, req.Entry)
	newEntry.Chunks = chunks // 使用清理后的 chunks

	// 【步骤 4: 检查是否真的有变化】
	// 如果新旧 Entry 完全相同，则无需更新
	// 这可以避免不必要的:
	//   - Store 写入操作
	//   - 通知事件
	//   - 日志记录
	if filer.EqualEntry(entry, newEntry) {
		return &filer_pb.UpdateEntryResponse{}, err
	}

	// 【步骤 5: 执行更新】
	if err = fs.filer.UpdateEntry(ctx, entry, newEntry); err == nil {
		// 【步骤 5.1: 更新成功，删除垃圾 chunks】
		fs.filer.DeleteChunksNotRecursive(garbage)

		// 【步骤 5.2: 发送更新通知事件】
		// 通知订阅者（其他 Filer 节点、mount clients 等）
		// 参数:
		//   - entry: 旧 Entry
		//   - newEntry: 新 Entry
		//   - true: 删除旧 Entry 的 chunks
		//   - req.IsFromOtherCluster: 是否来自其他集群
		//   - req.Signatures: 数字签名
		fs.filer.NotifyUpdateEvent(ctx, entry, newEntry, true, req.IsFromOtherCluster, req.Signatures)

	} else {
		glog.V(3).InfofCtx(ctx, "UpdateEntry %s: %v", filepath.Join(req.Directory, req.Entry.Name), err)
	}

	return &filer_pb.UpdateEntryResponse{}, err
}

// cleanupChunks 对比旧新 chunk 列表，返回需要保留的 chunks 与待删除的垃圾块
// 额外会处理 manifest chunk、追加 chunk 以及按路径推导存储选项
func (fs *FilerServer) cleanupChunks(ctx context.Context, fullpath string, existingEntry *filer.Entry, newEntry *filer_pb.Entry) (chunks, garbage []*filer_pb.FileChunk, err error) {

	// 【步骤 1: 找出不再需要的旧 chunks】
	// 如果 existingEntry 存在（更新操作），计算差集
	// MinusChunks 会找出在旧 Entry 中存在但新 Entry 中不存在的 chunks
	if existingEntry != nil {
		garbage, err = filer.MinusChunks(ctx, fs.lookupFileId, existingEntry.GetChunks(), newEntry.GetChunks())
		if err != nil {
			return newEntry.GetChunks(), nil, fmt.Errorf("MinusChunks: %w", err)
		}
	}

	// 【步骤 2: 分离 manifest chunks 和普通 chunks】
	// manifest chunk 是一种特殊的 chunk，包含其他 chunks 的元数据
	// 通常用于大文件（chunk 数量超过阈值时）
	// manifest chunks 不参与压缩，因为:
	//   1. 它们通常来自追加操作（append-only）
	//   2. 压缩 manifest 会导致额外的网络开销
	manifestChunks, nonManifestChunks := filer.SeparateManifestChunks(newEntry.GetChunks())

	// 【步骤 3: 压缩普通 chunks】
	// CompactFileChunks 会:
	//   1. 找出完全被其他 chunks 覆盖的 chunks（coveredChunks）
	//   2. 返回去重后的 chunks 列表
	// 例如: 文件的 offset 0-100 有 3 个重叠的 chunks，只保留最新的一个
	chunks, coveredChunks := filer.CompactFileChunks(ctx, fs.lookupFileId, nonManifestChunks)
	garbage = append(garbage, coveredChunks...) // 被覆盖的 chunks 也是垃圾

	// 【步骤 4: 可能生成 manifest chunk】
	// 如果 chunks 数量超过阈值，将它们合并为一个 manifest chunk
	if newEntry.Attributes != nil {
		// 检测存储选项（忽略只读错误，因为只需要容量信息）
		so, _ := fs.detectStorageOption(ctx, fullpath,
			"",
			"",
			newEntry.Attributes.TtlSec,
			"",
			"",
			"",
			"",
		)

		// MaybeManifestize 会:
		//   1. 检查 chunks 数量是否超过阈值（通常为 1000）
		//   2. 如果超过，将 chunks 序列化并保存为一个特殊的 chunk
		//   3. 返回包含 manifest chunk 的列表
		chunks, err = filer.MaybeManifestize(fs.saveAsChunk(ctx, so), chunks)
		if err != nil {
			// manifest 生成失败不是致命错误
			// 最坏情况是 chunks 列表较长，但功能仍然正常
			glog.V(0).InfofCtx(ctx, "MaybeManifestize: %v", err)
		}
	}

	// 【步骤 5: 合并 manifest chunks 和压缩后的 chunks】
	// manifest chunks 放在前面，普通 chunks 放在后面
	chunks = append(manifestChunks, chunks...)

	return
}

// AppendToEntry 将追加的 chunk 列表附加到指定 Entry 后面
// 在并发环境下通过集群分布式锁保证 offset 计算的一致性
// 返回:
//   - AppendToEntryResponse: 当前版本只携带错误信息
//   - error: 写入 filer store 失败或锁失败时返回
func (fs *FilerServer) AppendToEntry(ctx context.Context, req *filer_pb.AppendToEntryRequest) (*filer_pb.AppendToEntryResponse, error) {

	glog.V(4).InfofCtx(ctx, "AppendToEntry %v", req)
	fullpath := util.NewFullPath(req.Directory, req.EntryName)

	// 【步骤 1: 获取分布式锁】
	// 追加操作需要锁的原因:
	//   1. 并发追加时，需要计算正确的 offset
	//   2. 读取旧 Entry -> 计算 offset -> 更新 Entry 这个过程必须原子化
	// 使用短期锁（Short-Lived Lock）:
	//   - 自动续期，避免锁超时
	//   - defer 确保锁一定会释放
	lockClient := cluster.NewLockClient(fs.grpcDialOption, fs.option.Host)
	lock := lockClient.NewShortLivedLock(string(fullpath), string(fs.option.Host))
	defer lock.StopShortLivedLock() // 函数返回时自动释放锁

	// 【步骤 2: 查找现有 Entry 并计算起始 offset】
	var offset int64 = 0
	entry, err := fs.filer.FindEntry(ctx, fullpath)

	if err == filer_pb.ErrNotFound {
		// 【情况 1: 文件不存在，创建新 Entry】
		// 这是首次写入，offset 从 0 开始
		entry = &filer.Entry{
			FullPath: fullpath,
			Attr: filer.Attr{
				Crtime: time.Now(),        // 创建时间
				Mtime:  time.Now(),        // 修改时间
				Mode:   os.FileMode(0644), // 默认权限: rw-r--r--
				Uid:    OS_UID,            // 操作系统用户 ID
				Gid:    OS_GID,            // 操作系统组 ID
			},
		}
	} else {
		// 【情况 2: 文件已存在，计算追加起始位置】
		// offset = 现有所有 chunks 的总大小
		// 新的 chunks 会从这个 offset 开始
		offset = int64(filer.TotalSize(entry.GetChunks()))
	}

	// 【步骤 3: 为新 chunks 设置 offset】
	// 每个 chunk 的 offset 是连续的
	// 例如: 原文件大小 100，新增 3 个 chunks，大小分别为 50, 30, 20
	//       offset: [100, 150, 180]
	for _, chunk := range req.Chunks {
		chunk.Offset = offset            // 设置当前 chunk 的起始位置
		offset += int64(chunk.Size)      // 累加 offset
	}

	// 【步骤 4: 将新 chunks 追加到 Entry】
	entry.Chunks = append(entry.GetChunks(), req.Chunks...)

	// 【步骤 5: 检测存储选项】
	so, err := fs.detectStorageOption(ctx, string(fullpath), "", "", entry.TtlSec, "", "", "", "")
	if err != nil {
		glog.WarningfCtx(ctx, "detectStorageOption: %v", err)
		return &filer_pb.AppendToEntryResponse{}, err
	}

	// 【步骤 6: 可能生成 manifest chunk】
	// 如果 chunks 数量过多（通常 > 1000），合并为 manifest
	entry.Chunks, err = filer.MaybeManifestize(fs.saveAsChunk(ctx, so), entry.GetChunks())
	if err != nil {
		// manifest 生成失败不是致命错误
		glog.V(0).InfofCtx(ctx, "MaybeManifestize: %v", err)
	}

	// 【步骤 7: 写入 Filer Store】
	// 使用 context.Background() 而不是 ctx，确保写入不会因为请求超时而中断
	// 参数说明:
	//   - false: 不使用 O_EXCL（允许覆盖）
	//   - false: 不是来自其他集群
	//   - nil: 无签名
	//   - false: 不跳过父目录检查
	err = fs.filer.CreateEntry(context.Background(), entry, false, false, nil, false, fs.filer.MaxFilenameLength)

	return &filer_pb.AppendToEntryResponse{}, err
}

// DeleteEntry 删除目录项及其数据，支持递归与条件删除等选项
// 参数中的 IsDeleteData 决定是否同步删除底层数据块
func (fs *FilerServer) DeleteEntry(ctx context.Context, req *filer_pb.DeleteEntryRequest) (resp *filer_pb.DeleteEntryResponse, err error) {

	glog.V(4).InfofCtx(ctx, "DeleteEntry %v", req)

	err = fs.filer.DeleteEntryMetaAndData(ctx, util.JoinPath(req.Directory, req.Name), req.IsRecursive, req.IgnoreRecursiveError, req.IsDeleteData, req.IsFromOtherCluster, req.Signatures, req.IfNotModifiedAfter)
	resp = &filer_pb.DeleteEntryResponse{}
	if err != nil && err != filer_pb.ErrNotFound {
		resp.Error = err.Error()
	}
	return resp, nil
}

// AssignVolume 为客户端分配可写的文件 ID 及其 Volume 位置
// 会根据路径策略自动补齐副本、TTL、数据中心等约束，并调用 master.Assign
func (fs *FilerServer) AssignVolume(ctx context.Context, req *filer_pb.AssignVolumeRequest) (resp *filer_pb.AssignVolumeResponse, err error) {

	// 【步骤 1: 设置默认磁盘类型】
	// 如果请求未指定磁盘类型，使用 Filer 配置的默认类型
	// 磁盘类型例如: "hdd", "ssd", "nvme"
	if req.DiskType == "" {
		req.DiskType = fs.option.DiskType
	}

	// 【步骤 2: 检测存储选项】
	// 根据文件路径和请求参数推导完整的存储策略
	// detectStorageOption 会:
	//   1. 检查路径是否匹配配置文件中的规则（例如 filer.toml）
	//   2. 合并默认值和请求参数
	//   3. 验证参数的有效性
	// 返回的 so (Storage Option) 包含:
	//   - Collection: 集合名称
	//   - Replication: 副本策略（如 "001"）
	//   - TTL: 数据过期时间
	//   - DiskType: 磁盘类型
	//   - DataCenter, Rack, DataNode: 位置约束
	so, err := fs.detectStorageOption(ctx, req.Path, req.Collection, req.Replication, req.TtlSec, req.DiskType, req.DataCenter, req.Rack, req.DataNode)
	if err != nil {
		glog.V(3).InfofCtx(ctx, "AssignVolume: %v", err)
		return &filer_pb.AssignVolumeResponse{Error: fmt.Sprintf("assign volume: %v", err)}, nil
	}

	// 【步骤 3: 构建 Master 分配请求】
	// ToAssignRequests 生成主请求和备用请求
	// 主请求: 使用完整的存储选项
	// 备用请求: 放宽某些约束（例如忽略数据中心限制），用于降级处理
	assignRequest, altRequest := so.ToAssignRequests(int(req.Count))

	// 【步骤 4: 向 Master 请求分配 file ID】
	// operation.Assign 会:
	//   1. 连接到 Master 服务器
	//   2. 发送分配请求（包含副本、TTL、数据中心等约束）
	//   3. Master 选择合适的 Volume Server 并分配 file ID
	//   4. 如果主请求失败，自动尝试备用请求
	// 返回的 assignResult 包含:
	//   - Fid: 分配的文件 ID（格式: volumeId,fileKey[_cookie]）
	//   - Url: Volume Server 的 HTTP 地址
	//   - PublicUrl: Volume Server 的公网地址
	//   - GrpcPort: gRPC 端口
	//   - Auth: 鉴权令牌（JWT）
	assignResult, err := operation.Assign(ctx, fs.filer.GetMaster, fs.grpcDialOption, assignRequest, altRequest)
	if err != nil {
		glog.V(3).InfofCtx(ctx, "AssignVolume: %v", err)
		return &filer_pb.AssignVolumeResponse{Error: fmt.Sprintf("assign volume: %v", err)}, nil
	}

	// 【步骤 5: 检查分配结果中的错误】
	// 即使 operation.Assign 没有返回 error，assignResult.Error 也可能有值
	// 例如: 没有足够的 Volume Server 满足约束
	if assignResult.Error != "" {
		glog.V(3).InfofCtx(ctx, "AssignVolume error: %v", assignResult.Error)
		return &filer_pb.AssignVolumeResponse{Error: fmt.Sprintf("assign volume result: %v", assignResult.Error)}, nil
	}

	// 【步骤 6: 返回分配结果】
	// 客户端使用这些信息上传文件:
	//   1. 使用 FileId 作为文件的唯一标识
	//   2. 通过 Location.Url 访问 Volume Server
	//   3. 使用 Auth 进行鉴权（如果启用了安全模式）
	return &filer_pb.AssignVolumeResponse{
		FileId: assignResult.Fid,       // 文件 ID
		Count:  int32(assignResult.Count), // 实际分配的数量（通常等于请求的 Count）
		Location: &filer_pb.Location{
			Url:       assignResult.Url,       // HTTP 地址
			PublicUrl: assignResult.PublicUrl, // 公网地址
			GrpcPort:  uint32(assignResult.GrpcPort), // gRPC 端口
		},
		Auth:        string(assignResult.Auth), // 鉴权令牌
		Collection:  so.Collection,            // 使用的集合名称
		Replication: so.Replication,           // 使用的副本策略
	}, nil
}

// CollectionList 查询 Master，返回当前存在的所有 Collection 名称
// 支持普通卷与纠删码卷通过请求参数进行过滤
func (fs *FilerServer) CollectionList(ctx context.Context, req *filer_pb.CollectionListRequest) (resp *filer_pb.CollectionListResponse, err error) {

	glog.V(4).InfofCtx(ctx, "CollectionList %v", req)
	resp = &filer_pb.CollectionListResponse{}

	err = fs.filer.MasterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		masterResp, err := client.CollectionList(context.Background(), &master_pb.CollectionListRequest{
			IncludeNormalVolumes: req.IncludeNormalVolumes,
			IncludeEcVolumes:     req.IncludeEcVolumes,
		})
		if err != nil {
			return err
		}
		for _, c := range masterResp.Collections {
			resp.Collections = append(resp.Collections, &filer_pb.Collection{Name: c.Name})
		}
		return nil
	})

	return
}

// DeleteCollection 调用 Filer 的 DoDeleteCollection 删除整套卷集合
// 注意: 该操作属于危险操作，调用方需自行闭环授权
func (fs *FilerServer) DeleteCollection(ctx context.Context, req *filer_pb.DeleteCollectionRequest) (resp *filer_pb.DeleteCollectionResponse, err error) {

	glog.V(4).InfofCtx(ctx, "DeleteCollection %v", req)

	err = fs.filer.DoDeleteCollection(req.GetCollection())

	return &filer_pb.DeleteCollectionResponse{}, err
}
