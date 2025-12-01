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

	entry, err := fs.filer.FindEntry(ctx, util.JoinPath(req.Directory, req.Name))
	if err == filer_pb.ErrNotFound {
		return &filer_pb.LookupDirectoryEntryResponse{}, err
	}
	if err != nil {
		glog.V(3).InfofCtx(ctx, "LookupDirectoryEntry %s: %+v, ", filepath.Join(req.Directory, req.Name), err)
		return nil, err
	}

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

	limit := int(req.Limit)
	if limit == 0 {
		limit = fs.option.DirListingLimit
	}

	paginationLimit := filer.PaginationSize
	if limit < paginationLimit {
		paginationLimit = limit
	}

	lastFileName := req.StartFromFileName
	includeLastFile := req.InclusiveStartFrom
	var listErr error
	for limit > 0 {
		var hasEntries bool
		lastFileName, listErr = fs.filer.StreamListDirectoryEntries(stream.Context(), util.FullPath(req.Directory), lastFileName, includeLastFile, int64(paginationLimit), req.Prefix, "", "", func(entry *filer.Entry) bool {
			hasEntries = true
			if err = stream.Send(&filer_pb.ListEntriesResponse{
				Entry: entry.ToProtoEntry(),
			}); err != nil {
				return false
			}

			limit--
			if limit == 0 {
				return false
			}
			return true
		})

		if listErr != nil {
			return listErr
		}
		if err != nil {
			return err
		}
		if !hasEntries {
			return nil
		}

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

	resp := &filer_pb.LookupVolumeResponse{
		LocationsMap: make(map[string]*filer_pb.Locations),
	}

	// Use master client's lookup with fallback - it handles cache and master query
	vidLocations, err := fs.filer.MasterClient.LookupVolumeIdsWithFallback(ctx, req.VolumeIds)

	// Convert wdclient.Location to filer_pb.Location
	// Return partial results even if there was an error
	for vidString, locations := range vidLocations {
		resp.LocationsMap[vidString] = &filer_pb.Locations{
			Locations: wdclientLocationsToPb(locations),
		}
	}

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
	fid, err := needle.ParseFileIdFromString(fileId)
	if err != nil {
		return nil, err
	}
	locations, found := fs.filer.MasterClient.GetLocations(uint32(fid.VolumeId))
	if !found || len(locations) == 0 {
		return nil, fmt.Errorf("not found volume %d in %s", fid.VolumeId, fileId)
	}
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

	chunks, garbage, err2 := fs.cleanupChunks(ctx, util.Join(req.Directory, req.Entry.Name), nil, req.Entry)
	if err2 != nil {
		return &filer_pb.CreateEntryResponse{}, fmt.Errorf("CreateEntry cleanupChunks %s %s: %v", req.Directory, req.Entry.Name, err2)
	}

	so, err := fs.detectStorageOption(ctx, string(util.NewFullPath(req.Directory, req.Entry.Name)), "", "", 0, "", "", "", "")
	if err != nil {
		return nil, err
	}
	newEntry := filer.FromPbEntry(req.Directory, req.Entry)
	newEntry.Chunks = chunks
	newEntry.TtlSec = so.TtlSeconds

	createErr := fs.filer.CreateEntry(ctx, newEntry, req.OExcl, req.IsFromOtherCluster, req.Signatures, req.SkipCheckParentDirectory, so.MaxFileNameLength)

	if createErr == nil {
		fs.filer.DeleteChunksNotRecursive(garbage)
	} else {
		glog.V(3).InfofCtx(ctx, "CreateEntry %s: %v", filepath.Join(req.Directory, req.Entry.Name), createErr)
		resp.Error = createErr.Error()
	}

	return
}

// UpdateEntry 用于替换现有目录项的属性与 Chunk 信息
// 会在更新前读取旧 Entry，计算差异并在成功后触发通知事件
func (fs *FilerServer) UpdateEntry(ctx context.Context, req *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {

	glog.V(4).InfofCtx(ctx, "UpdateEntry %v", req)

	fullpath := util.Join(req.Directory, req.Entry.Name)
	entry, err := fs.filer.FindEntry(ctx, util.FullPath(fullpath))
	if err != nil {
		return &filer_pb.UpdateEntryResponse{}, fmt.Errorf("not found %s: %v", fullpath, err)
	}

	chunks, garbage, err2 := fs.cleanupChunks(ctx, fullpath, entry, req.Entry)
	if err2 != nil {
		return &filer_pb.UpdateEntryResponse{}, fmt.Errorf("UpdateEntry cleanupChunks %s: %v", fullpath, err2)
	}

	newEntry := filer.FromPbEntry(req.Directory, req.Entry)
	newEntry.Chunks = chunks

	if filer.EqualEntry(entry, newEntry) {
		return &filer_pb.UpdateEntryResponse{}, err
	}

	if err = fs.filer.UpdateEntry(ctx, entry, newEntry); err == nil {
		fs.filer.DeleteChunksNotRecursive(garbage)

		fs.filer.NotifyUpdateEvent(ctx, entry, newEntry, true, req.IsFromOtherCluster, req.Signatures)

	} else {
		glog.V(3).InfofCtx(ctx, "UpdateEntry %s: %v", filepath.Join(req.Directory, req.Entry.Name), err)
	}

	return &filer_pb.UpdateEntryResponse{}, err
}

// cleanupChunks 对比旧新 chunk 列表，返回需要保留的 chunks 与待删除的垃圾块
// 额外会处理 manifest chunk、追加 chunk 以及按路径推导存储选项
func (fs *FilerServer) cleanupChunks(ctx context.Context, fullpath string, existingEntry *filer.Entry, newEntry *filer_pb.Entry) (chunks, garbage []*filer_pb.FileChunk, err error) {

	// remove old chunks if not included in the new ones
	if existingEntry != nil {
		garbage, err = filer.MinusChunks(ctx, fs.lookupFileId, existingEntry.GetChunks(), newEntry.GetChunks())
		if err != nil {
			return newEntry.GetChunks(), nil, fmt.Errorf("MinusChunks: %w", err)
		}
	}

	// files with manifest chunks are usually large and append only, skip calculating covered chunks
	manifestChunks, nonManifestChunks := filer.SeparateManifestChunks(newEntry.GetChunks())

	chunks, coveredChunks := filer.CompactFileChunks(ctx, fs.lookupFileId, nonManifestChunks)
	garbage = append(garbage, coveredChunks...)

	if newEntry.Attributes != nil {
		so, _ := fs.detectStorageOption(ctx, fullpath,
			"",
			"",
			newEntry.Attributes.TtlSec,
			"",
			"",
			"",
			"",
		) // ignore readonly error for capacity needed to manifestize
		chunks, err = filer.MaybeManifestize(fs.saveAsChunk(ctx, so), chunks)
		if err != nil {
			// not good, but should be ok
			glog.V(0).InfofCtx(ctx, "MaybeManifestize: %v", err)
		}
	}

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

	lockClient := cluster.NewLockClient(fs.grpcDialOption, fs.option.Host)
	lock := lockClient.NewShortLivedLock(string(fullpath), string(fs.option.Host))
	defer lock.StopShortLivedLock()

	var offset int64 = 0
	entry, err := fs.filer.FindEntry(ctx, fullpath)
	if err == filer_pb.ErrNotFound {
		entry = &filer.Entry{
			FullPath: fullpath,
			Attr: filer.Attr{
				Crtime: time.Now(),
				Mtime:  time.Now(),
				Mode:   os.FileMode(0644),
				Uid:    OS_UID,
				Gid:    OS_GID,
			},
		}
	} else {
		offset = int64(filer.TotalSize(entry.GetChunks()))
	}

	for _, chunk := range req.Chunks {
		chunk.Offset = offset
		offset += int64(chunk.Size)
	}

	entry.Chunks = append(entry.GetChunks(), req.Chunks...)
	so, err := fs.detectStorageOption(ctx, string(fullpath), "", "", entry.TtlSec, "", "", "", "")
	if err != nil {
		glog.WarningfCtx(ctx, "detectStorageOption: %v", err)
		return &filer_pb.AppendToEntryResponse{}, err
	}
	entry.Chunks, err = filer.MaybeManifestize(fs.saveAsChunk(ctx, so), entry.GetChunks())
	if err != nil {
		// not good, but should be ok
		glog.V(0).InfofCtx(ctx, "MaybeManifestize: %v", err)
	}

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

	if req.DiskType == "" {
		req.DiskType = fs.option.DiskType
	}

	so, err := fs.detectStorageOption(ctx, req.Path, req.Collection, req.Replication, req.TtlSec, req.DiskType, req.DataCenter, req.Rack, req.DataNode)
	if err != nil {
		glog.V(3).InfofCtx(ctx, "AssignVolume: %v", err)
		return &filer_pb.AssignVolumeResponse{Error: fmt.Sprintf("assign volume: %v", err)}, nil
	}

	assignRequest, altRequest := so.ToAssignRequests(int(req.Count))

	assignResult, err := operation.Assign(ctx, fs.filer.GetMaster, fs.grpcDialOption, assignRequest, altRequest)
	if err != nil {
		glog.V(3).InfofCtx(ctx, "AssignVolume: %v", err)
		return &filer_pb.AssignVolumeResponse{Error: fmt.Sprintf("assign volume: %v", err)}, nil
	}
	if assignResult.Error != "" {
		glog.V(3).InfofCtx(ctx, "AssignVolume error: %v", assignResult.Error)
		return &filer_pb.AssignVolumeResponse{Error: fmt.Sprintf("assign volume result: %v", assignResult.Error)}, nil
	}

	return &filer_pb.AssignVolumeResponse{
		FileId: assignResult.Fid,
		Count:  int32(assignResult.Count),
		Location: &filer_pb.Location{
			Url:       assignResult.Url,
			PublicUrl: assignResult.PublicUrl,
			GrpcPort:  uint32(assignResult.GrpcPort),
		},
		Auth:        string(assignResult.Auth),
		Collection:  so.Collection,
		Replication: so.Replication,
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
