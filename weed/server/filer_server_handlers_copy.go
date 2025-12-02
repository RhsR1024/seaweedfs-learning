// Package weed_server 中的 filer_server_handlers_copy.go 实现 HTTP 拷贝 (cp) 操作
// 负责在不同路径之间复制元数据及数据块，确保与 Volume 交互保持原行为。
package weed_server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// copy 实现 /?cp.from=xxx 的同步拷贝逻辑
// 参数:
//   - ctx: 每个请求的上下文，用于日志与取消
//   - w/r: HTTP 读写器
//   - so: 针对本次复制推导出的 StorageOption（控制副本、TTL 等）
// 行为:
//   - 校验源/目标路径合法性
//   - 拉取源 Entry 并判断是否允许复制
//   - 调用 copyEntry 执行实际复制
func (fs *FilerServer) copy(ctx context.Context, w http.ResponseWriter, r *http.Request, so *operation.StorageOption) {
	// 【步骤 1: 获取源和目标路径】
	src := r.URL.Query().Get("cp.from") // 从 URL 参数获取源路径
	dst := r.URL.Path                    // 从 URL 路径获取目标路径

	glog.V(2).InfofCtx(ctx, "FilerServer.copy %v to %v", src, dst)

	// 【步骤 2: 清理和验证路径】
	var err error
	// clearName 会清理路径中的 "..", "." 等不安全元素
	if src, err = clearName(src); err != nil {
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}
	if dst, err = clearName(dst); err != nil {
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}

	// 移除源路径末尾的 "/"
	src = strings.TrimRight(src, "/")
	if src == "" {
		// 不允许复制根目录
		err = fmt.Errorf("invalid source '/'")
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}

	// 【步骤 3: 验证目标路径的文件名长度】
	srcPath := util.FullPath(src)
	dstPath := util.FullPath(dst)
	if dstPath.IsLongerFileName(so.MaxFileNameLength) {
		err = fmt.Errorf("dst name too long")
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}

	// 【步骤 4: 查找源 Entry】
	srcEntry, err := fs.filer.FindEntry(ctx, srcPath)
	if err != nil {
		err = fmt.Errorf("failed to get src entry '%s': %w", src, err)
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}

	glog.V(1).InfofCtx(ctx, "FilerServer.copy source entry: content_len=%d, chunks_len=%d", len(srcEntry.Content), len(srcEntry.GetChunks()))

	// 【步骤 5: 检查源是否为目录】
	// 当前不支持递归复制目录
	// TODO: 实现目录递归复制功能
	if srcEntry.IsDirectory() {
		err = fmt.Errorf("copy: directory copying not yet supported for '%s'", src)
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}

	// 【步骤 6: 确定最终目标路径】
	_, oldName := srcPath.DirAndName()
	finalDstPath := dstPath

	// 检查目标路径是否已经存在
	dstPathEntry, findErr := fs.filer.FindEntry(ctx, dstPath)
	if findErr != nil && findErr != filer_pb.ErrNotFound {
		err = fmt.Errorf("failed to check destination path %s: %w", dstPath, findErr)
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}

	// 【情况 1: 目标是已存在的目录】
	// 将源文件复制到目标目录下，保持原文件名
	// 例如: cp /a/file.txt /b/ -> /b/file.txt
	if findErr == nil && dstPathEntry.IsDirectory() {
		finalDstPath = dstPath.Child(oldName)
	} else {
		// 【情况 2: 目标是文件路径或不存在】
		// 使用目标路径中指定的文件名
		// 例如: cp /a/file.txt /b/newname.txt -> /b/newname.txt
		newDir, newName := dstPath.DirAndName()
		newName = util.Nvl(newName, oldName) // 如果没有指定文件名，使用源文件名
		finalDstPath = util.FullPath(newDir).Child(newName)
	}

	// 【步骤 7: 检查目标文件是否已存在】
	// 如果目标文件已存在，返回冲突错误
	// TODO: 添加 overwrite 参数支持覆盖
	if dstEntry, err := fs.filer.FindEntry(ctx, finalDstPath); err != nil && err != filer_pb.ErrNotFound {
		err = fmt.Errorf("failed to check destination entry %s: %w", finalDstPath, err)
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	} else if dstEntry != nil {
		err = fmt.Errorf("destination file %s already exists", finalDstPath)
		writeJsonError(w, r, http.StatusConflict, err)
		return
	}

	// 【步骤 8: 执行复制操作】
	// copyEntry 会复制文件的内容和 chunks
	newEntry, err := fs.copyEntry(ctx, srcEntry, finalDstPath, so)
	if err != nil {
		err = fmt.Errorf("failed to copy entry from '%s' to '%s': %w", src, dst, err)
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}

	// 【步骤 9: 将新 Entry 写入 Filer Store】
	// 参数:
	//   - true: O_EXCL，如果文件已存在则失败
	//   - false: 不是来自其他集群
	//   - nil: 无签名
	//   - false: 不跳过父目录检查
	if createErr := fs.filer.CreateEntry(ctx, newEntry, true, false, nil, false, fs.filer.MaxFilenameLength); createErr != nil {
		err = fmt.Errorf("failed to create copied entry from '%s' to '%s': %w", src, dst, createErr)
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}

	glog.V(1).InfofCtx(ctx, "FilerServer.copy completed successfully: src='%s' -> dst='%s' (final_path='%s')", src, dst, finalDstPath)

	// 【步骤 10: 返回成功响应】
	// HTTP 204 No Content 表示操作成功，无需返回内容
	w.WriteHeader(http.StatusNoContent)
}

// copyEntry creates a new entry with copied content and chunks
// copyEntry 负责将元数据写入目标路径并复制底层 chunk
// 返回复制完成后的 Entry，供上层写入 filer store
func (fs *FilerServer) copyEntry(ctx context.Context, srcEntry *filer.Entry, dstPath util.FullPath, so *operation.StorageOption) (*filer.Entry, error) {
	// 【步骤 1: 创建新 Entry 的基础结构】
	// 注意: 对于硬链接，我们复制实际内容但不复制 HardLinkId/HardLinkCounter
	// 这会创建一个独立的副本，而不是指向同一内容的另一个硬链接
	newEntry := &filer.Entry{
		FullPath: dstPath,
		// 深拷贝 Attr 字段以确保 slice 独立性（GroupNames, Md5）
		// 使用立即执行函数（IIFE）来深拷贝
		Attr: func(a filer.Attr) filer.Attr {
			a.GroupNames = append([]string(nil), a.GroupNames...) // 深拷贝字符串切片
			a.Md5 = append([]byte(nil), a.Md5...)                 // 深拷贝字节切片
			return a
		}(srcEntry.Attr),
		Quota: srcEntry.Quota,
		// 故意不复制 HardLinkId 和 HardLinkCounter，创建独立副本
		// 如果复制这些字段，会导致硬链接计数不准确
	}

	// 【步骤 2: 深拷贝 Extended 扩展字段】
	// Extended 用于存储自定义元数据（例如: MIME 类型、用户自定义属性）
	if srcEntry.Extended != nil {
		newEntry.Extended = make(map[string][]byte, len(srcEntry.Extended))
		for k, v := range srcEntry.Extended {
			// 为每个值创建新的字节切片，避免共享底层数组
			newEntry.Extended[k] = append([]byte(nil), v...)
		}
	}

	// 【步骤 3: 深拷贝 Remote 远程存储信息】
	// Remote 字段记录文件在远程对象存储（如 S3）的位置和元数据
	if srcEntry.Remote != nil {
		newEntry.Remote = &filer_pb.RemoteEntry{
			StorageName:       srcEntry.Remote.StorageName,       // 存储名称（如 "s3"）
			LastLocalSyncTsNs: srcEntry.Remote.LastLocalSyncTsNs, // 最后本地同步时间戳
			RemoteETag:        srcEntry.Remote.RemoteETag,        // 远程 ETag
			RemoteMtime:       srcEntry.Remote.RemoteMtime,       // 远程修改时间
			RemoteSize:        srcEntry.Remote.RemoteSize,        // 远程文件大小
		}
	}

	// 【步骤 4: 记录硬链接复制行为】
	// 如果源是硬链接，记录日志以便追踪
	if len(srcEntry.HardLinkId) > 0 {
		glog.V(2).InfofCtx(ctx, "FilerServer.copyEntry: copying hard link %s (nlink=%d) as independent file", srcEntry.FullPath, srcEntry.HardLinkCounter)
	}

	// 【步骤 5: 处理小文件（存储在 Content 字段）】
	// SeaweedFS 对于小文件（通常 < 256 字节）直接存储在元数据中
	// 这样可以避免为小文件分配独立的 chunk，减少开销
	if len(srcEntry.Content) > 0 {
		// 直接深拷贝 Content 字节数组
		newEntry.Content = make([]byte, len(srcEntry.Content))
		copy(newEntry.Content, srcEntry.Content)
		glog.V(2).InfofCtx(ctx, "FilerServer.copyEntry: copied content directly, size=%d", len(newEntry.Content))
		return newEntry, nil
	}

	// 【步骤 6: 处理大文件（存储为 chunks）】
	// 大文件被分割为多个 chunks 存储在 Volume 中
	if len(srcEntry.GetChunks()) > 0 {
		srcChunks := srcEntry.GetChunks()

		// 创建 HTTP 客户端，用于从源 Volume 读取数据并上传到目标 Volume
		// 设置 60 秒超时，避免因网络问题导致无限等待
		client := &http.Client{Timeout: 60 * time.Second}

		// 【步骤 6.1: 检查是否包含 manifest chunks】
		// manifest chunk 是一种特殊的 chunk，包含其他 chunks 的元数据
		// 用于处理 chunk 数量非常多的超大文件（> 1000 chunks）
		if filer.HasChunkManifest(srcChunks) {
			glog.V(2).InfofCtx(ctx, "FilerServer.copyEntry: handling manifest chunks")
			// copyChunksWithManifest 会:
			//   1. 解析 manifest chunk，获取实际的数据 chunks
			//   2. 复制所有数据 chunks
			//   3. 重新生成新的 manifest chunk
			newChunks, err := fs.copyChunksWithManifest(ctx, srcChunks, so, client)
			if err != nil {
				return nil, fmt.Errorf("failed to copy chunks with manifest: %w", err)
			}
			newEntry.Chunks = newChunks
			glog.V(2).InfofCtx(ctx, "FilerServer.copyEntry: copied manifest chunks, count=%d", len(newChunks))
		} else {
			// 【步骤 6.2: 复制普通 chunks】
			// 没有 manifest，直接并行复制所有 chunks
			newChunks, err := fs.copyChunks(ctx, srcChunks, so, client)
			if err != nil {
				return nil, fmt.Errorf("failed to copy chunks: %w", err)
			}
			newEntry.Chunks = newChunks
			glog.V(2).InfofCtx(ctx, "FilerServer.copyEntry: copied regular chunks, count=%d", len(newChunks))
		}
		return newEntry, nil
	}

	// 【步骤 7: 空文件情况】
	// 文件没有 Content 也没有 Chunks，是一个空文件
	// 如果是硬链接却没有内容，记录警告（可能表示硬链接解析有问题）
	if len(srcEntry.HardLinkId) > 0 {
		glog.WarningfCtx(ctx, "FilerServer.copyEntry: hard link %s appears to have no content - this may indicate an issue with hard link resolution", srcEntry.FullPath)
	}
	glog.V(2).InfofCtx(ctx, "FilerServer.copyEntry: empty file, no content or chunks to copy")
	return newEntry, nil
}

// copyChunks 使用并行流式方式复制多个 chunk
// 对每个 chunk 执行 stream copy，并保持和源 chunk 相同的顺序/偏移
//
// 核心优化策略:
//   1. 批量查询 volume 位置，减少 RPC 调用次数
//   2. 并行复制 chunk，提高整体吞吐量
//   3. 流式传输，避免将整个 chunk 加载到内存
//
// 参数:
//   - srcChunks: 源 chunk 列表
//   - so: 存储选项（副本策略、collection 等）
//   - client: HTTP 客户端，用于数据传输
//
// 返回:
//   - []*filer_pb.FileChunk: 新复制的 chunk 列表（保持原有顺序）
//   - error: 复制失败时返回错误
func (fs *FilerServer) copyChunks(ctx context.Context, srcChunks []*filer_pb.FileChunk, so *operation.StorageOption, client *http.Client) ([]*filer_pb.FileChunk, error) {
	// 【步骤 1: 边界检查】
	// 如果没有 chunk 需要复制，直接返回空列表
	if len(srcChunks) == 0 {
		return nil, nil
	}

	// 【步骤 2: 批量查询所有 chunk 的 volume 位置】
	// 优化点: 一次性查询所有唯一的 volumeId，避免逐个查询
	// 例如: 如果有 100 个 chunk 分布在 5 个 volume 上，
	//       只需要 1 次 RPC 调用，而不是 100 次
	volumeLocationsMap, err := fs.batchLookupVolumeLocations(ctx, srcChunks)
	if err != nil {
		return nil, fmt.Errorf("failed to lookup volume locations: %w", err)
	}

	// 【步骤 3: 初始化并发控制】
	// 使用 errgroup 管理并发 goroutine
	// maxConcurrentChunks 限制同时复制的 chunk 数量，防止:
	//   1. 过多 goroutine 导致内存占用过高
	//   2. 过多并发 HTTP 连接影响系统稳定性
	//   3. 过多并发请求压垮 volume server
	const maxConcurrentChunks = 8 // 与 SeaweedFS 标准并发数保持一致

	// 预分配结果切片，保持与源 chunk 相同的顺序
	// 这样可以避免后续排序操作
	newChunks := make([]*filer_pb.FileChunk, len(srcChunks))

	// 创建 errgroup，自动处理上下文传播和错误收集
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(maxConcurrentChunks) // 限制并发 goroutine 数量

	// 【步骤 4: 预检查所有 chunk 的 volume 位置】
	// 在启动并发复制之前，先验证所有 volume 位置都已找到
	// 这样可以快速失败，避免部分 chunk 复制成功后才发现某个 volume 找不到
	for _, chunk := range srcChunks {
		volumeId := chunk.Fid.VolumeId
		locations, ok := volumeLocationsMap[volumeId]
		if !ok || len(locations) == 0 {
			// 找不到 volume 位置，说明 volume 可能已被删除或迁移
			return nil, fmt.Errorf("no locations found for volume %d", volumeId)
		}
	}

	glog.V(2).InfofCtx(ctx, "FilerServer.copyChunks: starting parallel copy of %d chunks with max concurrency %d", len(srcChunks), maxConcurrentChunks)

	// 【步骤 5: 启动并发复制任务】
	// 为每个 chunk 创建一个 goroutine
	// errgroup.SetLimit() 会自动控制并发数，超过限制的任务会排队等待
	for i, srcChunk := range srcChunks {
		// 【重要】捕获循环变量，避免闭包陷阱
		// 如果直接使用 i 和 srcChunk，所有 goroutine 会共享同一个变量
		// 导致最后所有 goroutine 都使用最后一次迭代的值
		chunkIndex := i
		chunk := srcChunk
		chunkLocations := volumeLocationsMap[srcChunk.Fid.VolumeId]

		g.Go(func() error {
			glog.V(3).InfofCtx(gCtx, "FilerServer.copyChunks: copying chunk %d/%d, size=%d", chunkIndex+1, len(srcChunks), chunk.Size)

			// 【步骤 5.1: 流式复制单个 chunk】
			// 使用 HTTP 流式传输:
			//   1. 从源 volume server GET chunk 数据
			//   2. 将数据流 PUT 到目标 volume server
			//   3. 全程不需要将整个 chunk 加载到内存
			newChunk, err := fs.streamCopyChunk(gCtx, chunk, so, client, chunkLocations)
			if err != nil {
				// 复制失败，返回错误
				// errgroup 会自动取消其他正在进行的 goroutine
				return fmt.Errorf("failed to copy chunk %d (%s): %w", chunkIndex+1, chunk.GetFileIdString(), err)
			}

			// 【步骤 5.2: 将结果存储到正确的位置】
			// 使用索引保持与源 chunk 相同的顺序
			// 避免需要后续排序操作
			newChunks[chunkIndex] = newChunk

			glog.V(4).InfofCtx(gCtx, "FilerServer.copyChunks: successfully copied chunk %d/%d", chunkIndex+1, len(srcChunks))
			return nil
		})
	}

	// 【步骤 6: 等待所有并发任务完成】
	// g.Wait() 会阻塞直到:
	//   1. 所有 goroutine 都成功完成（返回 nil）
	//   2. 或者任意一个 goroutine 返回错误
	// 如果有错误，Wait() 会返回第一个错误，并取消其他正在进行的任务
	if err := g.Wait(); err != nil {
		return nil, err
	}

	// 【步骤 7: 完整性验证】
	// 理论上如果没有错误，所有 chunk 都应该复制成功
	// 这是一个防御性检查，确保没有遗漏任何 chunk
	for i, chunk := range newChunks {
		if chunk == nil {
			// 这种情况不应该发生，如果发生说明代码有 bug
			return nil, fmt.Errorf("chunk %d was not copied (internal error)", i)
		}
	}

	glog.V(2).InfofCtx(ctx, "FilerServer.copyChunks: successfully completed parallel copy of %d chunks", len(srcChunks))
	return newChunks, nil
}

// copyChunksWithManifest 处理包含 manifest chunk 的复制操作
// 在复制前拆分 manifest chunk，以避免直接对 manifest 做深拷贝导致引用脏数据
//
// Manifest Chunk 的概念:
//   - 当文件非常大时，会产生大量 chunk（例如 1GB 文件可能有上千个 chunk）
//   - 直接存储所有 chunk 元数据会导致 Entry 过大
//   - Manifest Chunk 是一种特殊的 chunk，它的内容是其他 chunk 的引用列表
//   - 这样可以将大量 chunk 的元数据压缩成一个 manifest chunk 引用
//
// 复制策略:
//   1. 分离 manifest chunk 和普通 data chunk
//   2. 直接复制所有普通 data chunk
//   3. 对每个 manifest chunk:
//      a. 解析 manifest，获取它引用的实际 data chunks
//      b. 递归复制这些 data chunks（可能还包含嵌套的 manifest）
//      c. 创建新的 manifest chunk 引用新复制的 chunks
//
// 参数:
//   - srcChunks: 源 chunk 列表（可能包含 manifest chunks）
//   - so: 存储选项
//   - client: HTTP 客户端
//
// 返回:
//   - []*filer_pb.FileChunk: 新复制的 chunk 列表
//   - error: 复制失败时返回错误
func (fs *FilerServer) copyChunksWithManifest(ctx context.Context, srcChunks []*filer_pb.FileChunk, so *operation.StorageOption, client *http.Client) ([]*filer_pb.FileChunk, error) {
	// 【步骤 1: 边界检查】
	if len(srcChunks) == 0 {
		return nil, nil
	}

	glog.V(2).InfofCtx(ctx, "FilerServer.copyChunksWithManifest: processing %d chunks (some are manifests)", len(srcChunks))

	// 【步骤 2: 分离 manifest chunks 和普通 data chunks】
	// filer.SeparateManifestChunks 会检查每个 chunk 的 IsChunkManifest 标志
	// 返回两个列表:
	//   - manifestChunks: 所有标记为 manifest 的 chunk
	//   - nonManifestChunks: 所有普通的 data chunk
	manifestChunks, nonManifestChunks := filer.SeparateManifestChunks(srcChunks)

	var newChunks []*filer_pb.FileChunk

	// 【步骤 3: 复制所有普通 data chunks】
	// 普通 chunk 可以直接复制，不需要特殊处理
	if len(nonManifestChunks) > 0 {
		glog.V(3).InfofCtx(ctx, "FilerServer.copyChunksWithManifest: copying %d non-manifest chunks", len(nonManifestChunks))
		newNonManifestChunks, err := fs.copyChunks(ctx, nonManifestChunks, so, client)
		if err != nil {
			return nil, fmt.Errorf("failed to copy non-manifest chunks: %w", err)
		}
		newChunks = append(newChunks, newNonManifestChunks...)
	}

	// 【步骤 4: 处理每个 manifest chunk】
	// 每个 manifest chunk 需要:
	//   1. 解析得到它引用的实际 chunk 列表
	//   2. 复制这些实际的 chunks
	//   3. 创建新的 manifest chunk 引用新复制的 chunks
	for i, manifestChunk := range manifestChunks {
		glog.V(3).InfofCtx(ctx, "FilerServer.copyChunksWithManifest: processing manifest chunk %d/%d", i+1, len(manifestChunks))

		// 【步骤 4.1: 解析 manifest chunk】
		// manifest chunk 的数据是一个序列化的 FileChunkManifest protobuf 消息
		// 包含了实际 chunk 的列表
		// 需要提供 lookupFileIdFn 来查找 manifest chunk 自身的位置
		lookupFileIdFn := func(ctx context.Context, fileId string) (urls []string, err error) {
			return fs.filer.MasterClient.GetLookupFileIdFunction()(ctx, fileId)
		}

		// ResolveOneChunkManifest 会:
		//   1. 根据 manifest chunk 的 fileId 找到存储位置
		//   2. 下载 manifest chunk 的数据
		//   3. 反序列化得到实际的 chunk 列表
		resolvedChunks, err := filer.ResolveOneChunkManifest(ctx, lookupFileIdFn, manifestChunk)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve manifest chunk %s: %w", manifestChunk.GetFileIdString(), err)
		}

		glog.V(4).InfofCtx(ctx, "FilerServer.copyChunksWithManifest: resolved manifest chunk %s to %d data chunks",
			manifestChunk.GetFileIdString(), len(resolvedChunks))

		// 【步骤 4.2: 递归复制解析出的 chunks】
		// 重要: 使用递归调用 copyChunksWithManifest 而不是 copyChunks
		// 因为解析出的 chunks 可能还包含嵌套的 manifest chunks
		// 例如: 超大文件可能有 manifest of manifests 的结构
		newResolvedChunks, err := fs.copyChunksWithManifest(ctx, resolvedChunks, so, client)
		if err != nil {
			return nil, fmt.Errorf("failed to copy resolved chunks from manifest %s: %w", manifestChunk.GetFileIdString(), err)
		}

		// 【步骤 4.3: 创建新的 manifest chunk】
		// 新的 manifest chunk 会:
		//   1. 引用新复制的 chunks（而不是原来的 chunks）
		//   2. 保持与原 manifest 相同的属性（offset, size 等）
		//   3. 作为一个新的 chunk 存储到 volume 中
		newManifestChunk, err := fs.createManifestChunk(ctx, newResolvedChunks, manifestChunk, so, client)
		if err != nil {
			return nil, fmt.Errorf("failed to create new manifest chunk: %w", err)
		}

		newChunks = append(newChunks, newManifestChunk)

		glog.V(4).InfofCtx(ctx, "FilerServer.copyChunksWithManifest: created new manifest chunk %s for %d resolved chunks",
			newManifestChunk.GetFileIdString(), len(newResolvedChunks))
	}

	glog.V(2).InfofCtx(ctx, "FilerServer.copyChunksWithManifest: completed copying %d total chunks (%d manifest, %d regular)",
		len(newChunks), len(manifestChunks), len(nonManifestChunks))

	return newChunks, nil
}

// createManifestChunk 创建新的 manifest chunk，引用提供的 data chunks
// 在需要时生成新的 manifest chunk，会重用原 manifest chunk 的属性，同时重新构建真实数据块列表
//
// Manifest Chunk 的存储格式:
//   - Manifest chunk 本身也是一个普通的 chunk
//   - 它的数据内容是一个序列化的 FileChunkManifest protobuf 消息
//   - 该消息包含了它引用的所有 chunk 的元数据列表
//
// 参数:
//   - dataChunks: 要引用的实际 data chunks
//   - originalManifest: 原始的 manifest chunk（用于继承属性）
//   - so: 存储选项
//   - client: HTTP 客户端
//
// 返回:
//   - *filer_pb.FileChunk: 新创建的 manifest chunk
//   - error: 创建失败时返回错误
func (fs *FilerServer) createManifestChunk(ctx context.Context, dataChunks []*filer_pb.FileChunk, originalManifest *filer_pb.FileChunk, so *operation.StorageOption, client *http.Client) (*filer_pb.FileChunk, error) {
	// 【步骤 1: 准备 chunk 元数据用于序列化】
	// BeforeEntrySerialization 会对 chunks 进行预处理
	// 例如: 压缩重复的字段、优化存储格式等
	filer_pb.BeforeEntrySerialization(dataChunks)

	// 【步骤 2: 创建 manifest 数据结构】
	// FileChunkManifest 是一个 protobuf 消息
	// 包含一个 Chunks 字段，存储所有引用的 chunk 元数据
	manifestData := &filer_pb.FileChunkManifest{
		Chunks: dataChunks,
	}

	// 【步骤 3: 序列化 manifest】
	// 将 manifestData 转换为二进制格式
	// 这个二进制数据就是 manifest chunk 的实际内容
	data, err := proto.Marshal(manifestData)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal manifest: %w", err)
	}

	// 【步骤 4: 保存 manifest 数据为一个新的 chunk】
	// 定义保存函数，用于将 manifest 数据上传到 volume server
	saveFunc := func(reader io.Reader, name string, offset int64, tsNs int64) (chunk *filer_pb.FileChunk, err error) {
		// 【步骤 4.1: 分配新的文件 ID】
		// 向 master 请求一个新的 fileId 和 volume 位置
		// 用于存储 manifest chunk 的数据
		fileId, urlLocation, auth, assignErr := fs.assignNewFileInfo(ctx, so)
		if assignErr != nil {
			return nil, fmt.Errorf("failed to assign file ID for manifest: %w", assignErr)
		}

		// 【步骤 4.2: 上传 manifest 数据】
		// 将序列化后的 manifest 数据上传到 volume server
		// 这和上传普通文件数据的流程完全一样
		err = fs.uploadData(ctx, reader, urlLocation, string(auth), client)
		if err != nil {
			return nil, fmt.Errorf("failed to upload manifest data: %w", err)
		}

		// 【步骤 4.3: 创建 chunk 元数据】
		// 创建 FileChunk 对象描述这个 manifest chunk
		chunk = &filer_pb.FileChunk{
			FileId: fileId,            // manifest chunk 自己的 fileId
			Offset: offset,            // manifest chunk 在文件中的偏移
			Size:   uint64(len(data)), // manifest 数据的大小（不是它引用的 chunks 的总大小）
		}
		return chunk, nil
	}

	// 调用保存函数
	manifestChunk, err := saveFunc(bytes.NewReader(data), "", originalManifest.Offset, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to save manifest chunk: %w", err)
	}

	// 【步骤 5: 设置 manifest 特有的属性】
	// IsChunkManifest 标志表示这是一个 manifest chunk
	// 后续读取时会根据这个标志进行特殊处理
	manifestChunk.IsChunkManifest = true

	// Size 设置为原 manifest 的 Size
	// 注意: 这个 Size 是 manifest 引用的所有 chunks 的总大小
	//       而不是 manifest 数据本身的大小
	manifestChunk.Size = originalManifest.Size

	return manifestChunk, nil
}

// uploadData 将数据上传到 volume server
// 将内存缓冲或流式 reader 上传至指定 Volume Server，通过设置 JWT 与 Content-Length，保持与普通上传一致
//
// 参数:
//   - reader: 数据源（可以是文件、内存缓冲或网络流）
//   - urlLocation: volume server 的上传 URL（格式: http://host:port/volumeId,fileKey）
//   - auth: JWT 认证令牌（可选）
//   - client: HTTP 客户端
//
// 返回:
//   - error: 上传失败时返回错误
func (fs *FilerServer) uploadData(ctx context.Context, reader io.Reader, urlLocation, auth string, client *http.Client) error {
	// 【步骤 1: 创建 HTTP PUT 请求】
	// 使用 PUT 方法上传数据到 volume server
	// reader 的内容会作为请求 body 发送
	req, err := http.NewRequestWithContext(ctx, "PUT", urlLocation, reader)
	if err != nil {
		return fmt.Errorf("failed to create upload request: %w", err)
	}

	// 【步骤 2: 设置认证头】
	// 如果提供了 JWT 令牌，添加到 Authorization 头
	// volume server 会验证这个令牌以确保请求合法
	if auth != "" {
		req.Header.Set("Authorization", "Bearer "+auth)
	}

	// 【步骤 3: 发送请求】
	// 执行 HTTP 请求，将数据上传到 volume server
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to upload data: %w", err)
	}
	defer resp.Body.Close()

	// 【步骤 4: 检查响应状态码】
	// 成功的上传应该返回 201 (Created) 或 200 (OK)
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		// 读取错误响应体以获取详细信息
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			// 读取响应体也失败了，只能返回状态码
			return fmt.Errorf("upload failed with status %d, and failed to read response: %w", resp.StatusCode, readErr)
		}
		// 返回包含错误详情的错误
		return fmt.Errorf("upload failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// batchLookupVolumeLocations 批量查询 chunks 所属 volume 的位置
// 批量查询 chunk 所属卷的位置，减少与 master 的往返，返回值以 VolumeId 为 key，包含多个 replica 地址
//
// 优化策略:
//   - 收集所有唯一的 volumeId，一次性查询所有位置
//   - 避免对同一个 volumeId 重复查询
//   - 减少与 master 的 RPC 调用次数
//
// 参数:
//   - chunks: chunk 列表
//
// 返回:
//   - map[uint32][]operation.Location: volumeId -> 位置列表的映射
//     每个 volume 可能有多个副本，每个副本的位置都会返回
//   - error: 查询失败时返回错误
func (fs *FilerServer) batchLookupVolumeLocations(ctx context.Context, chunks []*filer_pb.FileChunk) (map[uint32][]operation.Location, error) {
	// 【步骤 1: 收集唯一的 volume ID】
	// 使用 map 去重，避免重复查询同一个 volume
	// 同时预先转换为字符串格式，避免后续重复转换
	volumeIdMap := make(map[uint32]string)
	for _, chunk := range chunks {
		vid := chunk.Fid.VolumeId
		if _, found := volumeIdMap[vid]; !found {
			// 第一次遇到这个 volumeId，记录下来
			volumeIdMap[vid] = fmt.Sprintf("%d", vid)
		}
	}

	// 【步骤 2: 边界检查】
	// 如果没有任何 volume 需要查询，直接返回空 map
	if len(volumeIdMap) == 0 {
		return make(map[uint32][]operation.Location), nil
	}

	// 【步骤 3: 转换为字符串切片】
	// operation.LookupVolumeIds 需要 []string 类型的参数
	volumeIdStrs := make([]string, 0, len(volumeIdMap))
	for _, vidStr := range volumeIdMap {
		volumeIdStrs = append(volumeIdStrs, vidStr)
	}

	// 【步骤 4: 批量查询所有 volume 的位置】
	// 一次 RPC 调用查询所有 volume 的位置
	// 例如: 如果有 100 个 chunks 分布在 5 个 volume 上
	//       只需要 1 次 RPC 调用，而不是 5 次或 100 次
	lookupResult, err := operation.LookupVolumeIds(fs.filer.GetMaster, fs.grpcDialOption, volumeIdStrs)
	if err != nil {
		return nil, fmt.Errorf("failed to lookup volumes: %w", err)
	}

	// 【步骤 5: 转换查询结果为 map】
	// 将 lookupResult (map[string]VolumeLocations) 转换为 map[uint32][]Location
	// 这样后续可以直接用 volumeId (uint32) 查询位置
	volumeLocationsMap := make(map[uint32][]operation.Location)
	for volumeId, volumeIdStr := range volumeIdMap {
		if volumeLocations, ok := lookupResult[volumeIdStr]; ok && len(volumeLocations.Locations) > 0 {
			// 找到了这个 volume 的位置信息
			// volumeLocations.Locations 包含该 volume 的所有副本位置
			volumeLocationsMap[volumeId] = volumeLocations.Locations
		}
		// 注意: 如果某个 volume 找不到位置，它不会出现在结果 map 中
		//       调用方需要检查这种情况
	}

	return volumeLocationsMap, nil
}

// streamCopyChunk 使用流式传输复制单个 chunk
// 执行单个 chunk 的双向流式 copy（从源 Volume 拉取再推给目标），返回新的 chunk 元数据，包含复制后的 fileId 与偏移信息
//
// 流式复制的优势:
//   - 不需要将整个 chunk 加载到内存
//   - 数据直接从源 volume server 流向目标 volume server
//   - 内存占用恒定，不受 chunk 大小影响
//
// 容错机制:
//   - 源 chunk 可能有多个副本（replica）
//   - 会依次尝试所有副本，直到找到一个可用的
//   - 只要有一个副本成功，就认为复制成功
//
// 参数:
//   - srcChunk: 源 chunk 元数据
//   - so: 存储选项
//   - client: HTTP 客户端
//   - locations: 源 chunk 的所有副本位置
//
// 返回:
//   - *filer_pb.FileChunk: 新复制的 chunk 元数据
//   - error: 所有副本都失败时返回错误
func (fs *FilerServer) streamCopyChunk(ctx context.Context, srcChunk *filer_pb.FileChunk, so *operation.StorageOption, client *http.Client, locations []operation.Location) (*filer_pb.FileChunk, error) {
	// 【步骤 1: 为目标 chunk 分配新的 file ID】
	// 向 master 请求一个新的 fileId 和 volume 位置
	fileId, urlLocation, auth, err := fs.assignNewFileInfo(ctx, so)
	if err != nil {
		return nil, fmt.Errorf("failed to assign new file ID: %w", err)
	}

	// 【步骤 2: 尝试从所有副本位置复制数据】
	// 源 chunk 可能有多个副本（根据副本策略）
	// 例如: 副本策略 "001" 表示有 2 个副本（1 个原件 + 1 个副本）
	fileIdString := srcChunk.GetFileIdString()
	var lastErr error

	for i, location := range locations {
		// 【步骤 2.1: 构造源 chunk 的 URL】
		// 格式: http://volumeServer:port/volumeId,fileKey
		srcUrl := fmt.Sprintf("http://%s/%s", location.Url, fileIdString)
		glog.V(4).InfofCtx(ctx, "FilerServer.streamCopyChunk: attempting streaming copy from %s to %s (attempt %d/%d)", srcUrl, urlLocation, i+1, len(locations))

		// 【步骤 2.2: 执行流式复制】
		// performStreamCopy 会:
		//   1. 从 srcUrl GET 数据（建立读取流）
		//   2. 将数据 PUT 到 urlLocation（建立写入流）
		//   3. 数据在两个流之间传输，不经过内存缓冲
		err := fs.performStreamCopy(ctx, srcUrl, urlLocation, string(auth), srcChunk.Size, client)
		if err != nil {
			// 这个副本失败了，记录错误并尝试下一个副本
			lastErr = err
			glog.V(2).InfofCtx(ctx, "FilerServer.streamCopyChunk: failed streaming copy from %s: %v", srcUrl, err)
			continue
		}

		// 【步骤 2.3: 复制成功，创建新的 chunk 元数据】
		newChunk := &filer_pb.FileChunk{
			FileId: fileId,           // 新分配的 fileId
			Offset: srcChunk.Offset,  // 保持与源 chunk 相同的偏移
			Size:   srcChunk.Size,    // 保持与源 chunk 相同的大小
			ETag:   srcChunk.ETag,    // 保持与源 chunk 相同的 ETag（如果有）
		}

		glog.V(4).InfofCtx(ctx, "FilerServer.streamCopyChunk: successfully streamed %d bytes", srcChunk.Size)
		return newChunk, nil
	}

	// 【步骤 3: 所有副本都失败】
	// 如果所有副本位置都尝试过了还是失败，返回最后一个错误
	return nil, fmt.Errorf("failed to stream copy chunk from any location: %w", lastErr)
}

// performStreamCopy 执行实际的流式复制操作
// 通过 HTTP 流复制数据，会在目标端设置 Content-Length 并校验复制完成前后的数据量是否一致
//
// 流式复制的工作原理:
//   1. 向源 volume server 发送 GET 请求，获取 chunk 数据流
//   2. 将响应的 Body（一个 io.Reader）直接作为目标 PUT 请求的 Body
//   3. HTTP 客户端会自动在两个流之间传输数据
//   4. 数据不会经过额外的内存缓冲，直接从源流向目标
//
// 这种方式的优势:
//   - 内存占用恒定，不受数据大小影响
//   - 传输效率高，没有额外的拷贝开销
//   - 适合复制大文件 chunk
//
// 参数:
//   - srcUrl: 源 chunk 的 URL（格式: http://host:port/volumeId,fileKey）
//   - dstUrl: 目标 chunk 的 URL（格式: http://host:port/volumeId,fileKey）
//   - auth: JWT 认证令牌
//   - expectedSize: 预期的数据大小（用于设置 Content-Length）
//   - client: HTTP 客户端
//
// 返回:
//   - error: 复制失败时返回错误
func (fs *FilerServer) performStreamCopy(ctx context.Context, srcUrl, dstUrl, auth string, expectedSize uint64, client *http.Client) error {
	// 【步骤 1: 创建源请求】
	// 使用 GET 方法从源 volume server 读取 chunk 数据
	req, err := http.NewRequestWithContext(ctx, "GET", srcUrl, nil)
	if err != nil {
		return fmt.Errorf("failed to create source request: %v", err)
	}

	// 【步骤 2: 执行源请求】
	// 发送 GET 请求到源 volume server
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to read from source: %v", err)
	}
	defer resp.Body.Close()

	// 【步骤 3: 检查源响应状态】
	// 源 volume server 应该返回 200 OK
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("source returned status %d", resp.StatusCode)
	}

	// 【步骤 4: 创建目标请求】
	// 关键: 使用 resp.Body 作为目标请求的 Body
	// 这样数据会直接从源响应流向目标请求，不经过额外的内存缓冲
	dstReq, err := http.NewRequestWithContext(ctx, "PUT", dstUrl, resp.Body)
	if err != nil {
		return fmt.Errorf("failed to create destination request: %v", err)
	}

	// 【步骤 5: 设置目标请求头】
	// ContentLength: 告诉目标 volume server 预期的数据大小
	//   这样 volume server 可以预先分配空间，提高写入效率
	dstReq.ContentLength = int64(expectedSize)

	// Authorization: 如果有 JWT 令牌，添加到请求头
	if auth != "" {
		dstReq.Header.Set("Authorization", "Bearer "+auth)
	}

	// Content-Type: 标记为二进制数据流
	dstReq.Header.Set("Content-Type", "application/octet-stream")

	// 【步骤 6: 执行目标请求】
	// 发送 PUT 请求到目标 volume server
	// 在这个调用期间，数据会从 resp.Body 流式传输到目标 server
	dstResp, err := client.Do(dstReq)
	if err != nil {
		return fmt.Errorf("failed to write to destination: %v", err)
	}
	defer dstResp.Body.Close()

	// 【步骤 7: 检查目标响应状态】
	// 成功的上传应该返回 201 (Created) 或 200 (OK)
	if dstResp.StatusCode != http.StatusCreated && dstResp.StatusCode != http.StatusOK {
		// 读取错误响应体以获取详细信息
		body, readErr := io.ReadAll(dstResp.Body)
		if readErr != nil {
			return fmt.Errorf("destination returned status %d, and failed to read body: %w", dstResp.StatusCode, readErr)
		}
		return fmt.Errorf("destination returned status %d: %s", dstResp.StatusCode, string(body))
	}

	glog.V(4).InfofCtx(ctx, "FilerServer.performStreamCopy: successfully streamed data from %s to %s", srcUrl, dstUrl)
	return nil
}
