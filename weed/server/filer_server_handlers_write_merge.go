// Package weed_server 中的 filer_server_handlers_write_merge.go 负责小块合并逻辑
//
// 核心功能：
//   在写入过程中通过合并大量碎片块以提升读性能并减少索引项。
//   当文件存在大量小 chunk 时，合并为少量大 chunk 可以显著减少元数据开销和读取延迟。
//
// 应用场景：
//   - 追加写入场景：多次小量追加导致产生大量碎片 chunk
//   - 随机写入场景：覆盖写入产生的碎片化 chunk
//   - 优化读性能：合并后减少 chunk 数量，降低元数据查询和网络往返次数
//
// 工作原理：
//   1. 检测是否满足合并条件（小 chunk 数量超过阈值）
//   2. 从现有 chunk 读取数据并重新上传为大 chunk
//   3. 删除旧的碎片 chunk，减少存储空间占用
package weed_server

import (
	"context"
	"io"
	"math"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
)

// MergeChunkMinCount 定义触发 chunk 合并的最小 chunk 数量阈值
// 只有当小 chunk 数量超过此值时，才会考虑执行合并操作
// 取值 1000 是为了避免对少量 chunk 的文件进行不必要的合并
const MergeChunkMinCount int = 1000

// maybeMergeChunks 根据 chunk 数量与大小判断是否需要合并
//
// 参数:
//   - ctx: 上下文，用于传递请求级别的信息和取消信号
//   - so: 存储选项，包括副本策略、数据中心等配置
//   - inputChunks: 待检查的 chunk 列表
//
// 返回:
//   - mergedChunks: 合并后的 chunk 列表（如果不需要合并则返回原列表）
//   - err: 合并过程中的错误
//
// 合并条件:
//   1. 所有 chunk 都不是 SSE 加密的（SSE 加密的 chunk 需要保留原始元数据）
//   2. 小 chunk（小于 chunkSize/2）数量超过 MergeChunkMinCount（1000）
//   3. 小 chunk 数量超过总 chunk 数量的一半
//
// 为什么要合并:
//   - 减少元数据开销：1000 个小 chunk 合并为 10 个大 chunk，元数据量减少 99%
//   - 提升读取性能：减少网络往返次数和查询延迟
//   - 降低存储成本：删除冗余数据，减少碎片化
func (fs *FilerServer) maybeMergeChunks(ctx context.Context, so *operation.StorageOption, inputChunks []*filer_pb.FileChunk) (mergedChunks []*filer_pb.FileChunk, err error) {
	// 【检查 1：SSE 加密 chunk 不能合并】
	// SSE 加密的 chunk 需要保留原始元数据（IV、KeyMD5 等），合并会破坏加密结构
	// 因此直接跳过合并，返回原始 chunk 列表
	for _, chunk := range inputChunks {
		if chunk.GetSseType() != 0 { // 只要是 SSE 类型（SSE-C、SSE-KMS 或 SSE-S3）便跳过合并
			glog.V(3).InfofCtx(ctx, "Skipping chunk merge for SSE-encrypted chunks")
			return inputChunks, nil
		}
	}

	// 【检查 2：统计小 chunk 数量】
	// 仅当小块数量超过文件的一半且数量达到阈值时才进行合并
	// chunkSize 是配置的最大 chunk 大小（MaxMB * 1024 * 1024 字节）
	var chunkSize = fs.option.MaxMB * 1024 * 1024
	var smallChunk, sumChunk int         // smallChunk: 小 chunk 数量，sumChunk: 总 chunk 数量
	var minOffset int64 = math.MaxInt64  // 记录最小偏移量，用于确定合并起始位置
	for _, chunk := range inputChunks {
		// ChunkManifest 是大文件的 chunk 索引，不参与大小统计
		if chunk.IsChunkManifest {
			continue
		}
		// 判断是否为小 chunk：大小小于配置值的一半
		// 例如：MaxMB=4，则 chunkSize=4MB，小于 2MB 的 chunk 被视为小 chunk
		if chunk.Size < uint64(chunkSize/2) {
			smallChunk++
			// 记录最小偏移量，用于确定从哪里开始合并
			if chunk.Offset < minOffset {
				minOffset = chunk.Offset
			}
		}
		sumChunk++
	}

	// 【检查 3：判断是否满足合并条件】
	// 条件 1：小 chunk 数量必须超过 MergeChunkMinCount（1000）
	// 条件 2：小 chunk 数量必须超过总数的一半
	// 只有同时满足这两个条件，才认为碎片化严重，需要合并
	if smallChunk < MergeChunkMinCount || smallChunk < sumChunk/2 {
		return inputChunks, nil
	}

	// 满足合并条件，执行实际的合并操作
	return fs.mergeChunks(ctx, so, inputChunks, minOffset)
}

// mergeChunks 执行真正的合并操作
//
// 参数:
//   - ctx: 上下文
//   - so: 存储选项（副本策略、数据中心等）
//   - inputChunks: 需要合并的 chunk 列表
//   - chunkOffset: 合并的起始偏移量（从最小的小 chunk 偏移开始）
//
// 返回:
//   - mergedChunks: 合并后的新 chunk 列表
//   - mergeErr: 合并过程中的错误
//
// 工作流程:
//   1. 创建 ChunkStreamReader，从现有 chunk 读取数据
//   2. 将读取的数据重新上传为新的大 chunk
//   3. 保留未参与合并的 chunk（offset < chunkOffset 的 chunk 和 ChunkManifest）
//   4. 计算并删除冗余的旧 chunk（垃圾回收）
//
// 注意事项:
//   - 合并过程中会产生网络流量和磁盘 I/O
//   - 合并失败不会影响原有数据，因为旧 chunk 只有在合并成功后才会删除
//   - ChunkManifest 不参与合并，因为它们是大文件的索引结构
func (fs *FilerServer) mergeChunks(ctx context.Context, so *operation.StorageOption, inputChunks []*filer_pb.FileChunk, chunkOffset int64) (mergedChunks []*filer_pb.FileChunk, mergeErr error) {
	// 【步骤 1：创建 chunk 流读取器】
	// ChunkStreamReader 可以将多个 chunk 看作一个连续的字节流
	// 它会自动处理跨 chunk 的读取和数据拼接
	chunkedFileReader := filer.NewChunkStreamReaderFromFiler(ctx, fs.filer.MasterClient, inputChunks)

	// 【步骤 2：定位到合并起始位置】
	// 使用 SeekCurrent 从当前位置（0）移动到 chunkOffset
	// 这样后续读取就从 chunkOffset 开始，只合并需要的部分
	_, mergeErr = chunkedFileReader.Seek(chunkOffset, io.SeekCurrent)
	if mergeErr != nil {
		return nil, mergeErr
	}

	// 【步骤 3：重新上传为新的大 chunk】
	// uploadReaderToChunks 会将流式数据按 MaxMB 大小拆分并上传
	// 参数说明：
	//   - ctx: 上下文
	//   - nil: 不需要 HTTP 请求对象（不涉及 SSE-C 头信息）
	//   - chunkedFileReader: 数据源（从旧 chunk 读取）
	//   - chunkOffset: 起始偏移量
	//   - MaxMB*1024*1024: 每个新 chunk 的最大大小
	//   - "", "": 文件名和 ContentType 为空（元数据已存在）
	//   - true: isAppend=true，表示追加模式
	//   - so: 存储选项
	mergedChunks, _, _, mergeErr, _ = fs.uploadReaderToChunks(ctx, nil, chunkedFileReader, chunkOffset, int32(fs.option.MaxMB*1024*1024), "", "", true, so)
	if mergeErr != nil {
		return
	}

	// 【步骤 4：记录统计指标】
	// 增加 chunk 合并计数器，用于监控和分析
	stats.FilerHandlerCounter.WithLabelValues(stats.ChunkMerge).Inc()

	// 【步骤 5：保留未参与合并的 chunk】
	// 两类 chunk 需要保留：
	//   1. offset < chunkOffset 的 chunk：在合并起始位置之前，不需要合并
	//   2. ChunkManifest：大文件的索引结构，必须保留
	for _, chunk := range inputChunks {
		if chunk.Offset < chunkOffset || chunk.IsChunkManifest {
			mergedChunks = append(mergedChunks, chunk)
		}
	}

	// 【步骤 6：计算并删除垃圾 chunk】
	// MinusChunks 计算 inputChunks - mergedChunks，得到需要删除的冗余 chunk
	// 这些 chunk 的数据已经被重新上传，可以安全删除
	garbage, err := filer.MinusChunks(ctx, fs.lookupFileId, inputChunks, mergedChunks)
	if err != nil {
		glog.ErrorfCtx(ctx, "Failed to resolve old entry chunks when delete old entry chunks. new: %s, old: %s",
			mergedChunks, inputChunks)
		return mergedChunks, err
	}

	// 【步骤 7：异步删除垃圾 chunk】
	// DeleteChunksNotRecursive 将 chunk 标记为删除，后续由 vacuum 进程回收空间
	// 这是非阻塞操作，不会影响当前请求的响应时间
	fs.filer.DeleteChunksNotRecursive(garbage)
	return
}
