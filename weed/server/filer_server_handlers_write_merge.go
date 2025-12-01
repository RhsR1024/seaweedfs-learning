// Package weed_server 中的 filer_server_handlers_write_merge.go 负责小块合并逻辑
// 在写入过程中通过合并大量碎片块以提升读性能并减少索引项。
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

const MergeChunkMinCount int = 1000

// maybeMergeChunks 根据 chunk 数量与大小判断是否需要合并
// 若满足阈值则调用 mergeChunks，否则直接返回原列表
func (fs *FilerServer) maybeMergeChunks(ctx context.Context, so *operation.StorageOption, inputChunks []*filer_pb.FileChunk) (mergedChunks []*filer_pb.FileChunk, err error) {
	// SSE 加密的 chunk 需要保留原始元数据，这里直接跳过合并
	for _, chunk := range inputChunks {
		if chunk.GetSseType() != 0 { // 只要是 SSE 类型（SSE-C 或 SSE-KMS）便跳过合并
			glog.V(3).InfofCtx(ctx, "Skipping chunk merge for SSE-encrypted chunks")
			return inputChunks, nil
		}
	}

	// 仅当小块数量超过文件的一半且数量达到阈值时才进行合并
	var chunkSize = fs.option.MaxMB * 1024 * 1024
	var smallChunk, sumChunk int
	var minOffset int64 = math.MaxInt64
	for _, chunk := range inputChunks {
		if chunk.IsChunkManifest {
			continue
		}
		if chunk.Size < uint64(chunkSize/2) {
			smallChunk++
			if chunk.Offset < minOffset {
				minOffset = chunk.Offset
			}
		}
		sumChunk++
	}
	if smallChunk < MergeChunkMinCount || smallChunk < sumChunk/2 {
		return inputChunks, nil
	}

	return fs.mergeChunks(ctx, so, inputChunks, minOffset)
}

// mergeChunks 执行真正的合并操作
// 会重读原 chunk 数据上传为新的大块，并在完成后删除冗余数据
func (fs *FilerServer) mergeChunks(ctx context.Context, so *operation.StorageOption, inputChunks []*filer_pb.FileChunk, chunkOffset int64) (mergedChunks []*filer_pb.FileChunk, mergeErr error) {
	chunkedFileReader := filer.NewChunkStreamReaderFromFiler(ctx, fs.filer.MasterClient, inputChunks)
	_, mergeErr = chunkedFileReader.Seek(chunkOffset, io.SeekCurrent)
	if mergeErr != nil {
		return nil, mergeErr
	}
	mergedChunks, _, _, mergeErr, _ = fs.uploadReaderToChunks(ctx, nil, chunkedFileReader, chunkOffset, int32(fs.option.MaxMB*1024*1024), "", "", true, so)
	if mergeErr != nil {
		return
	}

	stats.FilerHandlerCounter.WithLabelValues(stats.ChunkMerge).Inc()
	for _, chunk := range inputChunks {
		if chunk.Offset < chunkOffset || chunk.IsChunkManifest {
			mergedChunks = append(mergedChunks, chunk)
		}
	}

	garbage, err := filer.MinusChunks(ctx, fs.lookupFileId, inputChunks, mergedChunks)
	if err != nil {
		glog.ErrorfCtx(ctx, "Failed to resolve old entry chunks when delete old entry chunks. new: %s, old: %s",
			mergedChunks, inputChunks)
		return mergedChunks, err
	}
	fs.filer.DeleteChunksNotRecursive(garbage)
	return
}
