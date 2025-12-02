// Package weed_server 实现 Volume Server 的增量复制 gRPC 接口
// 本文件提供 Volume 的增量同步功能
//
// 核心功能:
//   - 增量复制：只复制自上次同步后新增的数据
//   - 同步状态查询：获取 Volume 的当前同步状态
//   - 流式传输：使用 gRPC 流高效传输数据
//
// 增量复制原理:
//   - Volume 中的每个 Needle 都有 AppendAtNs 时间戳
//   - 通过 SinceNs 参数指定起始时间
//   - 使用二分查找定位起始 Needle
//   - 流式传输从起始位置到文件末尾的所有数据
//
// 应用场景:
//   - Volume 副本同步
//   - 跨数据中心复制
//   - 数据备份和恢复
//   - Volume 迁移
//
// 性能优化:
//   - 增量复制避免全量复制
//   - 2MB 缓冲区批量传输
//   - gRPC 流减少网络往返
package weed_server

import (
	"context"
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// VolumeIncrementalCopy 增量复制 Volume 数据
// gRPC Streaming API: VolumeIncrementalCopy
//
// 功能:
//   - 根据时间戳增量复制 Volume 数据
//   - 使用二分查找定位起始 Needle
//   - 流式传输增量数据到客户端
//
// 参数:
//   - VolumeId: 要复制的卷 ID
//   - SinceNs: 起始时间戳（纳秒），只复制此时间后的数据
//
// 复制流程:
//   【步骤 1：定位起始位置】
//   1. 获取 Volume 对象
//   2. 使用二分查找定位 SinceNs 对应的 Needle
//   3. 如果是最后一个 Needle，说明没有新数据，返回
//
//   【步骤 2：计算复制范围】
//   1. startOffset: 起始 Needle 的文件偏移量
//   2. stopOffset: 当前文件大小（末尾位置）
//   3. 复制区间：[startOffset, stopOffset)
//
//   【步骤 3：流式传输】
//   1. 使用 2MB 缓冲区读取数据
//   2. 通过 gRPC 流发送到客户端
//   3. 循环直到传输完成
//
// 增量复制原理:
//   - 每个 Needle 在写入时记录 AppendAtNs（追加时间）
//   - Volume 按时间顺序追加 Needle
//   - 二分查找时间复杂度 O(log N)
//   - 只传输增量数据，避免全量复制
//
// 使用场景:
//   - 副本同步：定期同步主副本到从副本
//   - 跨数据中心复制：异步复制到远程集群
//   - 数据恢复：从备份恢复数据
//   - 负载均衡：迁移 Volume 到其他服务器
//
// 返回:
//   - 流式返回 VolumeIncrementalCopyResponse
//   - FileContent: 文件内容块（最大 2MB）
//
// 注意:
//   - 复制过程中 Volume 可以继续写入
//   - 不保证强一致性，适合最终一致性场景
//   - 客户端需要处理部分数据的情况
func (vs *VolumeServer) VolumeIncrementalCopy(req *volume_server_pb.VolumeIncrementalCopyRequest, stream volume_server_pb.VolumeServer_VolumeIncrementalCopyServer) error {

	// 【步骤 1：定位起始位置】
	// 获取 Volume 对象
	v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
	if v == nil {
		return fmt.Errorf("not found volume id %d", req.VolumeId)
	}

	// 获取文件当前大小（停止位置）
	stopOffset, _, _ := v.FileStat()

	// 使用二分查找定位 SinceNs 对应的 Needle
	// foundOffset: 找到的 Needle 在索引中的位置
	// isLastOne: 是否是最后一个 Needle
	foundOffset, isLastOne, err := v.BinarySearchByAppendAtNs(req.SinceNs)
	if err != nil {
		return fmt.Errorf("fail to locate by appendAtNs %d: %s", req.SinceNs, err)
	}

	// 如果是最后一个 Needle，说明没有新数据
	if isLastOne {
		return nil
	}

	// 【步骤 2：计算复制范围】
	// 将索引位置转换为文件偏移量
	startOffset := foundOffset.ToActualOffset()

	// 【步骤 3：流式传输】
	// 创建 2MB 缓冲区
	buf := make([]byte, 1024*1024*2)
	// 流式发送文件内容
	return sendFileContent(v.DataBackend, buf, startOffset, int64(stopOffset), stream)

}

// VolumeSyncStatus 获取 Volume 的同步状态
// gRPC API: VolumeSyncStatus
//
// 功能:
//   - 返回 Volume 的当前同步状态
//   - 包括最后同步时间、文件大小等
//
// 参数:
//   - VolumeId: 要查询的卷 ID
//
// 返回信息:
//   - TailOffset: 当前文件末尾偏移量
//   - CompactRevision: 压缩版本号
//   - IdxFileSize: 索引文件大小
//
// 使用场景:
//   - 检查副本是否同步
//   - 监控复制进度
//   - 决定是否需要增量复制
//
// 返回:
//   - VolumeSyncStatusResponse: 同步状态信息
//   - error: 查询失败错误
func (vs *VolumeServer) VolumeSyncStatus(ctx context.Context, req *volume_server_pb.VolumeSyncStatusRequest) (*volume_server_pb.VolumeSyncStatusResponse, error) {

	// 获取 Volume 对象
	v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
	if v == nil {
		return nil, fmt.Errorf("not found volume id %d", req.VolumeId)
	}

	// 获取同步状态
	resp := v.GetVolumeSyncStatus()

	return resp, nil

}

// sendFileContent 流式发送文件内容
// 内部辅助函数，用于分块读取和发送文件数据
//
// 功能:
//   - 从文件中分块读取数据
//   - 通过 gRPC 流发送到客户端
//   - 处理 EOF 和错误情况
//
// 参数:
//   - datBackend: 数据后端（文件或对象存储）
//   - buf: 缓冲区（通常 2MB）
//   - startOffset: 起始偏移量
//   - stopOffset: 结束偏移量
//   - stream: gRPC 流对象
//
// 发送流程:
//   1. 循环读取文件块（每次最多 2MB）
//   2. 将读取的数据封装为响应消息
//   3. 通过 stream.Send() 发送到客户端
//   4. 处理 EOF（文件末尾）
//   5. 处理读取或发送错误
//
// 错误处理:
//   - EOF: 正常情况，继续发送
//   - 读取错误: 返回错误，中断传输
//   - 发送错误: 返回错误，中断传输
//
// 返回:
//   - error: 读取或发送失败错误
func sendFileContent(datBackend backend.BackendStorageFile, buf []byte, startOffset, stopOffset int64, stream volume_server_pb.VolumeServer_VolumeIncrementalCopyServer) error {
	// 缓冲区大小
	var blockSizeLimit = int64(len(buf))

	// 循环发送文件内容
	for i := int64(0); i < stopOffset-startOffset; i += blockSizeLimit {
		// 读取文件块
		n, readErr := datBackend.ReadAt(buf, startOffset+i)

		// 处理读取结果
		if readErr == nil || readErr == io.EOF {
			// 读取成功或到达文件末尾
			// 创建响应消息
			resp := &volume_server_pb.VolumeIncrementalCopyResponse{}
			resp.FileContent = buf[:int64(n)]

			// 发送到客户端
			sendErr := stream.Send(resp)
			if sendErr != nil {
				// 发送失败
				return sendErr
			}
		} else {
			// 读取失败
			return readErr
		}
	}
	return nil
}
