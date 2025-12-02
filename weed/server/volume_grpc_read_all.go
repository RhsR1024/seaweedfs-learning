// Package weed_server 实现 Volume Server 的批量读取功能
// 本文件提供读取整个 Volume 所有 Needle 的能力
//
// 核心功能:
//   - 批量读取：一次性读取 Volume 中的所有 Needle
//   - 流式返回：通过 gRPC 流式返回，支持大 Volume
//   - 多 Volume 支持：支持同时读取多个 Volume
//   - 按序扫描：从 SuperBlock 后开始，顺序扫描整个 Volume 文件
//
// 使用场景:
//   - Volume 备份：将整个 Volume 的数据复制到其他位置
//   - 数据迁移：在 Volume Server 之间迁移数据
//   - 数据分析：扫描 Volume 中的所有文件进行统计分析
//   - 灾难恢复：从损坏的 Volume 中恢复尽可能多的数据
//   - Vacuum 操作：扫描 Volume 找出可以压缩的空间
//
// 注意事项:
//   - 会读取整个 Volume 文件，I/O 开销大
//   - 适合批量操作，不适合单个文件读取
//   - 不检查 Needle 是否已删除（标记为删除的 Needle 也会返回）
//   - 需要足够的网络带宽和内存
//
// 扫描流程:
//   1. 从 SuperBlock 之后开始扫描（跳过前 8 字节）
//   2. 顺序读取每个 Needle（包括已删除的）
//   3. 通过 gRPC 流式返回 Needle 数据
//   4. 继续下一个 Volume
package weed_server

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// ReadAllNeedles 批量读取多个 Volume 的所有 Needle
// 通过 gRPC 流式返回所有 Needle 数据
//
// 参数:
//   - req: 读取请求，包含：
//     - VolumeIds: 要读取的 Volume ID 列表
//   - stream: gRPC 流，用于流式返回 Needle 数据
//
// 返回:
//   - error: 读取错误（Volume 不存在、磁盘错误等）
//
// 工作流程:
//   1. 遍历所有 VolumeIds
//   2. 对每个 Volume 调用 streamReadOneVolume
//   3. 顺序扫描 Volume 文件
//   4. 将所有 Needle 通过 stream 返回
//
// 示例:
//   req := &ReadAllNeedlesRequest{
//       VolumeIds: []uint32{1, 2, 3},
//   }
//   // 依次读取 Volume 1、2、3 的所有 Needle
func (vs *VolumeServer) ReadAllNeedles(req *volume_server_pb.ReadAllNeedlesRequest, stream volume_server_pb.VolumeServer_ReadAllNeedlesServer) (err error) {

	// 遍历所有要读取的 Volume ID
	for _, vid := range req.VolumeIds {
		// 流式读取单个 Volume 的所有 Needle
		if err := vs.streamReadOneVolume(needle.VolumeId(vid), stream); err != nil {
			return err
		}
	}
	return nil
}

// streamReadOneVolume 流式读取单个 Volume 的所有 Needle
// 从 SuperBlock 之后开始，顺序扫描整个 Volume 文件
//
// 参数:
//   - vid: Volume ID
//   - stream: gRPC 流，用于流式返回 Needle 数据
//
// 返回:
//   - error: 读取错误（Volume 不存在、扫描失败等）
//
// 工作流程:
//   1. 获取 Volume 对象
//   2. 创建 VolumeFileScanner4ReadAll 扫描器
//   3. 从 SuperBlock 后开始扫描（offset = SuperBlock.BlockSize()）
//   4. 顺序读取每个 Needle
//   5. 通过 scanner 的 Stream 发送 Needle 数据
//
// 扫描原理:
//   - Volume 文件结构：[SuperBlock(8B)][Needle1][Needle2][Needle3]...
//   - SuperBlock 包含 Version、副本策略、TTL 等元数据
//   - 从 SuperBlock 之后开始扫描，顺序读取每个 Needle
//   - Needle 格式根据 Version 不同（v1/v2/v3）
//
// 性能考虑:
//   - 顺序读取，充分利用磁盘顺序 I/O 性能
//   - 流式返回，避免一次性加载整个 Volume 到内存
//   - 不经过 Needle Index，直接扫描文件
func (vs *VolumeServer) streamReadOneVolume(vid needle.VolumeId, stream volume_server_pb.VolumeServer_ReadAllNeedlesServer) error {
	// 【获取 Volume 对象】
	v := vs.store.GetVolume(vid)
	if v == nil {
		return fmt.Errorf("未找到 volume id %d", vid)
	}

	// 【创建扫描器】
	// VolumeFileScanner4ReadAll 是专门用于 ReadAll 操作的扫描器
	// 它实现了 VolumeFileScanner 接口，负责处理扫描到的每个 Needle
	scanner := &storage.VolumeFileScanner4ReadAll{
		Stream: stream,  // gRPC 流，用于发送 Needle 数据
		V:      v,       // Volume 对象，提供版本和元数据信息
	}

	// 【计算扫描起始偏移量】
	// SuperBlock 占据 Volume 文件的前 8 字节（v1）或更多（v2/v3）
	// 需要跳过 SuperBlock，从第一个 Needle 开始扫描
	// BlockSize() 返回 SuperBlock 的实际大小
	offset := int64(v.SuperBlock.BlockSize())

	// 【开始扫描】
	// ScanVolumeFileFrom 从指定 offset 开始顺序扫描 Volume 文件
	// 参数:
	//   - v.Version(): Needle 版本（v1/v2/v3），决定 Needle 格式
	//   - v.DataBackend: Volume 文件的后端存储（本地文件/云存储等）
	//   - offset: 扫描起始位置（SuperBlock 之后）
	//   - scanner: 扫描器，处理每个扫描到的 Needle
	//
	// 扫描过程:
	//   1. 读取 Needle Header（Cookie、NeedleId、Size 等）
	//   2. 读取 Needle Data（文件内容）
	//   3. 读取 Needle Footer（Checksum、Flags、Metadata 等）
	//   4. 调用 scanner.VisitNeedle() 处理 Needle
	//   5. 继续下一个 Needle，直到文件末尾
	//
	// scanner.VisitNeedle() 内部逻辑（在 VolumeFileScanner4ReadAll 中实现）:
	//   - 将 Needle 数据序列化为 protobuf 格式
	//   - 通过 Stream.Send() 发送给客户端
	//   - 流式传输，避免内存溢出
	return storage.ScanVolumeFileFrom(v.Version(), v.DataBackend, offset, scanner)
}
