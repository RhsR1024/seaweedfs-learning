// Package weed_server 实现 Volume Server 的底层读写功能
// 本文件提供直接操作 Needle 二进制数据的 gRPC 接口
//
// 核心功能:
//   - 读取 Needle 原始数据：ReadNeedleBlob 读取指定偏移量的二进制数据
//   - 读取 Needle 元数据：ReadNeedleMeta 读取 Needle 的元数据（Cookie、TTL、Checksum 等）
//   - 写入 Needle 数据：WriteNeedleBlob 直接写入 Needle 二进制数据
//
// 使用场景:
//   - Volume 复制：在 Volume Server 之间复制 Needle 数据
//   - Volume 迁移：将 Needle 从一个 Volume 迁移到另一个 Volume
//   - Tiered Storage：将 Needle 从本地存储移动到远程存储
//   - EC 重建：从 EC Shard 重建完整的 Volume
//   - 增量备份：只备份变化的 Needle 数据
//
// 与普通读写的区别:
//   - 普通读写：通过 HTTP API 读写完整的文件（包含解析和验证）
//   - 底层读写：直接操作 Needle 的二进制数据（用于 Volume 管理）
//
// 注意事项:
//   - 仅供内部使用，不暴露给最终用户
//   - 需要精确的偏移量和大小信息
//   - 跳过大部分验证，性能更高但风险更大
//   - 通常与 ReadAllNeedles 配合使用
//
// 典型使用流程（Volume 复制）:
//   1. 源 Volume Server：ReadNeedleMeta 读取元数据
//   2. 源 Volume Server：ReadNeedleBlob 读取二进制数据
//   3. 目标 Volume Server：WriteNeedleBlob 写入二进制数据
//   4. 验证：对比 Checksum 确保数据完整性
package weed_server

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// ReadNeedleBlob 读取 Needle 的原始二进制数据
// 从指定偏移量读取指定大小的数据，不经过任何解析和验证
//
// 参数:
//   - ctx: 上下文
//   - req: 读取请求，包含：
//     - VolumeId: Volume ID
//     - Offset: 读取偏移量（在 Volume 文件中的绝对位置）
//     - Size: 读取大小（字节数）
//
// 返回:
//   - resp: 读取响应，包含：
//     - NeedleBlob: 读取的二进制数据
//   - error: 读取错误（Volume 不存在、偏移量越界等）
//
// 工作流程:
//   1. 获取 Volume 对象
//   2. 调用 Volume.ReadNeedleBlob() 读取数据
//   3. 返回原始二进制数据
//
// 偏移量计算:
//   - 偏移量是相对于 Volume 文件开头的绝对位置
//   - 包括 SuperBlock 的大小
//   - 通常通过扫描 Volume 或查询索引获得
//
// 性能特点:
//   - 无解析开销，直接读取二进制数据
//   - 无验证开销，不检查 Checksum
//   - 适合批量复制和迁移
//
// 使用示例（Volume 复制）:
//   // 假设通过 ReadAllNeedles 获得了 offset 和 size
//   req := &ReadNeedleBlobRequest{
//       VolumeId: 3,
//       Offset: 1024,  // Needle 在文件中的偏移量
//       Size: 4096,    // Needle 的总大小
//   }
//   resp, err := volumeServer.ReadNeedleBlob(ctx, req)
//   // resp.NeedleBlob 包含 Needle 的完整二进制数据
func (vs *VolumeServer) ReadNeedleBlob(ctx context.Context, req *volume_server_pb.ReadNeedleBlobRequest) (resp *volume_server_pb.ReadNeedleBlobResponse, err error) {
	resp = &volume_server_pb.ReadNeedleBlobResponse{}

	// 【获取 Volume 对象】
	v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
	if v == nil {
		return nil, fmt.Errorf("未找到 volume id %d", req.VolumeId)
	}

	// 【读取原始数据】
	// ReadNeedleBlob 从指定偏移量读取指定大小的二进制数据
	// 不进行任何解析、验证或解压缩
	// 返回的是 Needle 的完整二进制表示
	resp.NeedleBlob, err = v.ReadNeedleBlob(req.Offset, types.Size(req.Size))
	if err != nil {
		return nil, fmt.Errorf("读取 needle blob offset %d size %d: %v", req.Offset, req.Size, err)
	}

	return resp, nil
}

// ReadNeedleMeta 读取 Needle 的元数据
// 只读取 Needle Header 和 Footer，不读取 Data 部分
//
// 参数:
//   - ctx: 上下文
//   - req: 读取请求，包含：
//     - VolumeId: Volume ID
//     - NeedleId: Needle ID
//     - Offset: Needle 在文件中的偏移量
//     - Size: Needle 的总大小
//
// 返回:
//   - resp: 元数据响应，包含：
//     - Cookie: Needle Cookie（防猜测）
//     - LastModified: 最后修改时间（Unix 时间戳）
//     - Crc: CRC32 校验和
//     - Ttl: TTL 字符串（如 "3d" 表示 3 天）
//     - AppendAtNs: 追加时间戳（纳秒）
//   - error: 读取错误
//
// 工作流程:
//   1. 验证 Volume 存在（不支持从 EC Shard 读取元数据）
//   2. 调用 Store.ReadVolumeNeedleMetaAt() 读取元数据
//   3. 提取并返回元数据字段
//
// 与 ReadNeedleBlob 的区别:
//   - ReadNeedleBlob: 读取完整的二进制数据（包括 Data）
//   - ReadNeedleMeta: 只读取元数据（跳过 Data，更快）
//
// 使用场景:
//   - 验证 Needle 完整性：检查 CRC
//   - 检查 TTL：判断 Needle 是否过期
//   - 获取修改时间：用于增量备份
//   - 复制前验证：确保源 Needle 有效
//
// 使用示例（验证 Needle）:
//   req := &ReadNeedleMetaRequest{
//       VolumeId: 3,
//       NeedleId: 0x01e3b0756f,
//       Offset: 1024,
//       Size: 4096,
//   }
//   resp, err := volumeServer.ReadNeedleMeta(ctx, req)
//   // 检查 resp.Crc、resp.Ttl 等元数据
func (vs *VolumeServer) ReadNeedleMeta(ctx context.Context, req *volume_server_pb.ReadNeedleMetaRequest) (resp *volume_server_pb.ReadNeedleMetaResponse, err error) {
	resp = &volume_server_pb.ReadNeedleMetaResponse{}
	volumeId := needle.VolumeId(req.VolumeId)

	// 【创建 Needle 对象】
	// 只填充必要字段，用于元数据读取
	n := &needle.Needle{
		Id:    types.NeedleId(req.NeedleId),
		Flags: 0x08,  // 设置 HasLastModifiedDate 标志
	}
	size := req.Size
	offset := req.Offset

	// 【验证 Volume 存在】
	// 不支持从 EC Shard 读取元数据
	// EC Shard 只包含数据分片，不包含完整的元数据
	hasVolume := vs.store.HasVolume(volumeId)
	if !hasVolume {
		return nil, fmt.Errorf("未找到 volume id %d，不支持从 ec shards 读取 needle 元数据", req.VolumeId)
	}

	// 【读取元数据】
	// ReadVolumeNeedleMetaAt 从指定偏移量读取 Needle 的元数据
	// 参数:
	//   - volumeId: Volume ID
	//   - n: Needle 对象（填充 Id），结果会写入此对象
	//   - offset: Needle 在文件中的偏移量
	//   - size: Needle 的总大小
	//
	// 读取内容:
	//   - Cookie: 从 Needle Header 读取
	//   - LastModified: 从 Needle Footer 读取
	//   - Checksum: 从 Needle Footer 读取
	//   - TTL: 从 Needle Footer 读取
	//   - AppendAtNs: 从 Needle Footer 读取（v3）
	//
	// 跳过内容:
	//   - Data: 不读取文件数据，节省 I/O
	err = vs.store.ReadVolumeNeedleMetaAt(volumeId, n, offset, size)
	if err != nil {
		return nil, err
	}

	// 【提取元数据】
	// 将 Needle 对象中的元数据复制到响应中
	resp.Cookie = uint32(n.Cookie)
	resp.LastModified = n.LastModified
	resp.Crc = n.Checksum.Value()

	// TTL（生存时间）
	// 如果 Needle 有 TTL，转换为字符串格式（如 "3d"、"2h"）
	if n.HasTtl() {
		resp.Ttl = n.Ttl.String()
	}

	// AppendAtNs（追加时间戳，纳秒）
	// 仅 Needle v3 格式支持
	resp.AppendAtNs = n.AppendAtNs

	return resp, nil
}

// WriteNeedleBlob 直接写入 Needle 的原始二进制数据
// 用于 Volume 复制、迁移等内部操作
//
// 参数:
//   - ctx: 上下文
//   - req: 写入请求，包含：
//     - VolumeId: Volume ID
//     - NeedleId: Needle ID
//     - NeedleBlob: Needle 的完整二进制数据
//     - Size: 数据大小
//
// 返回:
//   - resp: 写入响应（空）
//   - error: 写入错误（Volume 不存在、磁盘满等）
//
// 工作流程:
//   1. 获取 Volume 对象
//   2. 调用 Volume.WriteNeedleBlob() 直接写入二进制数据
//   3. 返回成功
//
// 与普通写入的区别:
//   - 普通写入（PostUploadHandler）：解析 HTTP 请求，构造 Needle，写入
//   - WriteNeedleBlob：直接写入预先构造好的二进制数据
//
// 安全性:
//   - 不验证数据格式，假设调用者已验证
//   - 不更新 Needle Index（需要手动调用或依赖 Volume 内部逻辑）
//   - 跳过重复检测（可能覆盖已有 Needle）
//
// 使用场景:
//   - Volume 复制：从源 Volume 复制 Needle 到目标 Volume
//   - Volume 迁移：将 Needle 移动到新的 Volume
//   - EC 重建：从 EC Shard 重建完整的 Volume
//   - Tiered Storage：从远程存储恢复 Needle
//
// 使用示例（Volume 复制）:
//   // 1. 从源 Volume 读取
//   readReq := &ReadNeedleBlobRequest{
//       VolumeId: 3,
//       Offset: 1024,
//       Size: 4096,
//   }
//   readResp, _ := sourceVolumeServer.ReadNeedleBlob(ctx, readReq)
//
//   // 2. 写入目标 Volume
//   writeReq := &WriteNeedleBlobRequest{
//       VolumeId: 5,
//       NeedleId: 0x01e3b0756f,
//       NeedleBlob: readResp.NeedleBlob,
//       Size: 4096,
//   }
//   targetVolumeServer.WriteNeedleBlob(ctx, writeReq)
func (vs *VolumeServer) WriteNeedleBlob(ctx context.Context, req *volume_server_pb.WriteNeedleBlobRequest) (resp *volume_server_pb.WriteNeedleBlobResponse, err error) {
	resp = &volume_server_pb.WriteNeedleBlobResponse{}

	// 【获取 Volume 对象】
	v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
	if v == nil {
		return nil, fmt.Errorf("未找到 volume id %d", req.VolumeId)
	}

	// 【写入原始数据】
	// WriteNeedleBlob 直接将二进制数据追加到 Volume 文件
	// 参数:
	//   - needleId: Needle ID
	//   - needleBlob: 完整的 Needle 二进制数据
	//   - size: 数据大小
	//
	// 写入内容:
	//   - needleBlob 应该是完整的 Needle 二进制格式
	//   - 包括：Cookie + NeedleId + Size + Data + Footer
	//   - 不进行格式验证，直接写入
	//
	// 索引更新:
	//   - WriteNeedleBlob 内部会更新 Needle Index
	//   - 将 NeedleId → Offset 的映射添加到索引
	//
	// 注意事项:
	//   - 假设 needleBlob 格式正确
	//   - 如果格式错误，可能导致 Volume 损坏
	//   - 通常只在受信任的内部操作中使用
	if err = v.WriteNeedleBlob(types.NeedleId(req.NeedleId), req.NeedleBlob, types.Size(req.Size)); err != nil {
		return nil, fmt.Errorf("写入 blob needle %d size %d: %v", req.NeedleId, req.Size, err)
	}

	return resp, nil
}
