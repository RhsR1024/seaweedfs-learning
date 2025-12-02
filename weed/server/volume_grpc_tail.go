// Package weed_server 实现 Volume Server 的 Tail 增量同步功能
// 本文件提供实时增量复制 Volume 数据的能力
//
// 核心功能:
//   - 增量同步：VolumeTailSender 持续发送新写入的 Needle
//   - 增量接收：VolumeTailReceiver 接收并写入新的 Needle
//   - 时间戳定位：根据 AppendAtNs 定位增量数据
//   - 流式传输：通过 gRPC 流式传输大文件
//   - 心跳保活：空闲时发送心跳保持连接
//
// 使用场景:
//   - Volume 实时复制：将新写入的数据实时复制到备份 Volume
//   - 灾难恢复（DR）：持续同步数据到灾备站点
//   - 只读副本：创建只读副本用于读负载分担
//   - 跨数据中心同步：在不同数据中心间同步数据
//   - 增量备份：只备份增量数据，节省网络和存储
//
// Tail 同步架构:
//   1. 源端（Sender）：
//      - 根据 SinceNs 时间戳定位增量数据起点
//      - 使用 BinarySearchByAppendAtNs 快速定位
//      - 顺序扫描后续所有 Needle
//      - 通过 gRPC 流式发送 Needle Header + Body
//   2. 目标端（Receiver）：
//      - 接收 Needle 数据
//      - 写入本地 Volume
//      - 更新时间戳，继续接收后续数据
//
// AppendAtNs 时间戳:
//   - Needle v3 格式引入的追加时间戳（纳秒精度）
//   - 记录 Needle 被追加到 Volume 的准确时间
//   - 用于增量同步的定位和去重
//   - 索引中维护 AppendAtNs → Offset 的映射
//
// 空闲超时机制:
//   - IdleTimeoutSeconds: 空闲多久后自动断开
//   - drainingSeconds: 倒计时器
//   - 有新数据时重置倒计时
//   - 超时后优雅关闭连接
//
// 工作流程:
//   1. Receiver 向 Sender 发起连接，指定 SinceNs
//   2. Sender 定位到 SinceNs 对应的 Needle
//   3. Sender 顺序扫描并发送后续所有 Needle
//   4. Receiver 写入 Needle 到本地 Volume
//   5. Sender 扫描完毕后等待 2 秒，检查是否有新数据
//   6. 有新数据则继续发送，无新数据则倒计时
//   7. 超时或连接断开时结束同步
//
// 性能特点:
//   - 低延迟：新数据写入后 2 秒内可同步
//   - 低开销：只传输增量数据
//   - 可中断：支持断点续传，下次从最新时间戳继续
//   - 分块传输：大 Needle 分块发送，避免内存溢出
package weed_server

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

// VolumeTailSender 作为发送端，持续发送 Volume 的增量 Needle 数据
// 通过 gRPC 流式传输，支持断点续传和空闲超时
//
// 参数:
//   - req: Tail 发送请求，包含：
//     - VolumeId: Volume ID
//     - SinceNs: 起始时间戳（纳秒），只发送此时间之后的 Needle
//     - IdleTimeoutSeconds: 空闲超时时间（秒）
//       * 0：永不超时，持续同步
//       * >0：空闲指定时间后自动断开
//   - stream: gRPC 流，用于流式发送 Needle 数据
//
// 返回:
//   - error: 发送错误（Volume 不存在、网络错误等）
//
// 工作流程:
//   1. 验证 Volume 存在
//   2. 循环执行增量同步：
//      a. 调用 sendNeedlesSince 发送新 Needle
//      b. 等待 2 秒
//      c. 检查是否有新数据
//      d. 空闲超时处理
//   3. 超时或断开时返回
//
// 空闲超时逻辑:
//   - IdleTimeoutSeconds=0: 永不超时，持续同步（用于长期 DR）
//   - IdleTimeoutSeconds>0: 空闲倒计时
//     * 有新数据：重置倒计时
//     * 无新数据：倒计时减 1
//     * 倒计时到 0：断开连接
//
// 使用示例（灾难恢复）:
//   req := &VolumeTailSenderRequest{
//       VolumeId: 3,
//       SinceNs: 1234567890000000000,  // 上次同步的时间戳
//       IdleTimeoutSeconds: 0,          // 永不超时，持续同步
//   }
//   // 持续发送 Volume 3 的增量数据到备份站点
//
// 使用示例（增量备份）:
//   req := &VolumeTailSenderRequest{
//       VolumeId: 3,
//       SinceNs: lastBackupTimestamp,
//       IdleTimeoutSeconds: 300,  // 5 分钟无新数据后自动断开
//   }
//   // 备份增量数据，5 分钟无新数据后结束
func (vs *VolumeServer) VolumeTailSender(req *volume_server_pb.VolumeTailSenderRequest, stream volume_server_pb.VolumeServer_VolumeTailSenderServer) error {

	// 【验证 Volume 存在】
	v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
	if v == nil {
		return fmt.Errorf("未找到 volume id %d", req.VolumeId)
	}

	defer glog.V(1).Infof("tailing volume %d 完成", v.Id)

	// 【初始化同步状态】
	lastTimestampNs := req.SinceNs           // 上次同步的时间戳
	drainingSeconds := req.IdleTimeoutSeconds // 空闲倒计时

	// 【持续同步循环】
	for {
		// 【发送增量 Needle】
		// sendNeedlesSince 发送 lastTimestampNs 之后的所有 Needle
		// 返回最新处理的时间戳
		lastProcessedTimestampNs, err := sendNeedlesSince(stream, v, lastTimestampNs)
		if err != nil {
			glog.Infof("sendNeedlesSince: %v", err)
			return fmt.Errorf("streamFollow: %w", err)
		}

		// 【等待新数据】
		// 等待 2 秒后再次检查是否有新数据
		// 避免频繁扫描，降低 CPU 和磁盘负载
		time.Sleep(2 * time.Second)

		// 【空闲超时处理】
		// IdleTimeoutSeconds=0 表示永不超时
		if req.IdleTimeoutSeconds == 0 {
			// 更新时间戳，继续下一轮同步
			lastTimestampNs = lastProcessedTimestampNs
			continue
		}

		// 【检查是否有新数据】
		if lastProcessedTimestampNs == lastTimestampNs {
			// 无新数据，倒计时减 1
			drainingSeconds--
			if drainingSeconds <= 0 {
				// 倒计时到 0，空闲超时，断开连接
				return nil
			}
			glog.V(1).Infof("tailing volume %d 倒计时剩余 %d 秒", v.Id, drainingSeconds)
		} else {
			// 有新数据，重置倒计时
			lastTimestampNs = lastProcessedTimestampNs
			drainingSeconds = req.IdleTimeoutSeconds
			glog.V(1).Infof("tailing volume %d 重置倒计时为 %d 秒", v.Id, drainingSeconds)
		}

	}

}

// sendNeedlesSince 发送指定时间戳之后的所有 Needle
// 使用二分查找快速定位起始位置，然后顺序扫描
//
// 参数:
//   - stream: gRPC 流，用于发送 Needle 数据
//   - v: Volume 对象
//   - lastTimestampNs: 起始时间戳（纳秒）
//
// 返回:
//   - lastProcessedTimestampNs: 最新处理的时间戳
//   - err: 发送错误
//
// 工作流程:
//   1. 使用 BinarySearchByAppendAtNs 二分查找定位起始位置
//   2. 如果是最后一个 Needle，发送心跳保持连接
//   3. 否则，创建扫描器顺序扫描后续所有 Needle
//   4. 返回最新处理的时间戳
//
// 二分查找优化:
//   - Volume 索引维护 AppendAtNs → Offset 的映射
//   - 二分查找复杂度 O(log N)，快速定位
//   - 避免从头扫描整个 Volume
//
// 心跳保活:
//   - 无新数据时发送空消息（IsLastChunk=true）
//   - 保持 gRPC 连接活跃
//   - 避免连接超时断开
func sendNeedlesSince(stream volume_server_pb.VolumeServer_VolumeTailSenderServer, v *storage.Volume, lastTimestampNs uint64) (lastProcessedTimestampNs uint64, err error) {

	// 【二分查找定位】
	// BinarySearchByAppendAtNs 在索引中二分查找 >= lastTimestampNs 的第一个 Needle
	// 返回：
	//   - foundOffset: 找到的 Needle 的偏移量
	//   - isLastOne: 是否是最后一个 Needle
	//   - err: 查找错误
	foundOffset, isLastOne, err := v.BinarySearchByAppendAtNs(lastTimestampNs)
	if err != nil {
		return 0, fmt.Errorf("无法通过 appendAtNs %d 定位: %s", lastTimestampNs, err)
	}

	// log.Printf("reading ts %d offset %d isLast %v", lastTimestampNs, foundOffset, isLastOne)

	// 【发送心跳】
	// 如果是最后一个 Needle，说明没有新数据
	// 发送心跳消息保持连接活跃
	if isLastOne {
		// 构造心跳消息
		// IsLastChunk=true 表示无新数据，只是保活
		sendErr := stream.Send(&volume_server_pb.VolumeTailSenderResponse{
			IsLastChunk: true,
			Version:     uint32(v.Version()),
		})
		// 返回相同的时间戳，表示无新数据
		return lastTimestampNs, sendErr
	}

	// 【创建扫描器】
	// VolumeFileScanner4Tailing 专门用于 Tail 同步的扫描器
	// 负责扫描 Needle 并通过 stream 发送
	scanner := &VolumeFileScanner4Tailing{
		stream:  stream,
		version: uint32(v.Version()),
	}

	// 【顺序扫描并发送】
	// ScanVolumeFileFrom 从 foundOffset 开始顺序扫描 Volume 文件
	// 每扫描到一个 Needle，调用 scanner.VisitNeedle() 发送
	// ToActualOffset() 将索引偏移量转换为文件实际偏移量
	err = storage.ScanVolumeFileFrom(v.Version(), v.DataBackend, foundOffset.ToActualOffset(), scanner)

	// 返回扫描器处理的最新时间戳
	return scanner.lastProcessedTimestampNs, err

}

// VolumeTailReceiver 作为接收端，接收并写入增量 Needle 数据
// 调用 TailVolumeFromSource 从源 Volume Server 拉取数据
//
// 参数:
//   - ctx: 上下文
//   - req: Tail 接收请求，包含：
//     - VolumeId: 目标 Volume ID
//     - SourceVolumeServer: 源 Volume Server 地址
//     - SinceNs: 起始时间戳（纳秒）
//     - IdleTimeoutSeconds: 空闲超时时间（秒）
//
// 返回:
//   - resp: 接收响应（空）
//   - error: 接收错误（连接失败、写入失败等）
//
// 工作流程:
//   1. 验证目标 Volume 存在
//   2. 调用 operation.TailVolumeFromSource 从源拉取数据
//   3. 对每个接收的 Needle，调用回调函数写入本地 Volume
//   4. 返回成功或错误
//
// 使用示例（灾难恢复）:
//   req := &VolumeTailReceiverRequest{
//       VolumeId: 3,
//       SourceVolumeServer: "192.168.1.10:18080",
//       SinceNs: lastSyncTimestamp,
//       IdleTimeoutSeconds: 0,  // 永不超时，持续同步
//   }
//   // 从主站点持续接收 Volume 3 的增量数据
//
// 回调函数逻辑:
//   - 接收到 Needle 后立即写入本地 Volume
//   - WriteVolumeNeedle 参数：
//     * checkCookie=false：跳过 Cookie 检查（源已验证）
//     * fsync=false：延迟刷盘，提高性能
func (vs *VolumeServer) VolumeTailReceiver(ctx context.Context, req *volume_server_pb.VolumeTailReceiverRequest) (*volume_server_pb.VolumeTailReceiverResponse, error) {

	resp := &volume_server_pb.VolumeTailReceiverResponse{}

	// 【验证 Volume 存在】
	v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
	if v == nil {
		return resp, fmt.Errorf("receiver 未找到 volume id %d", req.VolumeId)
	}

	defer glog.V(1).Infof("接收 tailing volume %d 完成", v.Id)

	// 【从源拉取数据】
	// TailVolumeFromSource 连接到源 Volume Server，接收增量数据
	// 参数:
	//   - sourceAddress: 源 Volume Server 地址
	//   - grpcDialOption: gRPC 拨号选项（TLS 等）
	//   - volumeId: Volume ID
	//   - sinceNs: 起始时间戳
	//   - idleTimeoutSeconds: 空闲超时时间
	//   - writeFunc: 写入回调函数，接收 Needle 后写入本地
	//
	// 工作流程:
	//   1. 建立到源 Volume Server 的 gRPC 连接
	//   2. 调用 VolumeTailSender RPC
	//   3. 接收 Needle 数据流
	//   4. 对每个 Needle 调用 writeFunc 写入
	//   5. 连接断开或超时时返回
	return resp, operation.TailVolumeFromSource(
		pb.ServerAddress(req.SourceVolumeServer),
		vs.grpcDialOption,
		v.Id,
		req.SinceNs,
		int(req.IdleTimeoutSeconds),
		func(n *needle.Needle) error {
			// 【写入回调函数】
			// 接收到 Needle 后立即写入本地 Volume
			// 参数:
			//   - volumeId: 目标 Volume ID
			//   - needle: 接收到的 Needle
			//   - checkCookie=false: 跳过 Cookie 检查（源已验证，提高性能）
			//   - fsync=false: 延迟刷盘（批量刷盘，提高性能）
			_, err := vs.store.WriteVolumeNeedle(v.Id, n, false, false)
			return err
		})

}

// VolumeFileScanner4Tailing 专门用于 Tail 同步的 Volume 扫描器
// 实现 VolumeFileScanner 接口，负责将扫描到的 Needle 发送到 gRPC 流
//
// 字段:
//   - stream: gRPC 流，用于发送 Needle 数据
//   - lastProcessedTimestampNs: 最新处理的时间戳（用于断点续传）
//   - version: Volume 版本（v1/v2/v3）
//
// 扫描器接口方法:
//   - VisitSuperBlock: 访问 SuperBlock（不处理）
//   - ReadNeedleBody: 是否读取 Needle Body（返回 true）
//   - VisitNeedle: 访问 Needle（发送到 stream）
type VolumeFileScanner4Tailing struct {
	stream                   volume_server_pb.VolumeServer_VolumeTailSenderServer // gRPC 流
	lastProcessedTimestampNs uint64                                                // 最新处理的时间戳
	version                  uint32                                                // Volume 版本
}

// VisitSuperBlock 访问 SuperBlock（空实现）
// Tail 同步不需要处理 SuperBlock
//
// 参数:
//   - superBlock: SuperBlock 对象
//
// 返回:
//   - error: 总是返回 nil
func (scanner *VolumeFileScanner4Tailing) VisitSuperBlock(superBlock super_block.SuperBlock) error {
	return nil

}

// ReadNeedleBody 是否读取 Needle Body
// Tail 同步需要读取完整的 Needle（包括 Body）
//
// 返回:
//   - bool: 总是返回 true
func (scanner *VolumeFileScanner4Tailing) ReadNeedleBody() bool {
	return true
}

// VisitNeedle 访问 Needle，将其发送到 gRPC 流
// 大 Needle 分块发送，避免内存溢出
//
// 参数:
//   - n: Needle 对象
//   - offset: Needle 在文件中的偏移量
//   - needleHeader: Needle Header 的原始字节（Cookie、ID、Size 等）
//   - needleBody: Needle Body 的原始字节（Data、Checksum、Flags 等）
//
// 返回:
//   - error: 发送错误
//
// 工作流程:
//   1. 将 needleBody 分块（每块 BufferSizeLimit 字节）
//   2. 逐块发送到 gRPC 流
//   3. 最后一块设置 IsLastChunk=true
//   4. 更新 lastProcessedTimestampNs
//
// 分块原因:
//   - gRPC 消息大小限制（默认 4MB）
//   - 避免大文件占用过多内存
//   - 提高传输效率和稳定性
//
// BufferSizeLimit:
//   - 定义在其他文件中，通常为 2MB
//   - 平衡内存使用和传输效率
func (scanner *VolumeFileScanner4Tailing) VisitNeedle(n *needle.Needle, offset int64, needleHeader, needleBody []byte) error {
	isLastChunk := false

	// 【分块发送 Needle Body】
	// 大 Needle 分块发送，避免单个消息过大
	for i := 0; i < len(needleBody); i += BufferSizeLimit {
		// 计算当前块的结束位置
		stopOffset := i + BufferSizeLimit
		if stopOffset >= len(needleBody) {
			// 最后一块
			isLastChunk = true
			stopOffset = len(needleBody)
		}

		// 【发送块】
		// 构造 VolumeTailSenderResponse 消息
		// 第一块包含 NeedleHeader，后续块只包含 Body
		sendErr := scanner.stream.Send(&volume_server_pb.VolumeTailSenderResponse{
			NeedleHeader: needleHeader,                  // Needle Header（只在第一块包含）
			NeedleBody:   needleBody[i:stopOffset],      // Needle Body 的当前块
			IsLastChunk:  isLastChunk,                   // 是否是最后一块
			Version:      scanner.version,               // Volume 版本
		})
		if sendErr != nil {
			return sendErr
		}
	}

	// 【更新时间戳】
	// 记录最新处理的 Needle 的 AppendAtNs
	// 用于断点续传：下次从此时间戳继续
	scanner.lastProcessedTimestampNs = n.AppendAtNs
	return nil
}
