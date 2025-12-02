// Package weed_server 实现 Volume Server 的 Vacuum 垃圾回收功能
// 本文件提供 Volume 的垃圾回收和空间压缩能力
//
// 核心功能:
//   - VacuumVolumeCheck: 检查 Volume 的垃圾比例
//   - VacuumVolumeCompact: 压缩 Volume，移除已删除的 Needle
//   - VacuumVolumeCommit: 提交压缩结果，用新文件替换旧文件
//   - VacuumVolumeCleanup: 清理压缩过程中的临时文件
//
// 使用场景:
//   - 空间回收：删除文件后回收磁盘空间
//   - 性能优化：减少文件碎片，提高读写性能
//   - 成本优化：释放不再使用的存储空间
//   - 定期维护：定时清理删除的数据
//
// Vacuum 工作原理:
//   SeaweedFS 使用 append-only 的 Volume 文件格式：
//   1. 写入：Needle 追加到 Volume 文件末尾
//   2. 删除：在索引中标记 Needle 为已删除，但文件不变
//   3. 问题：删除的 Needle 仍占用磁盘空间
//   4. 解决：Vacuum 压缩 Volume，移除已删除的 Needle
//
// Vacuum 三阶段流程:
//   1. Check 阶段（VacuumVolumeCheck）:
//      - 统计 Volume 中已删除 Needle 的比例
//      - 计算 garbageRatio = deletedSize / totalSize
//      - 判断是否需要 Vacuum（如 garbageRatio > 0.3）
//
//   2. Compact 阶段（VacuumVolumeCompact）:
//      - 创建新的临时 Volume 文件（.cpd 和 .cpx）
//      - 扫描原 Volume，将未删除的 Needle 复制到新文件
//      - 跳过已删除的 Needle，实现空间压缩
//      - 流式报告压缩进度
//
//   3. Commit 阶段（VacuumVolumeCommit）:
//      - 验证新文件的完整性
//      - 用新文件（.cpd、.cpx）替换旧文件（.dat、.idx）
//      - 更新 Volume 引用
//      - 返回 Volume 的新大小和只读状态
//
//   4. Cleanup 阶段（VacuumVolumeCleanup）:
//      - 删除旧的 .dat 和 .idx 文件
//      - 释放磁盘空间
//
// 压缩策略:
//   - 根据 garbageRatio 决定是否压缩
//   - 通常 garbageRatio > 0.3 时触发
//   - 避免频繁压缩影响性能
//   - 可在业务低峰期执行
//
// 性能考虑:
//   - Compact 阶段 I/O 密集，影响性能
//   - 支持限速（compactionBytePerSecond）
//   - 监控系统负载（LoadAvg），负载高时减速
//   - 流式报告进度，支持监控和取消
//
// 安全保证:
//   - Compact 期间 Volume 只读
//   - Commit 前验证新文件完整性
//   - 原子替换文件，避免数据丢失
//   - 失败时保留原文件
//
// Prometheus 监控:
//   - volume_server_vacuuming_histogram: Vacuum 各阶段耗时
//   - volume_server_vacuuming_compact_counter: Compact 成功/失败计数
//   - volume_server_vacuuming_commit_counter: Commit 成功/失败计数
//
// 注意事项:
//   - Vacuum 期间 Volume 不可写
//   - 需要足够的磁盘空间（2 倍 Volume 大小）
//   - Commit 失败需要手动清理临时文件
//   - 建议在业务低峰期执行
package weed_server

import (
	"context"
	"strconv"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/stats"

	"runtime"

	"github.com/prometheus/procfs"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// numCPU 系统 CPU 核心数
// 用于计算归一化的系统负载（LoadAvg / numCPU）
var numCPU = runtime.NumCPU()

// VacuumVolumeCheck 检查 Volume 的垃圾比例
// 判断是否需要执行 Vacuum 压缩
//
// 参数:
//   - ctx: 上下文
//   - req: 检查请求，包含：
//     - VolumeId: 要检查的 Volume ID
//
// 返回:
//   - resp: 检查响应，包含：
//     - GarbageRatio: 垃圾比例（0.0 ~ 1.0）
//       * 计算公式：deletedSize / totalSize
//       * 例如：0.3 表示 30% 的空间是已删除的 Needle
//   - error: 检查错误（Volume 不存在等）
//
// 工作流程:
//   1. 调用 store.CheckCompactVolume() 统计垃圾
//   2. 返回 garbageRatio
//
// 使用场景:
//   - Master 定期调用，判断哪些 Volume 需要 Vacuum
//   - 根据 garbageRatio 决定 Vacuum 优先级
//   - 监控 Volume 的空间利用率
//
// 使用示例:
//   req := &VacuumVolumeCheckRequest{VolumeId: 3}
//   resp, _ := volumeServer.VacuumVolumeCheck(ctx, req)
//   if resp.GarbageRatio > 0.3 {
//       // 垃圾比例超过 30%，需要 Vacuum
//       volumeServer.VacuumVolumeCompact(...)
//   }
func (vs *VolumeServer) VacuumVolumeCheck(ctx context.Context, req *volume_server_pb.VacuumVolumeCheckRequest) (*volume_server_pb.VacuumVolumeCheckResponse, error) {

	resp := &volume_server_pb.VacuumVolumeCheckResponse{}

	// 【计算垃圾比例】
	// CheckCompactVolume 统计 Volume 中已删除 Needle 的大小
	// 返回 garbageRatio = deletedSize / totalSize
	garbageRatio, err := vs.store.CheckCompactVolume(needle.VolumeId(req.VolumeId))

	resp.GarbageRatio = garbageRatio

	if err != nil {
		glog.V(3).Infof("检查 volume %d: %v", req.VolumeId, err)
	}

	return resp, err

}

// VacuumVolumeCompact 压缩 Volume，移除已删除的 Needle
// 创建新的压缩文件（.cpd 和 .cpx），只包含未删除的 Needle
//
// 参数:
//   - req: 压缩请求，包含：
//     - VolumeId: 要压缩的 Volume ID
//     - Preallocate: 是否预分配新文件空间
//       * true: 预分配全部空间，避免碎片
//       * false: 动态增长，节省空间
//   - stream: gRPC 流，用于流式报告压缩进度
//
// 返回:
//   - error: 压缩错误（Volume 不存在、磁盘空间不足等）
//
// 工作流程:
//   1. 创建临时文件（.cpd 和 .cpx）
//   2. 扫描原 Volume，复制未删除的 Needle 到新文件
//   3. 跳过已删除的 Needle
//   4. 定期报告进度（每 128MB）
//   5. 监控系统负载，负载高时减速
//   6. 完成后临时文件准备就绪
//
// 进度报告:
//   - ProcessedBytes: 已处理的字节数
//   - LoadAvg_1M: 归一化的 1 分钟系统负载（LoadAvg / numCPU）
//     * 用于判断系统压力，负载高时可暂停压缩
//   - 每处理 128MB 报告一次进度
//
// 限速机制:
//   - compactionBytePerSecond: 压缩速度限制（字节/秒）
//   - 0 表示不限速（全速压缩）
//   - > 0 限制速度，避免影响正常业务
//
// 系统负载监控:
//   - 使用 procfs 读取系统 LoadAvg
//   - 归一化：LoadAvg / numCPU
//   - 负载过高时可暂停或减速
//
// 使用示例:
//   req := &VacuumVolumeCompactRequest{
//       VolumeId: 3,
//       Preallocate: true,  // 预分配空间
//   }
//   // 压缩 Volume 3，创建 3.cpd 和 3.cpx
func (vs *VolumeServer) VacuumVolumeCompact(req *volume_server_pb.VacuumVolumeCompactRequest, stream volume_server_pb.VolumeServer_VacuumVolumeCompactServer) error {
	start := time.Now()
	defer func(start time.Time) {
		// 【记录 Compact 耗时】
		// 监控 Compact 阶段的性能
		stats.VolumeServerVacuumingHistogram.WithLabelValues("compact").Observe(time.Since(start).Seconds())
	}(start)

	resp := &volume_server_pb.VacuumVolumeCompactResponse{}

	// 【进度报告配置】
	// 每处理 128MB 报告一次进度
	reportInterval := int64(1024 * 1024 * 128)
	nextReportTarget := reportInterval

	// 【初始化 procfs】
	// 用于读取系统 LoadAvg
	fs, fsErr := procfs.NewDefaultFS()
	var sendErr error

	// 【执行压缩】
	// CompactVolume 扫描 Volume 并创建压缩文件
	// 参数:
	//   - volumeId: Volume ID
	//   - preallocate: 是否预分配空间
	//   - compactionBytePerSecond: 速度限制（0 = 不限速）
	//   - progressCallback: 进度回调函数
	//
	// progressCallback 参数:
	//   - processed: 已处理的字节数
	//
	// progressCallback 返回:
	//   - bool: true 继续，false 取消
	err := vs.store.CompactVolume(needle.VolumeId(req.VolumeId), req.Preallocate, vs.compactionBytePerSecond, func(processed int64) bool {
		// 【进度报告】
		// 检查是否到达报告间隔
		if processed > nextReportTarget {
			resp.ProcessedBytes = processed

			// 【读取系统负载】
			if fsErr == nil && numCPU > 0 {
				if fsLa, err := fs.LoadAvg(); err == nil {
					// 归一化 LoadAvg：Load1 / numCPU
					// 例如：8 核系统，Load1=4.0，归一化后=0.5
					resp.LoadAvg_1M = float32(fsLa.Load1 / float64(numCPU))
				}
			}

			// 【发送进度报告】
			if sendErr = stream.Send(resp); sendErr != nil {
				// 发送失败，取消压缩
				return false
			}

			// 【更新下次报告目标】
			nextReportTarget = processed + reportInterval
		}

		// 继续压缩
		return true
	})

	// 【记录 Compact 结果】
	stats.VolumeServerVacuumingCompactCounter.WithLabelValues(strconv.FormatBool(err == nil && sendErr == nil)).Inc()

	if err != nil {
		glog.Errorf("压缩 volume %d 失败: %v", req.VolumeId, err)
		return err
	}
	if sendErr != nil {
		glog.Errorf("压缩 volume %d 报告进度失败: %v", req.VolumeId, sendErr)
		return sendErr
	}

	glog.V(1).Infof("压缩 volume %d 完成", req.VolumeId)
	return nil

}

// VacuumVolumeCommit 提交压缩结果，用新文件替换旧文件
// 原子操作，确保数据一致性
//
// 参数:
//   - ctx: 上下文
//   - req: 提交请求，包含：
//     - VolumeId: 要提交的 Volume ID
//
// 返回:
//   - resp: 提交响应，包含：
//     - IsReadOnly: Volume 是否只读
//       * true: Volume 满了或被标记为只读
//       * false: 可继续写入
//     - VolumeSize: Volume 压缩后的新大小（字节）
//   - error: 提交错误（文件替换失败等）
//
// 工作流程:
//   1. 验证压缩文件（.cpd 和 .cpx）完整性
//   2. 原子替换：
//      - 重命名 .dat → .dat.old
//      - 重命名 .idx → .idx.old
//      - 重命名 .cpd → .dat
//      - 重命名 .cpx → .idx
//   3. 重新加载 Volume
//   4. 返回新的 Volume 大小和只读状态
//
// 原子性保证:
//   - 使用文件系统的原子重命名操作
//   - 失败时可回滚（.old 文件保留）
//   - 不会丢失数据
//
// 使用示例:
//   req := &VacuumVolumeCommitRequest{VolumeId: 3}
//   resp, _ := volumeServer.VacuumVolumeCommit(ctx, req)
//   // Volume 3 已用压缩文件替换
//   // 新大小：resp.VolumeSize
//   // 只读：resp.IsReadOnly
func (vs *VolumeServer) VacuumVolumeCommit(ctx context.Context, req *volume_server_pb.VacuumVolumeCommitRequest) (*volume_server_pb.VacuumVolumeCommitResponse, error) {
	start := time.Now()
	defer func(start time.Time) {
		// 【记录 Commit 耗时】
		stats.VolumeServerVacuumingHistogram.WithLabelValues("commit").Observe(time.Since(start).Seconds())
	}(start)

	resp := &volume_server_pb.VacuumVolumeCommitResponse{}

	// 【提交压缩结果】
	// CommitCompactVolume 用新文件替换旧文件
	// 返回：
	//   - readOnly: Volume 是否只读
	//   - volumeSize: 新的 Volume 大小
	//   - err: 提交错误
	readOnly, volumeSize, err := vs.store.CommitCompactVolume(needle.VolumeId(req.VolumeId))

	if err != nil {
		glog.Errorf("提交 volume %d 失败: %v", req.VolumeId, err)
	} else {
		glog.V(1).Infof("提交 volume %d 完成", req.VolumeId)
	}

	// 【记录 Commit 结果】
	stats.VolumeServerVacuumingCommitCounter.WithLabelValues(strconv.FormatBool(err == nil)).Inc()

	resp.IsReadOnly = readOnly
	resp.VolumeSize = uint64(volumeSize)
	return resp, err

}

// VacuumVolumeCleanup 清理 Vacuum 过程中的临时文件
// 删除旧的 .dat 和 .idx 文件，释放磁盘空间
//
// 参数:
//   - ctx: 上下文
//   - req: 清理请求，包含：
//     - VolumeId: 要清理的 Volume ID
//
// 返回:
//   - resp: 清理响应（空）
//   - error: 清理错误（文件删除失败等）
//
// 工作流程:
//   1. 删除 .dat.old 文件
//   2. 删除 .idx.old 文件
//   3. 释放磁盘空间
//
// 注意事项:
//   - 只在 Commit 成功后调用
//   - 删除前确认新文件工作正常
//   - 失败不影响 Volume 使用
//
// 使用示例:
//   req := &VacuumVolumeCleanupRequest{VolumeId: 3}
//   volumeServer.VacuumVolumeCleanup(ctx, req)
//   // 删除 3.dat.old 和 3.idx.old
func (vs *VolumeServer) VacuumVolumeCleanup(ctx context.Context, req *volume_server_pb.VacuumVolumeCleanupRequest) (*volume_server_pb.VacuumVolumeCleanupResponse, error) {

	resp := &volume_server_pb.VacuumVolumeCleanupResponse{}

	// 【清理旧文件】
	// CommitCleanupVolume 删除 .dat.old 和 .idx.old 文件
	err := vs.store.CommitCleanupVolume(needle.VolumeId(req.VolumeId))

	if err != nil {
		glog.Errorf("清理 volume %d 失败: %v", req.VolumeId, err)
	} else {
		glog.V(1).Infof("清理 volume %d 完成", req.VolumeId)
	}

	return resp, err

}
