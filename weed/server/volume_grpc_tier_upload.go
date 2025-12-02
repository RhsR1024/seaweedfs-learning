// Package weed_server 实现 Volume Server 的 Tier 上传功能
// 本文件提供将 Volume 数据文件从本地上传��远程存储（云存储）的能力
//
// 核心功能:
//   - 本地到远程：VolumeTierMoveDatToRemote 将 .dat 文件上传到云存储
//   - 进度报告：流式报告上传进度和百分比
//   - 可选保留：支持上传后保留或删除本地文件
//   - 元数据同步：更新 VolumeInfo 记录远程副本位置
//   - 重复检测：避免重复上传到相同的存储后端
//
// 使用场景:
//   - Tiered Storage 冷化：将低频访问数据上传到云存储，释放本地空间
//   - 成本优化：将冷数据存储在廉价的云存储，降低总成本
//   - 数据备份：将 Volume 备份到云存储，增强数据安全性
//   - 容量扩展：本地磁盘不足时，将部分数据迁移到云存储
//
// Tiered Storage 冷化策略:
//   1. 识别冷数据：
//      - 访问频率低（如 30 天无访问）
//      - 数据年龄大（如创建时间超过 1 年）
//      - 根据业务规则标记（如历史订单数据）
//   2. 上传到云存储：
//      - 调用 VolumeTierMoveDatToRemote
//      - 选择合适的存储类型（S3 Standard、Glacier 等）
//      - 设置 KeepLocalDatFile=false 释放本地空间
//   3. 访问冷数据：
//      - Volume Server 自动从云存储读取
//      - 或调用 VolumeTierMoveDatFromRemote 热化
//
// 工作流程:
//   1. 验证 Volume 存在且数据在本地
//   2. 检查目标存储后端配置有效
//   3. 检查是否已上传到目标后端（避免重复）
//   4. 上传 .dat 文件到云存储
//   5. 流式报告上传进度
//   6. 更新 VolumeInfo 记录远程副本
//   7. 加载远程 DataBackend（切换到远程读取模式）
//   8. 可选：删除本地文件（默认删除）
//
// VolumeInfo RemoteFile:
//   - 每个远程副本一条记录
//   - 支持多个远程副本（不同云平台、不同区域）
//   - BackendType + BackendId 唯一标识存储位置
//
// 注意事项:
//   - 上传过程中 Volume 仍可读（读本地）
//   - 上传完成后切换到远程读取（性能下降）
//   - 云存储费用（存储、流量、API 调用）
//   - 删除本地文件前确保上传成功
package weed_server

import (
	"fmt"
	"os"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// VolumeTierMoveDatToRemote 将 .dat 文件从本地上传到远程存储
// 支持流式进度报告和可选的本地文件保留
//
// 参数:
//   - req: 上传请求，包含：
//     - VolumeId: Volume ID
//     - Collection: 集合名称（用于验证）
//     - DestinationBackendName: 目标存储后端名称
//       * 格式：<type>_<id>，例如 "s3_main"、"azure_backup"
//       * 必须在启动时配置并注册
//     - KeepLocalDatFile: 是否保留本地文件
//       * false（默认）：上传后删除本地文件，释放磁盘空间
//       * true：保留本地文件，远程作为备份
//   - stream: gRPC 流，用于流式报告上传进度
//
// 返回:
//   - error: 上传错误（Volume 不存在、存储配置无效、上传失败等）
//
// 工作流程:
//   1. 【验证 Volume 存在】
//   2. 【验证 Collection 匹配】
//   3. 【验证数据在本地】
//   4. 【验证目标存储后端】
//   5. 【检查是否已上传】避免重复上传
//   6. 【上传 .dat 文件】流式上传并报告进度
//   7. 【更新 VolumeInfo】添加远程文件记录
//   8. 【加载远程 DataBackend】切换到远程读取模式
//   9. 【可选：删除本地文件】释放磁盘空间
//
// 使用示例（冷化数据）:
//   // 场景：Volume 3 是冷数据，上传到 S3 释放本地空间
//   req := &VolumeTierMoveDatToRemoteRequest{
//       VolumeId: 3,
//       Collection: "logs",
//       DestinationBackendName: "s3_main",
//       KeepLocalDatFile: false,  // 上传后删除本地文件
//   }
//   // 上传 Volume 3 到 S3，删除本地副本
//
// 使用示例（云备份）:
//   req := &VolumeTierMoveDatToRemoteRequest{
//       VolumeId: 3,
//       Collection: "photos",
//       DestinationBackendName: "s3_backup",
//       KeepLocalDatFile: true,  // 上传后保留本地文件
//   }
//   // 备份 Volume 3 到 S3，保留本地副本
//
// 进度报告:
//   - 每秒最多报告一次进度
//   - 包含已上传字节数和完成百分比
//   - 客户端可显示进度条
func (vs *VolumeServer) VolumeTierMoveDatToRemote(req *volume_server_pb.VolumeTierMoveDatToRemoteRequest, stream volume_server_pb.VolumeServer_VolumeTierMoveDatToRemoteServer) error {

	// 【验证 Volume 存在】
	v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
	if v == nil {
		return fmt.Errorf("volume %d 未找到", req.VolumeId)
	}

	// 【验证 Collection】
	// Collection 不匹配可能表示配置错误或操作错误
	if v.Collection != req.Collection {
		return fmt.Errorf("现有 collection:%v 与输入不匹配: %v", v.Collection, req.Collection)
	}

	// 【验证数据在本地】
	// DiskFile 表示数据存储在本地磁盘
	// 只有本地文件才能上传到远程
	diskFile, ok := v.DataBackend.(*backend.DiskFile)
	if !ok {
		// 数据已经在远程，无需重复上传
		return nil // already copied to remove. fmt.Errorf("volume %d is not on local disk", req.VolumeId)
	}

	// 【验证目标存储后端】
	// BackendStorages 是全局注册的存储后端映射
	// 存储后端需要在启动时通过配置文件注册
	backendStorage, found := backend.BackendStorages[req.DestinationBackendName]
	if !found {
		// 列出所有支持的存储后端
		var keys []string
		for key := range backend.BackendStorages {
			keys = append(keys, key)
		}
		return fmt.Errorf("目标 %s 未找到，支持的存储: %v", req.DestinationBackendName, keys)
	}

	// 【检查是否已上传到目标后端】
	// 解析目标存储后端的类型和 ID
	// 格式：<type>_<id>，例如 "s3_main" → type="s3", id="main"
	backendType, backendId := backend.BackendNameToTypeId(req.DestinationBackendName)

	// 遍历 VolumeInfo 中的所有远程文件记录
	// 检查是否已有相同 BackendType 和 BackendId 的记录
	for _, remoteFile := range v.GetVolumeInfo().GetFiles() {
		if remoteFile.BackendType == backendType && remoteFile.BackendId == backendId {
			// 已经上传到目标后端，无需重复上传
			return fmt.Errorf("目标 %s 已存在", req.DestinationBackendName)
		}
	}

	// 【进度报告函数】
	// 限制报告频率为每秒最多一次，避免过多消息
	startTime := time.Now()
	fn := func(progressed int64, percentage float32) error {
		now := time.Now()
		if now.Sub(startTime) < time.Second {
			// 距离上次报告不足 1 秒，跳过
			return nil
		}
		startTime = now
		// 发送进度报告
		return stream.Send(&volume_server_pb.VolumeTierMoveDatToRemoteResponse{
			Processed:           progressed,   // 已上传字节数
			ProcessedPercentage: percentage,   // 完成百分比（0.0 ~ 100.0）
		})
	}

	// 【上传 .dat 文件】
	// CopyFile 将本地文件上传到远程存储
	// 参数:
	//   - diskFile.File: 本地 .dat 文件句柄
	//   - progressFn: 进度回调函数
	//
	// 工作流程:
	//   1. 读取本地 .dat 文件
	//   2. 上传到远程存储（S3/Azure/GCS）
	//   3. 定期调用 progressFn 报告进度
	//
	// 返回:
	//   - key: 远程存储中的 Key/Path
	//   - size: 上传的文件大小
	//   - err: 上传错误
	key, size, err := backendStorage.CopyFile(diskFile.File, fn)
	if err != nil {
		return fmt.Errorf("backend %s 复制文件 %s 失败: %v", req.DestinationBackendName, diskFile.Name(), err)
	}

	// 【更新 VolumeInfo】
	// 添加远程文件记录到 VolumeInfo
	// 记录远程副本的位置和元数据
	v.GetVolumeInfo().Files = append(v.GetVolumeInfo().GetFiles(), &volume_server_pb.RemoteFile{
		BackendType:  backendType,                     // 存储类型（s3、azure、gcs 等）
		BackendId:    backendId,                       // 存储实例 ID
		Key:          key,                             // 远程存储中的 Key/Path
		Offset:       0,                               // 偏移量（.dat 文件通常为 0）
		FileSize:     uint64(size),                    // 文件大小
		ModifiedTime: uint64(time.Now().Unix()),       // 上传时间（Unix 时间戳）
		Extension:    ".dat",                          // 文件扩展名
	})

	// 【保存 VolumeInfo】
	// SaveVolumeInfo 将更新后的 VolumeInfo 写入 .vif 文件
	// .vif 文件记录 Volume 的所有副本位置
	if err := v.SaveVolumeInfo(); err != nil {
		return fmt.Errorf("volume %d 保存远程文件信息失败: %v", v.Id, err)
	}

	// 【加载远程 DataBackend】
	// LoadRemoteFile 根据 VolumeInfo 加载远程文件
	// 将 Volume 的 DataBackend 切换到远程存储代理
	// 后续读取操作将从远程存储读取
	if err := v.LoadRemoteFile(); err != nil {
		return fmt.Errorf("volume %d 加载远程文件失败: %v", v.Id, err)
	}

	// 【可选：删除本地文件】
	// KeepLocalDatFile=false 时删除本地文件，释放磁盘空间
	if !req.KeepLocalDatFile {
		// 删除本地 .dat 文件
		// 注意：删除后只能从远程读取，访问延迟会增加
		os.Remove(v.FileName(".dat"))
	}

	return nil
}
