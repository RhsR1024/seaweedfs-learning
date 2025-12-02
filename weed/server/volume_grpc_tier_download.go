// Package weed_server 实现 Volume Server 的 Tier 下载功能
// 本文件提供从远程存储（云存储）下载 Volume 数据文件到本地的能力
//
// 核心功能:
//   - 远程到本地：VolumeTierMoveDatFromRemote 从远程存储下载 .dat 文件到本地磁盘
//   - 进度报告：流式报告下载进度和百分比
//   - 可选保留：支持下载后保留或删除远程文件
//   - 元数据同步：更新 VolumeInfo 反映数据位置变化
//
// 使用场景:
//   - Tiered Storage 热化：将冷数据从云存储下载回本地，提高访问性能
//   - 数据恢复：从云备份恢复 Volume 数据
//   - 迁移优化：将高频访问的 Volume 迁移到本地 SSD
//   - 成本优化：根据访问频率动态调整数据存储位置
//
// Tiered Storage 架构回顾:
//   1. 热数据层（本地 SSD/HDD）：
//      - 高频访问数据
//      - 低延迟，高成本
//   2. 温数据层（EC 纠删码）：
//      - 中频访问数据
//      - 降低存储成本，略高延迟
//   3. 冷数据层（云存储：S3/Azure/GCS）：
//      - 低频访问数据
//      - 最低成本，高延迟
//   4. 数据流动：
//      - 上传：本地 → 云存储（VolumeTierMoveDatToRemote）
//      - 下载：云存储 → 本地（VolumeTierMoveDatFromRemote）
//
// 工作流程:
//   1. 验证 Volume 存在且有远程副本
//   2. 检查远程存储配置是否有效
//   3. 从远程存储下载 .dat 文件到本地
//   4. 流式报告下载进度
//   5. 可选：删除远程文件（默认删除）
//   6. 更新 VolumeInfo 元数据
//   7. 重新加载本地 DataBackend
//
// VolumeInfo 文件:
//   - 存储 Volume 的元数据信息
//   - 文件名：<volumeId>.vif
//   - 记录 Volume 的所有副本位置（本地 + 远程）
//   - RemoteFile 结构：
//     * BackendType: 存储类型（s3、azure、gcs 等）
//     * BackendId: 存储实例 ID
//     * Key: 远程存储中的 Key/Path
//     * FileSize: 文件大小
//     * Offset: 偏移量（通常为 0）
//     * Extension: 文件扩展名（.dat、.idx 等）
//
// 注意事项:
//   - 需要足够的本地磁盘空间
//   - 下载过程中 Volume 可能只读
//   - 网络带宽影响下载速度
//   - 云存储费用（流量、API 调用）
package weed_server

import (
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// VolumeTierMoveDatFromRemote 从远程存储下载 .dat 文件到本地
// 支持流式进度报告和可选的远程文件保留
//
// 参数:
//   - req: 下载请求，包含：
//     - VolumeId: Volume ID
//     - Collection: 集合名称（用于验证）
//     - KeepRemoteDatFile: 是否保留远程文件
//       * false（默认）：下载后删除远程文件，释放云存储空间
//       * true：保留远程文件，作为额外备份
//   - stream: gRPC 流，用于流式报告下载进度
//
// 返回:
//   - error: 下载错误（Volume 不存在、存储配置无效、下载失败等）
//
// 工作流程:
//   1. 【验证 Volume 存在】
//   2. 【验证 Collection 匹配】
//   3. 【检查远程存储配置】从 VolumeInfo 读取
//   4. 【验证不是本地文件】避免重复下载
//   5. 【验证存储后端有效】
//   6. 【下载 .dat 文件】流式下载并报告进度
//   7. 【可选：删除远程文件】释放云存储空间
//   8. 【更新 VolumeInfo】移除远程文件记录
//   9. 【重新加载 DataBackend】切换到本地文件
//
// 使用示例（热化冷数据）:
//   // 场景：用户频繁访问某个冷数据 Volume，决定将其热化
//   req := &VolumeTierMoveDatFromRemoteRequest{
//       VolumeId: 3,
//       Collection: "photos",
//       KeepRemoteDatFile: false,  // 下载后删除远程文件
//   }
//   // 下载 Volume 3 的 .dat 文件到本地，删除远程副本
//
// 使用示例（数据恢复）:
//   req := &VolumeTierMoveDatFromRemoteRequest{
//       VolumeId: 3,
//       Collection: "photos",
//       KeepRemoteDatFile: true,  // 下载后保留远程文件作为备份
//   }
//   // 从云备份恢复 Volume 3，保留远程副本
//
// 进度报告:
//   - 每秒最多报告一次进度
//   - 包含已下载字节数和完成百分比
//   - 客户端可显示进度条
func (vs *VolumeServer) VolumeTierMoveDatFromRemote(req *volume_server_pb.VolumeTierMoveDatFromRemoteRequest, stream volume_server_pb.VolumeServer_VolumeTierMoveDatFromRemoteServer) error {

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

	// 【获取远程存储配置】
	// RemoteStorageNameKey() 从 VolumeInfo 读取远程存储信息
	// 返回：
	//   - storageName: 存储名称（如 "my-s3-storage"）
	//   - storageKey: 存储 Key（如 "volumes/3/3.dat"）
	storageName, storageKey := v.RemoteStorageNameKey()
	if storageName == "" || storageKey == "" {
		return fmt.Errorf("volume %d 已经在本地磁盘上", req.VolumeId)
	}

	// 【检查是否已是本地文件】
	// DiskFile 表示数据存储在本地磁盘
	// 如果已是 DiskFile，说明已下载，无需重复下载
	_, ok := v.DataBackend.(*backend.DiskFile)
	if ok {
		return fmt.Errorf("volume %d 已经在本地磁盘上", req.VolumeId)
	}

	// 【验证存储后端配置】
	// BackendStorages 是全局注册的存储后端映射
	// 存储后端需要在启动时通过配置文件注册
	backendStorage, found := backend.BackendStorages[storageName]
	if !found {
		// 列出所有支持的存储后端
		var keys []string
		for key := range backend.BackendStorages {
			keys = append(keys, key)
		}
		return fmt.Errorf("远程存储 %s 未在支持的存储中找到: %v", storageName, keys)
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
		return stream.Send(&volume_server_pb.VolumeTierMoveDatFromRemoteResponse{
			Processed:           progressed,   // 已下载字节数
			ProcessedPercentage: percentage,   // 完成百分比（0.0 ~ 100.0）
		})
	}

	// 【下载 .dat 文件】
	// DownloadFile 从远程存储下载文件到本地磁盘
	// 参数:
	//   - destFileName: 目标文件名（本地路径）
	//   - storageKey: 远程存储 Key
	//   - progressFn: 进度回调函数
	//
	// 工作流程:
	//   1. 连接到远程存储（S3/Azure/GCS）
	//   2. 下载文件数据
	//   3. 写入本地磁盘（v.FileName(".dat")）
	//   4. 定期调用 progressFn 报告进度
	//
	// 返回:
	//   - size: 下载的文件大小
	//   - err: 下载错误
	_, err := backendStorage.DownloadFile(v.FileName(".dat"), storageKey, fn)
	if err != nil {
		return fmt.Errorf("backend %s 复制文件 %s 失败: %v", storageName, v.FileName(".dat"), err)
	}

	// 【可选：删除远程文件】
	// KeepRemoteDatFile=false 时删除远程文件，释放云存储空间
	if req.KeepRemoteDatFile {
		// 保留远程文件，直接返回成功
		return nil
	}

	// 【删除远程文件】
	// DeleteFile 从远程存储删除文件
	// 释放云存储空间，降低成本
	if err := backendStorage.DeleteFile(storageKey); err != nil {
		return fmt.Errorf("volume %d 删除远程文件 %s 失败: %v", v.Id, storageKey, err)
	}

	// 【更新 VolumeInfo】
	// 移除远程文件记录，表示数据已迁移到本地
	// GetVolumeInfo().Files 包含所有副本位置（本地 + 远程）
	// Files[0] 通常是本地文件，Files[1:] 是远程文件
	v.GetVolumeInfo().Files = v.GetVolumeInfo().Files[1:]

	// 【保存 VolumeInfo】
	// SaveVolumeInfo 将更新后的 VolumeInfo 写入 .vif 文件
	if err := v.SaveVolumeInfo(); err != nil {
		return fmt.Errorf("volume %d 保存远程文件信息失败: %v", v.Id, err)
	}

	// 【关闭旧 DataBackend】
	// 旧 DataBackend 可能是远程存储的代理
	// 关闭它释放资源
	v.DataBackend.Close()
	v.DataBackend = nil

	// 【重新加载 DataBackend】
	// Volume 会自动检测本地 .dat 文件并加载为 DiskFile
	// 这通常在 Volume 重新打开时自动完成
	// 此处设置为 nil，下次访问时会自动重新加载

	return nil
}
