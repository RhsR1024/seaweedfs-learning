// Package weed_server 实现 Volume Server 的文件复制 gRPC 接口
// 本文件提供 Volume 的完整复制和文件流式传输功能
//
// 核心功能:
//   - Volume 完整复制：复制 .dat、.idx、.vif 文件到新位置
//   - 文件流式传输：gRPC 流式读取和写入大文件
//   - 进度报告：实时报告复制进度
//   - 流量控制：使用 throttler 限制复制带宽
//   - 文件完整性验证：检查复制后的文件大小
//
// 复制场景:
//   - Volume 副本创建：为 Volume 创建新副本
//   - Volume 迁移：将 Volume 从一个服务器迁移到另一个
//   - Volume 均衡：数据重分布时的 Volume 移动
//   - 灾难恢复：从备份恢复 Volume
//
// 核心接口:
//   - VolumeCopy: 完整复制 Volume（包括所有文件）
//   - CopyFile: 流式读取单个文件（服务端推送）
//   - ReceiveFile: 流式接收单个文件（客户端推送）
//   - ReadVolumeFileStatus: 读取 Volume 文件状态（大小、时间戳）
//
// 实现特点:
//   - 带宽限制：避免复制操作占用过多网络带宽
//   - 增量进度：定期报告复制进度
//   - 原子性：复制失败时自动清理部分文件
//   - 时间戳保持：保持源文件的修改时间
package weed_server

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// BufferSizeLimit 文件复制时的缓冲区大小
// 2MB 缓冲区在网络传输和内存使用之间提供良好平衡
const BufferSizeLimit = 1024 * 1024 * 2

// VolumeCopy 完整复制 Volume 到本地
// gRPC Streaming API: VolumeCopy
//
// 功能:
//   - 从源 Volume Server 复制整个 Volume
//   - 复制 .dat（数据文件）、.idx（索引文件）、.vif（Volume Info 文件）
//   - 自动挂载复制后的 Volume
//   - 支持进度报告和流量控制
//
// 参数:
//   - VolumeId: 要复制的卷 ID
//   - SourceDataNode: 源 Volume Server 地址
//   - Collection: 集合名称
//   - DiskType: 目标磁盘类型（hdd/ssd）
//   - IoBytePerSecond: 复制速度限制（字节/秒），0 表示使用默认值
//
// 复制流程:
//   【步骤 1：前置检查和清理】
//   1. 检查目标 Volume 是否已存在
//   2. 如果存在，先删除旧 Volume
//
//   【步骤 2：读取源文件信息】
//   1. 连接到源 Volume Server
//   2. 调用 ReadVolumeFileStatus 获取源文件信息:
//      - 文件大小（.dat、.idx）
//      - 修改时间戳
//      - CompactionRevision（压缩版本）
//      - Collection、DiskType 等元数据
//
//   【步骤 3：选择目标位置】
//   1. 根据 DiskType 选择存储位置
//   2. 如果没有匹配的 DiskType，返回错误
//   3. 创建 .note 文件标记正在复制
//
//   【步骤 4：预分配空间（可选）】
//   1. 向 Master 查询是否启用预分配
//   2. 如果启用，预分配 .dat 文件空间
//   3. 预分配可以减少文件碎片
//
//   【步骤 5：复制文件】
//   1. 复制 .dat 文件（最大，需要进度报告）
//   2. 复制 .idx 文件
//   3. 复制 .vif 文件（如果存在）
//   4. 每个文件复制后设置修改时间戳
//
//   【步骤 6：验证文件完整性】
//   1. 检查 .idx 文件大小是否匹配
//   2. 检查 .dat 文件大小是否匹配
//   3. 不匹配时返回错误并清理文件
//
//   【步骤 7：挂载 Volume】
//   1. 调用 store.MountVolume() 挂载新 Volume
//   2. 加载索引到内存
//   3. 将 Volume 标记为可用
//
// 进度报告:
//   - 每复制 128MB 发送一次进度更新
//   - 包含已复制的字节数
//   - 客户端可以实时显示进度
//
// 流量控制:
//   - 使用 WriteThrottler 限制复制速度
//   - 默认使用 compactionBytePerSecond
//   - 可通过 IoBytePerSecond 参数自定义
//
// 错误处理:
//   - 复制失败时自动删除部分文件
//   - 使用 defer 确保清理
//   - 返回详细的错误信息
//
// 返回:
//   - 流式返回 VolumeCopyResponse
//   - ProcessedBytes: 当前已复制字节数
//   - LastAppendAtNs: 最后一次追加的时间戳
//
// 注意:
//   - 复制过程中源 Volume 可能仍在写入
//   - 不保证强一致性，适合最终一致性场景
//   - .note 文件用于标记复制状态，防止并发复制
func (vs *VolumeServer) VolumeCopy(req *volume_server_pb.VolumeCopyRequest, stream volume_server_pb.VolumeServer_VolumeCopyServer) error {

	// 【步骤 1：前置检查和清理】
	// 检查目标 Volume 是否已存在
	v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
	if v != nil {
		// Volume 已存在，需要先删除
		glog.V(0).Infof("volume %d already exists. deleted before copying...", req.VolumeId)

		// 删除现有 Volume（包括 .dat、.idx、.vif 文件）
		err := vs.store.DeleteVolume(needle.VolumeId(req.VolumeId), false)
		if err != nil {
			return fmt.Errorf("failed to delete existing volume %d: %v", req.VolumeId, err)
		}

		glog.V(0).Infof("deleted existing volume %d before copying.", req.VolumeId)
	}

	// 【步骤 2-5：连接源服务器并复制文件】
	// Master 不会对只读 Volume 启动压缩，所以可以安全地直接复制文件
	//
	// 复制步骤:
	//   1. 读取 .idx .dat 文件大小和时间戳
	//   2. 发送 .idx 文件
	//   3. 发送 .dat 文件
	//   4. 确认大小和时间戳匹配

	// 声明变量
	var volFileInfoResp *volume_server_pb.ReadVolumeFileStatusResponse  // 源文件信息
	var dataBaseFileName, indexBaseFileName, idxFileName, datFileName string  // 目标文件路径
	var hasRemoteDatFile bool  // 是否使用远程存储（如 S3）

	// 连接到源 Volume Server 执行复制
	err := operation.WithVolumeServerClient(true, pb.ServerAddress(req.SourceDataNode), vs.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
		var err error

		// 【步骤 2：读取源文件信息】
		// 获取源 Volume 的文件状态（大小、时间戳等）
		volFileInfoResp, err = client.ReadVolumeFileStatus(context.Background(),
			&volume_server_pb.ReadVolumeFileStatusRequest{
				VolumeId: req.VolumeId,
			})
		if nil != err {
			return fmt.Errorf("read volume file status failed, %w", err)
		}

		// 【步骤 3：选择目标位置】
		// 确定磁盘类型：优先使用请求中的 DiskType，否则使用源文件的 DiskType
		diskType := volFileInfoResp.DiskType
		if req.DiskType != "" {
			diskType = req.DiskType
		}

		// 查找匹配 DiskType 的存储位置
		location := vs.store.FindFreeLocation(func(location *storage.DiskLocation) bool {
			return location.DiskType == types.ToDiskType(diskType)
		})
		if location == nil {
			return fmt.Errorf("no space left for disk type %s", types.ToDiskType(diskType).ReadableString())
		}

		// 构建目标文件路径
		// dataBaseFileName: /data/collection_volumeId (数据文件基础路径)
		// indexBaseFileName: /idx/collection_volumeId (索引文件基础路径)
		dataBaseFileName = storage.VolumeFileName(location.Directory, volFileInfoResp.Collection, int(req.VolumeId))
		indexBaseFileName = storage.VolumeFileName(location.IdxDirectory, volFileInfoResp.Collection, int(req.VolumeId))

		// 检查是否使用远程存储
		// 如果 VolumeInfo.Files 不为空，说明数据存储在远程（如 S3）
		hasRemoteDatFile = volFileInfoResp.VolumeInfo != nil && len(volFileInfoResp.VolumeInfo.Files) > 0

		// 创建 .note 文件标记正在复制
		// 防止并发复制或误操作
		util.WriteFile(dataBaseFileName+".note", []byte(fmt.Sprintf("copying from %s", req.SourceDataNode)), 0755)

		// 错误清理：如果复制失败，删除所有部分文件
		defer func() {
			if err != nil {
				os.Remove(dataBaseFileName + ".dat")
				os.Remove(indexBaseFileName + ".idx")
				os.Remove(dataBaseFileName + ".vif")
				os.Remove(dataBaseFileName + ".note")
			}
		}()

		// 【步骤 4：预分配空间（可选）】
		// 从 Master 获取配置，检查是否启用预分配
		var preallocateSize int64
		if grpcErr := pb.WithMasterClient(false, vs.GetMaster(context.Background()), vs.grpcDialOption, false, func(client master_pb.SeaweedClient) error {
			// 获取 Master 配置
			resp, err := client.GetMasterConfiguration(context.Background(), &master_pb.GetMasterConfigurationRequest{})
			if err != nil {
				return fmt.Errorf("get master %s configuration: %v", vs.GetMaster(context.Background()), err)
			}

			// 如果启用预分配，设置预分配大小
			// VolumePreallocate: 是否预分配磁盘空间
			// VolumeSizeLimitMB: Volume 大小限制（MB）
			if resp.VolumePreallocate {
				preallocateSize = int64(resp.VolumeSizeLimitMB) * (1 << 20)  // 转换为字节
			}
			return nil
		}); grpcErr != nil {
			// 连接 Master 失败，记录日志但继续执行
			glog.V(0).Infof("connect to %s: %v", vs.GetMaster(context.Background()), grpcErr)
		}

		// 如果需要预分配且不是远程存储，创建预分配文件
		// 预分配的优点：
		//   - 减少文件碎片
		//   - 提高写入性能
		//   - 确保有足够的磁盘空间
		if preallocateSize > 0 && !hasRemoteDatFile {
			volumeFile := dataBaseFileName + ".dat"
			_, err := backend.CreateVolumeFile(volumeFile, preallocateSize, 0)
			if err != nil {
				return fmt.Errorf("create volume file %s: %v", volumeFile, err)
			}
		}

		// 【步骤 5：复制文件】
		// 初始化进度报告变量
		copyResponse := &volume_server_pb.VolumeCopyResponse{}
		reportInterval := int64(1024 * 1024 * 128)  // 每 128MB 报告一次进度
		nextReportTarget := reportInterval  // 下次报告的阈值
		var modifiedTsNs int64  // 文件修改时间戳（纳秒）
		var sendErr error  // 发送进度时的错误

		// 确定复制速度限制
		var ioBytePerSecond int64
		if req.IoBytePerSecond <= 0 {
			// 使用默认的压缩速度限制
			ioBytePerSecond = vs.compactionBytePerSecond
		} else {
			// 使用请求中指定的速度限制
			ioBytePerSecond = req.IoBytePerSecond
		}

		// 创建流量限制器
		// 限制复制速度，避免占用过多带宽
		throttler := util.NewWriteThrottler(ioBytePerSecond)

		// 步骤 5.1：复制 .dat 文件（数据文件，最大）
		// 只有非远程存储才需要复制 .dat 文件
		// 远程存储（如 S3）的数据不在本地
		if !hasRemoteDatFile {
			// doCopyFileWithThrottler 参数说明：
			//   - client: gRPC 客户端
			//   - isEcVolume: false（普通 Volume）
			//   - collection: 集合名称
			//   - vid: Volume ID
			//   - compactionRevision: 压缩版本号
			//   - stopOffset: 复制大小（源文件大小）
			//   - baseFileName: 目标文件基础路径
			//   - ext: 文件扩展名
			//   - isAppend: false（覆盖写入）
			//   - ignoreSourceFileNotFound: true（源文件不存在时忽略）
			//   - progressFn: 进度回调函数
			//   - throttler: 流量限制器
			if modifiedTsNs, err = vs.doCopyFileWithThrottler(client, false, req.Collection, req.VolumeId, volFileInfoResp.CompactionRevision, volFileInfoResp.DatFileSize, dataBaseFileName, ".dat", false, true, func(processed int64) bool {
				// 进度回调：每复制 128MB 发送一次进度更新
				if processed > nextReportTarget {
					copyResponse.ProcessedBytes = processed
					// 通过 gRPC 流发送进度
					if sendErr = stream.Send(copyResponse); sendErr != nil {
						// 发送失败，中断复制
						return false
					}
					// 更新下次报告阈值
					nextReportTarget = processed + reportInterval
				}
				// 返回 true 继续复制
				return true
			}, throttler); err != nil {
				return err
			}

			// 检查进度发送错误
			if sendErr != nil {
				return sendErr
			}

			// 设置 .dat 文件的修改时间戳
			// 保持与源文件相同的时间戳
			if modifiedTsNs > 0 {
				os.Chtimes(dataBaseFileName+".dat", time.Unix(0, modifiedTsNs), time.Unix(0, modifiedTsNs))
			}
		}

		// 步骤 5.2：复制 .idx 文件（索引文件）
		// 索引文件用于快速查找 Needle
		if modifiedTsNs, err = vs.doCopyFileWithThrottler(client, false, req.Collection, req.VolumeId, volFileInfoResp.CompactionRevision, volFileInfoResp.IdxFileSize, indexBaseFileName, ".idx", false, false, nil, throttler); err != nil {
			return err
		}
		// 设置 .idx 文件的修改时间戳
		if modifiedTsNs > 0 {
			os.Chtimes(indexBaseFileName+".idx", time.Unix(0, modifiedTsNs), time.Unix(0, modifiedTsNs))
		}

		// 步骤 5.3：复制 .vif 文件（Volume Info 文件）
		// .vif 文件存储 Volume 的元数据信息
		// stopOffset 设置为 1MB（.vif 文件通常很小）
		if modifiedTsNs, err = vs.doCopyFileWithThrottler(client, false, req.Collection, req.VolumeId, volFileInfoResp.CompactionRevision, 1024*1024, dataBaseFileName, ".vif", false, true, nil, throttler); err != nil {
			return err
		}
		// 设置 .vif 文件的修改时间戳
		if modifiedTsNs > 0 {
			os.Chtimes(dataBaseFileName+".vif", time.Unix(0, modifiedTsNs), time.Unix(0, modifiedTsNs))
		}

		// 删除 .note 文件，标记复制完成
		os.Remove(dataBaseFileName + ".note")

		return nil
	})

	// 检查 WithVolumeServerClient 的错误
	if err != nil {
		return err
	}
	if dataBaseFileName == "" {
		return fmt.Errorf("not found volume %d file", req.VolumeId)
	}

	// 构建完整的文件路径
	idxFileName = indexBaseFileName + ".idx"
	datFileName = dataBaseFileName + ".dat"

	// 错误清理：如果后续步骤失败，删除已复制的文件
	defer func() {
		if err != nil && dataBaseFileName != "" {
			os.Remove(idxFileName)
			os.Remove(datFileName)
			os.Remove(dataBaseFileName + ".vif")
		}
	}()

	// 【步骤 6：验证文件完整性】
	// 检查复制后的文件大小是否与源文件一致
	// 这是确保数据完整性的关键步骤
	if err = checkCopyFiles(volFileInfoResp, hasRemoteDatFile, idxFileName, datFileName); err != nil {
		return err
	}

	// 【步骤 7：挂载 Volume】
	// 将复制好的 Volume 挂载到存储系统
	// 挂载过程包括：
	//   - 加载 .idx 索引到内存
	//   - 读取 .dat 文件的 SuperBlock
	//   - 将 Volume 添加到 store 中
	//   - 标记 Volume 为可用
	err = vs.store.MountVolume(needle.VolumeId(req.VolumeId))
	if err != nil {
		return fmt.Errorf("failed to mount volume %d: %v", req.VolumeId, err)
	}

	// 发送最终响应
	// LastAppendAtNs: 最后一次追加的时间戳（纳秒）
	// 客户端可以使用这个时间戳进行增量复制
	if err = stream.Send(&volume_server_pb.VolumeCopyResponse{
		LastAppendAtNs: volFileInfoResp.DatFileTimestampSeconds * uint64(time.Second),
	}); err != nil {
		glog.Errorf("send response: %v", err)
	}

	return err
}

// doCopyFile 复制单个文件（使用默认流量限制）
// 辅助函数，简化调用 doCopyFileWithThrottler
//
// 功能:
//   - 创建默认的 WriteThrottler
//   - 调用 doCopyFileWithThrottler 执行实际复制
//
// 参数:
//   - client: gRPC 客户端
//   - isEcVolume: 是否是 EC Volume
//   - collection: 集合名称
//   - vid: Volume ID
//   - compactRevision: 压缩版本号
//   - stopOffset: 复制字节数（0 表示复制整个文件）
//   - baseFileName: 目标文件基础路径（不含扩展名）
//   - ext: 文件扩展名（如 ".dat", ".idx"）
//   - isAppend: 是否追加模式（true: 追加, false: 覆盖）
//   - ignoreSourceFileNotFound: 源文件不存在时是否忽略错误
//   - progressFn: 进度回调函数
//
// 返回:
//   - modifiedTsNs: 源文件的修改时间戳（纳秒）
//   - err: 错误信息
func (vs *VolumeServer) doCopyFile(client volume_server_pb.VolumeServerClient, isEcVolume bool, collection string, vid, compactRevision uint32, stopOffset uint64, baseFileName, ext string, isAppend, ignoreSourceFileNotFound bool, progressFn storage.ProgressFunc) (modifiedTsNs int64, err error) {
	// 使用默认的压缩速度限制创建 throttler
	return vs.doCopyFileWithThrottler(client, isEcVolume, collection, vid, compactRevision, stopOffset, baseFileName, ext, isAppend, ignoreSourceFileNotFound, progressFn, util.NewWriteThrottler(vs.compactionBytePerSecond))
}

// doCopyFileWithThrottler 复制单个文件（带流量控制）
// 核心文件复制实现
//
// 功能:
//   - 从源 Volume Server 读取文件流
//   - 写入到本地文件
//   - 支持流量限制和进度报告
//
// 参数:
//   - client: gRPC 客户端，用于连接源服务器
//   - isEcVolume: 是否是 EC Volume
//   - collection: 集合名称
//   - vid: Volume ID
//   - compactRevision: 压缩版本号，用于检测源文件是否被压缩
//   - stopOffset: 复制字节数（源文件大小）
//   - baseFileName: 目标文件基础路径（不含扩展名）
//   - ext: 文件扩展名（如 ".dat", ".idx", ".vif"）
//   - isAppend: 是否追加模式
//   - ignoreSourceFileNotFound: 源文件不存在时是否忽略
//   - progressFn: 进度回调函数，接收已处理字节数
//   - throttler: 流量限制器
//
// 复制流程:
//   1. 调用 CopyFile gRPC 接口启动流式读取
//   2. 通过 writeToFile 将流写入本地文件
//   3. 返回文件修改时间戳
//
// 返回:
//   - modifiedTsNs: 源文件的修改时间戳（纳秒）
//   - err: 错误信息
func (vs *VolumeServer) doCopyFileWithThrottler(client volume_server_pb.VolumeServerClient, isEcVolume bool, collection string, vid, compactRevision uint32, stopOffset uint64, baseFileName, ext string, isAppend, ignoreSourceFileNotFound bool, progressFn storage.ProgressFunc, throttler *util.WriteThrottler) (modifiedTsNs int64, err error) {

	// 调用 CopyFile gRPC 接口，获取文件流
	// CopyFile 是一个服务端流式 API，返回文件内容
	copyFileClient, err := client.CopyFile(context.Background(), &volume_server_pb.CopyFileRequest{
		VolumeId:                 vid,
		Ext:                      ext,
		CompactionRevision:       compactRevision,  // 检测压缩版本
		StopOffset:               stopOffset,  // 复制大小
		Collection:               collection,
		IsEcVolume:               isEcVolume,
		IgnoreSourceFileNotFound: ignoreSourceFileNotFound,
	})
	if err != nil {
		return modifiedTsNs, fmt.Errorf("failed to start copying volume %d %s file: %v", vid, ext, err)
	}

	// 将 gRPC 流写入到本地文件
	// writeToFile 处理流式接收和写入
	modifiedTsNs, err = writeToFile(copyFileClient, baseFileName+ext, throttler, isAppend, progressFn)
	if err != nil {
		return modifiedTsNs, fmt.Errorf("failed to copy %s file: %v", baseFileName+ext, err)
	}

	return modifiedTsNs, nil

}

// checkCopyFiles 检查复制后的文件完整性
// 验证函数，确保文件复制成功
//
// 功能:
//   - 检查 .idx 文件大小是否与源文件一致
//   - 检查 .dat 文件大小是否与源文件一致（非远程存储）
//   - 确保数据完整性
//
// 参数:
//   - originFileInf: 源文件信息（大小、时间戳等）
//   - hasRemoteDatFile: 是否使用远程存储（远程存储跳过 .dat 检查）
//   - idxFileName: 目标 .idx 文件路径
//   - datFileName: 目标 .dat 文件路径
//
// 验证逻辑:
//   1. 检查 .idx 文件是否存在
//   2. 验证 .idx 文件大小是否匹配
//   3. 如果不是远程存储，检查 .dat 文件
//   4. 验证 .dat 文件大小是否匹配
//
// 返回:
//   - error: 验证失败时返回详细错误信息
//
// 注意:
//   - 目前只检查文件大小
//   - TODO: 可能需要检查 Volume 的接收计数和删除计数
//   - 远程存储（如 S3）的 .dat 文件不在本地，跳过检查
func checkCopyFiles(originFileInf *volume_server_pb.ReadVolumeFileStatusResponse, hasRemoteDatFile bool, idxFileName, datFileName string) error {
	// 检查 .idx 文件
	stat, err := os.Stat(idxFileName)
	if err != nil {
		return fmt.Errorf("stat idx file %s failed: %v", idxFileName, err)
	}

	// 验证 .idx 文件大小
	if originFileInf.IdxFileSize != uint64(stat.Size()) {
		return fmt.Errorf("idx file %s size [%v] is not same as origin file size [%v]",
			idxFileName, stat.Size(), originFileInf.IdxFileSize)
	}

	// 如果是远程存储，跳过 .dat 文件检查
	// 远程存储的数据文件不在本地
	if hasRemoteDatFile {
		return nil
	}

	// 检查 .dat 文件
	stat, err = os.Stat(datFileName)
	if err != nil {
		return fmt.Errorf("get dat file info failed, %w", err)
	}

	// 验证 .dat 文件大小
	if originFileInf.DatFileSize != uint64(stat.Size()) {
		return fmt.Errorf("the dat file size [%v] is not same as origin file size [%v]",
			stat.Size(), originFileInf.DatFileSize)
	}

	return nil
}

// writeToFile 从 gRPC 流写入到本地文件
// 底层文件写入实现
//
// 功能:
//   - 从 gRPC 流接收文件内容
//   - 写入到本地文件
//   - 支持流量限制和进度报告
//   - 提取源文件修改时间戳
//
// 参数:
//   - client: gRPC 流客户端，用于接收文件内容
//   - fileName: 目标文件路径
//   - wt: 写入流量限制器
//   - isAppend: 是否追加模式
//   - progressFn: 进度回调函数
//
// 写入流程:
//   1. 打开或创建目标文件
//   2. 循环接收 gRPC 流中的文件块
//   3. 写入到文件
//   4. 调用进度回调
//   5. 应用流量限制
//   6. 直到接收完成（EOF）
//
// 返回:
//   - modifiedTsNs: 源文件的修改时间戳（纳秒）
//   - err: 错误信息
func writeToFile(client volume_server_pb.VolumeServer_CopyFileClient, fileName string, wt *util.WriteThrottler, isAppend bool, progressFn storage.ProgressFunc) (modifiedTsNs int64, err error) {
	glog.V(4).Infof("writing to %s", fileName)

	// 确定文件打开模式
	flags := os.O_WRONLY | os.O_CREATE | os.O_TRUNC  // 覆盖模式
	if isAppend {
		flags = os.O_WRONLY | os.O_CREATE  // 追加模式（不设置 TRUNC）
	}

	// 打开目标文件
	dst, err := os.OpenFile(fileName, flags, 0644)
	if err != nil {
		return modifiedTsNs, nil
	}
	defer dst.Close()

	// 已写入字节数统计
	var progressedBytes int64

	// 循环接收 gRPC 流
	for {
		// 接收文件内容块
		resp, receiveErr := client.Recv()

		// 检查是否到达流末尾
		if receiveErr == io.EOF {
			break
		}

		// 提取源文件修改时间戳
		// 只在第一个响应中包含
		if resp != nil && resp.ModifiedTsNs != 0 {
			modifiedTsNs = resp.ModifiedTsNs
		}

		// 检查接收错误
		if receiveErr != nil {
			return modifiedTsNs, fmt.Errorf("receiving %s: %v", fileName, receiveErr)
		}

		// 写入文件内容
		dst.Write(resp.FileContent)

		// 更新进度计数
		progressedBytes += int64(len(resp.FileContent))

		// 调用进度回调
		if progressFn != nil {
			// 进度回调返回 false 时中断操作
			if !progressFn(progressedBytes) {
				return modifiedTsNs, fmt.Errorf("interrupted copy operation")
			}
		}

		// 应用流量限制
		// 根据写入字节数可能触发延迟
		wt.MaybeSlowdown(int64(len(resp.FileContent)))
	}

	return modifiedTsNs, nil
}

// ReadVolumeFileStatus 读取 Volume 文件状态
// gRPC API: ReadVolumeFileStatus
//
// 功能:
//   - 返回 Volume 的文件元数据
//   - 包括文件大小、时间戳、文件数量等
//   - 用于复制前获取源文件信息
//
// 参数:
//   - VolumeId: 要查询的卷 ID
//
// 返回信息:
//   - VolumeId: 卷 ID
//   - DatFileSize: .dat 文件大小（字节）
//   - IdxFileSize: .idx 文件大小（字节）
//   - DatFileTimestampSeconds: .dat 文件修改时间戳（秒）
//   - IdxFileTimestampSeconds: .idx 文件修改时间戳（秒）
//   - FileCount: Volume 中的文件数量
//   - CompactionRevision: 压缩版本号
//   - Collection: 集合名称
//   - DiskType: 磁盘类型（hdd/ssd）
//   - VolumeInfo: Volume 详细信息（包括远程存储配置）
//   - Version: Volume 格式版本
//
// 使用场景:
//   - 复制前获取源文件元数据
//   - 验证 Volume 状态
//   - 监控 Volume 大小
//   - 检查是否需要压缩
//
// 返回:
//   - ReadVolumeFileStatusResponse: Volume 文件状态
//   - error: 查询失败错误
func (vs *VolumeServer) ReadVolumeFileStatus(ctx context.Context, req *volume_server_pb.ReadVolumeFileStatusRequest) (*volume_server_pb.ReadVolumeFileStatusResponse, error) {
	resp := &volume_server_pb.ReadVolumeFileStatusResponse{}

	// 获取 Volume 对象
	v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
	if v == nil {
		return nil, fmt.Errorf("not found volume id %d", req.VolumeId)
	}

	// 填充响应信息
	resp.VolumeId = req.VolumeId

	// 获取文件统计信息
	// datSize: .dat 文件大小
	// idxSize: .idx 文件大小
	// modTime: 文件修改时间
	datSize, idxSize, modTime := v.FileStat()
	resp.DatFileSize = datSize
	resp.IdxFileSize = idxSize
	resp.DatFileTimestampSeconds = uint64(modTime.Unix())  // 转换为秒
	resp.IdxFileTimestampSeconds = uint64(modTime.Unix())

	// 其他元数据
	resp.FileCount = v.FileCount()  // Volume 中的 Needle 数量
	resp.CompactionRevision = uint32(v.CompactionRevision)  // 压缩版本号
	resp.Collection = v.Collection  // 集合名称
	resp.DiskType = string(v.DiskType())  // 磁盘类型
	resp.VolumeInfo = v.GetVolumeInfo()  // 远程存储配置等详细信息
	resp.Version = uint32(v.Version())  // Volume 格式版本（1/2/3）

	return resp, nil
}

// CopyFile 流式读取文件内容并发送到客户端
// gRPC Streaming API: CopyFile (服务端流式）
//
// 功能:
//   - 读取 Volume 相关文件（.dat、.idx、.vif 或 EC shard）
//   - 通过 gRPC 流发送到客户端
//   - 支持部分读取（StopOffset）
//   - 检查压缩版本以确保数据一致性
//
// 参数:
//   - VolumeId: 卷 ID
//   - Ext: 文件扩展名（".dat", ".idx", ".vif", ".ec00" 等）
//   - CompactionRevision: 期望的压缩版本号
//     - math.MaxUint32: 不检查压缩版本
//     - 其他值: 如果不匹配返回错误
//   - StopOffset: 读取字节数
//     - math.MaxUint64: 读取整个文件
//     - 其他值: 读取指定字节数
//   - Collection: 集合名称（用于 EC Volume）
//   - IsEcVolume: 是否是 EC Volume
//   - IgnoreSourceFileNotFound: 文件不存在时是否忽略错误
//
// 复制流程:
//   【步骤 1：查找文件】
//   - 普通 Volume: 通过 store.GetVolume() 查找
//   - EC Volume: 在所有存储位置搜索 EC shard 文件
//
//   【步骤 2：验证压缩版本】
//   - 如果 CompactionRevision != MaxUint32，检查是否匹配
//   - 不匹配返回错误，防止复制过时数据
//
//   【步骤 3：打开文件】
//   - 普通 Volume: 调用 v.SyncToDisk() 确保数据落盘
//   - 获取文件修改时间戳
//
//   【步骤 4：流式发送】
//   - 使用 2MB 缓冲区分块读取
//   - 通过 stream.Send() 发送到客户端
//   - 第一个响应包含修改时间戳
//   - 继续发送直到达到 StopOffset 或文件末尾
//
// 使用场景:
//   - VolumeCopy 内部调用，复制 Volume 文件
//   - EC Volume 复制 shard 文件
//   - 增量复制获取部分数据
//
// 返回:
//   - 流式返回 CopyFileResponse
//   - FileContent: 文件内容块（最大 2MB）
//   - ModifiedTsNs: 文件修改时间戳（只在第一个响应中）
//
// 注意:
//   - 压缩版本检查确保数据一致性
//   - StopOffset 允许部分读取
//   - 文件不存在时可选择忽略或返回错误
func (vs *VolumeServer) CopyFile(req *volume_server_pb.CopyFileRequest, stream volume_server_pb.VolumeServer_CopyFileServer) error {

	var fileName string

	// 【步骤 1：查找文件】
	if !req.IsEcVolume {
		// 普通 Volume
		v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
		if v == nil {
			return fmt.Errorf("not found volume id %d", req.VolumeId)
		}

		// 【步骤 2：验证压缩版本】
		// 如果指定了压缩版本（!= MaxUint32），检查是否匹配
		if uint32(v.CompactionRevision) != req.CompactionRevision && req.CompactionRevision != math.MaxUint32 {
			// 压缩版本不匹配，可能 Volume 已被压缩
			// 返回错误，防止复制不一致的数据
			return fmt.Errorf("volume %d is compacted", req.VolumeId)
		}

		// 确保数据落盘
		v.SyncToDisk()

		// 获取文件路径
		fileName = v.FileName(req.Ext)
	} else {
		// EC Volume
		// EC shard 文件命名格式: collection_volumeId.ec00, .ec01, ...
		baseFileName := erasure_coding.EcShardBaseFileName(req.Collection, int(req.VolumeId)) + req.Ext

		// 在所有存储位置搜索文件
		for _, location := range vs.store.Locations {
			// 检查数据目录
			tName := util.Join(location.Directory, baseFileName)
			if util.FileExists(tName) {
				fileName = tName
			}

			// 检查索引目录
			tName = util.Join(location.IdxDirectory, baseFileName)
			if util.FileExists(tName) {
				fileName = tName
			}
		}

		// 文件未找到
		if fileName == "" {
			if req.IgnoreSourceFileNotFound {
				// 忽略错误，返回成功
				return nil
			}
			return fmt.Errorf("CopyFile not found ec volume id %d", req.VolumeId)
		}
	}

	// 要读取的字节数
	bytesToRead := int64(req.StopOffset)

	// 【步骤 3：打开文件】
	file, err := os.Open(fileName)
	if err != nil {
		if req.IgnoreSourceFileNotFound && err == os.ErrNotExist {
			return nil
		}
		return err
	}
	defer file.Close()

	// 获取文件修改时间戳
	fileInfo, err := file.Stat()
	if err != nil {
		return err
	}
	fileModTsNs := fileInfo.ModTime().UnixNano()

	// 【步骤 4：流式发送】
	// 创建 2MB 缓冲区
	buffer := make([]byte, BufferSizeLimit)

	// 循环读取并发送
	for bytesToRead > 0 {
		// 读取文件块
		bytesread, err := file.Read(buffer)

		// 检查读取错误
		if err != nil {
			if err != io.EOF {
				return err
			}
			// 到达文件末尾，退出循环
			break
		}

		// 如果读取超过需要的字节数，截断
		if int64(bytesread) > bytesToRead {
			bytesread = int(bytesToRead)
		}

		// 发送到客户端
		err = stream.Send(&volume_server_pb.CopyFileResponse{
			FileContent:  buffer[:bytesread],
			ModifiedTsNs: fileModTsNs,  // 修改时间戳
		})
		if err != nil {
			return err
		}

		// 时间戳只发送一次
		fileModTsNs = 0

		// 更新剩余字节数
		bytesToRead -= int64(bytesread)
	}

	return nil
}

// ReceiveFile 从客户端接收文件流并写入存储
// gRPC Streaming API: ReceiveFile (客户端流式)
//
// 功能:
//   - 接收客户端推送的文件流
//   - 写入到 Volume 或 EC shard 文件
//   - 支持普通 Volume 和 EC Volume
//   - 返回写入字节数
//
// 协议:
//   第一个消息: ReceiveFileInfo（文件元数据）
//     - VolumeId: 卷 ID
//     - Ext: 文件扩展名
//     - Collection: 集合名称
//     - ShardId: EC shard ID（仅 EC Volume）
//     - FileSize: 文件大小
//     - IsEcVolume: 是否是 EC Volume
//
//   后续消息: FileContent（文件内容块）
//     - 分块发送文件内容
//     - 服务端累加写入
//
//   最后响应: ReceiveFileResponse
//     - BytesWritten: 实际写入字节数
//     - Error: 错误信息（如果有）
//
// 接收流程:
//   【步骤 1：接收文件元数据】
//   - 第一个消息必须是 Info
//   - 根据 IsEcVolume 确定文件路径
//   - EC Volume: 在 HDD 位置创建 shard 文件
//   - 普通 Volume: 使用 Volume 的文件路径
//
//   【步骤 2：创建目标文件】
//   - 使用 os.Create() 创建文件
//   - 覆盖已存在的文件
//
//   【步骤 3：接收并写入内容】
//   - 循环接收 FileContent 消息
//   - 写入到目标文件
//   - 累加已写入字节数
//
//   【步骤 4：完成或清理】
//   - 成功：调用 Sync() 落盘，返回写入字节数
//   - 失败：删除部分文件，返回错误
//
// 使用场景:
//   - EC 编码时推送 shard 文件
//   - Volume 迁移时推送文件
//   - 远程备份和恢复
//
// 返回:
//   - BytesWritten: 成功写入的字节数
//   - Error: 错误信息
//
// 注意:
//   - 必须先发送 Info，再发送 FileContent
//   - 错误时会自动清理部分文件
//   - EC Volume 优先使用 HDD 存储位置
func (vs *VolumeServer) ReceiveFile(stream volume_server_pb.VolumeServer_ReceiveFileServer) error {
	var fileInfo *volume_server_pb.ReceiveFileInfo  // 文件元数据
	var targetFile *os.File  // 目标文件句柄
	var filePath string  // 文件路径
	var bytesWritten uint64  // 已写入字节数

	// 确保文件关闭
	defer func() {
		if targetFile != nil {
			targetFile.Close()
		}
	}()

	// 循环接收流消息
	for {
		req, err := stream.Recv()

		// 检查流是否结束
		if err == io.EOF {
			// 流成功完成
			if targetFile != nil {
				// 落盘
				targetFile.Sync()
				glog.V(1).Infof("Successfully received file %s (%d bytes)", filePath, bytesWritten)
			}

			// 返回最终响应
			return stream.SendAndClose(&volume_server_pb.ReceiveFileResponse{
				BytesWritten: bytesWritten,
			})
		}

		// 检查接收错误
		if err != nil {
			// 清理部分文件
			if targetFile != nil {
				targetFile.Close()
				os.Remove(filePath)
			}
			glog.Errorf("Failed to receive stream: %v", err)
			return fmt.Errorf("failed to receive stream: %v", err)
		}

		// 处理消息
		switch data := req.Data.(type) {
		case *volume_server_pb.ReceiveFileRequest_Info:
			// 【步骤 1：接收文件元数据】
			// 第一个消息包含文件信息
			fileInfo = data.Info
			glog.V(1).Infof("ReceiveFile: volume %d, ext %s, collection %s, shard %d, size %d",
				fileInfo.VolumeId, fileInfo.Ext, fileInfo.Collection, fileInfo.ShardId, fileInfo.FileSize)

			// 根据文件类型创建路径
			if fileInfo.IsEcVolume {
				// EC Volume: 查找存储位置
				// 优先使用 HDD 类型的存储位置
				var targetLocation *storage.DiskLocation
				for _, location := range vs.store.Locations {
					if location.DiskType == types.HardDriveType {
						targetLocation = location
						break
					}
				}

				// 如果没有 HDD，使用第一个可用位置
				if targetLocation == nil && len(vs.store.Locations) > 0 {
					targetLocation = vs.store.Locations[0]
				}

				// 没有可用存储位置
				if targetLocation == nil {
					glog.Errorf("ReceiveFile: no storage location available")
					return stream.SendAndClose(&volume_server_pb.ReceiveFileResponse{
						Error: "no storage location available",
					})
				}

				// 构建 EC shard 文件路径
				// 格式: collection_volumeId.ec00, .ec01, ...
				baseFileName := erasure_coding.EcShardBaseFileName(fileInfo.Collection, int(fileInfo.VolumeId))
				filePath = util.Join(targetLocation.Directory, baseFileName+fileInfo.Ext)
			} else {
				// 普通 Volume
				v := vs.store.GetVolume(needle.VolumeId(fileInfo.VolumeId))
				if v == nil {
					glog.Errorf("ReceiveFile: volume %d not found", fileInfo.VolumeId)
					return stream.SendAndClose(&volume_server_pb.ReceiveFileResponse{
						Error: fmt.Sprintf("volume %d not found", fileInfo.VolumeId),
					})
				}

				// 使用 Volume 的文件路径
				filePath = v.FileName(fileInfo.Ext)
			}

			// 【步骤 2：创建目标文件】
			targetFile, err = os.Create(filePath)
			if err != nil {
				glog.Errorf("ReceiveFile: failed to create file %s: %v", filePath, err)
				return stream.SendAndClose(&volume_server_pb.ReceiveFileResponse{
					Error: fmt.Sprintf("failed to create file: %v", err),
				})
			}
			glog.V(1).Infof("ReceiveFile: created target file %s", filePath)

		case *volume_server_pb.ReceiveFileRequest_FileContent:
			// 【步骤 3：接收并写入内容】
			// 后续消息包含文件内容
			if targetFile == nil {
				glog.Errorf("ReceiveFile: file info must be sent first")
				return stream.SendAndClose(&volume_server_pb.ReceiveFileResponse{
					Error: "file info must be sent first",
				})
			}

			// 写入文件内容
			n, err := targetFile.Write(data.FileContent)
			if err != nil {
				// 写入失败，清理文件
				targetFile.Close()
				os.Remove(filePath)
				glog.Errorf("ReceiveFile: failed to write to file %s: %v", filePath, err)
				return stream.SendAndClose(&volume_server_pb.ReceiveFileResponse{
					Error: fmt.Sprintf("failed to write file: %v", err),
				})
			}

			// 累加已写入字节数
			bytesWritten += uint64(n)
			glog.V(2).Infof("ReceiveFile: wrote %d bytes to %s (total: %d)", n, filePath, bytesWritten)

		default:
			// 未知消息类型
			glog.Errorf("ReceiveFile: unknown message type")
			return stream.SendAndClose(&volume_server_pb.ReceiveFileResponse{
				Error: "unknown message type",
			})
		}
	}
}
