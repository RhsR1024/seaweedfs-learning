// Package weed_server 实现 Volume Server 的纠删码（Erasure Coding）gRPC 接口
// 本文件提供 EC Volume 的生成、复制、挂载、读取和删除功能
//
// 纠删码（Erasure Coding，EC）原理:
//   SeaweedFS 使用 Reed-Solomon 编码实现数据冗余
//   - 默认配置：10 个数据分片 + 4 个校验分片 = 14 个总分片
//   - 只需任意 10 个分片即可恢复原始数据
//   - 存储开销：1.4 倍（相比副本的 2-3 倍更高效）
//   - 可容忍 4 个分片丢失
//
// 核心功能:
//   - VolumeEcShardsGenerate: 生成 EC 分片（.ec00~.ec13 + .ecx 索引）
//   - VolumeEcShardsRebuild: 从现有分片重建丢失的分片
//   - VolumeEcShardsCopy: 复制 EC 分片到其他服务器
//   - VolumeEcShardsMount/Unmount: 挂载/卸载 EC 分片
//   - VolumeEcShardRead: 读取 EC 分片数据
//   - VolumeEcBlobDelete: 删除 EC Volume 中的文件
//   - VolumeEcShardsToVolume: 从 EC 分片恢复为普通 Volume
//   - VolumeEcShardsInfo: 获取 EC 分片信息
//
// 文件格式:
//   - .ec00 ~ .ec13: EC 数据分片和校验分片
//     - .ec00 ~ .ec09: 数据分片（原始数据切分）
//     - .ec10 ~ .ec13: 校验分片（通过 Reed-Solomon 编码生成）
//   - .ecx: EC 索引文件（从 .idx 转换而来）
//   - .ecj: EC journal 文件（记录删除操作）
//   - .vif: Volume 信息文件（包含 EC 配置、过期时间等）
//
// EC 应用流程（Erasure Coding Workflow）:
//   【步骤 0】确保 Volume 为只读状态
//   - EC 编码不能应用于正在写入的 Volume
//   - 需要先将 Volume 标记为只读
//
//   【步骤 1】生成 EC 分片
//   - 客户端调用 VolumeEcShardsGenerate
//   - 在源服务器上生成 .ecx 和 .ec00~.ec13 文件
//   - 这一步会读取 .dat 文件并进行 Reed-Solomon 编码
//
//   【步骤 2】询问 Master 分配目标服务器
//   - 客户端向 Master 请求可以存储 EC 分片的服务器列表
//   - Master 根据拓扑结构和容量分配服务器
//   - 通常分散在不同机架或数据中心
//
//   【步骤 3】复制 EC 分片到目标服务器
//   - 客户端调用目标服务器的 VolumeEcShardsCopy
//   - 目标服务器从源服务器拉取分片文件
//   - 支持并行复制多个分片
//
//   【步骤 4】目标服务器向 Master 报告
//   - 目标服务器成功接收分片后向 Master 报告
//   - Master 更新元数据：vid -> [14]*DataNode
//
//   【步骤 5】Master 存储映射关系
//   - Master 维护：volumeId -> [14个DataNode的列表]
//   - 每个 DataNode 存储部分分片
//
//   【步骤 6】删除原始文件
//   - 客户端检查 Master，确认所有 14 个分片都就绪
//   - 删除源服务器上的原始 .idx 和 .dat 文件
//   - 此时 Volume 完全转换为 EC Volume
//
// 使用场景:
//   - 温数据存储：访问频率低但需要保留的数据
//   - 降低存储成本：EC 比副本更节省空间
//   - 提高容错性：可容忍更多节点故障
//   - 大容量归档：长期保存的历史数据
//
// 性能权衡:
//   - 写入性能：EC Volume 为只读，不支持新写入
//   - 读取性能：需要从多个分片读取和解码，比普通 Volume 慢
//   - 存储效率：1.4x 开销，比副本（2-3x）更高效
//   - 容错能力：可容忍 4 个节点故障（默认配置）
package weed_server

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// VolumeEcShardsGenerate 生成 EC 分片文件
// gRPC API: VolumeEcShardsGenerate
//
// 功能:
//   - 从普通 Volume 生成 EC 分片
//   - 生成 .ec00~.ec13 数据/校验分片
//   - 生成 .ecx 索引文件
//   - 生成 .vif 信息文件
//
// 参数:
//   - VolumeId: 要编码的卷 ID
//   - Collection: 集合名称（必须匹配）
//
// 生成流程:
//   【步骤 1：验证 Volume】
//   - 检查 Volume 是否存在
//   - 验证 Collection 是否匹配
//
//   【步骤 2：创建 EC 上下文】
//   - 优先使用 .vif 中已有的 EC 配置（用于重新生成场景）
//   - 如果没有或配置无效，使用默认配置（10+4）
//
//   【步骤 3：生成 EC 分片】
//   - 读取 .dat 文件
//   - 使用 Reed-Solomon 编码生成分片
//   - 写入 .ec00~.ec13 文件
//
//   【步骤 4：生成 .ecx 索引】
//   - 从 .idx 文件转换为 .ecx 格式
//   - .ecx 是排序后的索引，用于 EC Volume 查找
//
//   【步骤 5：生成 .vif 文件】
//   - 保存 Volume 元数据
//   - 包括 EC 配置、过期时间、文件大小等
//
// Reed-Solomon 编码原理:
//   - 将原始数据分成 N 个数据块
//   - 生成 M 个校验块
//   - 只需任意 N 个块即可恢复原始数据
//   - 默认：N=10（数据块），M=4（校验块）
//
// 返回:
//   - VolumeEcShardsGenerateResponse: 空响应表示成功
//   - error: 生成失败错误
//
// 注意:
//   - Volume 必须是只读状态
//   - 生成失败时会自动清理部分文件
//   - 支持自定义 EC 配置（通过 .vif 文件）
func (vs *VolumeServer) VolumeEcShardsGenerate(ctx context.Context, req *volume_server_pb.VolumeEcShardsGenerateRequest) (*volume_server_pb.VolumeEcShardsGenerateResponse, error) {

	glog.V(0).Infof("VolumeEcShardsGenerate: %v", req)

	// 【步骤 1：验证 Volume】
	// 获取 Volume 对象
	v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
	if v == nil {
		return nil, fmt.Errorf("volume %d not found", req.VolumeId)
	}

	// 获取数据文件基础路径
	baseFileName := v.DataFileName()

	// 验证 Collection 是否匹配
	// Collection 必须一致，否则可能导致数据混乱
	if v.Collection != req.Collection {
		return nil, fmt.Errorf("existing collection:%v unexpected input: %v", v.Collection, req.Collection)
	}

	// 【步骤 2：创建 EC 上下文】
	// EC Context 包含编码配置（数据分片数、校验分片数）
	//
	// 策略：优先使用 .vif 中的现有配置（用于重新生成场景）
	// 场景示例：
	//   - 初次生成：使用默认配置 10+4
	//   - 重新生成：使用 .vif 中保存的配置（可能是自定义配置）
	ecCtx := erasure_coding.NewDefaultECContext(req.Collection, needle.VolumeId(req.VolumeId))

	// 尝试从 .vif 文件加载现有 EC 配置
	if volumeInfo, _, found, _ := volume_info.MaybeLoadVolumeInfo(baseFileName + ".vif"); found && volumeInfo.EcShardConfig != nil {
		ds := int(volumeInfo.EcShardConfig.DataShards)  // 数据分片数
		ps := int(volumeInfo.EcShardConfig.ParityShards)  // 校验分片数

		// 验证并使用现有 EC 配置
		// 约束：
		//   - 数据分片数 > 0
		//   - 校验分片数 > 0
		//   - 总分片数 <= MaxShardCount（32）
		if ds > 0 && ps > 0 && ds+ps <= erasure_coding.MaxShardCount {
			ecCtx.DataShards = ds
			ecCtx.ParityShards = ps
			glog.V(0).Infof("Using existing EC config for volume %d: %s", req.VolumeId, ecCtx.String())
		} else {
			// 配置无效，回退到默认配置
			glog.Warningf("Invalid EC config in .vif for volume %d (data=%d, parity=%d), using defaults", req.VolumeId, ds, ps)
		}
	} else {
		// 没有 .vif 文件或没有 EC 配置，使用默认配置
		glog.V(0).Infof("Using default EC config for volume %d: %s", req.VolumeId, ecCtx.String())
	}

	// 错误清理标志
	// 如果生成失败，自动删除已生成的部分文件
	shouldCleanup := true
	defer func() {
		if !shouldCleanup {
			return
		}

		// 删除所有 EC 分片文件
		for i := 0; i < ecCtx.Total(); i++ {
			os.Remove(baseFileName + ecCtx.ToExt(i))
		}

		// 删除 .ecx 索引文件
		os.Remove(v.IndexFileName() + ".ecx")
	}()

	// 【步骤 3：生成 EC 分片】
	// 使用 Reed-Solomon 编码生成 .ec00 ~ .ec[Total-1] 文件
	//
	// WriteEcFilesWithContext 流程：
	//   1. 读取 .dat 文件
	//   2. 按照 DataShards 切分数据
	//   3. 使用 Reed-Solomon 编码生成 ParityShards 个校验块
	//   4. 写入 .ec00~.ec[Total-1] 文件
	//
	// 例如默认配置 10+4：
	//   - .ec00 ~ .ec09: 数据分片（.dat 文件切分）
	//   - .ec10 ~ .ec13: 校验分片（编码生成）
	if err := erasure_coding.WriteEcFilesWithContext(baseFileName, ecCtx); err != nil {
		return nil, fmt.Errorf("WriteEcFilesWithContext %s: %v", baseFileName, err)
	}

	// 【步骤 4：生成 .ecx 索引】
	// 从 .idx 文件转换为 .ecx 格式
	//
	// .idx vs .ecx：
	//   - .idx: 普通 Volume 索引（HashMap 格式）
	//   - .ecx: EC Volume 索引（排序数组格式）
	//
	// .ecx 优势：
	//   - 排序后可以二分查找
	//   - 适合只读场景
	//   - 内存占用更小
	if err := erasure_coding.WriteSortedFileFromIdx(v.IndexFileName(), ".ecx"); err != nil {
		return nil, fmt.Errorf("WriteSortedFileFromIdx %s: %v", v.IndexFileName(), err)
	}

	// 【步骤 5：生成 .vif 文件】
	// Volume Info 文件包含 Volume 元数据
	//
	// 步骤 5.1：计算过期时间
	var expireAtSec uint64
	if v.Ttl != nil {
		ttlSecond := v.Ttl.ToSeconds()
		if ttlSecond > 0 {
			// 计算过期时间：当前时间 + TTL
			expireAtSec = uint64(time.Now().Unix()) + ttlSecond
		}
	}

	// 步骤 5.2：创建 VolumeInfo 对象
	volumeInfo := &volume_server_pb.VolumeInfo{Version: uint32(v.Version())}
	volumeInfo.ExpireAtSec = expireAtSec

	// 获取 .dat 文件大小
	datSize, _, _ := v.FileStat()
	volumeInfo.DatFileSize = int64(datSize)

	// 步骤 5.3：验证 EC 配置
	// 保存前再次验证，防止配置错误
	if ecCtx.DataShards <= 0 || ecCtx.ParityShards <= 0 || ecCtx.Total() > erasure_coding.MaxShardCount {
		return nil, fmt.Errorf("invalid EC config before saving: data=%d, parity=%d, total=%d (max=%d)",
			ecCtx.DataShards, ecCtx.ParityShards, ecCtx.Total(), erasure_coding.MaxShardCount)
	}

	// 步骤 5.4：保存 EC 配置到 VolumeInfo
	// 这个配置会用于后续的 EC 操作（读取、重建等）
	volumeInfo.EcShardConfig = &volume_server_pb.EcShardConfig{
		DataShards:   uint32(ecCtx.DataShards),
		ParityShards: uint32(ecCtx.ParityShards),
	}
	glog.V(1).Infof("Saving EC config to .vif for volume %d: %d+%d (total: %d)",
		req.VolumeId, ecCtx.DataShards, ecCtx.ParityShards, ecCtx.Total())

	// 步骤 5.5：写入 .vif 文件
	if err := volume_info.SaveVolumeInfo(baseFileName+".vif", volumeInfo); err != nil {
		return nil, fmt.Errorf("SaveVolumeInfo %s: %v", baseFileName, err)
	}

	// 成功：不清理文件
	shouldCleanup = false

	return &volume_server_pb.VolumeEcShardsGenerateResponse{}, nil
}

// VolumeEcShardsRebuild 重建丢失的 EC 分片
// gRPC API: VolumeEcShardsRebuild
//
// 功能:
//   - 从现有分片重建丢失的分片
//   - 利用 Reed-Solomon 编码的特性
//   - 只要有足够的分片（>= DataShards），就能重建所有分片
//
// 参数:
//   - VolumeId: 要重建的卷 ID
//   - Collection: 集合名称
//
// 重建原理:
//   Reed-Solomon 编码特性：
//   - 总共 N+M 个分片（N 数据，M 校验）
//   - 只需任意 N 个分片即可恢复所有数据
//   - 可以重建任意丢失的分片（数据或校验）
//
// 重建流程:
//   【步骤 1：查找现有分片】
//   - 遍历所有存储位置
//   - 检查每个位置的 EC 分片数量
//
//   【步骤 2：检查是否可以重建】
//   - 需要有 .ecx 索引文件
//   - 现有分片数 >= DataShards
//
//   【步骤 3：重建数据分片】
//   - 调用 RebuildEcFiles() 重建丢失的 .ec00~.ec[Total-1] 文件
//   - 使用 Reed-Solomon 解码算法
//
//   【步骤 4：重建索引文件】
//   - 如果 .ecx 文件损坏或丢失，从现有数据重建
//
// 使用场景:
//   - 节点故障后恢复数据
//   - 磁盘损坏后重建分片
//   - 数据迁移后补全分片
//   - 定期数据完整性检查
//
// 返回:
//   - RebuiltShardIds: 成功重建的分片 ID 列表
//   - error: 重建失败错误
//
// 注意:
//   - 需要至少 DataShards 个完好的分片
//   - 重建过程消耗 CPU 和 I/O 资源
//   - 大文件重建可能需要较长时间
func (vs *VolumeServer) VolumeEcShardsRebuild(ctx context.Context, req *volume_server_pb.VolumeEcShardsRebuildRequest) (*volume_server_pb.VolumeEcShardsRebuildResponse, error) {

	glog.V(0).Infof("VolumeEcShardsRebuild: %v", req)

	// 构建 EC 分片基础文件名
	// 格式: collection_volumeId
	baseFileName := erasure_coding.EcShardBaseFileName(req.Collection, int(req.VolumeId))

	// 记录重建的分片 ID
	var rebuiltShardIds []uint32

	// 【步骤 1：查找现有分片】
	// 遍历所有存储位置
	for _, location := range vs.store.Locations {
		// 检查 EC Volume 状态
		// hasEcxFile: 是否有 .ecx 索引文件
		// hasIdxFile: 是否有 .idx 文件
		// existingShardCount: 现有分片数量
		_, _, existingShardCount, err := checkEcVolumeStatus(baseFileName, location)
		if err != nil {
			return nil, err
		}

		// 没有分片，跳过
		if existingShardCount == 0 {
			continue
		}

		// 【步骤 2：检查是否可以重建】
		// 需要有 .ecx 索引文件才能重建
		if util.FileExists(path.Join(location.IdxDirectory, baseFileName+".ecx")) {
			// 【步骤 3：重建数据分片】
			// 从现有分片重建丢失的 .ec00 ~ .ec[Total-1] 文件
			//
			// RebuildEcFiles 原理：
			//   1. 读取所有现有分片
			//   2. 使用 Reed-Solomon 解码恢复原始数据
			//   3. 重新编码生成所有分片（包括丢失的）
			//   4. 写入新生成的分片文件
			dataBaseFileName := path.Join(location.Directory, baseFileName)
			if generatedShardIds, err := erasure_coding.RebuildEcFiles(dataBaseFileName); err != nil {
				return nil, fmt.Errorf("RebuildEcFiles %s: %v", dataBaseFileName, err)
			} else {
				rebuiltShardIds = generatedShardIds
			}

			// 【步骤 4：重建索引文件】
			// 如果 .ecx 损坏，从现有数据重建
			indexBaseFileName := path.Join(location.IdxDirectory, baseFileName)
			if err := erasure_coding.RebuildEcxFile(indexBaseFileName); err != nil {
				return nil, fmt.Errorf("RebuildEcxFile %s: %v", dataBaseFileName, err)
			}

			// 找到并处理完一个位置后退出
			break
		}
	}

	return &volume_server_pb.VolumeEcShardsRebuildResponse{
		RebuiltShardIds: rebuiltShardIds,
	}, nil
}

// VolumeEcShardsCopy 复制 EC 分片到本地
// gRPC API: VolumeEcShardsCopy
//
// 功能:
//   - 从源服务器复制指定的 EC 分片
//   - 支持复制 .ecx（索引）、.ecj（journal）、.vif（信息）文件
//   - 支持指定目标磁盘
//
// 参数:
//   - VolumeId: 卷 ID
//   - Collection: 集合名称
//   - ShardIds: 要复制的分片 ID 列表
//   - SourceDataNode: 源服务器地址
//   - CopyEcxFile: 是否复制 .ecx 索引文件
//   - CopyEcjFile: 是否复制 .ecj journal 文件
//   - CopyVifFile: 是否复制 .vif 信息文件
//   - DiskId: 目标磁盘 ID（可选）
//
// 复制流程:
//   【步骤 1：选择目标位置】
//   - 优先使用 DiskId 指定的磁盘
//   - 如果没有指定，根据文件类型选择：
//     - .ecx 索引：优先 HDD（节省 SSD 空间）
//     - 数据分片：任意可用位置
//
//   【步骤 2：复制数据分片】
//   - 遍历 ShardIds 列表
//   - 调用 doCopyFile 复制每个 .ecXX 文件
//   - 使用源服务器的 CopyFile 接口
//
//   【步骤 3：复制索引文件（可选）】
//   - 如果 CopyEcxFile=true，复制 .ecx 文件
//   - .ecx 用于查找 Needle 位置
//
//   【步骤 4：复制 Journal 文件（可选）】
//   - 如果 CopyEcjFile=true，复制 .ecj 文件
//   - .ecj 记录删除操作
//   - 使用追加模式（isAppend=true）
//
//   【步骤 5：复制信息文件（可选）】
//   - 如果 CopyVifFile=true，复制 .vif 文件
//   - .vif 包含 Volume 元数据和 EC 配置
//
// 使用场景:
//   - EC 编码后分发分片到多个服务器
//   - 副本迁移和均衡
//   - 数据恢复和重建
//   - 跨数据中心复制
//
// 返回:
//   - VolumeEcShardsCopyResponse: 空响应表示成功
//   - error: 复制失败错误
//
// 注意:
//   - 支持磁盘感知存储（通过 DiskId）
//   - .ecx 索引优先存储在 HDD
//   - .ecj 使用追加模式，不覆盖现有内容
func (vs *VolumeServer) VolumeEcShardsCopy(ctx context.Context, req *volume_server_pb.VolumeEcShardsCopyRequest) (*volume_server_pb.VolumeEcShardsCopyResponse, error) {

	glog.V(0).Infof("VolumeEcShardsCopy: %v", req)

	var location *storage.DiskLocation

	// 【步骤 1：选择目标位置】
	// 策略 1：使用指定的磁盘 ID（磁盘感知存储）
	if req.DiskId > 0 || (req.DiskId == 0 && len(vs.store.Locations) > 0) {
		// 验证磁盘 ID 是否有效
		if int(req.DiskId) >= len(vs.store.Locations) {
			return nil, fmt.Errorf("invalid disk_id %d: only have %d disks", req.DiskId, len(vs.store.Locations))
		}

		// 使用指定的磁盘位置
		location = vs.store.Locations[req.DiskId]
		glog.V(1).Infof("Using disk %d for EC shard copy: %s", req.DiskId, location.Directory)
	} else {
		// 策略 2：自动选择磁盘（向后兼容）
		if req.CopyEcxFile {
			// 复制 .ecx 索引文件：优先使用 HDD
			// 原因：索引文件通常访问频率较低，HDD 更经济
			location = vs.store.FindFreeLocation(func(location *storage.DiskLocation) bool {
				return location.DiskType == types.HardDriveType
			})
		} else {
			// 复制数据分片：任意可用位置
			location = vs.store.FindFreeLocation(func(location *storage.DiskLocation) bool {
				return true
			})
		}

		// 没有可用空间
		if location == nil {
			return nil, fmt.Errorf("no space left")
		}
	}

	// 构建目标文件路径
	dataBaseFileName := storage.VolumeFileName(location.Directory, req.Collection, int(req.VolumeId))
	indexBaseFileName := storage.VolumeFileName(location.IdxDirectory, req.Collection, int(req.VolumeId))

	// 连接到源服务器执行复制
	err := operation.WithVolumeServerClient(true, pb.ServerAddress(req.SourceDataNode), vs.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {

		// 【步骤 2：复制数据分片】
		// 遍历所有要复制的分片 ID
		for _, shardId := range req.ShardIds {
			// 复制单个分片文件（.ec00, .ec01, ... .ec13）
			//
			// doCopyFile 参数说明：
			//   - client: gRPC 客户端
			//   - isEcVolume: true（EC Volume）
			//   - collection: 集合名称
			//   - volumeId: 卷 ID
			//   - compactRevision: MaxUint32（不检查压缩版本）
			//   - stopOffset: MaxInt64（复制整个文件）
			//   - baseFileName: 目标文件基础路径
			//   - ext: ToExt(shardId) 生成扩展名（如 ".ec00"）
			//   - isAppend: false（覆盖写入）
			//   - ignoreSourceFileNotFound: false（文件不存在时报错）
			//   - progressFn: nil（无进度回调）
			if _, err := vs.doCopyFile(client, true, req.Collection, req.VolumeId, math.MaxUint32, math.MaxInt64, dataBaseFileName, erasure_coding.ToExt(int(shardId)), false, false, nil); err != nil {
				return err
			}
		}

		// 【步骤 3：复制索引文件（可选）】
		if req.CopyEcxFile {
			// 复制 .ecx 索引文件
			// .ecx 用于快速查找 Needle 位置
			if _, err := vs.doCopyFile(client, true, req.Collection, req.VolumeId, math.MaxUint32, math.MaxInt64, indexBaseFileName, ".ecx", false, false, nil); err != nil {
				return err
			}
		}

		// 【步骤 4：复制 Journal 文件（可选）】
		if req.CopyEcjFile {
			// 复制 .ecj journal 文件
			// .ecj 记录删除操作的日志
			//
			// 注意：isAppend=true 使用追加模式
			// 原因：.ecj 是增量的，不应覆盖现有内容
			//
			// ignoreSourceFileNotFound=true：
			// 如果源文件不存在（没有删除操作），忽略错误
			if _, err := vs.doCopyFile(client, true, req.Collection, req.VolumeId, math.MaxUint32, math.MaxInt64, indexBaseFileName, ".ecj", true, true, nil); err != nil {
				return err
			}
		}

		// 【步骤 5：复制信息文件（可选）】
		if req.CopyVifFile {
			// 复制 .vif 信息文件
			// .vif 包含 Volume 元数据：
			//   - EC 配置（DataShards + ParityShards）
			//   - 过期时间（ExpireAtSec）
			//   - 文件大小（DatFileSize）
			//   - Volume 版本（Version）
			//
			// ignoreSourceFileNotFound=true：
			// 旧 Volume 可能没有 .vif 文件
			if _, err := vs.doCopyFile(client, true, req.Collection, req.VolumeId, math.MaxUint32, math.MaxInt64, dataBaseFileName, ".vif", false, true, nil); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("VolumeEcShardsCopy volume %d: %v", req.VolumeId, err)
	}

	return &volume_server_pb.VolumeEcShardsCopyResponse{}, nil
}

// VolumeEcShardsDelete 删除本地 EC 分片
// gRPC API: VolumeEcShardsDelete
//
// 功能:
//   - 删除指定的 EC 分片文件
//   - 如果所有分片都被删除，同时删除 .ecx 和 .ecj 文件
//   - 自动清理相关的元数据文件
//
// 参数:
//   - VolumeId: 卷 ID
//   - Collection: 集合名称
//   - ShardIds: 要删除的分片 ID 列表
//
// 删除流程:
//   1. 遍历所有存储位置
//   2. 删除指定的分片文件
//   3. 检查剩余分片数量
//   4. 如果没有剩余分片，清理 .ecx、.ecj、.vif 文件
//
// 使用场景:
//   - EC Volume 转换回普通 Volume 后清理
//   - 分片迁移后删除旧分片
//   - 节省磁盘空间
//
// 返回:
//   - VolumeEcShardsDeleteResponse: 空响应表示成功
//   - error: 删除失败错误
//
// 注意:
//   - 分片必须先卸载（unmount）才能删除
//   - 删除操作不可逆
//   - 如果分片数量 < DataShards，数据将无法恢复
func (vs *VolumeServer) VolumeEcShardsDelete(ctx context.Context, req *volume_server_pb.VolumeEcShardsDeleteRequest) (*volume_server_pb.VolumeEcShardsDeleteResponse, error) {

	// 构建分片基础文件名
	bName := erasure_coding.EcShardBaseFileName(req.Collection, int(req.VolumeId))

	glog.V(0).Infof("ec volume %s shard delete %v", bName, req.ShardIds)

	// 遍历所有存储位置，删除分片
	for _, location := range vs.store.Locations {
		if err := deleteEcShardIdsForEachLocation(bName, location, req.ShardIds); err != nil {
			glog.Errorf("deleteEcShards from %s %s.%v: %v", location.Directory, bName, req.ShardIds, err)
			return nil, err
		}
	}

	return &volume_server_pb.VolumeEcShardsDeleteResponse{}, nil
}

// deleteEcShardIdsForEachLocation 在单个存储位置删除 EC 分片
// 辅助函数，用于 VolumeEcShardsDelete
//
// 功能:
//   - 删除指定的分片文件
//   - 如果所有分片都被删除，清理元数据文件
//
// 参数:
//   - bName: 分片基础文件名（如 "collection_123"）
//   - location: 存储位置
//   - shardIds: 要删除的分片 ID 列表
//
// 清理策略:
//   - 删除指定的分片文件
//   - 如果剩余分片数为 0：
//     - 删除 .ecx 索引文件
//     - 删除 .ecj journal 文件
//     - 如果没有 .idx 文件，删除 .vif 文件
//
// 返回:
//   - error: 删除失败错误
func deleteEcShardIdsForEachLocation(bName string, location *storage.DiskLocation, shardIds []uint32) error {

	found := false

	// 构建文件路径
	indexBaseFilename := path.Join(location.IdxDirectory, bName)
	dataBaseFilename := path.Join(location.Directory, bName)

	// 检查是否有 .ecx 文件
	if util.FileExists(path.Join(location.IdxDirectory, bName+".ecx")) {
		// 删除指定的分片文件
		for _, shardId := range shardIds {
			shardFileName := dataBaseFilename + erasure_coding.ToExt(int(shardId))
			if util.FileExists(shardFileName) {
				found = true
				os.Remove(shardFileName)
			}
		}
	}

	// 如果没有删除任何文件，直接返回
	if !found {
		return nil
	}

	// 检查剩余的 EC Volume 状态
	hasEcxFile, hasIdxFile, existingShardCount, err := checkEcVolumeStatus(bName, location)
	if err != nil {
		return err
	}

	// 如果所有分片都被删除，清理元数据文件
	if hasEcxFile && existingShardCount == 0 {
		// 删除 .ecx 索引文件
		if err := os.Remove(indexBaseFilename + ".ecx"); err != nil {
			return err
		}

		// 删除 .ecj journal 文件（忽略错误）
		os.Remove(indexBaseFilename + ".ecj")

		// 如果没有普通 Volume 的 .idx 文件，删除 .vif
		// .vif 文件可以被 EC Volume 和普通 Volume 共享
		if !hasIdxFile {
			os.Remove(dataBaseFilename + ".vif")
		}
	}

	return nil
}

// checkEcVolumeStatus 检查 EC Volume 状态
// 辅助函数，用于判断是否需要清理元数据文件
//
// 功能:
//   - 统计现有的 EC 分片数量
//   - 检查是否存在 .ecx 和 .idx 文件
//
// 参数:
//   - bName: 分片基础文件名
//   - location: 存储位置
//
// 返回:
//   - hasEcxFile: 是否有 .ecx 或 .ecj 文件
//   - hasIdxFile: 是否有 .idx 文件
//   - existingShardCount: 现有分片数量
//   - err: 错误信息
func checkEcVolumeStatus(bName string, location *storage.DiskLocation) (hasEcxFile bool, hasIdxFile bool, existingShardCount int, err error) {
	// 读取数据目录的文件列表
	fileInfos, err := os.ReadDir(location.Directory)
	if err != nil {
		return false, false, 0, err
	}

	// 如果索引目录和数据目录分离，读取索引目录
	if location.IdxDirectory != location.Directory {
		idxFileInfos, err := os.ReadDir(location.IdxDirectory)
		if err != nil {
			return false, false, 0, err
		}
		fileInfos = append(fileInfos, idxFileInfos...)
	}

	// 遍历文件，统计状态
	for _, fileInfo := range fileInfos {
		// 检查 .ecx 或 .ecj 文件
		if fileInfo.Name() == bName+".ecx" || fileInfo.Name() == bName+".ecj" {
			hasEcxFile = true
			continue
		}

		// 检查 .idx 文件
		if fileInfo.Name() == bName+".idx" {
			hasIdxFile = true
			continue
		}

		// 统计 EC 分片文件（.ec00, .ec01, ...）
		if strings.HasPrefix(fileInfo.Name(), bName+".ec") {
			existingShardCount++
		}
	}

	return hasEcxFile, hasIdxFile, existingShardCount, nil
}

// VolumeEcShardsMount 挂载 EC 分片
// gRPC API: VolumeEcShardsMount
//
// 功能:
//   - 将 EC 分片挂载到内存
//   - 使分片可以被读取
//
// 参数:
//   - VolumeId: 卷 ID
//   - Collection: 集合名称
//   - ShardIds: 要挂载的分片 ID 列表
//
// 挂载过程:
//   - 加载分片文件到内存
//   - 注册到 EC Volume 中
//   - 使分片可以参与读取和恢复操作
//
// 返回:
//   - VolumeEcShardsMountResponse: 空响应表示成功
//   - error: 挂载失败错误
func (vs *VolumeServer) VolumeEcShardsMount(ctx context.Context, req *volume_server_pb.VolumeEcShardsMountRequest) (*volume_server_pb.VolumeEcShardsMountResponse, error) {

	glog.V(0).Infof("VolumeEcShardsMount: %v", req)

	// 遍历并挂载每个分片
	for _, shardId := range req.ShardIds {
		err := vs.store.MountEcShards(req.Collection, needle.VolumeId(req.VolumeId), erasure_coding.ShardId(shardId))

		if err != nil {
			glog.Errorf("ec shard mount %v: %v", req, err)
		} else {
			glog.V(2).Infof("ec shard mount %v", req)
		}

		if err != nil {
			return nil, fmt.Errorf("mount %d.%d: %v", req.VolumeId, shardId, err)
		}
	}

	return &volume_server_pb.VolumeEcShardsMountResponse{}, nil
}

// VolumeEcShardsUnmount 卸载 EC 分片
// gRPC API: VolumeEcShardsUnmount
//
// 功能:
//   - 从内存中卸载 EC 分片
//   - 释放相关资源
//
// 参数:
//   - VolumeId: 卷 ID
//   - ShardIds: 要卸载的分片 ID 列表
//
// 卸载过程:
//   - 关闭分片文件句柄
//   - 从 EC Volume 中注销
//   - 释放内存资源
//
// 返回:
//   - VolumeEcShardsUnmountResponse: 空响应表示成功
//   - error: 卸载失败错误
func (vs *VolumeServer) VolumeEcShardsUnmount(ctx context.Context, req *volume_server_pb.VolumeEcShardsUnmountRequest) (*volume_server_pb.VolumeEcShardsUnmountResponse, error) {

	glog.V(0).Infof("VolumeEcShardsUnmount: %v", req)

	// 遍历并卸载每个分片
	for _, shardId := range req.ShardIds {
		err := vs.store.UnmountEcShards(needle.VolumeId(req.VolumeId), erasure_coding.ShardId(shardId))

		if err != nil {
			glog.Errorf("ec shard unmount %v: %v", req, err)
		} else {
			glog.V(2).Infof("ec shard unmount %v", req)
		}

		if err != nil {
			return nil, fmt.Errorf("unmount %d.%d: %v", req.VolumeId, shardId, err)
		}
	}

	return &volume_server_pb.VolumeEcShardsUnmountResponse{}, nil
}

// VolumeEcShardRead 读取 EC 分片数据
// gRPC Streaming API: VolumeEcShardRead (服务端流式)
//
// 功能:
//   - 从指定 EC 分片读取数据
//   - 支持范围读取（offset + size）
//   - 检查文件是否已删除
//
// 参数:
//   - VolumeId: 卷 ID
//   - ShardId: 分片 ID
//   - Offset: 读取起始偏移量
//   - Size: 读取字节数
//   - FileKey: 文件 Key（可选，用于删除检查）
//
// 读取流程:
//   【步骤 1：查找 EC Volume 和分片】
//   - 查找 EC Volume
//   - 查找指定的分片
//
//   【步骤 2：检查文件删除状态（可选）】
//   - 如果提供了 FileKey，检查是否已删除
//   - 已删除则返回 IsDeleted=true
//
//   【步骤 3：流式读取和发送】
//   - 使用 2MB 缓冲区分块读取
//   - 通过 gRPC 流发送到客户端
//   - 循环直到读取完成
//
// 使用场景:
//   - 客户端从多个分片读取数据
//   - 配合 Reed-Solomon 解码恢复原始数据
//   - 跨服务器分片读取
//
// 返回:
//   - 流式返回 VolumeEcShardReadResponse
//   - Data: 分片数据块（最大 2MB）
//   - IsDeleted: 文件是否已删除
//
// 注意:
//   - 客户端需要从多个分片读取并解码
//   - 读取性能比普通 Volume 慢（需要解码）
func (vs *VolumeServer) VolumeEcShardRead(req *volume_server_pb.VolumeEcShardReadRequest, stream volume_server_pb.VolumeServer_VolumeEcShardReadServer) error {

	// 【步骤 1：查找 EC Volume 和分片】
	ecVolume, found := vs.store.FindEcVolume(needle.VolumeId(req.VolumeId))
	if !found {
		return fmt.Errorf("VolumeEcShardRead not found ec volume id %d", req.VolumeId)
	}

	// 查找指定的分片
	ecShard, found := ecVolume.FindEcVolumeShard(erasure_coding.ShardId(req.ShardId))
	if !found {
		return fmt.Errorf("not found ec shard %d.%d", req.VolumeId, req.ShardId)
	}

	// 【步骤 2：检查文件删除状态（可选）】
	// 如果提供了 FileKey，检查文件是否已删除
	if req.FileKey != 0 {
		_, size, _ := ecVolume.FindNeedleFromEcx(types.Uint64ToNeedleId(req.FileKey))
		if size.IsDeleted() {
			// 文件已删除，返回标记
			return stream.Send(&volume_server_pb.VolumeEcShardReadResponse{
				IsDeleted: true,
			})
		}
	}

	// 【步骤 3：流式读取和发送】
	// 确定缓冲区大小
	bufSize := req.Size
	if bufSize > BufferSizeLimit {
		bufSize = BufferSizeLimit  // 最大 2MB
	}
	buffer := make([]byte, bufSize)

	// 初始化读取参数
	startOffset, bytesToRead := req.Offset, req.Size

	// 循环读取并发送
	for bytesToRead > 0 {
		// 计算本次读取大小
		bufferSize := bufSize
		if bufferSize > bytesToRead {
			bufferSize = bytesToRead
		}

		// 从分片读取数据
		bytesread, err := ecShard.ReadAt(buffer[0:bufferSize], startOffset)

		// 处理读取结果
		if bytesread > 0 {
			// 截断到实际需要的大小
			if int64(bytesread) > bytesToRead {
				bytesread = int(bytesToRead)
			}

			// 发送数据到客户端
			err = stream.Send(&volume_server_pb.VolumeEcShardReadResponse{
				Data: buffer[:bytesread],
			})
			if err != nil {
				return err
			}

			// 更新偏移量和剩余字节数
			startOffset += int64(bytesread)
			bytesToRead -= int64(bytesread)
		}

		// 检查读取错误
		if err != nil {
			if err != io.EOF {
				return err
			}
			// 到达文件末尾，正常结束
			return nil
		}
	}

	return nil

}

// VolumeEcBlobDelete 删除 EC Volume 中的文件
// gRPC API: VolumeEcBlobDelete
//
// 功能:
//   - 在 .ecx 索引中标记文件为已删除
//   - 不删除实际数据（软删除）
//   - 记录删除操作到 .ecj journal
//
// 参数:
//   - VolumeId: 卷 ID
//   - FileKey: 文件 Key（Needle ID）
//   - Version: Needle 版本
//
// 删除流程:
//   1. 查找 EC Volume
//   2. 定位 Needle 位置
//   3. 检查是否已删除
//   4. 在 .ecx 中标记为删除
//
// 删除机制:
//   - 软删除：只标记，不删除物理数据
//   - 在 .ecx 中设置删除标志
//   - 空间通过 Vacuum 或重建回收
//
// 返回:
//   - VolumeEcBlobDeleteResponse: 空响应表示成功
//   - error: 删除失败错误
//
// 注意:
//   - 重复删除返回成功（幂等）
//   - 不立即释放空间
func (vs *VolumeServer) VolumeEcBlobDelete(ctx context.Context, req *volume_server_pb.VolumeEcBlobDeleteRequest) (*volume_server_pb.VolumeEcBlobDeleteResponse, error) {

	glog.V(0).Infof("VolumeEcBlobDelete: %v", req)

	resp := &volume_server_pb.VolumeEcBlobDeleteResponse{}

	// 遍历存储位置，查找 EC Volume
	for _, location := range vs.store.Locations {
		if localEcVolume, found := location.FindEcVolume(needle.VolumeId(req.VolumeId)); found {

			// 定位 Needle 在 EC 分片中的位置
			_, size, _, err := localEcVolume.LocateEcShardNeedle(types.NeedleId(req.FileKey), needle.Version(req.Version))
			if err != nil {
				return nil, fmt.Errorf("locate in local ec volume: %w", err)
			}

			// 检查是否已删除
			if size.IsDeleted() {
				// 已删除，直接返回（幂等）
				return resp, nil
			}

			// 在 .ecx 中标记为删除
			// 实际数据仍然存在，只是索引标记
			err = localEcVolume.DeleteNeedleFromEcx(types.NeedleId(req.FileKey))
			if err != nil {
				return nil, err
			}

			break
		}
	}

	return resp, nil
}

// VolumeEcShardsToVolume 从 EC 分片恢复为普通 Volume
// gRPC API: VolumeEcShardsToVolume
//
// 功能:
//   - 从 EC 分片重建 .dat 和 .idx 文件
//   - 将 EC Volume 转换回普通 Volume
//   - 使用 Reed-Solomon 解码恢复原始数据
//
// 参数:
//   - VolumeId: 卷 ID
//   - Collection: 集合名称
//
// 恢复流程:
//   【步骤 1：收集 EC 分片】
//   - 从 .vif 文件加载 EC 配置
//   - 收集所有数据分片（.ec00 ~ .ec[DataShards-1]）
//   - 验证所有数据分片都存在
//
//   【步骤 2：计算 .dat 文件大小】
//   - 从 .ecx 索引计算原始文件大小
//   - 去除 EC 编码的填充
//
//   【步骤 3：重建 .dat 文件】
//   - 从数据分片恢复原始数据
//   - 写入 .dat 文件
//
//   【步骤 4：重建 .idx 索引】
//   - 从 .ecx 和 .ecj 转换为 .idx 格式
//   - 写入 .idx 文件
//
// 使用场景:
//   - EC Volume 转换回普通 Volume（提高性能）
//   - 温数据变为热数据
//   - 需要写入新数据时
//
// 返回:
//   - VolumeEcShardsToVolumeResponse: 空响应表示成功
//   - error: 恢复失败错误
//
// 注意:
//   - 需要所有数据分片（至少 DataShards 个）
//   - 恢复过程消耗 CPU 和 I/O 资源
//   - 恢复后可以删除 EC 分片节省空间
func (vs *VolumeServer) VolumeEcShardsToVolume(ctx context.Context, req *volume_server_pb.VolumeEcShardsToVolumeRequest) (*volume_server_pb.VolumeEcShardsToVolumeResponse, error) {

	glog.V(0).Infof("VolumeEcShardsToVolume: %v", req)

	// 【步骤 1：收集 EC 分片】
	// NewEcVolume 会从 .vif 文件加载 EC 配置到 v.ECContext
	// 使用 MaxShardCount（32）支持自定义 EC 比率（最多 32 个分片）
	tempShards := make([]string, erasure_coding.MaxShardCount)
	v, found := vs.store.CollectEcShards(needle.VolumeId(req.VolumeId), tempShards)
	if !found {
		return nil, fmt.Errorf("ec volume %d not found", req.VolumeId)
	}

	// 验证 Collection 是否匹配
	if v.Collection != req.Collection {
		return nil, fmt.Errorf("existing collection:%v unexpected input: %v", v.Collection, req.Collection)
	}

	// 使用 EC 上下文（已从 .vif 加载）确定数据分片数量
	dataShards := v.ECContext.DataShards

	// 防御性验证：防止损坏的 ECContext 导致 panic
	if dataShards <= 0 || dataShards > erasure_coding.MaxShardCount {
		return nil, fmt.Errorf("invalid data shard count %d for volume %d (must be 1..%d)", dataShards, req.VolumeId, erasure_coding.MaxShardCount)
	}

	// 只需要数据分片（不需要校验分片）
	shardFileNames := tempShards[:dataShards]
	glog.V(1).Infof("Using EC config from volume %d: %d data shards", req.VolumeId, dataShards)

	// 验证所有数据分片都存在
	for shardId := 0; shardId < dataShards; shardId++ {
		if shardFileNames[shardId] == "" {
			return nil, fmt.Errorf("ec volume %d missing shard %d", req.VolumeId, shardId)
		}
	}

	// 获取文件路径
	dataBaseFileName, indexBaseFileName := v.DataBaseFileName(), v.IndexBaseFileName()

	// 【步骤 2：计算 .dat 文件大小】
	// 从 .ecx 索引计算原始文件大小
	datFileSize, err := erasure_coding.FindDatFileSize(dataBaseFileName, indexBaseFileName)
	if err != nil {
		return nil, fmt.Errorf("FindDatFileSize %s: %v", dataBaseFileName, err)
	}

	// 【步骤 3：重建 .dat 文件】
	// 从数据分片（.ec00 ~ .ec[DataShards-1]）恢复原始数据
	// WriteDatFile 会：
	//   1. 读取所有数据分片
	//   2. 拼接成原始数据
	//   3. 去除 EC 编码填充
	//   4. 写入 .dat 文件
	if err := erasure_coding.WriteDatFile(dataBaseFileName, datFileSize, shardFileNames); err != nil {
		return nil, fmt.Errorf("WriteDatFile %s: %v", dataBaseFileName, err)
	}

	// 【步骤 4：重建 .idx 索引】
	// 从 .ecx 和 .ecj 文件转换为 .idx 格式
	// .idx 是 HashMap 格式，用于可写 Volume
	if err := erasure_coding.WriteIdxFileFromEcIndex(indexBaseFileName); err != nil {
		return nil, fmt.Errorf("WriteIdxFileFromEcIndex %s: %v", v.IndexBaseFileName(), err)
	}

	return &volume_server_pb.VolumeEcShardsToVolumeResponse{}, nil
}

// VolumeEcShardsInfo 获取 EC 分片信息
// gRPC API: VolumeEcShardsInfo
//
// 功能:
//   - 返回 Volume 的所有 EC 分片信息
//   - 包括分片 ID、大小、集合名称
//
// 参数:
//   - VolumeId: 卷 ID
//
// 返回信息:
//   - ShardId: 分片 ID
//   - Size: 分片文件大小（字节）
//   - Collection: 集合名称
//
// 使用场景:
//   - 监控 EC Volume 状态
//   - 检查分片完整性
//   - 决定是否需要重建
//
// 返回:
//   - VolumeEcShardsInfoResponse: 分片信息列表
//   - error: 查询失败错误
func (vs *VolumeServer) VolumeEcShardsInfo(ctx context.Context, req *volume_server_pb.VolumeEcShardsInfoRequest) (*volume_server_pb.VolumeEcShardsInfoResponse, error) {
	glog.V(0).Infof("VolumeEcShardsInfo: volume %d", req.VolumeId)

	var ecShardInfos []*volume_server_pb.EcShardInfo

	// 查找 EC Volume
	for _, location := range vs.store.Locations {
		if v, found := location.FindEcVolume(needle.VolumeId(req.VolumeId)); found {
			// 获取分片详细信息
			shardDetails := v.ShardDetails()
			for _, shardDetail := range shardDetails {
				ecShardInfo := &volume_server_pb.EcShardInfo{
					ShardId:    uint32(shardDetail.ShardId),
					Size:       int64(shardDetail.Size),
					Collection: v.Collection,
				}
				ecShardInfos = append(ecShardInfos, ecShardInfo)
			}
			break
		}
	}

	return &volume_server_pb.VolumeEcShardsInfoResponse{
		EcShardInfos: ecShardInfos,
	}, nil
}
