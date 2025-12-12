package erasure_coding

import (
	"fmt"
	"io"
	"os"

	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// WriteIdxFileFromEcIndex 从 EC 索引文件（.ecx 和 .ecj）生成普通索引文件（.idx）
// 用于将 EC Volume 转换回普通 Volume 时重建索引
//
// 参数:
//   - baseFileName: 文件基础名称（不含扩展名）
// 返回值:
//   - error: 错误信息
// 工作流程:
//   1. 复制 .ecx 文件内容到 .idx 文件（.ecx 包含所有未删除的 Needle）
//   2. 追加 .ecj 文件中的删除记录到 .idx 文件（.ecj 包含增量删除）
// 文件说明:
//   .ecx: EC Volume 的排序索引文件，包含所有有效 Needle
//   .ecj: EC Volume 的日志文件，记录增量删除的 Needle ID
//   .idx: 普通 Volume 的索引文件
func WriteIdxFileFromEcIndex(baseFileName string) (err error) {

	// 打开 .ecx 文件（只读）
	ecxFile, openErr := os.OpenFile(baseFileName+".ecx", os.O_RDONLY, 0644)
	if openErr != nil {
		return fmt.Errorf("cannot open ec index %s.ecx: %v", baseFileName, openErr)
	}
	defer ecxFile.Close()

	// 创建 .idx 文件（写入模式，截断）
	idxFile, openErr := os.OpenFile(baseFileName+".idx", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if openErr != nil {
		return fmt.Errorf("cannot open %s.idx: %v", baseFileName, openErr)
	}
	defer idxFile.Close()

	// 步骤1：将 .ecx 文件内容完整复制到 .idx 文件
	io.Copy(idxFile, ecxFile)

	// 步骤2：遍历 .ecj 文件，将删除记录追加到 .idx 文件
	err = iterateEcjFile(baseFileName, func(key types.NeedleId) error {
		// 为每个删除的 Needle 创建墓碑记录
		// Offset 为零，Size 为 TombstoneFileSize 表示已删除
		bytes := needle_map.ToBytes(key, types.Offset{}, types.TombstoneFileSize)
		idxFile.Write(bytes)

		return nil
	})

	return err
}

// FindDatFileSize 从 EC 索引文件计算原始 .dat 文件的大小
// 通过查找最大偏移量的 Needle 来确定文件大小
//
// 参数:
//   - dataBaseFileName: 数据文件基础名称（用于读取版本信息）
//   - indexBaseFileName: 索引文件基础名称（用于查找最大偏移量）
// 返回值:
//   - datSize: 计算出的 .dat 文件大小
//   - error: 错误信息
// 说明:
//   虽然最大偏移量之后可能还有删除记录，但这不影响文件大小计算
//   因为删除操作不会改变文件大小，只是标记数据为已删除
func FindDatFileSize(dataBaseFileName, indexBaseFileName string) (datSize int64, err error) {

	// 读取 Volume 版本号，用于计算 Needle 实际大小
	version, err := readEcVolumeVersion(dataBaseFileName)
	if err != nil {
		return 0, fmt.Errorf("read ec volume %s version: %v", dataBaseFileName, err)
	}

	// 遍历 .ecx 索引文件，查找最大的 Needle 结束位置
	err = iterateEcxFile(indexBaseFileName, func(key types.NeedleId, offset types.Offset, size types.Size) error {

		// 跳过已删除的 Needle
		if size.IsDeleted() {
			return nil
		}

		// 计算 Needle 的结束偏移量 = 起始偏移 + 实际大小
		entryStopOffset := offset.ToActualOffset() + needle.GetActualSize(size, version)
		// 更新最大偏移量
		if datSize < entryStopOffset {
			datSize = entryStopOffset
		}

		return nil
	})

	return
}

// readEcVolumeVersion 读取 EC Volume 的版本号
// 从第一个 EC 分片（.ec00）的 SuperBlock 中读取
//
// 参数:
//   - baseFileName: 文件基础名称（不含扩展名）
// 返回值:
//   - version: Needle 版本号
//   - error: 错误信息
func readEcVolumeVersion(baseFileName string) (version needle.Version, err error) {

	// 打开第一个 EC 分片文件（.ec00）
	// 所有分片的 SuperBlock 都相同，读取第一个即可
	datFile, err := os.OpenFile(baseFileName+".ec00", os.O_RDONLY, 0644)
	if err != nil {
		return 0, fmt.Errorf("open ec volume %s superblock: %v", baseFileName, err)
	}
	datBackend := backend.NewDiskFile(datFile)

	// 读取 SuperBlock（Volume 元数据）
	superBlock, err := super_block.ReadSuperBlock(datBackend)
	datBackend.Close()
	if err != nil {
		return 0, fmt.Errorf("read ec volume %s superblock: %v", baseFileName, err)
	}

	return superBlock.Version, nil

}

// iterateEcxFile 遍历 .ecx 索引文件中的所有 Needle 条目
// 对每个条目调用处理函数
//
// 参数:
//   - baseFileName: 文件基础名称（不含扩展名）
//   - processNeedleFn: 处理每个 Needle 的回调函数
// 返回值:
//   - error: 错误信息
// 说明:
//   .ecx 文件格式：连续的 NeedleMapEntry 记录
//   每个 Entry 包含：NeedleId(8字节) + Offset(4字节) + Size(4字节)
func iterateEcxFile(baseFileName string, processNeedleFn func(key types.NeedleId, offset types.Offset, size types.Size) error) error {
	// 打开 .ecx 索引文件
	ecxFile, openErr := os.OpenFile(baseFileName+".ecx", os.O_RDONLY, 0644)
	if openErr != nil {
		return fmt.Errorf("cannot open ec index %s.ecx: %v", baseFileName, openErr)
	}
	defer ecxFile.Close()

	// 读取缓冲区，大小为一个 NeedleMapEntry
	buf := make([]byte, types.NeedleMapEntrySize)
	for {
		// 读取一个条目
		n, err := ecxFile.Read(buf)
		if n != types.NeedleMapEntrySize {
			if err == io.EOF {
				// 正常结束
				return nil
			}
			return err
		}
		// 解析条目：NeedleId, Offset, Size
		key, offset, size := idx.IdxFileEntry(buf)
		// 调用处理函数
		if processNeedleFn != nil {
			err = processNeedleFn(key, offset, size)
		}
		if err != nil {
			if err != io.EOF {
				return err
			}
			return nil
		}
	}

}

// iterateEcjFile 遍历 .ecj 日志文件中的所有删除记录
// 对每个删除的 Needle ID 调用处理函数
//
// 参数:
//   - baseFileName: 文件基础名称（不含扩展名）
//   - processNeedleFn: 处理每个删除 Needle ID 的回调函数
// 返回值:
//   - error: 错误信息
// 说明:
//   .ecj 文件格式：连续的 NeedleId（每个 8 字节）
//   记录了所有增量删除的 Needle ID
func iterateEcjFile(baseFileName string, processNeedleFn func(key types.NeedleId) error) error {
	// 如果 .ecj 文件不存在，直接返回（没有删除记录）
	if !util.FileExists(baseFileName + ".ecj") {
		return nil
	}

	// 打开 .ecj 日志文件
	ecjFile, openErr := os.OpenFile(baseFileName+".ecj", os.O_RDONLY, 0644)
	if openErr != nil {
		return fmt.Errorf("cannot open ec index %s.ecj: %v", baseFileName, openErr)
	}
	defer ecjFile.Close()

	// 读取缓冲区，大小为一个 NeedleId（8 字节）
	buf := make([]byte, types.NeedleIdSize)
	for {
		// 读取一个 NeedleId
		n, err := ecjFile.Read(buf)
		if n != types.NeedleIdSize {
			if err == io.EOF {
				// 正常结束
				return nil
			}
			return err
		}
		// 调用处理函数
		if processNeedleFn != nil {
			err = processNeedleFn(types.BytesToNeedleId(buf))
		}
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}

}

// WriteDatFile 从 EC 数据分片（.ec00 ~ .ec09）重建原始 .dat 文件
// 这是 EC 解码的核心函数，将分片数据合并还原为原始文件
//
// 参数:
//   - baseFileName: 文件基础名称（不含扩展名）
//   - datFileSize: 目标 .dat 文件大小
//   - shardFileNames: 分片文件名列表（只需要数据分片，不需要校验分片）
// 返回值:
//   - error: 错误信息
// 工作原理:
//   假设使用 10 数据分片，原始文件大小 25GB:
//   1. 大块阶段：每次从 10 个分片各读取 1GB，交错写入 .dat 文件（处理 20GB）
//   2. 小块阶段：每次从 10 个分片各读取 1MB，交错写入 .dat 文件（处理剩余 5GB）
//   最终重建的 .dat 文件与原始文件完全相同
func WriteDatFile(baseFileName string, datFileSize int64, shardFileNames []string) error {

	// 创建输出 .dat 文件
	datFile, openErr := os.OpenFile(baseFileName+".dat", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if openErr != nil {
		return fmt.Errorf("cannot write volume %s.dat: %v", baseFileName, openErr)
	}
	defer datFile.Close()

	// 打开所有数据分片文件（.ec00 ~ .ec09）
	inputFiles := make([]*os.File, DataShardsCount)

	defer func() {
		for shardId := 0; shardId < DataShardsCount; shardId++ {
			if inputFiles[shardId] != nil {
				inputFiles[shardId].Close()
			}
		}
	}()

	// 打开所有数据分片文件
	for shardId := 0; shardId < DataShardsCount; shardId++ {
		inputFiles[shardId], openErr = os.OpenFile(shardFileNames[shardId], os.O_RDONLY, 0)
		if openErr != nil {
			return openErr
		}
	}

	// 阶段1：处理大块数据
	// 当剩余数据 >= 10GB 时，每次从 10 个分片各读取 1GB
	for datFileSize >= DataShardsCount*ErasureCodingLargeBlockSize {
		for shardId := 0; shardId < DataShardsCount; shardId++ {
			// 从分片中读取 1GB 并写入 .dat 文件
			w, err := io.CopyN(datFile, inputFiles[shardId], ErasureCodingLargeBlockSize)
			if w != ErasureCodingLargeBlockSize {
				return fmt.Errorf("copy %s large block on shardId %d: %v", baseFileName, shardId, err)
			}
			datFileSize -= ErasureCodingLargeBlockSize
		}
	}

	// 阶段2：处理小块数据
	// 处理不足 10GB 的剩余数据，每次从 10 个分片各读取最多 1MB
	for datFileSize > 0 {
		for shardId := 0; shardId < DataShardsCount; shardId++ {
			// 计算本次读取大小（最多 1MB）
			toRead := min(datFileSize, ErasureCodingSmallBlockSize)
			w, err := io.CopyN(datFile, inputFiles[shardId], toRead)
			if w != toRead {
				return fmt.Errorf("copy %s small block %d: %v", baseFileName, shardId, err)
			}
			datFileSize -= toRead
		}
	}

	return nil
}

// min 返回两个 int64 数值中的较小值
func min(x, y int64) int64 {
	if x > y {
		return y
	}
	return x
}
