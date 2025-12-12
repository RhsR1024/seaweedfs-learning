package erasure_coding

import (
	"fmt"
	"io"
	"os"

	"github.com/klauspost/reedsolomon"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	// DataShardsCount 默认数据分片数量
	// 原始数据将被均匀分成 10 个数据分片
	DataShardsCount = 10

	// ParityShardsCount 默认校验分片数量
	// 系统将生成 4 个校验分片用于数据恢复
	// 可以容忍最多 4 个分片丢失
	ParityShardsCount = 4

	// TotalShardsCount 总分片数量（数据分片 + 校验分片）
	// 默认为 10 + 4 = 14 个分片
	TotalShardsCount = DataShardsCount + ParityShardsCount

	// MaxShardCount 最大分片数量限制
	// 由于使用 uint32 位图(ShardBits)表示分片，最多支持 32 个分片(0-31位)
	MaxShardCount = 32

	// MinTotalDisks 最小磁盘数量要求
	// 计算公式: TotalShardsCount / ParityShardsCount + 1
	// 默认为 14 / 4 + 1 = 4 个磁盘
	MinTotalDisks = TotalShardsCount/ParityShardsCount + 1

	// ErasureCodingLargeBlockSize 大块编码块大小
	// 用于编码大文件的主体部分，每块 1GB
	// 大块可以提高编码效率，减少 CPU 开销
	ErasureCodingLargeBlockSize = 1024 * 1024 * 1024 // 1GB

	// ErasureCodingSmallBlockSize 小块编码块大小
	// 用于编码文件尾部不足 1GB 的部分，每块 1MB
	// 使用小块可以减少内存占用和编码延迟
	ErasureCodingSmallBlockSize = 1024 * 1024 // 1MB
)

// WriteSortedFileFromIdx 从现有的 .idx 文件生成 .ecx 文件
// .ecx 文件是 EC Volume 的排序索引文件，所有 Needle ID 按升序排列
//
// 参数:
//   - baseFileName: 文件基础名称（不含扩展名）
//   - ext: 输出文件扩展名（通常为 ".ecx"）
// 返回值:
//   - error: 错误信息
// 说明:
//   .idx 文件: 普通 Volume 的索引文件，可能无序
//   .ecx 文件: EC Volume 的索引文件，必须按 Needle ID 升序排列
//   排序索引支持二分查找，提高查询效率
func WriteSortedFileFromIdx(baseFileName string, ext string) (e error) {

	// 读取 .idx 文件到内存中的 Needle 映射
	nm, err := readNeedleMap(baseFileName)
	if nm != nil {
		defer nm.Close()
	}
	if err != nil {
		return fmt.Errorf("readNeedleMap: %w", err)
	}

	// 创建 .ecx 输出文件（截断模式）
	ecxFile, err := os.OpenFile(baseFileName+ext, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open ecx file: %w", err)
	}
	defer ecxFile.Close()

	// 按升序遍历 Needle 映射，写入 .ecx 文件
	// AscendingVisit 保证 Needle ID 按升序访问
	err = nm.AscendingVisit(func(value needle_map.NeedleValue) error {
		// 将 NeedleValue 序列化为字节数组
		bytes := value.ToBytes()
		_, writeErr := ecxFile.Write(bytes)
		return writeErr
	})

	if err != nil {
		return fmt.Errorf("failed to visit idx file: %w", err)
	}

	return nil
}

// WriteEcFiles 使用默认 EC 配置生成 EC 分片文件
// 生成文件: .ec00 ~ .ec13 (默认 10 数据分片 + 4 校验分片)
//
// 参数:
//   - baseFileName: 文件基础名称（不含扩展名）
// 返回值:
//   - error: 错误信息
func WriteEcFiles(baseFileName string) error {
	ctx := NewDefaultECContext("", 0)
	return WriteEcFilesWithContext(baseFileName, ctx)
}

// WriteEcFilesWithContext 使用指定的 EC 上下文生成 EC 分片文件
// 参数:
//   - baseFileName: 文件基础名称（不含扩展名）
//   - ctx: EC 编码上下文（包含分片配置）
// 返回值:
//   - error: 错误信息
func WriteEcFilesWithContext(baseFileName string, ctx *ECContext) error {
	// 256KB 缓冲区，1GB 大块，1MB 小块
	return generateEcFiles(baseFileName, 256*1024, ErasureCodingLargeBlockSize, ErasureCodingSmallBlockSize, ctx)
}

// RebuildEcFiles 重建缺失的 EC 分片文件
// 从现有分片恢复缺失的分片，利用纠删码的冗余特性
//
// 参数:
//   - baseFileName: 文件基础名称（不含扩展名）
// 返回值:
//   - []uint32: 新生成的分片 ID 列表
//   - error: 错误信息
// 工作流程:
//   1. 尝试从 .vif 文件加载原始 EC 配置
//   2. 如果 .vif 不存在或配置无效，使用默认配置
//   3. 使用 Reed-Solomon 算法从现有分片重建缺失分片
func RebuildEcFiles(baseFileName string) ([]uint32, error) {
	// 尝试从 .vif 文件加载 EC 配置，保留原始配置
	var ctx *ECContext
	if volumeInfo, _, found, _ := volume_info.MaybeLoadVolumeInfo(baseFileName + ".vif"); found && volumeInfo.EcShardConfig != nil {
		ds := int(volumeInfo.EcShardConfig.DataShards)
		ps := int(volumeInfo.EcShardConfig.ParityShards)

		// 验证 EC 配置有效性
		if ds > 0 && ps > 0 && ds+ps <= MaxShardCount {
			ctx = &ECContext{
				DataShards:   ds,
				ParityShards: ps,
			}
			glog.V(0).Infof("Rebuilding EC files for %s with config from .vif: %s", baseFileName, ctx.String())
		} else {
			glog.Warningf("Invalid EC config in .vif for %s (data=%d, parity=%d), using default", baseFileName, ds, ps)
			ctx = NewDefaultECContext("", 0)
		}
	} else {
		glog.V(0).Infof("Rebuilding EC files for %s with default config", baseFileName)
		ctx = NewDefaultECContext("", 0)
	}

	return RebuildEcFilesWithContext(baseFileName, ctx)
}

// RebuildEcFilesWithContext 使用指定的 EC 上下文重建缺失的 EC 分片文件
// 参数:
//   - baseFileName: 文件基础名称（不含扩展名）
//   - ctx: EC 编码上下文
// 返回值:
//   - []uint32: 新生成的分片 ID 列表
//   - error: 错误信息
func RebuildEcFilesWithContext(baseFileName string, ctx *ECContext) ([]uint32, error) {
	return generateMissingEcFiles(baseFileName, 256*1024, ErasureCodingLargeBlockSize, ErasureCodingSmallBlockSize, ctx)
}

// ToExt 返回指定 EC 索引的文件扩展名
// 参数:
//   - ecIndex: EC 分片索引
// 返回值:
//   - string: 文件扩展名，格式为 ".ecXX"（如 ".ec00", ".ec13"）
func ToExt(ecIndex int) string {
	return fmt.Sprintf(".ec%02d", ecIndex)
}

// generateEcFiles 生成 EC 分片文件的核心实现
// 从 .dat 文件读取数据，使用 Reed-Solomon 编码生成所有分片
//
// 参数:
//   - baseFileName: 文件基础名称（不含扩展名）
//   - bufferSize: 编码缓冲区大小（通常为 256KB）
//   - largeBlockSize: 大块大小（通常为 1GB）
//   - smallBlockSize: 小块大小（通常为 1MB）
//   - ctx: EC 编码上下文
// 返回值:
//   - error: 错误信息
func generateEcFiles(baseFileName string, bufferSize int, largeBlockSize int64, smallBlockSize int64, ctx *ECContext) error {
	// 打开原始 .dat 文件
	file, err := os.OpenFile(baseFileName+".dat", os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("failed to open dat file: %w", err)
	}
	defer file.Close()

	// 获取文件大小
	fi, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat dat file: %w", err)
	}

	// 执行纠删码编码
	glog.V(0).Infof("encodeDatFile %s.dat size:%d with EC context %s", baseFileName, fi.Size(), ctx.String())
	err = encodeDatFile(fi.Size(), baseFileName, bufferSize, largeBlockSize, file, smallBlockSize, ctx)
	if err != nil {
		return fmt.Errorf("encodeDatFile: %w", err)
	}
	return nil
}

// generateMissingEcFiles 生成缺失的 EC 分片文件
// 利用 Reed-Solomon 纠删码的特性，从现有分片重建缺失的分片
//
// 参数:
//   - baseFileName: 文件基础名称（不含扩展名）
//   - bufferSize: 编码缓冲区大小
//   - largeBlockSize: 大块大小
//   - smallBlockSize: 小块大小
//   - ctx: EC 编码上下文
// 返回值:
//   - generatedShardIds: 新生成的分片 ID 列表
//   - error: 错误信息
// 工作原理:
//   假设使用 10+4 配置，只要有任意 10 个分片，就能重建所有 14 个分片
//   例如：如果缺失 .ec03 和 .ec11，可以从其他 12 个分片恢复这 2 个
func generateMissingEcFiles(baseFileName string, bufferSize int, largeBlockSize int64, smallBlockSize int64, ctx *ECContext) (generatedShardIds []uint32, err error) {

	shardHasData := make([]bool, ctx.Total())   // 标记哪些分片存在
	inputFiles := make([]*os.File, ctx.Total()) // 输入文件（已存在的分片）
	outputFiles := make([]*os.File, ctx.Total()) // 输出文件（需要重建的分片）

	// 遍历所有分片，区分已存在和缺失的分片
	for shardId := 0; shardId < ctx.Total(); shardId++ {
		shardFileName := baseFileName + ctx.ToExt(shardId)
		if util.FileExists(shardFileName) {
			// 分片存在，打开为输入文件
			shardHasData[shardId] = true
			inputFiles[shardId], err = os.OpenFile(shardFileName, os.O_RDONLY, 0)
			if err != nil {
				return nil, err
			}
			defer inputFiles[shardId].Close()
		} else {
			// 分片缺失，创建为输出文件
			outputFiles[shardId], err = os.OpenFile(shardFileName, os.O_TRUNC|os.O_WRONLY|os.O_CREATE, 0644)
			if err != nil {
				return nil, err
			}
			defer outputFiles[shardId].Close()
			generatedShardIds = append(generatedShardIds, uint32(shardId))
		}
	}

	// 使用 Reed-Solomon 算法重建缺失的分片
	err = rebuildEcFiles(shardHasData, inputFiles, outputFiles, ctx)
	if err != nil {
		return nil, fmt.Errorf("rebuildEcFiles: %w", err)
	}
	return
}

// encodeData 对数据块进行纠删码编码
// 将数据块分成多个批次(batch)，每个批次单独编码
//
// 参数:
//   - file: 源数据文件
//   - enc: Reed-Solomon 编码器
//   - startOffset: 数据块在文件中的起始偏移量
//   - blockSize: 每个数据分片的块大小
//   - buffers: 编码缓冲区数组（包含数据分片和校验分片）
//   - outputs: 输出文件数组（所有分片文件）
//   - ctx: EC 编码上下文
// 返回值:
//   - error: 错误信息
func encodeData(file *os.File, enc reedsolomon.Encoder, startOffset, blockSize int64, buffers [][]byte, outputs []*os.File, ctx *ECContext) error {

	bufferSize := int64(len(buffers[0]))
	if bufferSize == 0 {
		glog.Fatal("unexpected zero buffer size")
	}

	// 计算需要处理的批次数
	// 例如：blockSize=1GB, bufferSize=256KB -> batchCount=4096
	batchCount := blockSize / bufferSize
	if blockSize%bufferSize != 0 {
		glog.Fatalf("unexpected block size %d buffer size %d", blockSize, bufferSize)
	}

	// 逐批次编码数据
	for b := int64(0); b < batchCount; b++ {
		err := encodeDataOneBatch(file, enc, startOffset+b*bufferSize, blockSize, buffers, outputs, ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

// openEcFiles 打开所有 EC 分片文件
// 参数:
//   - baseFileName: 文件基础名称（不含扩展名）
//   - forRead: true 表示以只读模式打开，false 表示以写入模式打开
//   - ctx: EC 编码上下文
// 返回值:
//   - files: 打开的文件句柄数组
//   - error: 错误信息
func openEcFiles(baseFileName string, forRead bool, ctx *ECContext) (files []*os.File, err error) {
	for i := 0; i < ctx.Total(); i++ {
		fname := baseFileName + ctx.ToExt(i)
		openOption := os.O_TRUNC | os.O_CREATE | os.O_WRONLY
		if forRead {
			openOption = os.O_RDONLY
		}
		f, err := os.OpenFile(fname, openOption, 0644)
		if err != nil {
			return files, fmt.Errorf("failed to open file %s: %v", fname, err)
		}
		files = append(files, f)
	}
	return
}

// closeEcFiles 关闭所有 EC 分片文件
// 参数:
//   - files: 要关闭的文件句柄数组
func closeEcFiles(files []*os.File) {
	for _, f := range files {
		if f != nil {
			f.Close()
		}
	}
}

// encodeDataOneBatch 对一个批次的数据进行纠删码编码
// 这是编码过程的最小单元，处理一个缓冲区大小的数据
//
// 参数:
//   - file: 源数据文件
//   - enc: Reed-Solomon 编码器
//   - startOffset: 当前批次在文件中的起始偏移量
//   - blockSize: 每个数据分片的块大小
//   - buffers: 编码缓冲区数组（前 N 个为数据分片，后 M 个为校验分片）
//   - outputs: 输出文件数组（所有分片文件）
//   - ctx: EC 编码上下文
// 返回值:
//   - error: 错误信息
// 工作流程:
//   1. 从源文件读取数据到数据分片缓冲区（buffers[0] ~ buffers[9]）
//   2. 使用 Reed-Solomon 编码生成校验分片（buffers[10] ~ buffers[13]）
//   3. 将所有分片写入对应的输出文件
func encodeDataOneBatch(file *os.File, enc reedsolomon.Encoder, startOffset, blockSize int64, buffers [][]byte, outputs []*os.File, ctx *ECContext) error {

	// 步骤1：读取数据到数据分片缓冲区
	// 例如：10 个数据分片，每个分片从不同偏移量读取
	for i := 0; i < ctx.DataShards; i++ {
		n, err := file.ReadAt(buffers[i], startOffset+blockSize*int64(i))
		if err != nil {
			if err != io.EOF {
				return err
			}
		}
		// 如果读取的数据不足缓冲区大小，用 0 填充尾部
		// 这对于文件末尾数据很重要
		if n < len(buffers[i]) {
			for t := len(buffers[i]) - 1; t >= n; t-- {
				buffers[i][t] = 0
			}
		}
	}

	// 步骤2：使用 Reed-Solomon 算法生成校验分片
	// 编码后，buffers 中的校验分片部分会被填充
	err := enc.Encode(buffers)
	if err != nil {
		return err
	}

	// 步骤3：将所有分片（数据 + 校验）写入对应的输出文件
	for i := 0; i < ctx.Total(); i++ {
		_, err := outputs[i].Write(buffers[i])
		if err != nil {
			return err
		}
	}

	return nil
}

// encodeDatFile 对 .dat 文件进行纠删码编码的核心函数
// 采用分段编码策略：先用大块编码主体数据，再用小块编码尾部数据
//
// 参数:
//   - remainingSize: 待编码的数据大小
//   - baseFileName: 文件基础名称（不含扩展名）
//   - bufferSize: 编码缓冲区大小（通常为 256KB）
//   - largeBlockSize: 大块大小（通常为 1GB）
//   - file: 源数据文件
//   - smallBlockSize: 小块大小（通常为 1MB）
//   - ctx: EC 编码上下文
// 返回值:
//   - error: 错误信息
// 编码策略:
//   假设文件大小为 25GB，使用 10 数据分片:
//   - 大块阶段：处理 20GB（2次，每次 10GB = 10分片 × 1GB）
//   - 小块阶段：处理剩余 5GB（5次，每次 10MB = 10分片 × 1MB）
func encodeDatFile(remainingSize int64, baseFileName string, bufferSize int, largeBlockSize int64, file *os.File, smallBlockSize int64, ctx *ECContext) error {

	var processedSize int64

	// 创建 Reed-Solomon 编码器
	enc, err := ctx.CreateEncoder()
	if err != nil {
		return fmt.Errorf("failed to create encoder: %w", err)
	}

	// 为所有分片（数据 + 校验）分配缓冲区
	buffers := make([][]byte, ctx.Total())
	for i := range buffers {
		buffers[i] = make([]byte, bufferSize)
	}

	// 打开所有输出分片文件
	outputs, err := openEcFiles(baseFileName, false, ctx)
	defer closeEcFiles(outputs)
	if err != nil {
		return fmt.Errorf("failed to open ec files %s: %v", baseFileName, err)
	}

	// 预计算行大小，避免循环中重复计算
	// largeRowSize: 一行大块的总大小 = 1GB × 10 = 10GB
	// smallRowSize: 一行小块的总大小 = 1MB × 10 = 10MB
	largeRowSize := largeBlockSize * int64(ctx.DataShards)
	smallRowSize := smallBlockSize * int64(ctx.DataShards)

	// 阶段1：使用大块编码主体数据
	// 只要剩余数据 >= largeRowSize，就使用大块编码
	for remainingSize >= largeRowSize {
		err = encodeData(file, enc, processedSize, largeBlockSize, buffers, outputs, ctx)
		if err != nil {
			return fmt.Errorf("failed to encode large chunk data: %w", err)
		}
		remainingSize -= largeRowSize
		processedSize += largeRowSize
	}

	// 阶段2：使用小块编码尾部数据
	// 处理不足一个 largeRowSize 的剩余数据
	for remainingSize > 0 {
		err = encodeData(file, enc, processedSize, smallBlockSize, buffers, outputs, ctx)
		if err != nil {
			return fmt.Errorf("failed to encode small chunk data: %w", err)
		}
		remainingSize -= smallRowSize
		processedSize += smallRowSize
	}
	return nil
}

// rebuildEcFiles 从现有分片重建缺失的分片文件
// 利用 Reed-Solomon 纠删码的重建(Reconstruct)功能
//
// 参数:
//   - shardHasData: 标记哪些分片存在数据（true=已存在，false=需要重建）
//   - inputFiles: 输入文件数组（已存在的分片文件）
//   - outputFiles: 输出文件数组（需要重建的分片文件）
//   - ctx: EC 编码上下文
// 返回值:
//   - error: 错误信息
// 工作原理:
//   使用 10+4 配置时，只要有任意 10 个分片，就能重建所有 14 个分片
//   Reed-Solomon 算法通过线性代数运算恢复缺失数据
func rebuildEcFiles(shardHasData []bool, inputFiles []*os.File, outputFiles []*os.File, ctx *ECContext) error {

	// 创建 Reed-Solomon 编码器（用于重建）
	enc, err := ctx.CreateEncoder()
	if err != nil {
		return fmt.Errorf("failed to create encoder: %w", err)
	}

	// 为现有分片分配缓冲区
	// 缺失的分片不分配缓冲区（设置为 nil）
	buffers := make([][]byte, ctx.Total())
	for i := range buffers {
		if shardHasData[i] {
			buffers[i] = make([]byte, ErasureCodingSmallBlockSize)
		}
	}

	var startOffset int64       // 当前处理的偏移量
	var inputBufferDataSize int // 每次读取的数据大小
	for {

		// 步骤1：从现有分片文件读取数据
		for i := 0; i < ctx.Total(); i++ {
			if shardHasData[i] {
				n, _ := inputFiles[i].ReadAt(buffers[i], startOffset)
				if n == 0 {
					// 所有分片都读取完毕
					return nil
				}
				if inputBufferDataSize == 0 {
					inputBufferDataSize = n
				}
				// 确保所有分片读取的数据大小一致
				if inputBufferDataSize != n {
					return fmt.Errorf("ec shard size expected %d actual %d", inputBufferDataSize, n)
				}
			} else {
				// 缺失的分片设置为 nil，重建时会被填充
				buffers[i] = nil
			}
		}

		// 步骤2：使用 Reed-Solomon 重建缺失的分片
		// Reconstruct 会根据现有分片计算并填充 buffers 中的 nil 项
		err = enc.Reconstruct(buffers)
		if err != nil {
			return fmt.Errorf("reconstruct: %w", err)
		}

		// 步骤3：将重建的数据写入输出文件
		for i := 0; i < ctx.Total(); i++ {
			if !shardHasData[i] {
				n, _ := outputFiles[i].WriteAt(buffers[i][:inputBufferDataSize], startOffset)
				if inputBufferDataSize != n {
					return fmt.Errorf("fail to write to %s", outputFiles[i].Name())
				}
			}
		}
		// 移动到下一个块
		startOffset += int64(inputBufferDataSize)
	}

}

// readNeedleMap 从 .idx 文件读取 Needle 映射到内存
// 参数:
//   - baseFileName: 文件基础名称（不含扩展名）
// 返回值:
//   - *needle_map.MemDb: 内存中的 Needle 映射
//   - error: 错误信息
func readNeedleMap(baseFileName string) (*needle_map.MemDb, error) {
	// 打开 .idx 索引文件
	indexFile, err := os.OpenFile(baseFileName+".idx", os.O_RDONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("cannot read Volume Index %s.idx: %v", baseFileName, err)
	}
	defer indexFile.Close()

	// 创建内存 Needle 映射
	cm := needle_map.NewMemDb()

	// 遍历 .idx 文件中的所有条目
	err = idx.WalkIndexFile(indexFile, 0, func(key types.NeedleId, offset types.Offset, size types.Size) error {
		if !offset.IsZero() && !size.IsDeleted() {
			// 有效的 Needle，添加到映射中
			cm.Set(key, offset, size)
		} else {
			// 已删除的 Needle，从映射中移除
			cm.Delete(key)
		}
		return nil
	})
	return cm, err
}
