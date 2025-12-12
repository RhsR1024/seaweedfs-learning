// Package erasure_coding 纠删码模块测试
// 本文件测试纠删码的编码、解码功能以及数据定位算法
//
// ============================================================================
// 纠删码（Erasure Coding）核心概念
// ============================================================================
//
// 1. Reed-Solomon 编码原理：
//    - 将原始数据分成 k 个数据分片（Data Shards）
//    - 通过数学计算生成 m 个校验分片（Parity Shards）
//    - 只要任意 k 个分片存在，就能恢复全部原始数据
//    - SeaweedFS 默认使用 10+4 配置（10 数据 + 4 校验）
//
// 2. 存储空间效率：
//    - 副本模式（3 副本）：存储开销 = 200%
//    - 纠删码（10+4）：存储开销 = 40%，同时容忍 4 个分片故障
//
// 3. 分块策略：
//    - 大块（Large Block）：用于存储数据的主体部分
//    - 小块（Small Block）：用于处理不足一个大块的尾部数据
//
// ============================================================================
package erasure_coding

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/klauspost/reedsolomon"
	"github.com/stretchr/testify/assert"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// 测试用的块大小常量
const (
	// largeBlockSize 大块大小（10000 字节）
	// 在纠删码中，数据首先按大块进行条带化分布
	// 实际生产环境使用 ErasureCodingLargeBlockSize = 1GB
	largeBlockSize = 10000

	// smallBlockSize 小块大小（100 字节）
	// 用于处理不足一个大块的尾部数据
	// 实际生产环境使用 ErasureCodingSmallBlockSize = 1MB
	smallBlockSize = 100
)

// TestEncodingDecoding 测试纠删码的完整编码和解码流程
//
// 测试流程：
// 1. 生成测试用的 EC 文件（.ec00 ~ .ec13）
// 2. 生成排序后的索引文件（.ecx）
// 3. 验证 EC 文件内容与原始 .dat 文件一致
// 4. 清理测试文件
//
// 这个测试验证了：
// - EC 编码后数据可以正确读取
// - 数据定位算法（LocateData）工作正确
// - 分片偏移计算准确
func TestEncodingDecoding(t *testing.T) {
	// bufferSize：生成测试数据时使用的缓冲区大小
	bufferSize := 50
	// baseFileName：测试文件的基础名称，实际会生成 1.dat, 1.idx, 1.ec00 等文件
	baseFileName := "1"

	// 创建默认的 EC 上下文（10 数据分片 + 4 校验分片）
	ctx := NewDefaultECContext("", 0)

	// ========== 步骤 1：生成 EC 文件 ==========
	// generateEcFiles 会：
	// - 创建测试用的 .dat 文件（包含随机数据）
	// - 使用 Reed-Solomon 编码生成 14 个 EC 分片文件
	err := generateEcFiles(baseFileName, bufferSize, largeBlockSize, smallBlockSize, ctx)
	if err != nil {
		t.Logf("generateEcFiles: %v", err)
	}

	// ========== 步骤 2：生成排序索引文件 ==========
	// WriteSortedFileFromIdx 从 .idx 文件生成 .ecx 文件
	// .ecx 文件是按 NeedleId 排序的索引，支持二分查找
	// 这对于 EC 卷的高效读取至关重要
	err = WriteSortedFileFromIdx(baseFileName, ".ecx")
	if err != nil {
		t.Logf("WriteSortedFileFromIdx: %v", err)
	}

	// ========== 步骤 3：验证数据正确性 ==========
	// 验证从 EC 文件读取的数据与原始 .dat 文件完全一致
	err = validateFiles(baseFileName, ctx)
	if err != nil {
		t.Logf("WriteSortedFileFromIdx: %v", err)
	}

	// ========== 步骤 4：清理测试文件 ==========
	removeGeneratedFiles(baseFileName, ctx)
}

// validateFiles 验证 EC 文件内容与原始数据文件的一致性
//
// 验证过程：
// 1. 读取 .idx 索引文件，获取所有 Needle 的位置信息
// 2. 对每个 Needle，分别从 .dat 和 EC 文件读取数据
// 3. 比较两者是否完全相同
//
// 参数:
//   - baseFileName: 文件基础名（不含扩展名）
//   - ctx: EC 上下文，包含分片数量等配置
//
// 返回:
//   - error: 验证失败时返回错误信息
func validateFiles(baseFileName string, ctx *ECContext) error {
	// 读取 Needle 索引映射
	// nm 提供 NeedleId -> (offset, size) 的映射
	nm, err := readNeedleMap(baseFileName)
	if err != nil {
		return fmt.Errorf("readNeedleMap: %v", err)
	}
	defer nm.Close()

	// 打开原始数据文件
	datFile, err := os.OpenFile(baseFileName+".dat", os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("failed to open dat file: %v", err)
	}
	defer datFile.Close()

	// 获取数据文件大小，用于边界检查
	fi, err := datFile.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat dat file: %v", err)
	}

	// 打开所有 EC 分片文件（.ec00 ~ .ec13）
	ecFiles, err := openEcFiles(baseFileName, true, ctx)
	if err != nil {
		return fmt.Errorf("error opening ec files: %w", err)
	}
	defer closeEcFiles(ecFiles)

	// 遍历所有 Needle，验证数据一致性
	// AscendingVisit 按 NeedleId 升序遍历
	err = nm.AscendingVisit(func(value needle_map.NeedleValue) error {
		// 对每个 Needle，比较从 .dat 和 EC 文件读取的数据
		return assertSame(datFile, fi.Size(), ecFiles, value.Offset, value.Size)
	})
	if err != nil {
		return fmt.Errorf("failed to check ec files: %v", err)
	}
	return nil
}

// assertSame 断言从原始数据文件和 EC 文件读取的数据相同
//
// 这是数据一致性验证的核心函数，确保：
// 1. EC 编码没有引入数据错误
// 2. 数据定位算法计算的偏移量正确
// 3. 跨分片读取能正确拼接数据
//
// 参数:
//   - datFile: 原始 .dat 数据文件
//   - datSize: 数据文件大小
//   - ecFiles: EC 分片文件数组
//   - offset: Needle 在文件中的偏移量
//   - size: Needle 的大小
func assertSame(datFile *os.File, datSize int64, ecFiles []*os.File, offset types.Offset, size types.Size) error {
	// 从原始 .dat 文件读取数据
	data, err := readDatFile(datFile, offset, size)
	if err != nil {
		return fmt.Errorf("failed to read dat file: %v", err)
	}

	// 获取 EC 分片文件大小（用于计算数据位置）
	ecFileStat, _ := ecFiles[0].Stat()

	// 从 EC 文件读取相同位置的数据
	// readEcFile 会处理数据可能跨多个分片的情况
	ecData, err := readEcFile(ecFileStat.Size(), ecFiles, offset, size)
	if err != nil {
		return fmt.Errorf("failed to read ec file: %v", err)
	}

	// 比较两份数据是否完全相同
	if bytes.Compare(data, ecData) != 0 {
		return fmt.Errorf("unexpected data read")
	}

	return nil
}

// readDatFile 从原始数据文件读取指定位置的数据
//
// 参数:
//   - datFile: 数据文件句柄
//   - offset: 逻辑偏移量（需要转换为实际偏移量）
//   - size: 要读取的数据大小
//
// 返回:
//   - []byte: 读取的数据
//   - error: 读取错误
func readDatFile(datFile *os.File, offset types.Offset, size types.Size) ([]byte, error) {
	// 分配读取缓冲区
	data := make([]byte, size)

	// ReadAt 在指定偏移量处读取数据
	// ToActualOffset() 将逻辑偏移转换为文件中的实际字节偏移
	// 这是因为 SeaweedFS 使用 8 字节对齐存储
	n, err := datFile.ReadAt(data, offset.ToActualOffset())
	if err != nil {
		return nil, fmt.Errorf("failed to ReadAt dat file: %v", err)
	}

	// 验证读取的字节数与预期一致
	if n != int(size) {
		return nil, fmt.Errorf("unexpected read size %d, expected %d", n, size)
	}
	return data, nil
}

// readEcFile 从 EC 分片文件中读取数据
//
// EC 数据存储原理：
// 原始数据被条带化分布到多个分片中。读取时需要：
// 1. 使用 LocateData 计算数据在哪些分片的哪些位置
// 2. 从各个分片读取对应的数据片段
// 3. 按顺序拼接成完整数据
//
// 参数:
//   - shardDatSize: 单个分片文件的大小
//   - ecFiles: 所有分片文件句柄
//   - offset: 原始数据的逻辑偏移量
//   - size: 要读取的数据大小
//
// 返回:
//   - data: 读取并拼接后的完整数据
//   - err: 读取错误
func readEcFile(shardDatSize int64, ecFiles []*os.File, offset types.Offset, size types.Size) (data []byte, err error) {
	// LocateData 是纠删码的核心算法
	// 它计算原始数据在 EC 分片中的存储位置
	// 返回一系列 Interval，描述数据如何分布在各分片中
	intervals := LocateData(largeBlockSize, smallBlockSize, shardDatSize, offset.ToActualOffset(), size)

	// 遍历所有数据区间，从对应分片读取数据
	for i, interval := range intervals {
		if d, e := readOneInterval(interval, ecFiles); e != nil {
			return nil, e
		} else {
			// 将读取的数据片段拼接成完整数据
			if i == 0 {
				data = d
			} else {
				data = append(data, d...)
			}
		}
	}

	return data, nil
}

// readOneInterval 从 EC 文件读取一个数据区间
//
// 每个 Interval 描述了一段连续数据在某个 EC 分片中的位置。
// 这个函数负责从正确的分片文件中读取该区间的数据。
//
// 参数:
//   - interval: 数据区间描述（包含分片索引、偏移量、大小等）
//   - ecFiles: 所有 EC 分片文件句柄
//
// 返回:
//   - data: 读取的数据
//   - err: 读取错误
func readOneInterval(interval Interval, ecFiles []*os.File) (data []byte, err error) {
	// 将 Interval 转换为具体的分片 ID 和分片内偏移量
	// ecFileIndex: 数据所在的分片编号（0-13）
	// ecFileOffset: 数据在该分片文件中的字节偏移
	ecFileIndex, ecFileOffset := interval.ToShardIdAndOffset(largeBlockSize, smallBlockSize)

	// 从指定分片读取数据
	data = make([]byte, interval.Size)
	err = readFromFile(ecFiles[ecFileIndex], data, ecFileOffset)

	// 以下代码（条件为 false）用于测试 EC 恢复功能
	// 开启后会故意跳过目标分片，使用其他分片重建数据
	if false {
		// 使用其他分片重建数据，验证 Reed-Solomon 恢复功能
		ecData, err := readFromOtherEcFiles(ecFiles, int(ecFileIndex), ecFileOffset, interval.Size)
		if err != nil {
			return nil, fmt.Errorf("ec reconstruct error: %v", err)
		}
		// 比较直接读取和重建的数据是否一致
		if bytes.Compare(data, ecData) != 0 {
			return nil, fmt.Errorf("ec compare error")
		}
	}
	return
}

// readFromOtherEcFiles 从其他 EC 分片重建数据（跳过指定分片）
//
// 这个函数演示了 Reed-Solomon 纠删码的数据恢复能力：
// 即使某个分片不可用，也能从其他分片重建数据。
//
// 算法原理：
// 1. 随机选择 DataShardsCount（10）个可用分片
// 2. 故意跳过目标分片（模拟分片故障）
// 3. 使用 Reed-Solomon 解码重建缺失的数据
//
// 参数:
//   - ecFiles: 所有 EC 分片文件
//   - ecFileIndex: 要跳过的分片索引（模拟该分片不可用）
//   - ecFileOffset: 数据在分片中的偏移量
//   - size: 要重建的数据大小
//
// 返回:
//   - data: 重建的数据
//   - err: 重建错误
func readFromOtherEcFiles(ecFiles []*os.File, ecFileIndex int, ecFileOffset int64, size types.Size) (data []byte, err error) {
	// 创建 Reed-Solomon 编解码器
	// DataShardsCount = 10（数据分片数）
	// ParityShardsCount = 4（校验分片数）
	enc, err := reedsolomon.New(DataShardsCount, ParityShardsCount)
	if err != nil {
		return nil, fmt.Errorf("failed to create encoder: %v", err)
	}

	// 准备分片数据缓冲区
	// bufs[i] = nil 表示该分片不可用
	// bufs[i] = data 表示该分片的数据
	bufs := make([][]byte, TotalShardsCount)

	// 随机选择 DataShardsCount 个分片（排除目标分片）
	// 这模拟了分布式系统中部分节点不可用的场景
	for i := 0; i < DataShardsCount; {
		// 随机选择一个分片
		n := int(rand.Int31n(TotalShardsCount))
		// 跳过目标分片和已选择的分片
		if n == ecFileIndex || bufs[n] != nil {
			continue
		}
		bufs[n] = make([]byte, size)
		i++
	}

	// 从选中的分片读取数据
	for i, buf := range bufs {
		if buf == nil {
			continue // 跳过未选中的分片
		}
		err = readFromFile(ecFiles[i], buf, ecFileOffset)
		if err != nil {
			return
		}
	}

	// 使用 Reed-Solomon 算法重建缺失的数据分片
	// ReconstructData 只重建数据分片，不重建校验分片
	// 这比完全重建更高效
	if err = enc.ReconstructData(bufs); err != nil {
		return nil, err
	}

	// 返回重建的目标分片数据
	return bufs[ecFileIndex], nil
}

// readFromFile 从文件指定偏移量读取数据
//
// 参数:
//   - file: 文件句柄
//   - data: 数据缓冲区
//   - ecFileOffset: 读取偏移量
func readFromFile(file *os.File, data []byte, ecFileOffset int64) (err error) {
	_, err = file.ReadAt(data, ecFileOffset)
	return
}

// removeGeneratedFiles 清理测试生成的文件
//
// 删除所有 EC 分片文件和索引文件
//
// 参数:
//   - baseFileName: 文件基础名
//   - ctx: EC 上下文
func removeGeneratedFiles(baseFileName string, ctx *ECContext) {
	// 删除所有 EC 分片文件（.ec00 ~ .ec13）
	for i := 0; i < ctx.Total(); i++ {
		fname := baseFileName + ctx.ToExt(i)
		os.Remove(fname)
	}
	// 删除排序索引文件
	os.Remove(baseFileName + ".ecx")
}

// TestLocateData 测试数据定位算法的基本功能
//
// LocateData 是纠删码读取的核心算法，它计算：
// - 原始数据在 EC 分片中的存储位置
// - 数据跨越了哪些分片
// - 每个分片中需要读取的偏移量和大小
//
// 测试场景 1：读取正好在大块边界开始的小数据
// 测试场景 2：读取跨越多个分片的大数据
func TestLocateData(t *testing.T) {
	// 场景 1：在大块边界读取 1 字节数据
	// 偏移量 = DataShardsCount * largeBlockSize = 10 * 10000 = 100000
	// 这个偏移量正好是第一个大块行的结束位置
	intervals := LocateData(largeBlockSize, smallBlockSize, largeBlockSize+1, DataShardsCount*largeBlockSize, 1)
	if len(intervals) != 1 {
		t.Errorf("unexpected interval size %d", len(intervals))
	}
	// 验证返回的 Interval 结构
	// BlockIndex=0: 在分片 0 中
	// InnerBlockOffset=0: 从块内偏移 0 开始
	// Size=1: 读取 1 字节
	// IsLargeBlock=false: 使用小块模式
	// LargeBlockRowsCount=1: 大块行数
	if !intervals[0].sameAs(Interval{0, 0, 1, false, 1}) {
		t.Errorf("unexpected interval %+v", intervals[0])
	}

	// 场景 2：读取跨越多个分片的数据
	// 从中间位置开始读取，跨越到大块结束
	// 这测试了数据跨分片时的正确计算
	intervals = LocateData(largeBlockSize, smallBlockSize, largeBlockSize+1, DataShardsCount*largeBlockSize/2+100, DataShardsCount*largeBlockSize+1-DataShardsCount*largeBlockSize/2-100)
	fmt.Printf("%+v\n", intervals)
}

// sameAs 比较两个 Interval 是否相同
//
// 用于测试中验证 LocateData 返回的结果是否符合预期
func (this Interval) sameAs(that Interval) bool {
	return this.IsLargeBlock == that.IsLargeBlock &&
		this.InnerBlockOffset == that.InnerBlockOffset &&
		this.BlockIndex == that.BlockIndex &&
		this.Size == that.Size
}

// TestLocateData2 测试实际生产环境参数下的数据定位
//
// 使用真实的 EC 块大小参数测试：
// - ErasureCodingLargeBlockSize = 1GB（1073741824 字节）
// - ErasureCodingSmallBlockSize = 1MB（1048576 字节）
//
// 测试场景：读取一个约 4MB 的数据块，验证：
// 1. 数据被正确分割成多个 Interval
// 2. 每个 Interval 的分片索引和偏移量正确
// 3. 所有 Interval 的大小之和等于请求的总大小
func TestLocateData2(t *testing.T) {
	// 参数说明：
	// shardDatSize = 3221225472（约 3GB）- 单个分片文件大小
	// offset = 21479557912 - 原始数据偏移量（约 20GB）
	// size = 4194339 - 要读取的数据大小（约 4MB）
	intervals := LocateData(ErasureCodingLargeBlockSize, ErasureCodingSmallBlockSize, 3221225472, 21479557912, 4194339)

	// 预期结果：数据被分割到分片 4、5、6、7、8 中
	// 每个分片读取约 1MB（除了首尾可能不完整）
	assert.Equal(t, intervals, []Interval{
		// 分片 4：从偏移 527128 开始，读取 521448 字节
		{BlockIndex: 4, InnerBlockOffset: 527128, Size: 521448, IsLargeBlock: false, LargeBlockRowsCount: 2},
		// 分片 5：从偏移 0 开始，读取完整的 1MB
		{BlockIndex: 5, InnerBlockOffset: 0, Size: 1048576, IsLargeBlock: false, LargeBlockRowsCount: 2},
		// 分片 6：从偏移 0 开始，读取完整的 1MB
		{BlockIndex: 6, InnerBlockOffset: 0, Size: 1048576, IsLargeBlock: false, LargeBlockRowsCount: 2},
		// 分片 7：从偏移 0 开始，读取完整的 1MB
		{BlockIndex: 7, InnerBlockOffset: 0, Size: 1048576, IsLargeBlock: false, LargeBlockRowsCount: 2},
		// 分片 8：从偏移 0 开始，读取 527163 字节（尾部数据）
		{BlockIndex: 8, InnerBlockOffset: 0, Size: 527163, IsLargeBlock: false, LargeBlockRowsCount: 2},
	})
	// 验证：521448 + 1048576 + 1048576 + 1048576 + 527163 = 4194339 ✓
}

// TestLocateData3 测试小数据块的定位
//
// 测试场景：读取一个小于 1MB 的数据块，验证：
// 1. 小数据不会被不必要地分割
// 2. 分片索引计算正确（这里是分片 8876）
// 3. 偏移量和大小计算准确
func TestLocateData3(t *testing.T) {
	// 参数说明：
	// shardDatSize = 3221225472（约 3GB）
	// offset = 30782909808（约 28.7GB）
	// size = 112568 字节（约 110KB）
	intervals := LocateData(ErasureCodingLargeBlockSize, ErasureCodingSmallBlockSize, 3221225472, 30782909808, 112568)

	// 打印调试信息
	for _, interval := range intervals {
		fmt.Printf("%+v\n", interval)
	}

	// 预期结果：数据完全在分片 8876 内，不跨分片
	// 这说明小数据块可以在单个分片内完成读取
	assert.Equal(t, intervals, []Interval{
		{BlockIndex: 8876, InnerBlockOffset: 912752, Size: 112568, IsLargeBlock: false, LargeBlockRowsCount: 2},
	})
}
