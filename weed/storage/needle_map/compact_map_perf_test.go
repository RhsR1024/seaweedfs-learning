// Package needle_map 实现了 SeaweedFS 的 Needle 索引映射
// 本文件包含 CompactMap 的内存使用性能测试
package needle_map

// =====================================================
// CompactMap 内存使用性能测试说明
// =====================================================
// 本文件用于测试和分析 CompactMap 的内存效率
//
// 测试目的：
//   1. 测量 CompactMap 每个条目的平均内存占用
//   2. 分析内存分配的增长模式
//   3. 验证 CompactMap 的内存效率设计
//
// 运行方式：
//   go test -run TestMemoryUsage ./weed/storage/needle_map/
//   go test -run TestMemoryUsage -memprofile=mem.out ./weed/storage/needle_map/
//   go tool pprof --alloc_space needle.test mem.out
//
// 预期结果：
//   - 每个条目约 10 字节（CompactNeedleValue 的大小）
//   - 加上 Go 运行时开销，实际约 10-14 字节/条目
//   - 相比旧版 CompactMap（16字节/条目），新版节省约 35% 内存
// =====================================================

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"testing"
	"time"

	// 使用点导入，直接使用 types 包中的类型和常量
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
)

/*

To see the memory usage:

go test -run TestMemoryUsage
The Alloc section shows the in-use memory increase for each iteration.

go test -run TestMemoryUsage -memprofile=mem.out
go tool pprof --alloc_space needle.test mem.out


*/

// TestMemoryUsage 测试 CompactMap 的内存使用情况
// 通过多次加载相同的索引文件，观察内存增长模式
//
// 测试流程：
//   1. 循环 10 次加载同一个索引文件
//   2. 每次加载后打印内存使用统计
//   3. 计算每个条目的平均内存占用
//
// 输出指标说明：
//   - Each X Bytes：每条记录平均占用的内存
//   - Alloc：当前堆内存分配量（活跃对象占用）
//   - TotalAlloc：累计分配的总内存（包括已释放的）
//   - Sys：从操作系统获取的总内存
//   - NumGC：GC 执行次数
//   - CompactMap：CompactMap 的状态信息（段数、长度、容量）
//   - Taken：本次迭代耗时
//
// 注意事项：
//   - 需要预先准备测试索引文件 ../../../test/data/sample.idx
//   - 测试文件格式：每 16 字节一条记录 (NeedleId + Offset + Size)
func TestMemoryUsage(t *testing.T) {

	// 存储所有加载的 CompactMap，防止被 GC 回收
	// 这样可以准确测量累计内存使用
	var maps []*CompactMap
	totalRowCount := uint64(0)

	startTime := time.Now()

	// 循环 10 次加载索引文件
	// 每次加载会创建一个新的 CompactMap 实例
	for i := 0; i < 10; i++ {
		// 打开测试索引文件
		// 路径相对于测试执行位置
		indexFile, ie := os.OpenFile("../../../test/data/sample.idx", os.O_RDWR|os.O_RDONLY, 0644)
		if ie != nil {
			log.Fatalln(ie)
		}

		// 加载索引文件到 CompactMap
		m, rowCount := loadNewNeedleMap(indexFile)
		maps = append(maps, m)
		totalRowCount += rowCount

		// 关闭文件句柄
		indexFile.Close()

		// 打印内存使用统计
		// totalRowCount 累计所有迭代加载的条目数
		PrintMemUsage(totalRowCount)

		// 打印 CompactMap 状态
		now := time.Now()
		fmt.Printf("\tCompactMap = %s", m.String())

		// 打印本次迭代耗时
		fmt.Printf("\tTaken = %v\n", now.Sub(startTime))
		startTime = now
	}

}

// loadNewNeedleMap 从索引文件加载数据到 CompactMap
// 这是一个辅助函数，模拟真实场景下的索引加载过程
//
// 参数:
//   - file: 打开的索引文件句柄
//
// 返回:
//   - *CompactMap: 加载完成的 CompactMap
//   - uint64: 加载的条目数量
//
// 索引文件格式（每条记录 16 字节）：
//   - NeedleId:  8 字节（用于查找的唯一标识）
//   - Offset:    4 字节（在 Volume 文件中的位置）
//   - Size:      4 字节（数据部分的大小）
//
// 处理逻辑：
//   - Offset 非零：正常记录，调用 Set 添加到映射
//   - Offset 为零：删除标记，调用 Delete 从映射中移除
//
// 注意：
//   - 此函数逐字节读取，效率较低，仅用于测试
//   - 生产环境使用批量读取或内存映射
func loadNewNeedleMap(file *os.File) (*CompactMap, uint64) {
	// 创建新的 CompactMap
	m := NewCompactMap()

	// 分配读取缓冲区，大小为单条记录大小
	// NeedleMapEntrySize = NeedleIdSize(8) + OffsetSize(4) + SizeSize(4) = 16
	bytes := make([]byte, NeedleMapEntrySize)
	rowCount := uint64(0)

	// 逐条读取索引记录
	count, e := file.Read(bytes)
	for count > 0 && e == nil {
		// 解析缓冲区中的所有记录
		for i := 0; i < count; i += NeedleMapEntrySize {
			rowCount++

			// 解析 NeedleId（8 字节）
			// 使用大端序编码
			key := BytesToNeedleId(bytes[i : i+NeedleIdSize])

			// 解析 Offset（4 字节）
			// 表示 Needle 在 Volume 文件中的位置
			offset := BytesToOffset(bytes[i+NeedleIdSize : i+NeedleIdSize+OffsetSize])

			// 解析 Size（4 字节）
			// 表示 Needle 数据部分的大小
			size := BytesToSize(bytes[i+NeedleIdSize+OffsetSize : i+NeedleIdSize+OffsetSize+SizeSize])

			// 根据 Offset 决定操作类型
			if !offset.IsZero() {
				// Offset 非零：正常记录，添加到映射
				m.Set(NeedleId(key), offset, size)
			} else {
				// Offset 为零：删除标记，从映射中删除
				// 这种情况发生在文件被删除时，会写入一条 offset=0 的记录
				m.Delete(key)
			}
		}

		// 继续读取下一批数据
		count, e = file.Read(bytes)
	}

	return m, rowCount

}

// PrintMemUsage 打印当前内存使用统计
// 强制执行 GC 后获取最新的内存统计信息
//
// 参数:
//   - totalRowCount: 已加载的总条目数，用于计算每条目平均内存
//
// 输出格式：
//   Each X.XX Bytes  Alloc = Y MiB  TotalAlloc = Z MiB  Sys = W MiB  NumGC = N
//
// 指标说明：
//   - Each：totalRowCount 条记录的平均内存占用
//     预期值约 10 字节（CompactNeedleValue 结构体大小）
//   - Alloc：当前堆内存中活跃对象占用的字节数
//     这是最重要的指标，反映实际内存使用
//   - TotalAlloc：程序启动以来累计分配的内存（包括已释放）
//     用于分析内存分配频率
//   - Sys：从操作系统获取的总内存（包括未使用的部分）
//     反映 Go 运行时的内存预留
//   - NumGC：垃圾回收执行次数
//     用于判断 GC 压力
func PrintMemUsage(totalRowCount uint64) {

	// 强制执行 GC，确保统计数据反映实际使用情况
	// 避免未回收的对象影响测量结果
	runtime.GC()

	// 获取内存统计信息
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// For info on each, see: https://golang.org/pkg/runtime/#MemStats

	// 打印每条记录的平均内存占用
	// 计算公式：当前堆内存 / 总记录数
	fmt.Printf("Each %.02f Bytes", float64(m.Alloc)/float64(totalRowCount))

	// 打印当前堆内存分配量（活跃对象）
	fmt.Printf("\tAlloc = %v MiB", bToMb(m.Alloc))

	// 打印累计分配的总内存
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))

	// 打印从操作系统获取的内存
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))

	// 打印 GC 执行次数
	fmt.Printf("\tNumGC = %v", m.NumGC)
}

// bToMb 将字节数转换为 MiB (Mebibyte)
// 参数:
//   - b: 字节数
//
// 返回:
//   - MiB 数（1 MiB = 1024 * 1024 = 1,048,576 字节）
//
// 注意：使用二进制单位 MiB 而非十进制单位 MB
func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
