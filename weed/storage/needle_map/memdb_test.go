// Package needle_map 实现了 SeaweedFS 的 Needle 索引映射
// 本文件包含 MemDb 的基准测试
package needle_map

// =====================================================
// MemDb 基准测试说明
// =====================================================
// 本文件包含 MemDb 的性能基准测试
//
// 测试目的：
//   1. 测量 MemDb 的创建、写入、关闭的完整生命周期性能
//   2. 分析每次操作的内存分配情况
//   3. 评估 LevelDB 内存后端的开销
//
// 运行方式：
//   go test -bench=BenchmarkMemDb -benchmem ./weed/storage/needle_map/
//
// 输出指标说明：
//   - ns/op: 每次操作的纳秒数
//   - B/op: 每次操作分配的字节数
//   - allocs/op: 每次操作的内存分配次数
//
// 预期结果：
//   - 由于使用内存 LevelDB，创建和关闭有一定开销
//   - 每次完整生命周期约需分配若干 KB 内存
// =====================================================

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// BenchmarkMemDb 测试 MemDb 的完整生命周期性能
// 每次迭代包含：创建 → 写入一条记录 → 关闭
//
// 测试场景：
//   1. 创建新的 MemDb 实例（初始化内存 LevelDB）
//   2. 写入一条 Needle 索引记录
//   3. 关闭 MemDb（释放 LevelDB 资源）
//
// 为什么测试完整生命周期？
//   - MemDb 主要用于临时索引存储
//   - 在索引重建等场景下，会频繁创建和销毁
//   - 需要评估这种使用模式的开销
//
// 运行示例：
//   $ go test -bench=BenchmarkMemDb -benchmem ./weed/storage/needle_map/
//   BenchmarkMemDb-8   xxxxx   xxxx ns/op   xxxx B/op   xx allocs/op
//
// 输出解读：
//   - xxxxx: 在时间限制内完成的迭代次数
//   - xxxx ns/op: 单次迭代（创建+写入+关闭）的平均耗时
//   - xxxx B/op: 单次迭代分配的平均内存
//   - xx allocs/op: 单次迭代的平均内存分配次数
func BenchmarkMemDb(b *testing.B) {
	// 启用内存分配统计
	// 这会在结果中显示 B/op 和 allocs/op
	b.ReportAllocs()

	// 循环执行 b.N 次（由 testing 框架自动调整）
	for i := 0; i < b.N; i++ {
		// 步骤 1：创建新的 MemDb 实例
		// 这会初始化内存 LevelDB，有一定开销
		nm := NewMemDb()

		// 步骤 2：写入一条测试记录
		// 构造测试数据：
		//   - NeedleId: 345
		//   - Offset: 零值（OffsetHigher 和 OffsetLower 都为 0）
		//   - Size: 324
		nid := types.NeedleId(345)
		offset := types.Offset{
			OffsetHigher: types.OffsetHigher{}, // 高位 Offset，默认为 0
			OffsetLower:  types.OffsetLower{},  // 低位 Offset，默认为 0
		}
		nm.Set(nid, offset, 324)

		// 步骤 3：关闭 MemDb
		// 释放 LevelDB 资源，包括内存存储后端
		nm.Close()
	}

}
