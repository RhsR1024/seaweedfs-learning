// Package needle_map 实现了 SeaweedFS 的 Needle 索引映射（旧版实现）
// 本文件包含 CompactMap 的功能单元测试
package needle_map

import (
	"fmt"
	"log"
	"os"
	"testing"

	// 导入雪花算法序列号生成器
	"github.com/seaweedfs/seaweedfs/weed/sequence"
	// 导入存储类型定义（NeedleId, Offset, Size 等）
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"

	// 导入新版 needle_map 包，用于 NeedleValue 类型
	new_map "github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
)

// =====================================================
// CompactMap 功能测试说明
// =====================================================
// 本文件包含 CompactMap 的完整功能测试，验证：
//   1. 基本的 Set/Get/Delete 操作
//   2. 雪花算法生成的大 ID 支持
//   3. 乱序插入处理
//   4. Overflow 机制
//   5. 升序遍历功能
//   6. 边界情况处理
//
// 测试运行命令：
//   go test -v ./weed/storage/needle_map/old/
//   go test -v -run TestCompactMap ./weed/storage/needle_map/old/
// =====================================================

// TestSnowflakeSequencer 测试 CompactMap 对雪花算法 ID 的支持
// 雪花算法 (Snowflake) 生成 64 位唯一 ID，SeaweedFS 用它生成 NeedleId
//
// 测试场景：
//   - 使用雪花算法生成 20 万个递增的 NeedleId
//   - 验证每次 Set 操作的旧值为 0（即都是新插入）
//
// 雪花算法 ID 特点：
//   - 64 位整数，大致递增
//   - 包含时间戳、机器ID、序列号
//   - 可能出现轻微乱序（跨毫秒边界时）
func TestSnowflakeSequencer(t *testing.T) {
	m := NewCompactMap()

	// 创建雪花算法序列号生成器
	// 参数说明：
	//   - "for_test": 标识用途
	//   - 1: 机器 ID
	seq, _ := sequence.NewSnowflakeSequencer("for_test", 1)

	// 插入 20 万条记录
	for i := 0; i < 200000; i++ {
		// 获取下一个 NeedleId
		id := seq.NextFileId(1)

		// 设置记录：offset=8, size=3000073
		// oldSize 应为 0，因为是新记录
		oldOffset, oldSize := m.Set(NeedleId(id), ToOffset(8), 3000073)
		if oldSize != 0 {
			// 如果 oldSize 不为 0，说明发生了 ID 冲突
			t.Errorf("id %d oldOffset %v oldSize %d", id, oldOffset, oldSize)
		}
	}

}

// TestOverflow2 测试 CompactMap 的更新操作和升序遍历
// 验证：
//   1. 更新已存在的记录会返回旧值
//   2. 乱序插入能正确处理
//   3. AscendingVisit 按正确顺序遍历
func TestOverflow2(t *testing.T) {
	m := NewCompactMap()

	// 第一次插入 key=150088
	// 预期：oldSize 为 0（新记录）
	_, oldSize := m.Set(NeedleId(150088), ToOffset(8), 3000073)
	if oldSize != 0 {
		t.Fatalf("expecting no previous data")
	}

	// 第二次插入相同的 key=150088（更新操作）
	// 预期：oldSize 为 3000073（返回上一次的值）
	_, oldSize = m.Set(NeedleId(150088), ToOffset(8), 3000073)
	if oldSize != 3000073 {
		t.Fatalf("expecting previous data size is %d, not %d", 3000073, oldSize)
	}

	// 插入多个乱序的 key
	// 这些 key 都比 150088 小或大，测试乱序处理
	m.Set(NeedleId(150073), ToOffset(8), 3000073) // 小于 150088
	m.Set(NeedleId(150089), ToOffset(8), 3000073) // 大于 150088
	m.Set(NeedleId(150076), ToOffset(8), 3000073) // 乱序
	m.Set(NeedleId(150124), ToOffset(8), 3000073)
	m.Set(NeedleId(150137), ToOffset(8), 3000073)
	m.Set(NeedleId(150147), ToOffset(8), 3000073)
	m.Set(NeedleId(150145), ToOffset(8), 3000073) // 乱序
	m.Set(NeedleId(150158), ToOffset(8), 3000073)
	m.Set(NeedleId(150162), ToOffset(8), 3000073)

	// 升序遍历所有记录
	// 预期输出顺序：150073, 150076, 150088, 150089, 150124, 150137, 150145, 150147, 150158, 150162
	m.AscendingVisit(func(value new_map.NeedleValue) error {
		println("needle key:", value.Key)
		return nil
	})
}

// TestIssue52 测试 GitHub Issue #52 的修复
// 问题描述：插入一个较小的 key 后，之前插入的较大 key 无法查找到
//
// 重现步骤：
//   1. 先插入 key=10002
//   2. 再插入 key=10001（较小的 key）
//   3. 查找 key=10002 失败
//
// 根本原因：乱序插入时数组移动逻辑有 bug
// 修复后：乱序插入不影响已有记录的查找
func TestIssue52(t *testing.T) {
	m := NewCompactMap()

	// 先插入较大的 key=10002
	m.Set(NeedleId(10002), ToOffset(10002), 10002)
	if element, ok := m.Get(NeedleId(10002)); ok {
		fmt.Printf("key %d ok %v %d, %v, %d\n", 10002, ok, element.Key, element.Offset, element.Size)
	}

	// 再插入较小的 key=10001（触发乱序插入逻辑）
	m.Set(NeedleId(10001), ToOffset(10001), 10001)

	// 验证 key=10002 仍然可以查找到
	if element, ok := m.Get(NeedleId(10002)); ok {
		fmt.Printf("key %d ok %v %d, %v, %d\n", 10002, ok, element.Key, element.Offset, element.Size)
	} else {
		// 如果查找失败，说明 Issue #52 的 bug 复现
		t.Fatal("key 10002 missing after setting 10001")
	}
}

// TestCompactMap 综合测试 CompactMap 的基本功能
// 测试场景：
//   1. 大量数据的 Set 操作（100 * MaxSectionBucketSize 条）
//   2. 部分数据的 Delete 操作（每 37 条删除一条）
//   3. 部分数据的更新操作（每 3 条更新一条）
//   4. 验证所有数据的 Get 操作
//
// 数据分布：
//   - 偶数 key 被 Set
//   - 37 的倍数被 Delete
//   - 3 的倍数被更新（前 10 * MaxSectionBucketSize 范围）
func TestCompactMap(t *testing.T) {
	m := NewCompactMap()

	// 步骤 1：插入偶数 key (0, 2, 4, 6, ...)
	// 共插入 100 * MaxSectionBucketSize / 2 条记录
	for i := uint32(0); i < 100*MaxSectionBucketSize; i += 2 {
		m.Set(NeedleId(i), ToOffset(int64(i)), Size(i))
	}

	// 步骤 2：删除 37 的倍数 (0, 37, 74, 111, ...)
	for i := uint32(0); i < 100*MaxSectionBucketSize; i += 37 {
		m.Delete(NeedleId(i))
	}

	// 步骤 3：更新 3 的倍数（前 10 * MaxSectionBucketSize 范围）
	// 更新后：offset = i+11, size = i+5
	for i := uint32(0); i < 10*MaxSectionBucketSize; i += 3 {
		m.Set(NeedleId(i), ToOffset(int64(i+11)), Size(i+5))
	}

	// 步骤 4：验证前 10 * MaxSectionBucketSize 范围的数据
	for i := uint32(0); i < 10*MaxSectionBucketSize; i++ {
		v, ok := m.Get(NeedleId(i))
		if i%3 == 0 {
			// 3 的倍数应该被更新，size = i+5
			if !ok {
				t.Fatal("key", i, "missing!")
			}
			if v.Size != Size(i+5) {
				t.Fatal("key", i, "size", v.Size)
			}
		} else if i%37 == 0 {
			// 37 的倍数应该被删除（Size 为负或 ok=false）
			if ok && v.Size.IsValid() {
				t.Fatal("key", i, "should have been deleted needle value", v)
			}
		} else if i%2 == 0 {
			// 其他偶数应该保持原值，size = i
			if v.Size != Size(i) {
				t.Fatal("key", i, "size", v.Size)
			}
		}
	}

	// 步骤 5：验证后 90 * MaxSectionBucketSize 范围的数据
	// 这部分没有更新操作，只有原始插入和删除
	for i := uint32(10 * MaxSectionBucketSize); i < 100*MaxSectionBucketSize; i++ {
		v, ok := m.Get(NeedleId(i))
		if i%37 == 0 {
			// 37 的倍数应该被删除
			if ok && v.Size.IsValid() {
				t.Fatal("key", i, "should have been deleted needle value", v)
			}
		} else if i%2 == 0 {
			// 偶数应该存在，size = i
			if v == nil {
				t.Fatal("key", i, "missing")
			}
			if v.Size != Size(i) {
				t.Fatal("key", i, "size", v.Size)
			}
		}
	}

}

// TestOverflow 测试 CompactSection 的 overflow 机制
// Overflow 用于处理乱序插入的数据
//
// 测试操作：
//   1. 向 overflow 添加多个条目
//   2. 更新 overflow 中的条目
//   3. 删除 overflow 中的条目（软删除）
//   4. 在删除后重新添加
//
// 预期行为：
//   - overflow 始终保持有序
//   - 软删除只改变 Size 符号，不移除条目
//   - 重新添加后可以正常查找
func TestOverflow(t *testing.T) {
	// 创建一个起始 ID 为 1 的 CompactSection
	cs := NewCompactSection(1)

	// 向 overflow 添加 5 个条目 (key: 1, 2, 3, 4, 5)
	cs.setOverflowEntry(1, ToOffset(12), 12)
	cs.setOverflowEntry(2, ToOffset(12), 12)
	cs.setOverflowEntry(3, ToOffset(12), 12)
	cs.setOverflowEntry(4, ToOffset(12), 12)
	cs.setOverflowEntry(5, ToOffset(12), 12)

	// 验证 overflow[2] 的 key 是 3
	// overflow 数组按 key 排序：[1, 2, 3, 4, 5]
	if cs.overflow[2].Key != 3 {
		t.Fatalf("expecting o[2] has key 3: %+v", cs.overflow[2].Key)
	}

	// 更新 key=3 的条目：offset=24, size=24
	cs.setOverflowEntry(3, ToOffset(24), 24)

	// 验证更新后 overflow[2] 仍然是 key=3
	if cs.overflow[2].Key != 3 {
		t.Fatalf("expecting o[2] has key 3: %+v", cs.overflow[2].Key)
	}

	// 验证 size 已更新为 24
	if cs.overflow[2].Size != 24 {
		t.Fatalf("expecting o[2] has size 24: %+v", cs.overflow[2].Size)
	}

	// 软删除 key=4
	cs.deleteOverflowEntry(4)

	// 验证 overflow 长度仍为 5（软删除不移除条目）
	if len(cs.overflow) != 5 {
		t.Fatalf("expecting 5 entries now: %+v", cs.overflow)
	}

	// 验证 key=5 仍然可以查找到
	x, _ := cs.findOverflowEntry(5)
	if x.Key != 5 {
		t.Fatalf("expecting entry 5 now: %+v", x)
	}

	// 打印当前 overflow 状态
	for i, x := range cs.overflow {
		println("overflow[", i, "]:", x.Key)
	}
	println()

	// 软删除 key=1
	cs.deleteOverflowEntry(1)

	// 打印删除后的 overflow 状态（包含 Size 以显示删除标记）
	for i, x := range cs.overflow {
		println("overflow[", i, "]:", x.Key, "size", x.Size)
	}
	println()

	// 重新设置已删除的 key=4（覆盖软删除标记）
	cs.setOverflowEntry(4, ToOffset(44), 44)
	for i, x := range cs.overflow {
		println("overflow[", i, "]:", x.Key)
	}
	println()

	// 重新设置已删除的 key=1
	cs.setOverflowEntry(1, ToOffset(11), 11)

	// 打印最终状态
	for i, x := range cs.overflow {
		println("overflow[", i, "]:", x.Key)
	}
	println()

}

// TestCompactSection_Get 测试从真实索引文件加载后的查找功能
// 使用预先准备的测试索引文件进行测试
//
// 测试场景：
//   1. 从文件加载索引数据
//   2. 添加新记录
//   3. 查找不存在的记录
//   4. 添加后再查找
//   5. 删除后再查找
func TestCompactSection_Get(t *testing.T) {
	var maps []*CompactMap
	totalRowCount := uint64(0)

	// 打开测试索引文件
	indexFile, ie := os.OpenFile("../../../../test/data/sample.idx",
		os.O_RDWR|os.O_RDONLY, 0644)
	defer indexFile.Close()
	if ie != nil {
		log.Fatalln(ie)
	}

	// 加载索引文件到 CompactMap
	m, rowCount := loadNewNeedleMap(indexFile)
	maps = append(maps, m)
	totalRowCount += rowCount

	// 测试 1：添加一个大 ID 的记录（雪花算法风格的 ID）
	// 1574318345753513987 是一个典型的雪花算法生成的 ID
	m.Set(1574318345753513987, ToOffset(10002), 10002)
	nv, ok := m.Get(1574318345753513987)
	if ok {
		t.Log(uint64(nv.Key))
	}

	// 测试 2：查找一个不存在的大 ID
	// 预期：返回 false
	nv1, ok := m.Get(1574318350048481283)
	if ok {
		// 如果找到了，说明有问题
		t.Error(uint64(nv1.Key))
	}

	// 测试 3：添加之前不存在的 ID
	m.Set(1574318350048481283, ToOffset(10002), 10002)
	nv2, ok1 := m.Get(1574318350048481283)
	if ok1 {
		t.Log(uint64(nv2.Key))
	}

	// 测试 4：删除刚添加的记录，验证删除后查找失败
	m.Delete(nv2.Key)
	nv3, has := m.Get(nv2.Key)
	if has && nv3.Size > 0 {
		// 如果删除后还能找到有效记录，说明删除失败
		t.Error(uint64(nv3.Size))
	}
}

// TestCompactSection_PutOutOfOrderItemBeyondLookBackWindow 测试超出回溯窗口的乱序插入
// 测试场景：
//   1. 按顺序插入 1 到 LookBackWindowSize*3 的记录，但跳过 LookBackWindowSize
//   2. 最后插入 LookBackWindowSize（超出回溯窗口范围）
//   3. 验证该记录可以正确查找
//
// 这测试了 overflow 机制：当乱序插入的位置超出回溯窗口时，
// 数据会被放入 overflow 而不是 values 数组
func TestCompactSection_PutOutOfOrderItemBeyondLookBackWindow(t *testing.T) {
	m := NewCompactMap()

	// 步骤 1：按顺序插入，但跳过 LookBackWindowSize
	// 插入 1, 2, 3, ..., LookBackWindowSize-1, LookBackWindowSize+1, ..., LookBackWindowSize*3
	for i := 1; i <= LookBackWindowSize*3; i++ {
		if i != LookBackWindowSize {
			m.Set(NeedleId(i), ToOffset(int64(i)), Size(i))
		}
	}

	// 步骤 2：插入跳过的 LookBackWindowSize
	// 此时该位置距离 values 数组末尾超过 LookBackWindowSize
	// 因此会被插入到 overflow 中
	m.Set(NeedleId(LookBackWindowSize), ToOffset(int64(LookBackWindowSize)), Size(LookBackWindowSize))

	// 步骤 3：验证可以正确查找到
	if v, ok := m.Get(NeedleId(LookBackWindowSize)); !ok || v.Offset != ToOffset(LookBackWindowSize) || v.Size != Size(LookBackWindowSize) {
		t.Fatalf("expected to find LookBackWindowSize at offset %d with size %d, but got %v", LookBackWindowSize, LookBackWindowSize, v)
	}
}
