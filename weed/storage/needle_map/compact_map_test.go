// Package needle_map 实现了 SeaweedFS 的 Needle 索引映射
// 本文件包含 CompactMap 和 CompactMapSegment 的单元测试
package needle_map

// =====================================================
// CompactMap 测试说明
// =====================================================
// 本文件包含 CompactMap 的完整单元测试，验证：
//   1. 二分查找功能 (bsearchKey)
//   2. Set 操作（插入和更新）
//   3. Get 操作（查找）
//   4. Delete 操作（软删除）
//   5. 分段路由 (segmentForKey)
//   6. 升序遍历 (AscendingVisit)
//   7. 随机插入和排序保证
//
// 测试运行命令：
//   go test -v ./weed/storage/needle_map/
//   go test -v -run TestSegment ./weed/storage/needle_map/
// =====================================================

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// TestSegmentBsearchKey 测试 CompactMapSegment 的二分查找功能
// 验证 bsearchKey 方法在各种场景下的正确性
//
// 测试场景：
//   1. 空段中查找（应返回插入位置 0，未找到）
//   2. 在有序列表开头位置查找/插入
//   3. 在有序列表末尾位置查找/插入
//   4. 在有序列表中间位置查找/插入
//   5. 精确匹配已存在的 key
//
// bsearchKey 返回值说明：
//   - index: 找到时返回 key 的位置；未找到时返回应插入的位置
//   - found: 是否精确找到该 key
func TestSegmentBsearchKey(t *testing.T) {
	// 创建测试用的 Segment，包含 5 个有序的 key: [10, 20, 21, 26, 30]
	testSegment := &CompactMapSegment{
		list: []CompactNeedleValue{
			CompactNeedleValue{key: 10},
			CompactNeedleValue{key: 20},
			CompactNeedleValue{key: 21},
			CompactNeedleValue{key: 26},
			CompactNeedleValue{key: 30},
		},
		firstKey: 10, // 段内最小 key
		lastKey:  30, // 段内最大 key
	}

	// 测试用例定义
	testCases := []struct {
		name      string                // 测试用例名称
		cs        *CompactMapSegment    // 被测试的 Segment
		key       types.NeedleId        // 要查找的 key
		wantIndex int                   // 期望返回的索引位置
		wantFound bool                  // 期望的查找结果
	}{
		{
			// 场景 1：空段查找
			// 在空的 Segment 中查找任意 key，应返回位置 0（插入位置）
			name:      "empty segment",
			cs:        newCompactMapSegment(0),
			key:       123,
			wantIndex: 0,
			wantFound: false,
		},
		{
			// 场景 2：在列表开头插入
			// key=5 小于所有现有 key，应插入到位置 0
			name:      "new key, insert at beggining",
			cs:        testSegment,
			key:       5,
			wantIndex: 0,
			wantFound: false,
		},
		{
			// 场景 3：在列表末尾插入
			// key=100 大于所有现有 key，应插入到位置 5（末尾）
			name:      "new key, insert at end",
			cs:        testSegment,
			key:       100,
			wantIndex: 5,
			wantFound: false,
		},
		{
			// 场景 4：在第二个位置插入
			// key=12 在 10 和 20 之间，应插入到位置 1
			name:      "new key, insert second",
			cs:        testSegment,
			key:       12,
			wantIndex: 1,
			wantFound: false,
		},
		{
			// 场景 5：在中间位置插入
			// key=23 在 21 和 26 之间，应插入到位置 3
			name:      "new key, insert in middle",
			cs:        testSegment,
			key:       23,
			wantIndex: 3,
			wantFound: false,
		},
		{
			// 场景 6-10：精确匹配测试
			// 验证每个已存在的 key 都能被正确找到
			name:      "key #1",
			cs:        testSegment,
			key:       10,
			wantIndex: 0,
			wantFound: true,
		},
		{
			name:      "key #2",
			cs:        testSegment,
			key:       20,
			wantIndex: 1,
			wantFound: true,
		},
		{
			name:      "key #3",
			cs:        testSegment,
			key:       21,
			wantIndex: 2,
			wantFound: true,
		},
		{
			name:      "key #4",
			cs:        testSegment,
			key:       26,
			wantIndex: 3,
			wantFound: true,
		},
		{
			name:      "key #5",
			cs:        testSegment,
			key:       30,
			wantIndex: 4,
			wantFound: true,
		},
	}

	// 执行所有测试用例
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			index, found := tc.cs.bsearchKey(tc.key)
			// 验证返回的索引位置
			if got, want := index, tc.wantIndex; got != want {
				t.Errorf("expected %v, got %v", want, got)
			}
			// 验证是否找到
			if got, want := found, tc.wantFound; got != want {
				t.Errorf("expected %v, got %v", want, got)
			}
		})
	}
}

// TestSegmentSet 测试 CompactMapSegment 的 Set 操作
// 验证插入和更新功能的正确性
//
// 测试场景：
//   1. 在列表开头插入新 key
//   2. 在列表末尾插入新 key
//   3. 在列表中间插入新 key
//   4. 更新已存在的 key（应返回旧值）
//
// Set 返回值说明：
//   - oldOffset: 更新操作时返回旧的 Offset；新插入时为零值
//   - oldSize: 更新操作时返回旧的 Size；新插入时为 0
func TestSegmentSet(t *testing.T) {
	// 创建初始测试 Segment，包含 3 个条目
	// key=10: offset=0, size=100
	// key=20: offset=100, size=200
	// key=30: offset=300, size=300
	testSegment := &CompactMapSegment{
		list: []CompactNeedleValue{
			CompactNeedleValue{key: 10, offset: OffsetToCompact(types.Uint32ToOffset(0)), size: 100},
			CompactNeedleValue{key: 20, offset: OffsetToCompact(types.Uint32ToOffset(100)), size: 200},
			CompactNeedleValue{key: 30, offset: OffsetToCompact(types.Uint32ToOffset(300)), size: 300},
		},
		firstKey: 10,
		lastKey:  30,
	}

	// 验证初始状态：长度和容量都是 3
	if got, want := testSegment.len(), 3; got != want {
		t.Errorf("got starting size %d, want %d", got, want)
	}
	if got, want := testSegment.cap(), 3; got != want {
		t.Errorf("got starting capacity %d, want %d", got, want)
	}

	// 测试 Set 操作的用例
	testSets := []struct {
		name       string        // 测试名称
		key        types.NeedleId // 要设置的 key
		offset     types.Offset  // 新的 Offset
		size       types.Size    // 新的 Size
		wantOffset types.Offset  // 期望返回的旧 Offset
		wantSize   types.Size    // 期望返回的旧 Size
	}{
		{
			// 场景 1：在开头插入
			// key=5 将成为新的最小 key
			name: "insert at beggining",
			key:  5, offset: types.Uint32ToOffset(1000), size: 123,
			wantOffset: types.Uint32ToOffset(0), wantSize: 0, // 新插入，旧值为零
		},
		{
			// 场景 2：在末尾插入
			// key=51 将成为新的最大 key
			name: "insert at end",
			key:  51, offset: types.Uint32ToOffset(7000), size: 456,
			wantOffset: types.Uint32ToOffset(0), wantSize: 0,
		},
		{
			// 场景 3：在中间插入
			// key=25 将插入到 20 和 30 之间
			name: "insert in middle",
			key:  25, offset: types.Uint32ToOffset(8000), size: 789,
			wantOffset: types.Uint32ToOffset(0), wantSize: 0,
		},
		{
			// 场景 4：更新已存在的 key
			// key=30 已存在，应返回旧值 (offset=300, size=300)
			name: "update existing",
			key:  30, offset: types.Uint32ToOffset(9000), size: 999,
			wantOffset: types.Uint32ToOffset(300), wantSize: 300, // 返回旧值
		},
	}

	// 执行 Set 操作并验证返回值
	for _, ts := range testSets {
		offset, size := testSegment.set(ts.key, ts.offset, ts.size)
		if offset != ts.wantOffset {
			t.Errorf("%s: got offset %v, want %v", ts.name, offset, ts.wantOffset)
		}
		if size != ts.wantSize {
			t.Errorf("%s: got size %v, want %v", ts.name, size, ts.wantSize)
		}
	}

	// 验证最终的 Segment 状态
	// 应该包含 6 个有序条目: [5, 10, 20, 25, 30, 51]
	wantSegment := &CompactMapSegment{
		list: []CompactNeedleValue{
			CompactNeedleValue{key: 5, offset: OffsetToCompact(types.Uint32ToOffset(1000)), size: 123},
			CompactNeedleValue{key: 10, offset: OffsetToCompact(types.Uint32ToOffset(0)), size: 100},
			CompactNeedleValue{key: 20, offset: OffsetToCompact(types.Uint32ToOffset(100)), size: 200},
			CompactNeedleValue{key: 25, offset: OffsetToCompact(types.Uint32ToOffset(8000)), size: 789},
			CompactNeedleValue{key: 30, offset: OffsetToCompact(types.Uint32ToOffset(9000)), size: 999}, // 已更新
			CompactNeedleValue{key: 51, offset: OffsetToCompact(types.Uint32ToOffset(7000)), size: 456},
		},
		firstKey: 5,  // 更新为新的最小 key
		lastKey:  51, // 更新为新的最大 key
	}
	if !reflect.DeepEqual(testSegment, wantSegment) {
		t.Errorf("got result segment %v, want %v", testSegment, wantSegment)
	}

	// 验证最终状态：长度和容量都应该是 6
	if got, want := testSegment.len(), 6; got != want {
		t.Errorf("got result size %d, want %d", got, want)
	}
	if got, want := testSegment.cap(), 6; got != want {
		t.Errorf("got result capacity %d, want %d", got, want)
	}
}

// TestSegmentSetOrdering 测试乱序插入后的排序保证
// 验证无论以何种顺序插入，Segment 内部列表始终保持有序
//
// 测试方法：
//   1. 生成 0 到 SegmentChunkSize-1 的连续 key
//   2. 使用固定种子随机打乱顺序
//   3. 按打乱后的顺序插入到 Segment
//   4. 验证最终列表是严格升序的
//
// 这个测试验证了 CompactMapSegment 的核心不变量：
// 无论插入顺序如何，list 始终保持按 key 升序排列
func TestSegmentSetOrdering(t *testing.T) {
	// 生成连续的 key 序列 [0, 1, 2, ..., SegmentChunkSize-1]
	keys := []types.NeedleId{}
	for i := 0; i < SegmentChunkSize; i++ {
		keys = append(keys, types.NeedleId(i))
	}

	// 使用固定种子的随机数生成器，确保测试可重复
	r := rand.New(rand.NewSource(123456789))
	// 随机打乱 key 的顺序
	r.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })

	// 创建空的 Segment 并按乱序插入所有 key
	cs := newCompactMapSegment(0)
	for _, k := range keys {
		_, _ = cs.set(k, types.Uint32ToOffset(123), 456)
	}

	// 验证插入的数量正确
	if got, want := cs.len(), SegmentChunkSize; got != want {
		t.Errorf("expected size %d, got %d", want, got)
	}

	// 验证列表是严格升序的
	// 遍历列表，检查每对相邻元素，前一个的 key 必须小于后一个
	for i := 1; i < cs.len(); i++ {
		if ka, kb := cs.list[i-1].key, cs.list[i].key; ka >= kb {
			t.Errorf("found out of order entries at (%d, %d) = (%d, %d)", i-1, i, ka, kb)
		}
	}
}

// TestSegmentGet 测试 CompactMapSegment 的 Get 操作
// 验证查找功能的正确性
//
// 测试场景：
//   1. 查找不存在的 key（返回 nil, false）
//   2. 查找存在的 key（返回对应的 CompactNeedleValue 指针）
func TestSegmentGet(t *testing.T) {
	// 创建测试用的 Segment
	testSegment := &CompactMapSegment{
		list: []CompactNeedleValue{
			CompactNeedleValue{key: 10, offset: OffsetToCompact(types.Uint32ToOffset(0)), size: 100},
			CompactNeedleValue{key: 20, offset: OffsetToCompact(types.Uint32ToOffset(100)), size: 200},
			CompactNeedleValue{key: 30, offset: OffsetToCompact(types.Uint32ToOffset(300)), size: 300},
		},
		firstKey: 10,
		lastKey:  30,
	}

	// 测试用例
	testCases := []struct {
		name      string              // 测试名称
		key       types.NeedleId      // 要查找的 key
		wantValue *CompactNeedleValue // 期望返回的值（nil 表示未找到）
		wantFound bool                // 期望的查找结果
	}{
		{
			// 场景 1：查找不存在的 key
			name:      "invalid key",
			key:       99,
			wantValue: nil,
			wantFound: false,
		},
		{
			// 场景 2-4：查找存在的 key
			// 返回的是指向 list 元素的指针
			name:      "key #1",
			key:       10,
			wantValue: &testSegment.list[0],
			wantFound: true,
		},
		{
			name:      "key #2",
			key:       20,
			wantValue: &testSegment.list[1],
			wantFound: true,
		},
		{
			name:      "key #3",
			key:       30,
			wantValue: &testSegment.list[2],
			wantFound: true,
		},
	}

	// 执行测试
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			value, found := testSegment.get(tc.key)
			if got, want := value, tc.wantValue; got != want {
				t.Errorf("got %v, want %v", got, want)
			}
			if got, want := found, tc.wantFound; got != want {
				t.Errorf("got %v, want %v", got, want)
			}
		})
	}
}

// TestSegmentDelete 测试 CompactMapSegment 的 Delete 操作
// 验证软删除功能的正确性
//
// 测试场景：
//   1. 删除不存在的 key（返回 0）
//   2. 删除存在的 key（返回原 Size，Size 变为负数）
//
// 软删除说明：
//   - Delete 不会从列表中移除条目
//   - 而是将 Size 设为负数（取负）来标记删除
//   - 这样可以在 vacuum 压缩时识别已删除的 Needle
func TestSegmentDelete(t *testing.T) {
	// 创建测试用的 Segment，包含 4 个条目
	testSegment := &CompactMapSegment{
		list: []CompactNeedleValue{
			CompactNeedleValue{key: 10, offset: OffsetToCompact(types.Uint32ToOffset(0)), size: 100},
			CompactNeedleValue{key: 20, offset: OffsetToCompact(types.Uint32ToOffset(100)), size: 200},
			CompactNeedleValue{key: 30, offset: OffsetToCompact(types.Uint32ToOffset(300)), size: 300},
			CompactNeedleValue{key: 40, offset: OffsetToCompact(types.Uint32ToOffset(600)), size: 400},
		},
		firstKey: 10,
		lastKey:  40,
	}

	// 测试删除操作
	testDeletes := []struct {
		name string         // 测试名称
		key  types.NeedleId // 要删除的 key
		want types.Size     // 期望返回的原 Size
	}{
		{
			// 场景 1：删除不存在的 key
			// 应返回 0
			name: "invalid key",
			key:  99,
			want: 0,
		},
		{
			// 场景 2：删除 key=20
			// 应返回原 Size=200
			name: "delete key #2",
			key:  20,
			want: 200,
		},
		{
			// 场景 3：删除 key=40
			// 应返回原 Size=400
			name: "delete key #4",
			key:  40,
			want: 400,
		},
	}

	// 执行删除操作并验证返回值
	for _, td := range testDeletes {
		size := testSegment.delete(td.key)
		if got, want := size, td.want; got != want {
			t.Errorf("%s: got %v, want %v", td.name, got, want)
		}
	}

	// 验证删除后的 Segment 状态
	// key=20 和 key=40 的 Size 应该变为负数
	wantSegment := &CompactMapSegment{
		list: []CompactNeedleValue{
			CompactNeedleValue{key: 10, offset: OffsetToCompact(types.Uint32ToOffset(0)), size: 100},      // 未删除
			CompactNeedleValue{key: 20, offset: OffsetToCompact(types.Uint32ToOffset(100)), size: -200},   // 已删除，Size 变负
			CompactNeedleValue{key: 30, offset: OffsetToCompact(types.Uint32ToOffset(300)), size: 300},    // 未删除
			CompactNeedleValue{key: 40, offset: OffsetToCompact(types.Uint32ToOffset(600)), size: -400},   // 已删除，Size 变负
		},
		firstKey: 10,
		lastKey:  40,
	}
	if !reflect.DeepEqual(testSegment, wantSegment) {
		t.Errorf("got result segment %v, want %v", testSegment, wantSegment)
	}
}

// TestSegmentForKey 测试 CompactMap 的分段路由功能
// 验证 segmentForKey 方法能正确地为 key 分配或获取对应的 Segment
//
// 分段策略说明：
//   - NeedleId 被分成多个 Chunk，每个 Chunk 对应一个 Segment
//   - Chunk 编号 = NeedleId / SegmentChunkSize
//   - 同一个 Chunk 内的所有 NeedleId 共享同一个 Segment
//
// 测试场景：
//   1. 第一个 Segment (chunk=0)
//   2. 连续的第二个 Segment (chunk=1)
//   3. 不连续的 Segment (chunk=5)，跳过中间的 chunk
func TestSegmentForKey(t *testing.T) {
	// 创建空的 CompactMap
	testMap := NewCompactMap()

	// 测试用例
	tests := []struct {
		name string              // 测试名称
		key  types.NeedleId      // 输入的 key
		want *CompactMapSegment  // 期望返回的 Segment
	}{
		{
			// 场景 1：第一个 Segment
			// key=12 属于 chunk 0 (12 / SegmentChunkSize = 0)
			name: "first segment",
			key:  12,
			want: &CompactMapSegment{
				list:     []CompactNeedleValue{},
				chunk:    0,
				firstKey: MaxCompactKey, // 空段的初始 firstKey 是最大值
				lastKey:  0,             // 空段的初始 lastKey 是 0
			},
		},
		{
			// 场景 2：连续的第二个 Segment
			// key = SegmentChunkSize + 34 属于 chunk 1
			name: "second segment, gapless",
			key:  SegmentChunkSize + 34,
			want: &CompactMapSegment{
				list:     []CompactNeedleValue{},
				chunk:    1,
				firstKey: MaxCompactKey,
				lastKey:  0,
			},
		},
		{
			// 场景 3：不连续的 Segment
			// key = 5 * SegmentChunkSize + 56 属于 chunk 5
			// 跳过了 chunk 2, 3, 4（稀疏分配）
			name: "gapped segment",
			key:  (5 * SegmentChunkSize) + 56,
			want: &CompactMapSegment{
				list:     []CompactNeedleValue{},
				chunk:    5,
				firstKey: MaxCompactKey,
				lastKey:  0,
			},
		},
	}

	// 执行测试
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cs := testMap.segmentForKey(tc.key)
			if !reflect.DeepEqual(cs, tc.want) {
				t.Errorf("got segment %v, want %v", cs, tc.want)
			}
		})
	}

	// 验证 CompactMap 的最终状态
	// 应该只包含被访问过的 3 个 Segment (chunk 0, 1, 5)
	wantMap := &CompactMap{
		segments: map[Chunk]*CompactMapSegment{
			0: &CompactMapSegment{
				list:     []CompactNeedleValue{},
				chunk:    0,
				firstKey: MaxCompactKey,
				lastKey:  0,
			},
			1: &CompactMapSegment{
				list:     []CompactNeedleValue{},
				chunk:    1,
				firstKey: MaxCompactKey,
				lastKey:  0,
			},
			5: &CompactMapSegment{
				list:     []CompactNeedleValue{},
				chunk:    5,
				firstKey: MaxCompactKey,
				lastKey:  0,
			},
		},
	}
	if !reflect.DeepEqual(testMap, wantMap) {
		t.Errorf("got map %v, want %v", testMap, wantMap)
	}
}

// TestAscendingVisit 测试 CompactMap 的升序遍历功能
// 验证 AscendingVisit 能按 NeedleId 升序访问所有条目
//
// 测试方法：
//   1. 以乱序插入多个 NeedleId
//   2. 使用 AscendingVisit 遍历
//   3. 验证遍历结果是严格升序的
//
// 注意：
//   - AscendingVisit 会跨越多个 Segment 进行遍历
//   - 先遍历 chunk 号小的 Segment，再遍历 chunk 号大的
//   - 每个 Segment 内部按 key 升序遍历
func TestAscendingVisit(t *testing.T) {
	// 创建 CompactMap 并以乱序插入多个 key
	// 这些 key 分布在不同的 Segment 中
	cm := NewCompactMap()
	for _, nid := range []types.NeedleId{20, 7, 40000, 300000, 0, 100, 500, 10000, 200000} {
		cm.Set(nid, types.Uint32ToOffset(123), 456)
	}

	// 使用 AscendingVisit 收集所有条目
	got := []NeedleValue{}
	err := cm.AscendingVisit(func(nv NeedleValue) error {
		got = append(got, nv)
		return nil
	})
	if err != nil {
		t.Errorf("got error %v, expected none", err)
	}

	// 期望的结果：按 NeedleId 严格升序
	// [0, 7, 20, 100, 500, 10000, 40000, 200000, 300000]
	want := []NeedleValue{
		NeedleValue{Key: 0, Offset: types.Uint32ToOffset(123), Size: 456},
		NeedleValue{Key: 7, Offset: types.Uint32ToOffset(123), Size: 456},
		NeedleValue{Key: 20, Offset: types.Uint32ToOffset(123), Size: 456},
		NeedleValue{Key: 100, Offset: types.Uint32ToOffset(123), Size: 456},
		NeedleValue{Key: 500, Offset: types.Uint32ToOffset(123), Size: 456},
		NeedleValue{Key: 10000, Offset: types.Uint32ToOffset(123), Size: 456},
		NeedleValue{Key: 40000, Offset: types.Uint32ToOffset(123), Size: 456},
		NeedleValue{Key: 200000, Offset: types.Uint32ToOffset(123), Size: 456},
		NeedleValue{Key: 300000, Offset: types.Uint32ToOffset(123), Size: 456},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got values %v, want %v", got, want)
	}
}

// TestRandomInsert 测试大规模随机插入的正确性
// 这是一个综合性测试，验证：
//   1. 大量乱序插入后数据完整性
//   2. 所有条目按升序排列
//   3. 内存使用效率（len == cap）
//
// 测试规模：
//   - 插入 8 * SegmentChunkSize 个 key（跨越 8 个 Segment）
//   - 使用固定种子随机打乱插入顺序
func TestRandomInsert(t *testing.T) {
	// 生成大量连续的 key
	count := 8 * SegmentChunkSize
	keys := []types.NeedleId{}
	for i := 0; i < count; i++ {
		keys = append(keys, types.NeedleId(i))
	}

	// 使用固定种子随机打乱
	r := rand.New(rand.NewSource(123456789))
	r.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })

	// 创建 CompactMap 并插入所有 key
	cm := NewCompactMap()
	for _, k := range keys {
		_, _ = cm.Set(k, types.Uint32ToOffset(123), 456)
	}

	// 验证总数正确
	if got, want := cm.Len(), count; got != want {
		t.Errorf("expected size %d, got %d", want, got)
	}

	// 使用 AscendingVisit 验证所有条目按升序排列
	last := -1
	err := cm.AscendingVisit(func(nv NeedleValue) error {
		key := int(nv.Key)
		// 检查当前 key 必须大于上一个 key
		if key <= last {
			return fmt.Errorf("found out of order entries (%d vs %d)", key, last)
		}
		last = key
		return nil
	})
	if err != nil {
		t.Errorf("got error %v, expected none", err)
	}

	// 验证内存使用效率
	// 由于插入的是 SegmentChunkSize 的整数倍，所有 Segment 应该被完全利用
	// 此时 len 应该等于 cap（没有浪费的容量）
	if l, c := cm.Len(), cm.Cap(); l != c {
		t.Errorf("map length (%d) doesn't match capacity (%d)", l, c)
	}
}
