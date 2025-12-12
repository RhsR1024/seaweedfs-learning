// Package needle_map 提供 Needle 索引映射的接口和实现
// 本文件实现了新版 CompactMap - 一个内存优化的 Needle 索引映射
package needle_map

// =====================================================
// CompactMap 设计概述（新版实现）
// =====================================================
// CompactMap 是 SeaweedFS 新版的内存索引实现，相比旧版有以下改进：
//
// 核心设计：
//   - 分段存储：将 NeedleId 空间按 SegmentChunkSize (50000) 分段
//   - 紧凑键：使用 16 位 CompactKey 代替 64 位 NeedleId，节省内存
//   - 有序数组：每个段内维护有序数组，支持二分查找
//
// 数据结构层次：
//   CompactMap
//     └── segments map[Chunk]*CompactMapSegment  // 分段映射表
//           └── CompactMapSegment
//                 ├── list []CompactNeedleValue  // 有序的紧凑值数组
//                 ├── chunk Chunk                // 分段编号
//                 ├── firstKey CompactKey        // 段内最小键
//                 └── lastKey CompactKey         // 段内最大键
//
// 内存优化：
//   - CompactKey: 16 位 (2 字节) vs NeedleId 64 位 (8 字节)，节省 6 字节/条目
//   - CompactOffset: 直接存储字节数组，避免结构体对齐开销
//   - 分段设计：每段最多 50000 条目，避免单个大数组的内存碎片
//
// 时间复杂度：
//   - 最佳情况（顺序插入）：O(1) 追加
//   - 最坏情况（乱序插入）：O(log n) 二分查找 + O(n) 数组移动
//   - 查询：O(log n) 二分查找
//
// 与旧版区别：
//   - 旧版使用 overflow 机制处理乱序，新版直接插入排序
//   - 旧版分段基于 NeedleId 范围，新版基于固定大小
//   - 新版 CompactKey 更小，内存效率更高
// =====================================================

/* CompactMap is an in-memory map of needle indeces, optimized for memory usage.
 *
 * It's implemented as a map of sorted indeces segments, which are in turn accessed through binary
 * search. This guarantees a best-case scenario (ordered inserts/updates) of O(1) and a worst case
 * scenario of O(log n) runtime, with memory usage unaffected by insert ordering.
 *
 * Note that even at O(log n), the clock time for both reads and writes is very low, so CompactMap
 * will seldom bottleneck index operations.
 */

import (
	"fmt"
	"math"
	"slices"
	"sort"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// =====================================================
// 常量定义
// =====================================================
const (
	// MaxCompactKey 是 CompactKey 的最大值 (2^16 - 1 = 65535)
	// 限制每个分段内可表示的键范围
	MaxCompactKey = math.MaxUint16

	// SegmentChunkSize 是每个分段的最大条目数
	// 设置为 50000，小于 MaxCompactKey (65535)
	// 这个值的选择平衡了：
	//   - 内存效率：太小会导致分段过多
	//   - 插入性能：太大会增加数组移动开销
	//   - 查询性能：二分查找效率与大小关系不大
	SegmentChunkSize = 50000 // should be <= MaxCompactKey
)

// =====================================================
// 紧凑类型定义
// =====================================================

// CompactKey 是段内的紧凑键类型
// 使用 16 位无符号整数，相比 64 位 NeedleId 节省 6 字节
//
// 计算方式：
//   CompactKey = NeedleId % SegmentChunkSize
//   NeedleId = Chunk * SegmentChunkSize + CompactKey
//
// 范围：0 ~ 65535 (MaxCompactKey)
type CompactKey uint16

// CompactOffset 是紧凑的偏移量存储类型
// 直接使用字节数组存储，避免 Offset 结构体的对齐开销
// 大小等于 types.OffsetSize (通常为 4 或 5 字节)
type CompactOffset [types.OffsetSize]byte

// CompactNeedleValue 是紧凑的 Needle 索引值
// 相比标准 NeedleValue，节省了 Key 的存储空间
//
// 内存布局（假设 OffsetSize=4）：
//   - key:    2 字节 (CompactKey)
//   - offset: 4 字节 (CompactOffset)
//   - size:   4 字节 (types.Size)
//   - 总计:   10 字节 (vs NeedleValue 的 16 字节)
//
// 注意：完整的 NeedleId 需要通过 key + chunk 计算得出
type CompactNeedleValue struct {
	key    CompactKey    // 段内紧凑键
	offset CompactOffset // 磁盘偏移量（字节数组形式）
	size   types.Size    // 数据大小
}

// =====================================================
// 分段类型定义
// =====================================================

// Chunk 是分段编号类型
// 用于将 NeedleId 空间划分为多个段
//
// 计算方式：
//   Chunk = NeedleId / SegmentChunkSize
//
// 示例（SegmentChunkSize=50000）：
//   - NeedleId 0-49999 → Chunk 0
//   - NeedleId 50000-99999 → Chunk 1
//   - NeedleId 100000-149999 → Chunk 2
type Chunk uint64

// CompactMapSegment 管理一个分段内的所有 Needle 索引
// 每个分段最多存储 SegmentChunkSize 个条目
//
// 数据组织：
//   - list: 有序数组，按 key 升序排列
//   - firstKey/lastKey: 快速判断 key 是否在范围内
//
// 设计特点：
//   - 使用有序数组而非哈希表，支持范围查询和遍历
//   - 通过 firstKey/lastKey 优化边界查询
//   - 满容量时固定数组大小，避免内存浪费
type CompactMapSegment struct {
	list     []CompactNeedleValue // 有序的紧凑值数组
	chunk    Chunk                // 分段编号，用于还原完整 NeedleId
	firstKey CompactKey           // 段内最小键（用于快速边界检查）
	lastKey  CompactKey           // 段内最大键（用于快速边界检查）
}

// =====================================================
// CompactMap 主结构
// =====================================================

// CompactMap 是内存优化的 Needle 索引映射
// 实现了 NeedleValueMap 接口
//
// 线程安全：
//   - 使用 RWMutex 实现读写锁
//   - 多个读操作可以并发执行
//   - 写操作独占锁
//
// 内存特性：
//   - 按需创建分段，不预分配
//   - 分段满后固定容量，避免 Go 切片自动扩容的内存浪费
//   - 每个条目约 10 字节（不含 Go 运行时开销）
type CompactMap struct {
	sync.RWMutex // 读写锁，保护 segments 的并发访问

	segments map[Chunk]*CompactMapSegment // 分段映射表
}

// =====================================================
// 类型转换方法
// =====================================================

// Key 将 CompactKey 还原为完整的 NeedleId
// 参数:
//   - chunk: 分段编号
//
// 返回值:
//   - types.NeedleId: 完整的 Needle 标识符
//
// 计算公式：
//
//	NeedleId = SegmentChunkSize * chunk + CompactKey
//
// 示例：
//
//	chunk=2, ck=1000 → NeedleId = 50000*2 + 1000 = 101000
func (ck CompactKey) Key(chunk Chunk) types.NeedleId {
	return (types.NeedleId(SegmentChunkSize) * types.NeedleId(chunk)) + types.NeedleId(ck)
}

// OffsetToCompact 将 Offset 转换为 CompactOffset
// 将结构体形式的偏移量转换为字节数组形式
// 参数:
//   - offset: 标准偏移量
//
// 返回值:
//   - CompactOffset: 字节数组形式的偏移量
func OffsetToCompact(offset types.Offset) CompactOffset {
	var co CompactOffset
	types.OffsetToBytes(co[:], offset)
	return co
}

// Offset 将 CompactOffset 还原为标准 Offset
// 返回值:
//   - types.Offset: 标准偏移量结构体
func (co CompactOffset) Offset() types.Offset {
	return types.BytesToOffset(co[:])
}

// NeedleValue 将 CompactNeedleValue 转换为标准 NeedleValue
// 参数:
//   - chunk: 分段编号（用于还原完整 NeedleId）
//
// 返回值:
//   - NeedleValue: 标准的 Needle 索引值
func (cnv CompactNeedleValue) NeedleValue(chunk Chunk) NeedleValue {
	return NeedleValue{
		Key:    cnv.key.Key(chunk), // 还原完整 NeedleId
		Offset: cnv.offset.Offset(),
		Size:   cnv.size,
	}
}

// =====================================================
// CompactMapSegment 方法
// =====================================================

// newCompactMapSegment 创建新的分段
// 参数:
//   - chunk: 分段编号
//
// 返回值:
//   - *CompactMapSegment: 初始化的空分段
func newCompactMapSegment(chunk Chunk) *CompactMapSegment {
	return &CompactMapSegment{
		list:     []CompactNeedleValue{}, // 初始为空
		chunk:    chunk,
		firstKey: MaxCompactKey, // 初始最小键设为最大值
		lastKey:  0,             // 初始最大键设为 0
	}
}

// len 返回分段中的条目数量
func (cs *CompactMapSegment) len() int {
	return len(cs.list)
}

// cap 返回分段的容量
func (cs *CompactMapSegment) cap() int {
	return cap(cs.list)
}

// compactKey 将 NeedleId 转换为段内 CompactKey
// 参数:
//   - key: 完整的 NeedleId
//
// 返回值:
//   - CompactKey: 段内紧凑键
//
// 计算公式：
//
//	CompactKey = NeedleId - SegmentChunkSize * chunk
func (cs *CompactMapSegment) compactKey(key types.NeedleId) CompactKey {
	return CompactKey(key - (types.NeedleId(SegmentChunkSize) * types.NeedleId(cs.chunk)))
}

// bsearchKey 在分段中二分查找指定 key
// 参数:
//   - key: 要查找的 NeedleId
//
// 返回值:
//   - int: 找到时返回索引；未找到时返回应插入的位置
//   - bool: 是否找到
//
// 优化策略：
//   - 先检查边界（firstKey/lastKey），避免不必要的二分查找
//   - 空列表直接返回 0
//
// bsearchKey returns the CompactNeedleValue index for a given ID key.
// If the key is not found, it returns the index where it should be inserted instead.
func (cs *CompactMapSegment) bsearchKey(key types.NeedleId) (int, bool) {
	ck := cs.compactKey(key)

	switch {
	case len(cs.list) == 0:
		// 空列表：返回插入位置 0
		return 0, false
	case ck == cs.firstKey:
		// 等于最小键：直接返回第一个位置
		return 0, true
	case ck <= cs.firstKey:
		// 小于最小键：插入到开头
		return 0, false
	case ck == cs.lastKey:
		// 等于最大键：直接返回最后一个位置
		return len(cs.list) - 1, true
	case ck > cs.lastKey:
		// 大于最大键：追加到末尾
		return len(cs.list), false
	}

	// 标准二分查找
	i := sort.Search(len(cs.list), func(i int) bool {
		return cs.list[i].key >= ck
	})
	return i, cs.list[i].key == ck
}

// set 在分段中插入或更新条目
// 参数:
//   - key: NeedleId
//   - offset: 磁盘偏移量
//   - size: 数据大小
//
// 返回值:
//   - oldOffset: 更新时返回旧偏移量
//   - oldSize: 更新时返回旧大小
//
// 插入策略：
//   - 使用二分查找定位插入位置
//   - 如果已存在则更新
//   - 否则插入并保持有序
//
// set inserts/updates a CompactNeedleValue.
// If the operation is an update, returns the overwritten value's previous offset and size.
func (cs *CompactMapSegment) set(key types.NeedleId, offset types.Offset, size types.Size) (oldOffset types.Offset, oldSize types.Size) {
	i, found := cs.bsearchKey(key)
	if found {
		// 更新已存在的条目
		// update
		o := cs.list[i].offset.Offset()
		oldOffset.OffsetLower = o.OffsetLower
		oldOffset.OffsetHigher = o.OffsetHigher
		oldSize = cs.list[i].size

		// 更新为新值
		o.OffsetLower = offset.OffsetLower
		o.OffsetHigher = offset.OffsetHigher
		cs.list[i].offset = OffsetToCompact(o)
		cs.list[i].size = size
		return
	}

	// 插入新条目
	// insert
	// 检查是否超出分段容量限制
	if len(cs.list) >= SegmentChunkSize {
		panic(fmt.Sprintf("attempted to write more than %d entries on CompactMapSegment %p!!!", SegmentChunkSize, cs))
	}
	if len(cs.list) == SegmentChunkSize-1 {
		// 即将达到最大容量：创建固定大小的新数组
		// 这样可以避免 Go 切片自动扩容带来的内存浪费
		// if we max out our segment storage, pin its capacity to minimize memory usage
		nl := make([]CompactNeedleValue, SegmentChunkSize, SegmentChunkSize)
		copy(nl, cs.list[:i])
		copy(nl[i+1:], cs.list[i:])
		cs.list = nl
	} else {
		// 正常插入：扩展切片并移动元素
		cs.list = append(cs.list, CompactNeedleValue{})
		copy(cs.list[i+1:], cs.list[i:])
	}

	// 设置新条目的值
	ck := cs.compactKey(key)
	cs.list[i] = CompactNeedleValue{
		key:    ck,
		offset: OffsetToCompact(offset),
		size:   size,
	}

	// 更新边界键
	if ck < cs.firstKey {
		cs.firstKey = ck
	}
	if ck > cs.lastKey {
		cs.lastKey = ck
	}

	return
}

// get 在分段中查找指定 key
// 参数:
//   - key: 要查找的 NeedleId
//
// 返回值:
//   - *CompactNeedleValue: 找到时返回条目指针
//   - bool: 是否找到
//
// get seeks a map entry by key. Returns an entry pointer, with a boolean specifiying if the entry was found.
func (cs *CompactMapSegment) get(key types.NeedleId) (*CompactNeedleValue, bool) {
	if i, found := cs.bsearchKey(key); found {
		return &cs.list[i], true
	}

	return nil, false
}

// delete 删除分段中指定 key 的条目（软删除）
// 采用软删除策略：将 Size 设为负数
// 参数:
//   - key: 要删除的 NeedleId
//
// 返回值:
//   - types.Size: 被删除条目的原始大小；未找到返回 0
//
// delete deletes a map entry by key. Returns the entries' previous Size, if available.
func (cs *CompactMapSegment) delete(key types.NeedleId) types.Size {
	if i, found := cs.bsearchKey(key); found {
		// 检查是否已被删除或无效
		if cs.list[i].size > 0 && cs.list[i].size.IsValid() {
			ret := cs.list[i].size
			// 软删除：Size 取负
			cs.list[i].size = -cs.list[i].size
			return ret
		}
	}

	return types.Size(0)
}

// =====================================================
// CompactMap 方法
// =====================================================

// NewCompactMap 创建新的 CompactMap
// 返回值:
//   - *CompactMap: 初始化的空映射
func NewCompactMap() *CompactMap {
	return &CompactMap{
		segments: map[Chunk]*CompactMapSegment{},
	}
}

// Len 返回映射中的总条目数
// 返回值:
//   - int: 所有分段的条目数之和
func (cm *CompactMap) Len() int {
	l := 0
	for _, s := range cm.segments {
		l += s.len()
	}
	return l
}

// Cap 返回映射的总容量
// 返回值:
//   - int: 所有分段的容量之和
func (cm *CompactMap) Cap() int {
	c := 0
	for _, s := range cm.segments {
		c += s.cap()
	}
	return c
}

// String 返回映射的字符串描述
// 用于调试和日志输出
// 返回值:
//   - string: 格式如 "100/128 elements on 3 segments, 78.13% efficiency"
func (cm *CompactMap) String() string {
	if cm.Len() == 0 {
		return "empty"
	}
	return fmt.Sprintf(
		"%d/%d elements on %d segments, %.02f%% efficiency",
		cm.Len(), cm.Cap(), len(cm.segments),
		float64(100)*float64(cm.Len())/float64(cm.Cap()))
}

// segmentForKey 获取或创建指定 key 所属的分段
// 参数:
//   - key: NeedleId
//
// 返回值:
//   - *CompactMapSegment: key 所属的分段
//
// 说明：
//   - 如果分段不存在，会创建新的分段
//   - 分段编号 = key / SegmentChunkSize
func (cm *CompactMap) segmentForKey(key types.NeedleId) *CompactMapSegment {
	chunk := Chunk(key / SegmentChunkSize)
	if cs, ok := cm.segments[chunk]; ok {
		return cs
	}

	// 创建新分段
	cs := newCompactMapSegment(chunk)
	cm.segments[chunk] = cs
	return cs
}

// Set 插入或更新 Needle 索引
// 实现 NeedleValueMap 接口
// 参数:
//   - key: NeedleId
//   - offset: 磁盘偏移量
//   - size: 数据大小
//
// 返回值:
//   - oldOffset: 更新时返回旧偏移量
//   - oldSize: 更新时返回旧大小
//
// Set inserts/updates a NeedleValue.
// If the operation is an update, returns the overwritten value's previous offset and size.
func (cm *CompactMap) Set(key types.NeedleId, offset types.Offset, size types.Size) (oldOffset types.Offset, oldSize types.Size) {
	cm.RLock()
	defer cm.RUnlock()

	cs := cm.segmentForKey(key)
	return cs.set(key, offset, size)
}

// Get 查找指定 key 的索引信息
// 实现 NeedleValueMap 接口
// 参数:
//   - key: 要查找的 NeedleId
//
// 返回值:
//   - *NeedleValue: 找到时返回索引信息
//   - bool: 是否找到
//
// Get seeks a map entry by key. Returns an entry pointer, with a boolean specifiying if the entry was found.
func (cm *CompactMap) Get(key types.NeedleId) (*NeedleValue, bool) {
	cm.RLock()
	defer cm.RUnlock()

	cs := cm.segmentForKey(key)
	if cnv, found := cs.get(key); found {
		nv := cnv.NeedleValue(cs.chunk)
		return &nv, true
	}
	return nil, false
}

// Delete 删除指定 key 的索引条目
// 实现 NeedleValueMap 接口
// 参数:
//   - key: 要删除的 NeedleId
//
// 返回值:
//   - types.Size: 被删除条目的原始大小
//
// Delete deletes a map entry by key. Returns the entries' previous Size, if available.
func (cm *CompactMap) Delete(key types.NeedleId) types.Size {
	cm.RLock()
	defer cm.RUnlock()

	cs := cm.segmentForKey(key)
	return cs.delete(key)
}

// AscendingVisit 按 NeedleId 升序遍历所有条目
// 实现 NeedleValueMap 接口
// 参数:
//   - visit: 访问函数
//
// 返回值:
//   - error: visit 返回的错误
//
// 遍历顺序：
//   - 先按 Chunk 升序遍历分段
//   - 每个分段内按 list 顺序遍历（已有序）
//
// AscendingVisit runs a function on all entries, in ascending key order. Returns any errors hit while visiting.
func (cm *CompactMap) AscendingVisit(visit func(NeedleValue) error) error {
	cm.RLock()
	defer cm.RUnlock()

	// 收集所有 Chunk 编号
	chunks := []Chunk{}
	for c := range cm.segments {
		chunks = append(chunks, c)
	}
	// 按 Chunk 编号排序
	slices.Sort(chunks)

	// 按序遍历每个分段
	for _, c := range chunks {
		cs := cm.segments[c]
		for _, cnv := range cs.list {
			nv := cnv.NeedleValue(cs.chunk)
			if err := visit(nv); err != nil {
				return err
			}
		}
	}
	return nil
}
