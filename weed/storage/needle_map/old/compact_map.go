// Package needle_map 实现了 SeaweedFS 的 Needle 索引映射（旧版实现）
//
// =====================================================
// CompactMap 设计概述
// =====================================================
// CompactMap 是 SeaweedFS 早期的内存索引实现，用于在内存中维护
// NeedleId 到磁盘位置 (Offset, Size) 的映射关系。
//
// 设计目标：
//   - 最小化内存占用：每个 Needle 条目仅需约 16 字节
//   - 高效查找：使用二分查找，时间复杂度 O(log n)
//   - 支持乱序插入：通过 overflow 机制处理非顺序写入
//
// 核心数据结构：
//   CompactMap
//     └── list []*CompactSection  // 分段列表，每段管理一定范围的 NeedleId
//           └── CompactSection
//                 ├── values []SectionalNeedleValue  // 主存储（有序）
//                 ├── overflow []SectionalNeedleValue // 溢出存储（处理乱序）
//                 ├── start NeedleId                  // 本段起始 ID
//                 └── end NeedleId                    // 本段结束 ID
//
// 为什么使用分段设计？
//   1. NeedleId 是 64 位整数，但大多数场景下相邻的 Needle 会有相近的 ID
//   2. 使用 32 位的 SectionalNeedleId 可以节省 50% 的内存
//   3. 分段后每段独立加锁，提高并发性能
//
// 注意：这是旧版实现，新版请参考 weed/storage/needle_map/ 目录
// =====================================================
package needle_map

import (
	"sort"
	"sync"

	// 导入存储类型定义（NeedleId, Offset, Size 等）
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"

	// 导入新版 needle_map 包，用于 NeedleValue 类型
	new_map "github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
)

// =====================================================
// 常量定义
// =====================================================
const (
	// MaxSectionBucketSize 每个 CompactSection 的主存储最大容量
	// 超过此容量后，新条目会进入 overflow 区
	// 8192 个条目 × 16 字节/条目 ≈ 128 KB
	MaxSectionBucketSize = 1024 * 8

	// LookBackWindowSize 回溯窗口大小
	// 当插入新条目时，如果 key 小于最后一个 key，会在最近的 1024 个条目中
	// 查找合适的插入位置。这是为了处理轻微乱序的写入场景。
	// 如果超出回溯窗口范围，则进入 overflow 区
	LookBackWindowSize = 1024 // how many entries to look back when inserting into a section
)

// =====================================================
// 分段 Needle ID 类型
// =====================================================

// SectionalNeedleId 是分段内的 Needle ID
// 使用 32 位整数表示，相对于段起始 ID 的偏移量
// 这样可以将 64 位的 NeedleId 压缩为 32 位，节省内存
//
// 计算公式：SectionalNeedleId = NeedleId - CompactSection.start
// 还原公式：NeedleId = SectionalNeedleId + CompactSection.start
type SectionalNeedleId uint32

// SectionalNeedleIdLimit 分段 ID 的最大值（2^32 - 1）
// 当 NeedleId - start 超过此值时，需要创建新的 CompactSection
const SectionalNeedleIdLimit = 1<<32 - 1

// =====================================================
// 分段 Needle 值结构
// =====================================================

// SectionalNeedleValue 存储单个 Needle 在分段内的索引信息
// 内存布局（共 16 字节）：
//   - Key:          4 字节 (SectionalNeedleId，相对于段起始的偏移)
//   - OffsetLower:  4 字节 (磁盘偏移的低 32 位)
//   - Size:         4 字节 (数据大小，负数表示已删除)
//   - OffsetHigher: 4 字节 (磁盘偏移的高 32 位)
//
// 为什么 Offset 分成两部分？
//   - 历史原因：早期版本只有 32 位 Offset（最大支持 32GB × 8 = 256GB）
//   - 后来扩展为 64 位 Offset，为兼容性分为高低两部分
//   - OffsetLower 是 8 字节对齐的，实际支持范围：4GB × 8 = 32GB
type SectionalNeedleValue struct {
	Key          SectionalNeedleId                       // 分段内的 Needle ID
	OffsetLower  OffsetLower       `comment:"Volume offset"` // 磁盘偏移低位，8 字节对齐后范围为 32GB
	Size         Size              `comment:"Size of the data portion"` // 数据大小，负数表示已删除
	OffsetHigher OffsetHigher                            // 磁盘偏移高位
}

// =====================================================
// CompactSection：分段存储结构
// =====================================================

// CompactSection 管理一段连续范围的 NeedleId 索引
// 每个 Section 独立加锁，支持并发读写
//
// 数据组织：
//   values:   主存储数组，按 Key 有序排列，使用二分查找
//   overflow: 溢出存储，用于处理乱序插入的条目
//   start:    本段管理的起始 NeedleId
//   end:      本段管理的最大 NeedleId
//
// 插入策略：
//   1. 如果新 key >= 最后一个 key 且未满：直接追加到 values
//   2. 如果新 key 在回溯窗口内且未满：在 values 中插入排序
//   3. 其他情况：插入到 overflow（有序）
//
// 查找策略：
//   1. 先在 overflow 中二分查找
//   2. 再在 values 中二分查找
type CompactSection struct {
	sync.RWMutex                          // 读写锁，支持并发读
	values       []SectionalNeedleValue   // 主存储数组（有序）
	overflow     Overflow                 // 溢出存储（有序）
	start        NeedleId                 // 本段起始 NeedleId
	end          NeedleId                 // 本段当前最大 NeedleId
}

// Overflow 溢出存储类型
// 当主存储满或乱序插入超出回溯窗口时，数据会存入 overflow
// 同样保持有序，查找时优先检查
type Overflow []SectionalNeedleValue

// NewCompactSection 创建新的 CompactSection
// 参数:
//   - start: 本段管理的起始 NeedleId
// 返回:
//   - 初始化后的 CompactSection 指针
func NewCompactSection(start NeedleId) *CompactSection {
	return &CompactSection{
		values:   make([]SectionalNeedleValue, 0),   // 初始为空
		overflow: Overflow(make([]SectionalNeedleValue, 0)), // 初始为空
		start:    start,
	}
}

// Set 在分段中设置或更新 Needle 的索引信息
// 参数:
//   - key:    NeedleId（64 位）
//   - offset: 磁盘偏移量
//   - size:   数据大小
// 返回:
//   - oldOffset: 旧的偏移量（如果是更新操作）
//   - oldSize:   旧的大小（如果是更新操作）
//
// 插入策略详解：
//   1. 先在 values 中查找是否已存在
//   2. 如果存在，更新并返回旧值
//   3. 如果不存在，根据以下规则选择插入位置：
//      a) values 未满且新 key >= 最后 key：追加到 values 末尾
//      b) values 未满且新 key 在回溯窗口内：插入排序到 values
//      c) 其他情况：插入到 overflow
func (cs *CompactSection) Set(key NeedleId, offset Offset, size Size) (oldOffset Offset, oldSize Size) {
	cs.Lock()
	defer cs.Unlock()

	// 更新段的最大 NeedleId
	if key > cs.end {
		cs.end = key
	}

	// 将 64 位 NeedleId 转换为 32 位 SectionalNeedleId
	// skey = key - start，存储相对偏移以节省内存
	skey := SectionalNeedleId(key - cs.start)

	// 在 values 中查找是否已存在
	if i := cs.binarySearchValues(skey); i >= 0 {
		// 找到已存在的条目，执行更新操作
		// 保存旧值用于返回
		oldOffset.OffsetHigher, oldOffset.OffsetLower, oldSize = cs.values[i].OffsetHigher, cs.values[i].OffsetLower, cs.values[i].Size
		// 更新为新值
		cs.values[i].OffsetHigher, cs.values[i].OffsetLower, cs.values[i].Size = offset.OffsetHigher, offset.OffsetLower, size
		return
	}

	// 获取 values 中最后一个 key（用于判断是否顺序插入）
	var lkey SectionalNeedleId
	if len(cs.values) > 0 {
		lkey = cs.values[len(cs.values)-1].Key
	}

	hasAdded := false
	switch {
	case len(cs.values) < MaxSectionBucketSize && lkey <= skey:
		// 情况 1：values 未满且新 key >= 最后 key（顺序插入）
		// 直接追加到末尾，这是最高效的插入方式
		cs.values = append(cs.values, SectionalNeedleValue{
			Key:          skey,
			OffsetLower:  offset.OffsetLower,
			Size:         size,
			OffsetHigher: offset.OffsetHigher,
		})
		hasAdded = true

	case len(cs.values) < MaxSectionBucketSize:
		// 情况 2：values 未满但需要乱序插入
		// 计算回溯窗口的起始位置
		lookBackIndex := len(cs.values) - LookBackWindowSize
		if lookBackIndex < 0 {
			lookBackIndex = 0
		}

		// 检查新 key 是否在回溯窗口范围内
		if cs.values[lookBackIndex].Key <= skey {
			// 在回溯窗口内查找插入位置
			for ; lookBackIndex < len(cs.values); lookBackIndex++ {
				if cs.values[lookBackIndex].Key >= skey {
					break
				}
			}
			// 在 lookBackIndex 位置插入新条目
			// 先扩展切片，然后移动元素
			cs.values = append(cs.values, SectionalNeedleValue{})
			copy(cs.values[lookBackIndex+1:], cs.values[lookBackIndex:])
			cs.values[lookBackIndex].Key, cs.values[lookBackIndex].Size = skey, size
			cs.values[lookBackIndex].OffsetLower, cs.values[lookBackIndex].OffsetHigher = offset.OffsetLower, offset.OffsetHigher
			hasAdded = true
		}
	}

	// 情况 3：插入到 overflow（values 已满或超出回溯窗口）
	if !hasAdded {
		// 检查 overflow 中是否已存在，如果存在则获取旧值
		if oldValue, found := cs.findOverflowEntry(skey); found {
			oldOffset.OffsetHigher, oldOffset.OffsetLower, oldSize = oldValue.OffsetHigher, oldValue.OffsetLower, oldValue.Size
		}
		// 设置或更新 overflow 中的条目
		cs.setOverflowEntry(skey, offset, size)
	} else {
		// 如果 values 达到最大容量，重新分配以固定容量
		// 这可以避免 Go 切片自动扩容带来的额外内存占用
		if len(cs.values) == MaxSectionBucketSize {
			bucket := make([]SectionalNeedleValue, len(cs.values))
			copy(bucket, cs.values)
			cs.values = bucket
		}
	}

	return
}

// setOverflowEntry 在 overflow 中设置或更新条目
// overflow 保持有序，使用二分查找定位插入/更新位置
// 参数:
//   - skey:   分段内的 NeedleId
//   - offset: 磁盘偏移量
//   - size:   数据大小
func (cs *CompactSection) setOverflowEntry(skey SectionalNeedleId, offset Offset, size Size) {
	needleValue := SectionalNeedleValue{Key: skey, OffsetLower: offset.OffsetLower, Size: size, OffsetHigher: offset.OffsetHigher}

	// 使用二分查找定位插入位置
	// sort.Search 返回第一个使 overflow[i].Key >= needleValue.Key 的索引
	insertCandidate := sort.Search(len(cs.overflow), func(i int) bool {
		return cs.overflow[i].Key >= needleValue.Key
	})

	// 检查是否是更新操作（key 已存在）
	if insertCandidate != len(cs.overflow) && cs.overflow[insertCandidate].Key == needleValue.Key {
		// 更新已存在的条目
		cs.overflow[insertCandidate] = needleValue
		return
	}

	// 插入新条目：先扩展切片，然后在正确位置插入
	cs.overflow = append(cs.overflow, SectionalNeedleValue{})
	copy(cs.overflow[insertCandidate+1:], cs.overflow[insertCandidate:])
	cs.overflow[insertCandidate] = needleValue
}

// findOverflowEntry 在 overflow 中查找指定 key 的条目
// 参数:
//   - key: 分段内的 NeedleId
// 返回:
//   - nv:    找到的条目
//   - found: 是否找到
func (cs *CompactSection) findOverflowEntry(key SectionalNeedleId) (nv SectionalNeedleValue, found bool) {
	// 使用二分查找
	foundCandidate := sort.Search(len(cs.overflow), func(i int) bool {
		return cs.overflow[i].Key >= key
	})
	// 验证是否确实找到
	if foundCandidate != len(cs.overflow) && cs.overflow[foundCandidate].Key == key {
		return cs.overflow[foundCandidate], true
	}
	return nv, false
}

// deleteOverflowEntry 在 overflow 中标记删除指定 key 的条目
// 注意：不是真正删除，而是将 Size 设为负数（软删除）
// 参数:
//   - key: 分段内的 NeedleId
func (cs *CompactSection) deleteOverflowEntry(key SectionalNeedleId) {
	length := len(cs.overflow)
	// 使用二分查找定位
	deleteCandidate := sort.Search(length, func(i int) bool {
		return cs.overflow[i].Key >= key
	})
	// 如果找到且未被删除，则标记删除（Size 取负）
	if deleteCandidate != length && cs.overflow[deleteCandidate].Key == key {
		if cs.overflow[deleteCandidate].Size.IsValid() {
			// 将 Size 设为负数表示删除
			// IsValid() 检查 Size 是否为有效的正数
			cs.overflow[deleteCandidate].Size = -cs.overflow[deleteCandidate].Size
		}
	}
}

// Delete 删除指定 NeedleId 的索引条目
// 采用软删除策略：将 Size 设为负数，而不是真正移除
// 这样可以在 vacuum 压缩时识别已删除的 Needle
// 参数:
//   - key: 要删除的 NeedleId
// 返回:
//   - 被删除条目的原始 Size（如果找到）
func (cs *CompactSection) Delete(key NeedleId) Size {
	cs.Lock()
	defer cs.Unlock()

	ret := Size(0)

	// 如果 key 超出本段范围，直接返回
	if key > cs.end {
		return ret
	}

	// 转换为分段内 ID
	skey := SectionalNeedleId(key - cs.start)

	// 先在 values 中查找
	if i := cs.binarySearchValues(skey); i >= 0 {
		if cs.values[i].Size > 0 && cs.values[i].Size.IsValid() {
			ret = cs.values[i].Size
			// 软删除：Size 取负
			cs.values[i].Size = -cs.values[i].Size
		}
	}

	// 再在 overflow 中查找
	if v, found := cs.findOverflowEntry(skey); found {
		cs.deleteOverflowEntry(skey)
		ret = v.Size
	}

	return ret
}

// Get 获取指定 NeedleId 的索引信息
// 查找顺序：先 overflow，后 values
// 这是因为 overflow 中可能包含更新后的数据
// 参数:
//   - key: 要查找的 NeedleId
// 返回:
//   - *NeedleValue: 找到的索引信息
//   - bool: 是否找到
func (cs *CompactSection) Get(key NeedleId) (*new_map.NeedleValue, bool) {
	cs.RLock()
	defer cs.RUnlock()

	// 如果 key 超出本段范围，直接返回
	if key > cs.end {
		return nil, false
	}

	skey := SectionalNeedleId(key - cs.start)

	// 优先在 overflow 中查找（可能有更新的数据）
	if v, ok := cs.findOverflowEntry(skey); ok {
		nv := toNeedleValue(v, cs)
		return &nv, true
	}

	// 在 values 中查找
	if i := cs.binarySearchValues(skey); i >= 0 {
		nv := toNeedleValue(cs.values[i], cs)
		return &nv, true
	}

	return nil, false
}

// binarySearchValues 在 values 数组中二分查找指定 key
// 参数:
//   - key: 要查找的分段内 NeedleId
// 返回:
//   - >= 0: 找到，返回索引
//   - -1:   key 超出范围
//   - -2:   key 不存在
func (cs *CompactSection) binarySearchValues(key SectionalNeedleId) int {
	// 使用标准库的二分查找
	x := sort.Search(len(cs.values), func(i int) bool {
		return cs.values[i].Key >= key
	})
	// 检查是否超出范围
	if x >= len(cs.values) {
		return -1
	}
	// 检查是否精确匹配
	if cs.values[x].Key > key {
		return -2
	}
	return x
}

// =====================================================
// CompactMap：完整的 Needle 索引映射
// =====================================================

// CompactMap 是完整的 Needle 索引映射实现
// 内部由多个 CompactSection 组成，每个 Section 管理一段 NeedleId 范围
//
// 设计假设：
//   - 大多数插入是递增顺序的
//   - 相邻的 NeedleId 会被分配到同一个 Section
//
// 注意：这是旧版实现，内存效率较高但功能有限
// 新版实现请参考 weed/storage/needle_map/ 目录
type CompactMap struct {
	list []*CompactSection // CompactSection 列表，按 start 排序
}

// NewCompactMap 创建新的 CompactMap
func NewCompactMap() *CompactMap {
	return &CompactMap{}
}

// Set 设置或更新 Needle 的索引信息
// 参数:
//   - key:    NeedleId
//   - offset: 磁盘偏移量
//   - size:   数据大小
// 返回:
//   - oldOffset: 旧的偏移量（如果是更新）
//   - oldSize:   旧的大小（如果是更新）
func (cm *CompactMap) Set(key NeedleId, offset Offset, size Size) (oldOffset Offset, oldSize Size) {
	// 查找 key 应该属于哪个 Section
	x := cm.binarySearchCompactSection(key)

	// 如果没有合适的 Section，或 key 超出当前 Section 的范围限制
	// 则创建新的 Section
	if x < 0 || (key-cm.list[x].start) > SectionalNeedleIdLimit {
		// 创建新的 Section，以 key 作为起始
		cs := NewCompactSection(key)
		cm.list = append(cm.list, cs)
		x = len(cm.list) - 1

		// 保持 Section 列表按 start 排序
		// 通过交换将新 Section 移动到正确位置
		for x >= 0 {
			if x > 0 && cm.list[x-1].start > key {
				cm.list[x] = cm.list[x-1]
				x = x - 1
			} else {
				cm.list[x] = cs
				break
			}
		}
	}

	// 在找到的 Section 中设置条目
	return cm.list[x].Set(key, offset, size)
}

// Delete 删除指定 NeedleId 的索引条目
// 参数:
//   - key: 要删除的 NeedleId
// 返回:
//   - 被删除条目的原始 Size
func (cm *CompactMap) Delete(key NeedleId) Size {
	x := cm.binarySearchCompactSection(key)
	if x < 0 {
		return Size(0)
	}
	return cm.list[x].Delete(key)
}

// Get 获取指定 NeedleId 的索引信息
// 参数:
//   - key: 要查找的 NeedleId
// 返回:
//   - *NeedleValue: 找到的索引信息
//   - bool: 是否找到
func (cm *CompactMap) Get(key NeedleId) (*new_map.NeedleValue, bool) {
	x := cm.binarySearchCompactSection(key)
	if x < 0 {
		return nil, false
	}
	return cm.list[x].Get(key)
}

// binarySearchCompactSection 查找 key 应该属于哪个 Section
// 使用二分查找在 Section 列表中定位
// 参数:
//   - key: 要查找的 NeedleId
// 返回:
//   - >= 0: 找到合适的 Section 索引
//   - -3:   key 不在任何 Section 范围内
//   - -4:   key 超出最后一个 Section 的范围
//   - -5:   Section 列表为空
func (cm *CompactMap) binarySearchCompactSection(key NeedleId) int {
	l, h := 0, len(cm.list)-1

	// 空列表
	if h < 0 {
		return -5
	}

	// 检查是否在最后一个 Section 范围内
	if cm.list[h].start <= key {
		// 如果最后一个 Section 未满，或 key 在其 end 范围内
		if len(cm.list[h].values) < MaxSectionBucketSize || key <= cm.list[h].end {
			return h
		}
		return -4
	}

	// 标准二分查找
	for l <= h {
		m := (l + h) / 2
		if key < cm.list[m].start {
			h = m - 1
		} else { // cm.list[m].start <= key
			if cm.list[m+1].start <= key {
				l = m + 1
			} else {
				return m
			}
		}
	}

	return -3
}

// AscendingVisit 按 NeedleId 升序遍历所有条目
// 遍历时会合并 values 和 overflow，确保按序访问
// 参数:
//   - visit: 访问函数，返回 error 时终止遍历
// 返回:
//   - error: visit 函数返回的错误（如果有）
//
// 实现细节：
//   - 使用归并策略合并 values 和 overflow
//   - overflow 中的条目会覆盖 values 中相同 key 的条目
//   - 这确保了返回最新的数据
func (cm *CompactMap) AscendingVisit(visit func(new_map.NeedleValue) error) error {
	// 遍历所有 Section
	for _, cs := range cm.list {
		cs.RLock()

		// 使用双指针归并 overflow 和 values
		var i, j int
		for i, j = 0, 0; i < len(cs.overflow) && j < len(cs.values); {
			if cs.overflow[i].Key < cs.values[j].Key {
				// overflow 中的 key 较小，先访问
				if err := visit(toNeedleValue(cs.overflow[i], cs)); err != nil {
					cs.RUnlock()
					return err
				}
				i++
			} else if cs.overflow[i].Key == cs.values[j].Key {
				// key 相同，跳过 values 中的条目（使用 overflow 中的更新数据）
				j++
			} else {
				// values 中的 key 较小，先访问
				if err := visit(toNeedleValue(cs.values[j], cs)); err != nil {
					cs.RUnlock()
					return err
				}
				j++
			}
		}

		// 处理 overflow 中剩余的条目
		for ; i < len(cs.overflow); i++ {
			if err := visit(toNeedleValue(cs.overflow[i], cs)); err != nil {
				cs.RUnlock()
				return err
			}
		}

		// 处理 values 中剩余的条目
		for ; j < len(cs.values); j++ {
			if err := visit(toNeedleValue(cs.values[j], cs)); err != nil {
				cs.RUnlock()
				return err
			}
		}

		cs.RUnlock()
	}
	return nil
}

// =====================================================
// 辅助函数
// =====================================================

// toNeedleValue 将 SectionalNeedleValue 转换为 NeedleValue
// 主要是将分段内的相对 ID 还原为绝对 NeedleId
// 参数:
//   - snv: 分段内的 Needle 值
//   - cs:  所属的 CompactSection（提供起始 ID）
// 返回:
//   - NeedleValue: 完整的 Needle 索引信息
func toNeedleValue(snv SectionalNeedleValue, cs *CompactSection) new_map.NeedleValue {
	// 组合高低位 Offset
	offset := Offset{
		OffsetHigher: snv.OffsetHigher,
		OffsetLower:  snv.OffsetLower,
	}
	// 还原绝对 NeedleId = 相对 ID + Section 起始 ID
	return new_map.NeedleValue{Key: NeedleId(snv.Key) + cs.start, Offset: offset, Size: snv.Size}
}

// toSectionalNeedleValue 将 NeedleValue 转换为 SectionalNeedleValue
// 主要是将绝对 NeedleId 转换为分段内的相对 ID
// 参数:
//   - nv: 完整的 Needle 索引信息
//   - cs: 目标 CompactSection（提供起始 ID）
// 返回:
//   - SectionalNeedleValue: 分段内的 Needle 值
func toSectionalNeedleValue(nv new_map.NeedleValue, cs *CompactSection) SectionalNeedleValue {
	return SectionalNeedleValue{
		// 计算相对 ID = 绝对 ID - Section 起始 ID
		Key:          SectionalNeedleId(nv.Key - cs.start),
		OffsetLower:  nv.Offset.OffsetLower,
		Size:         nv.Size,
		OffsetHigher: nv.Offset.OffsetHigher,
	}
}
