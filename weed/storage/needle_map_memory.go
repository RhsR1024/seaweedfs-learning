// Package storage 实现 SeaweedFS 的存储引擎
//
// 本文件实现基于内存的 Needle Map（内存索引）
//
// 核心概念：
//
// 【Needle Map 的作用】
// Needle Map 是 SeaweedFS 中最关键的索引结构，用于快速查找 Needle 的位置。
// 它维护了 Needle ID 到磁盘偏移量的映射关系，是 O(1) 查找的基础。
//
// 【内存索引 vs LevelDB 索引】
//
// 1. 内存索引（本文件）：
//    - 优点：查找速度极快（纯内存操作，O(1)）
//    - 优点：没有磁盘 I/O 延迟
//    - 缺点：占用大量内存（每个 Needle 约 16 字节）
//    - 适用场景：小型 Volume、高性能要求
//
// 2. LevelDB 索引：
//    - 优点：内存占用小（大部分数据在磁盘）
//    - 优点：支持超大 Volume（数亿个 Needle）
//    - 缺点：查找有磁盘 I/O（但 LevelDB 有缓存）
//    - 适用场景：大型 Volume、内存有限
//
// 【内存占用估算】
//
// 每个 Needle 的索引条目占用约 16 字节：
// - NeedleId: 8 字节（uint64）
// - Offset: 5 字节（压缩后）
// - Size: 3 字节（压缩后）
//
// 示例：
// - 1000 万个文件 ≈ 160 MB 内存
// - 1 亿个文件 ≈ 1.6 GB 内存
// - 10 亿个文件 ≈ 16 GB 内存
//
// 【索引文件格式】
//
// .idx 文件存储所有索引条目，每个条目 16 字节：
// - Bytes 0-7: Needle ID（8 字节，big-endian）
// - Bytes 8-11: Offset（4 字节，单位：8 字节块）
// - Bytes 12-15: Size（4 字节，实际大小）
//
// 注意：Offset 是"压缩"的，实际偏移量 = Offset * 8
//
// 【索引加载流程】
//
// 1. 启动时扫描 .idx 文件
// 2. 逐条读取索引记录
// 3. 加载到内存的 HashMap 中
// 4. 更新统计信息（文件数、删除数、字节数）
//
// 【索引更新机制】
//
// 写入新 Needle：
// 1. 追加到 .dat 文件
// 2. 更新内存 HashMap
// 3. 追加索引记录到 .idx 文件
//
// 删除 Needle：
// 1. 追加删除标记到 .dat 文件
// 2. 更新内存 HashMap（标记为删除）
// 3. 追加删除记录到 .idx 文件（Size = TombstoneFileSize）
//
// 【CompactMap 的优化】
//
// CompactMap 是 SeaweedFS 的自定义 HashMap 实现，相比标准 map[uint64]NeedleValue：
// - 更紧凑的内存布局（减少指针开销）
// - 更好的缓存局部性（提高 CPU 缓存命中率）
// - 更快的序列化/反序列化
//
// 【线程安全性】
//
// NeedleMap 本身不是线程安全的，需要外部同步：
// - Volume 使用 dataFileAccessLock 保护所有 NeedleMap 操作
// - 同一时间只有一个 goroutine 可以读写 NeedleMap
//
// 使用场景：
// - 默认的索引实现（内存充足时）
// - 高性能读写场景
// - 小到中型 Volume（< 1 亿个文件）
package storage

import (
	"os"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

// NeedleMap - 基于内存的 Needle 索引实现
//
// 结构说明：
// NeedleMap 是 SeaweedFS 默认的索引实现，将所有 Needle 的位置信息存储在内存中。
//
// 字段说明：
//
// 【baseNeedleMapper】（嵌入）
// 提供索引的基础功能和统计信息：
// - indexFile: .idx 索引文件句柄
// - indexFileOffset: 当前 .idx 文件的写入偏移量
// - FileCounter: 有效文件数量
// - FileByteCounter: 有效文件的总字节数
// - DeletionCounter: 已删除文件数量
// - DeletionByteCounter: 已删除文件的总字节数
// - MaxFileKey: 最大的 Needle ID（用于生成新 ID）
//
// 【m needle_map.NeedleValueMap】（核心数据结构）
// 实际存储索引数据的 HashMap：
// - 键：NeedleId（uint64，文件的唯一标识符）
// - 值：NeedleValue（包含 Offset 和 Size）
//
// NeedleValue 结构：
// - Offset: Needle 在 .dat 文件中的偏移量（压缩存储，实际偏移 = Offset * 8）
// - Size: Needle 的数据大小（不包含头部和填充）
//
// 内存布局：
// NeedleMap 使用 CompactMap 实现，优化了内存使用：
// - 每个条目约 16 字节（NeedleId 8B + Offset 5B + Size 3B）
// - 1000 万个文件 ≈ 160 MB 内存
// - 1 亿个文件 ≈ 1.6 GB 内存
//
// 性能特性：
// - 查找：O(1) 时间复杂度（HashMap）
// - 插入：O(1) 时间复杂度
// - 删除：O(1) 时间复杂度
// - 空间：O(n) 空间复杂度，n 为 Needle 数量
//
// 并发安全：
// - NeedleMap 本身不是线程安全的
// - 需要外部同步（Volume.dataFileAccessLock）
//
// 持久化：
// - 内存数据的变更会同步追加到 .idx 文件
// - 重启时从 .idx 文件重新加载到内存
//
// 使用场景：
// - 默认的索引类型（内存充足时）
// - 高性能读写场景
// - 小到中型 Volume（< 5000 万个文件）
type NeedleMap struct {
	baseNeedleMapper                 // 嵌入基础索引功能和统计信息
	m                needle_map.NeedleValueMap // 内存 HashMap，存储 NeedleId -> NeedleValue 映射
}

// NewCompactNeedleMap - 创建一个新的空内存索引
//
// 参数：
//   - file: .idx 索引文件句柄（用于持久化索引变更）
//
// 返回值：
//   - *NeedleMap: 新创建的内存索引实例
//
// 功能说明：
// 创建一个空的内存索引，初始化 CompactMap 数据结构。
// 这个方法用于创建新 Volume 或初始化空索引。
//
// 工作流程：
// 1. 创建 NeedleMap 实例
// 2. 初始化 CompactMap（空的 HashMap）
// 3. 设置索引文件句柄
// 4. 获取索引文件的当前大小（用于追加写入）
//
// CompactMap 特性：
// - 自定义的 HashMap 实现
// - 比 Go 标准 map 更节省内存
// - 更好的缓存局部性
// - 初始容量可动态扩展
//
// 索引文件偏移量：
// - indexFileOffset 记录当前 .idx 文件的写入位置
// - 新的索引条目会从这个位置开始追加
// - 每次写入后更新偏移量（+16 字节）
//
// 使用场景：
// - 创建新 Volume 时
// - Compaction 后重建索引时
// - 测试或调试时创建临时索引
//
// 注意事项：
// - 如果文件句柄无效，会触发 Fatal 错误
// - 初始索引为空，没有任何 Needle 记录
func NewCompactNeedleMap(file *os.File) *NeedleMap {
	// 步骤 1: 创建 NeedleMap 实例并初始化 CompactMap
	nm := &NeedleMap{
		m: needle_map.NewCompactMap(), // 创建空的 CompactMap
	}

	// 步骤 2: 设置索引文件句柄
	nm.indexFile = file

	// 步骤 3: 获取索引文件的当前大小
	stat, err := file.Stat()
	if err != nil {
		// 无法获取文件状态，这是严重错误，触发 Fatal
		glog.Fatalf("stat file %s: %v", file.Name(), err)
	}

	// 步骤 4: 记录当前文件偏移量（用于追加写入）
	nm.indexFileOffset = stat.Size()

	return nm
}

// LoadCompactNeedleMap - 从 .idx 文件加载内存索引
//
// 参数：
//   - file: .idx 索引文件句柄
//
// 返回值：
//   - *NeedleMap: 加载完成的内存索引
//   - error: 加载过程中的错误
//
// 功能说明：
// 这是 Volume 启动时加载索引的入口方法。
// 它会扫描整个 .idx 文件，将所有索引条目加载到内存中。
//
// 工作流程：
// 1. 创建空的 NeedleMap 实例
// 2. 调用 doLoading 扫描并加载索引文件
// 3. 返回加载完成的索引和可能的错误
//
// 性能特性：
// - 启动时间：约 1-2 秒 / GB 索引文件（取决于磁盘速度）
// - 内存占用：每个 Needle 约 16 字节
// - 大文件：1 亿个 Needle 的索引文件约 1.6 GB
//
// 错误处理：
// - 如果索引文件损坏，返回错误
// - 如果内存不足，可能触发 OOM
// - 建议使用 LevelDB 索引处理超大 Volume
//
// 使用场景：
// - Volume Server 启动时加载现有 Volume
// - Volume 从只读模式切换到读写模式
// - 测试或恢复 Volume 数据
func LoadCompactNeedleMap(file *os.File) (*NeedleMap, error) {
	// 步骤 1: 创建空的 NeedleMap
	nm := NewCompactNeedleMap(file)

	// 步骤 2: 加载索引文件内容到内存
	return doLoading(file, nm)
}

// doLoading - 扫描索引文件并加载到内存（内部实现）
//
// 参数：
//   - file: .idx 索引文件句柄
//   - nm: 要填充的 NeedleMap 实例
//
// 返回值：
//   - *NeedleMap: 填充完成的 NeedleMap
//   - error: 加载过程中的错误
//
// 功能说明：
// 这是索引加载的核心实现，逐条扫描 .idx 文件并构建内存索引。
// 同时更新统计信息（文件数、删除数、字节数）。
//
// 工作流程：
// 1. 调用 WalkIndexFile 遍历 .idx 文件的所有索引条目
// 2. 对每个条目调用回调函数处理
// 3. 更新 MaxFileKey（用于生成新 Needle ID）
// 4. 根据条目类型（有效/删除）更新索引和统计
//
// 【索引条目处理逻辑】
//
// 有效条目（!offset.IsZero() && !size.IsDeleted()）：
// - 将 Needle 添加到内存索引
// - FileCounter++（有效文件数+1）
// - FileByteCounter += size（累加文件大小）
// - 如果是覆盖（oldOffset 存在），DeletionCounter++
//
// 删除条目（offset.IsZero() || size.IsDeleted()）：
// - 从内存索引中删除 Needle
// - DeletionCounter++（删除文件数+1）
// - DeletionByteCounter += oldSize（累加删除大小）
//
// 【统计信息说明】
//
// FileCounter: 当前有效的 Needle 数量
// FileByteCounter: 当前有效 Needle 的总大小
// DeletionCounter: 累计删除的 Needle 数量（包括覆盖）
// DeletionByteCounter: 累计删除的 Needle 总大小
// MaxFileKey: 最大的 Needle ID（用于生成新 ID）
//
// 【索引文件格式】
//
// 每个索引条目 16 字节：
// - Bytes 0-7: Needle ID（8 字节）
// - Bytes 8-11: Offset（4 字节，压缩格式）
// - Bytes 12-15: Size（4 字节）
//
// 特殊值：
// - offset.IsZero(): 偏移量为 0，表示删除标记
// - size.IsDeleted(): Size 为负数或特殊值，表示已删除
// - TombstoneFileSize: 删除标记的特殊大小值
//
// 【性能特性】
//
// 加载速度：
// - 约 1-2 秒 / GB 索引文件（SSD）
// - 约 3-5 秒 / GB 索引文件（HDD）
// - 1 亿个 Needle ≈ 1.6 GB ≈ 2-3 秒加载时间
//
// 内存占用：
// - 每个 Needle 约 16 字节
// - 1000 万个 Needle ≈ 160 MB
// - 1 亿个 Needle ≈ 1.6 GB
//
// 使用场景：
// - LoadCompactNeedleMap 的内部实现
// - Volume 启动时加载索引
// - 索引恢复和验证
func doLoading(file *os.File, nm *NeedleMap) (*NeedleMap, error) {
	// 遍历索引文件的所有条目
	e := idx.WalkIndexFile(file, 0, func(key NeedleId, offset Offset, size Size) error {
		// 更新最大文件键（用于生成新 ID）
		nm.MaybeSetMaxFileKey(key)

		// 判断是有效条目还是删除条目
		if !offset.IsZero() && !size.IsDeleted() {
			// 【有效条目处理】
			// 增加文件计数
			nm.FileCounter++
			// 累加文件大小
			nm.FileByteCounter = nm.FileByteCounter + uint64(size)

			// 将 Needle 添加到内存索引（可能覆盖旧值）
			oldOffset, oldSize := nm.m.Set(NeedleId(key), offset, size)

			// 如果是覆盖操作（旧值存在且有效）
			if !oldOffset.IsZero() && oldSize.IsValid() {
				// 旧值被覆盖，相当于删除了旧 Needle
				nm.DeletionCounter++
				nm.DeletionByteCounter = nm.DeletionByteCounter + uint64(oldSize)
			}
		} else {
			// 【删除条目处理】
			// 从内存索引中删除 Needle
			oldSize := nm.m.Delete(NeedleId(key))

			// 更新删除统计
			nm.DeletionCounter++
			nm.DeletionByteCounter = nm.DeletionByteCounter + uint64(oldSize)
		}

		return nil
	})

	// 打印加载统计信息
	glog.V(1).Infof("max file key: %v count: %d deleted: %d for file: %s",
		nm.MaxFileKey(), nm.FileCount(), nm.DeletedCount(), file.Name())

	return nm, e
}

// Put - 添加或更新 Needle 的索引记录
//
// 参数：
//   - key: Needle ID（文件的唯一标识符）
//   - offset: Needle 在 .dat 文件中的偏移量
//   - size: Needle 的数据大小
//
// 返回值：
//   - error: 错误信息（通常为 nil）
//
// 功能说明：
// 在内存索引中添加或更新一个 Needle 的位置信息。
// 同时将索引记录追加到 .idx 文件，保证持久化。
//
// 工作流程：
// 1. 更新内存 HashMap（Set 操作）
// 2. 调用 logPut 更新统计信息
// 3. 调用 appendToIndexFile 将记录追加到 .idx 文件
//
// 覆盖行为：
// - 如果 key 已存在，会覆盖旧值
// - 旧值会被计入 DeletionCounter（统计删除数）
// - 新值会更新 FileCounter 和 FileByteCounter
//
// 持久化：
// - 内存更新后立即追加到 .idx 文件
// - .idx 文件只追加，不修改旧记录
// - 最新的记录总是在文件末尾
//
// 性能特性：
// - 内存操作：O(1) 时间复杂度
// - 磁盘操作：追加写入，顺序 I/O
// - 无需 fsync（由 Volume 的 asyncWorker 批量处理）
//
// 线程安全：
// - 需要外部同步（Volume.dataFileAccessLock）
// - 不能并发调用 Put
//
// 使用场景：
// - Volume.writeNeedle2 写入新文件后更新索引
// - Volume.WriteNeedleBlob 恢复文件后更新索引
// - Compaction 后重建索引
func (nm *NeedleMap) Put(key NeedleId, offset Offset, size Size) error {
	// 步骤 1: 更新内存索引（返回旧值用于统计）
	_, oldSize := nm.m.Set(NeedleId(key), offset, size)

	// 步骤 2: 更新统计信息（FileCounter、DeletionCounter 等）
	nm.logPut(key, oldSize, size)

	// 步骤 3: 将索引记录追加到 .idx 文件
	return nm.appendToIndexFile(key, offset, size)
}

// Get - 查询 Needle 的索引记录
//
// 参数：
//   - key: Needle ID
//
// 返回值：
//   - element: NeedleValue 指针（包含 Offset 和 Size）
//   - ok: 是否找到（true 表示存在，false 表示不存在）
//
// 功能说明：
// 从内存索引中查询 Needle 的位置信息。
// 这是读取文件的第一步，获取 Needle 在 .dat 文件中的位置。
//
// 查询流程：
// 1. 在内存 HashMap 中查找 key
// 2. 如果找到，返回 NeedleValue 和 true
// 3. 如果未找到，返回 nil 和 false
//
// NeedleValue 字段：
// - Offset: Needle 在 .dat 文件中的偏移量（需要调用 ToActualOffset() 转换）
// - Size: Needle 的数据大小（不包含头部）
//
// 性能特性：
// - 时间复杂度：O(1)（HashMap 查找）
// - 纯内存操作，无磁盘 I/O
// - 非常快速（纳秒级）
//
// 线程安全：
// - 读操作相对安全，但建议外部同步
// - 与 Put/Delete 操作需要互斥
//
// 使用场景：
// - Volume.readNeedle 读取文件前查询位置
// - 文件存在性检查
// - 统计和分析工具
func (nm *NeedleMap) Get(key NeedleId) (element *needle_map.NeedleValue, ok bool) {
	// 从内存 HashMap 查询
	element, ok = nm.m.Get(NeedleId(key))
	return
}

// Delete - 删除 Needle 的索引记录
//
// 参数：
//   - key: Needle ID
//   - offset: 删除标记在 .dat 文件中的偏移量
//
// 返回值：
//   - error: 错误信息（通常为 nil）
//
// 功能说明：
// 在内存索引中删除 Needle 的记录，并将删除标记追加到 .idx 文件。
// 注意：这是"软删除"，原始数据仍在 .dat 文件中。
//
// 工作流程：
// 1. 从内存 HashMap 中删除 key
// 2. 调用 logDelete 更新统计信息
// 3. 将删除记录追加到 .idx 文件（Size = TombstoneFileSize）
//
// 软删除机制：
// - 内存索引中移除记录
// - .idx 文件追加删除标记（Size = TombstoneFileSize）
// - .dat 文件中的数据不会被物理删除
// - Compaction 时才会真正回收空间
//
// TombstoneFileSize：
// - 特殊的 Size 值，表示这是删除标记
// - 通常是负数或特殊标志位
// - 用于区分有效记录和删除记录
//
// 持久化：
// - 删除标记会追加到 .idx 文件
// - 重启后加载索引时会应用删除标记
//
// 性能特性：
// - 内存操作：O(1) 时间复杂度
// - 磁盘操作：追加写入，顺序 I/O
//
// 线程安全：
// - 需要外部同步（Volume.dataFileAccessLock）
// - 不能并发调用 Delete
//
// 使用场景：
// - Volume.deleteNeedle2 删除文件后更新索引
// - 用户删除文件操作
// - Compaction 时清理过期数据
func (nm *NeedleMap) Delete(key NeedleId, offset Offset) error {
	// 步骤 1: 从内存索引中删除（返回旧大小用于统计）
	deletedBytes := nm.m.Delete(NeedleId(key))

	// 步骤 2: 更新删除统计信息
	nm.logDelete(deletedBytes)

	// 步骤 3: 将删除标记追加到 .idx 文件（Size = TombstoneFileSize）
	return nm.appendToIndexFile(key, offset, TombstoneFileSize)
}

// Close - 关闭索引并同步到磁盘
//
// 功能说明：
// 关闭 .idx 索引文件，确保所有未写入的数据刷新到磁盘。
// 这是优雅关闭的一部分，保证索引数据不丢失。
//
// 工作流程：
// 1. 检查 indexFile 是否为 nil
// 2. 调用 Sync() 刷新操作系统缓存到磁盘
// 3. 调用 Close() 关闭文件句柄
//
// 同步操作：
// - Sync() 相当于 fsync 系统调用
// - 强制将缓冲区数据写入磁盘
// - 保证数据持久化（即使掉电也不丢失）
//
// 错误处理：
// - 如果 Sync() 失败，记录警告日志
// - 仍然会尝试 Close() 文件
// - Close() 的错误被忽略（使用 _ 丢弃）
//
// 使用场景：
// - Volume 关闭时
// - 优雅关闭流程
// - 切换索引类型前
//
// 注意事项：
// - 关闭后不能再调用 Put/Delete
// - 内存数据仍然保留（但无法持久化）
func (nm *NeedleMap) Close() {
	// 检查文件句柄是否有效
	if nm.indexFile == nil {
		return
	}

	// 记录文件名（用于日志）
	indexFileName := nm.indexFile.Name()

	// 同步数据到磁盘（fsync）
	if err := nm.indexFile.Sync(); err != nil {
		glog.Warningf("sync file %s failed, %v", indexFileName, err)
	}

	// 关闭文件句柄
	_ = nm.indexFile.Close()
}

// Destroy - 销毁索引并删除索引文件
//
// 功能说明：
// 关闭索引并删除 .idx 文件。
// 这是完全删除 Volume 的一部分。
//
// 工作流程：
// 1. 调用 Close() 关闭文件
// 2. 调用 os.Remove() 删除 .idx 文件
//
// 使用场景：
// - Volume.Destroy() 删除整个 Volume
// - 测试清理
// - 错误恢复（重建索引）
//
// 危险操作：
// - 会永久删除索引文件
// - 无法恢复（除非从 .dat 文件重建）
// - 使用前确认是否真的要删除
//
// 返回值：
// - 返回删除文件的错误
// - 如果文件不存在，返回 os.ErrNotExist
func (nm *NeedleMap) Destroy() error {
	// 关闭索引文件
	nm.Close()

	// 删除索引文件
	return os.Remove(nm.indexFile.Name())
}

// UpdateNeedleMap - 更新 Volume 的索引实现
// 用于切换索引类型或替换索引
//
// 参数：
//   - v: 要更新的 Volume
//   - indexFile: 新的 .idx 索引文件句柄
//   - opts: LevelDB 选项（本方法中未使用，保留用于接口兼容）
//   - ldbTimeout: LevelDB 超时设置（本方法中未使用）
//
// 返回值：
//   - error: 错误信息
//
// 功能说明：
// 将 Volume 的索引从一种实现切换到另一种（例如从 LevelDB 切换到内存索引）。
// 这个方法会关闭旧索引，安装新索引。
//
// 工作流程：
// 1. 关闭并清除 Volume 的旧索引（v.nm）
// 2. 清理临时索引（v.tmpNm）
// 3. 设置新的索引文件句柄
// 4. 获取索引文件的当前大小
// 5. 将当前 NeedleMap 设置为 Volume 的索引
//
// 索引切换场景：
// - 从 LevelDB 切换到内存索引（提高性能）
// - 从内存索引切换到 LevelDB（节省内存）
// - Compaction 后重建索引
// - 索引损坏时恢复
//
// tmpNm 的作用：
// - v.tmpNm 用于存储临时索引（如 Compaction 过程中）
// - 切换完成后会被清理
//
// 注意事项：
// - 此方法假设 nm（当前对象）已经包含正确的索引数据
// - 旧索引会被关闭，数据不会迁移
// - 调用前需要确保新索引已正确加载
//
// 线程安全：
// - 需要外部同步（在 Volume 级别）
// - 不能在读写操作进行时调用
func (nm *NeedleMap) UpdateNeedleMap(v *Volume, indexFile *os.File, opts *opt.Options, ldbTimeout int64) error {
	// 步骤 1: 关闭并清除 Volume 的旧索引
	if v.nm != nil {
		v.nm.Close()
		v.nm = nil
	}

	// 步骤 2: 清理临时索引（使用 defer 确保执行）
	defer func() {
		if v.tmpNm != nil {
			v.tmpNm.Close()
			v.tmpNm = nil
		}
	}()

	// 步骤 3: 设置新的索引文件句柄
	nm.indexFile = indexFile

	// 步骤 4: 获取索引文件的当前大小
	stat, err := indexFile.Stat()
	if err != nil {
		glog.Fatalf("stat file %s: %v", indexFile.Name(), err)
		return err
	}
	nm.indexFileOffset = stat.Size()

	// 步骤 5: 将当前 NeedleMap 设置为 Volume 的索引
	v.nm = nm
	v.tmpNm = nil

	return nil
}

// DoOffsetLoading - 从指定偏移量开始增量加载索引
// 用于索引的增量更新和同步
//
// 参数：
//   - v: 要更新的 Volume（本方法中未使用）
//   - indexFile: .idx 索引文件句柄
//   - startFrom: 开始读取的字节偏移量
//
// 返回值：
//   - error: 加载过程中的错误
//
// 功能说明：
// 从 .idx 文件的指定偏移量开始读取索引记录，增量更新内存索引。
// 这个方法用于加载新追加的索引记录，而不是重新加载整个索引。
//
// 工作流程：
// 1. 打印日志记录开始偏移量
// 2. 调用 WalkIndexFile 从 startFrom 开始遍历
// 3. 对每个索引条目调用回调函数处理
// 4. 更新 MaxFileKey、FileCounter 等统计信息
//
// 【增量加载逻辑】
//
// 有效条目处理：
// - 更新 MaxFileKey（可能发现更大的 ID）
// - FileCounter++（文件数+1）
// - FileByteCounter += size（累加大小）
// - 如果是覆盖，更新 DeletionCounter
//
// 删除条目处理：
// - 从内存索引中删除记录
// - DeletionCounter++
// - DeletionByteCounter += oldSize
//
// 【使用场景】
//
// 1. Volume 复制同步：
//    - Replica 从 Master 同步新增的索引记录
//    - 只加载增量部分，不重新加载全部
//
// 2. 索引恢复：
//    - 从上次同步点继续加载
//    - 避免重复处理已加载的记录
//
// 3. 热重载：
//    - 动态加载新写入的索引记录
//    - 不需要重启 Volume
//
// 【偏移量计算】
//
// startFrom 的含义：
// - .idx 文件中的字节偏移量
// - 每个索引记录 16 字节
// - 应该是 16 的倍数（否则可能读取错误）
//
// 示例：
// - startFrom = 0: 从头加载（完整加载）
// - startFrom = 160: 跳过前 10 个记录（160 / 16 = 10）
// - startFrom = fileSize: 不加载任何记录（已同步）
//
// 【性能特性】
//
// 加载速度：
// - 只读取增量部分，速度快
// - 约 1-2 秒 / GB（SSD）
//
// 内存占用：
// - 只增加新记录的内存（每个约 16 字节）
// - 不会重新分配整个 HashMap
//
// 【注意事项】
//
// 1. 统计信息的准确性：
//    - FileCounter 是累加的，可能不准确
//    - 如果需要准确统计，需要完整重新加载
//
// 2. 索引一致性：
//    - 假设 startFrom 之前的记录已正确加载
//    - 如果基础索引有问题，增量加载也会有问题
//
// 3. 线程安全：
//    - 需要外部同步（Volume.dataFileAccessLock）
//    - 不能在读写操作进行时调用
func (nm *NeedleMap) DoOffsetLoading(v *Volume, indexFile *os.File, startFrom uint64) error {
	// 打印日志：记录开始加载的偏移量
	glog.V(0).Infof("loading idx from offset %d for file: %s", startFrom, indexFile.Name())

	// 从指定偏移量开始遍历索引文件
	e := idx.WalkIndexFile(indexFile, startFrom, func(key NeedleId, offset Offset, size Size) error {
		// 更新最大文件键
		nm.MaybeSetMaxFileKey(key)

		// 增加文件计数（注意：这里总是增加，不区分有效/删除）
		nm.FileCounter++

		// 判断是有效条目还是删除条目
		if !offset.IsZero() && size.IsValid() {
			// 【有效条目处理】
			// 累加文件大小
			nm.FileByteCounter = nm.FileByteCounter + uint64(size)

			// 将 Needle 添加到内存索引（可能覆盖旧值）
			oldOffset, oldSize := nm.m.Set(NeedleId(key), offset, size)

			// 如果是覆盖操作（旧值存在且有效）
			if !oldOffset.IsZero() && oldSize.IsValid() {
				// 旧值被覆盖，相当于删除了旧 Needle
				nm.DeletionCounter++
				nm.DeletionByteCounter = nm.DeletionByteCounter + uint64(oldSize)
			}
		} else {
			// 【删除条目处理】
			// 从内存索引中删除 Needle
			oldSize := nm.m.Delete(NeedleId(key))

			// 更新删除统计
			nm.DeletionCounter++
			nm.DeletionByteCounter = nm.DeletionByteCounter + uint64(oldSize)
		}

		return nil
	})

	return e
}
