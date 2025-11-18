// disk_location_ec.go - EC (Erasure Coding，纠删码) 卷的磁盘位置管理
//
// 本文件实现了 DiskLocation 对 EC 卷的管理功能，包括：
// 1. EC 卷和分片的查找、加载、卸载
// 2. EC 卷的验证和清理
// 3. 处理分布式 EC 和本地 EC 的不同场景
//
// EC 存储核心概念：
// - EC 卷由多个分片（shard）组成，默认 10 个数据分片 + 4 个校验分片
// - 分片文件命名：{collection}_{volumeId}.ec{shardId}，如 "myfiles_1.ec00"
// - 索引文件：.ecx (索引)、.ecj (日志)
// - 原始文件：.dat (EC 编码前的原始 volume 文件)
//
// 本地 EC vs 分布式 EC：
// - 本地 EC：所有分片在同一服务器，.dat 文件被删除后仍保留在本地
// - 分布式 EC：分片分散在多个服务器，每个服务器只保留部分分片
// - 区分方法：检查 .dat 文件是否存在
//   * .dat 存在 -> 可能是未完成的 EC 编码，需要验证分片完整性
//   * .dat 不存在 -> 正常的 EC 卷（可能是分布式的）
package storage

import (
	"fmt"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"

	"slices"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

var (
	// re - EC 分片文件扩展名的正则表达式
	// 匹配 .ec00 到 .ec999 (当前只使用 .ec00-.ec31)
	// 使用 \d{2,3} 是为了未来扩展，如果 MaxShardCount 超过 99 时能继续工作
	// 示例匹配：.ec00, .ec01, ..., .ec31 (当前范围), .ec100 (未来可能)
	re = regexp.MustCompile(`\.ec\d{2,3}`)
)

// FindEcVolume - 查找指定 ID 的 EC 卷
//
// 功能说明：
// 在当前磁盘位置的 EC 卷列表中查找指定 VolumeId 的 EC 卷。
// 这是一个线程安全的只读操作。
//
// 参数：
//   - vid: 要查找的 Volume ID
//
// 返回值：
//   - *erasure_coding.EcVolume: 找到的 EC 卷对象
//   - bool: 是否找到（true=找到，false=未找到）
//
// 并发安全：
// 使用读锁 (RLock) 保护，允许多个 goroutine 并发读取
//
// 使用场景：
// - 读取 EC 卷中的文件
// - 检查 EC 卷是否存在
// - 获取 EC 卷的元数据信息
func (l *DiskLocation) FindEcVolume(vid needle.VolumeId) (*erasure_coding.EcVolume, bool) {
	// 获取读锁，允许多个并发读取
	l.ecVolumesLock.RLock()
	defer l.ecVolumesLock.RUnlock()

	// 在 EC 卷 map 中查找指定的 VolumeId
	ecVolume, ok := l.ecVolumes[vid]
	if ok {
		// 找到了，返回 EC 卷对象和 true
		return ecVolume, true
	}
	// 未找到，返回 nil 和 false
	return nil, false
}

// DestroyEcVolume - 销毁指定的 EC 卷（删除内存和磁盘数据）
//
// 功能说明：
// 彻底删除 EC 卷，包括：
// 1. 调用 EC 卷的 Destroy() 方法删除磁盘上的所有分片文件
// 2. 从内存的 ecVolumes map 中移除该卷
//
// 参数：
//   - vid: 要销毁的 Volume ID
//
// 并发安全：
// 使用写锁 (Lock) 保护，确保独占访问
//
// 注意事项：
// - 这是一个破坏性操作，会永久删除磁盘上的文件
// - 与 unloadEcVolume 不同，此方法会删除磁盘文件
// - 如果 Volume 不存在，方法静默返回（不报错）
//
// 使用场景：
// - 删除不再需要的 EC 卷
// - 清理损坏的 EC 卷
// - Volume 迁移后清理源数据
func (l *DiskLocation) DestroyEcVolume(vid needle.VolumeId) {
	// 获取写锁，独占访问
	l.ecVolumesLock.Lock()
	defer l.ecVolumesLock.Unlock()

	// 查找要销毁的 EC 卷
	ecVolume, found := l.ecVolumes[vid]
	if found {
		// 调用 Destroy() 删除所有分片文件和索引文件
		ecVolume.Destroy()
		// 从内存 map 中移除
		delete(l.ecVolumes, vid)
	}
}

// unloadEcVolume - 从内存卸载 EC 卷（不删除磁盘文件）
//
// 功能说明：
// 从内存中卸载 EC 卷，但保留磁盘上的所有文件。
// 这对于分布式 EC 卷特别有用，因为分片可能在其他服务器上。
//
// 参数：
//   - vid: 要卸载的 Volume ID
//
// 并发安全：
// 使用写锁保护 map 操作，但在锁外关闭卷以避免在 I/O 时持有锁
//
// 实现细节：
// 1. 在锁内从 map 中移除卷的引用
// 2. 释放锁后再执行 Close() 操作
// 3. 这样避免了在持有写锁期间执行可能耗时的 I/O 操作
//
// 与 DestroyEcVolume 的区别：
// - unloadEcVolume: 只从内存卸载，保留磁盘文件
// - DestroyEcVolume: 同时删除内存和磁盘文件
//
// 使用场景：
// - 热重载配置时临时卸载卷
// - 清理加载失败的卷（保留文件供后续诊断）
// - 分布式 EC 场景中移除部分分片
func (l *DiskLocation) unloadEcVolume(vid needle.VolumeId) {
	var toClose *erasure_coding.EcVolume

	// 在锁内快速移除引用
	l.ecVolumesLock.Lock()
	if ecVolume, found := l.ecVolumes[vid]; found {
		toClose = ecVolume  // 保存引用供稍后关闭
		delete(l.ecVolumes, vid)  // 从 map 中移除
	}
	l.ecVolumesLock.Unlock()  // 尽快释放写锁

	// 在锁外关闭卷，避免在 I/O 操作期间持有写锁
	// Close() 可能包含：刷新缓冲区、关闭文件句柄等耗时操作
	if toClose != nil {
		toClose.Close()
	}
}

// CollectEcShards - 收集指定 EC 卷的所有分片文件名
//
// 功能说明：
// 遍历 EC 卷的所有分片，将分片文件的完整路径填充到提供的数组中。
// 这个方法通常用于获取 EC 卷的分片分布信息。
//
// 参数：
//   - vid: Volume ID
//   - shardFileNames: 预分配的字符串数组，用于存储分片文件名
//
// 返回值：
//   - ecVolume: EC 卷对象
//   - found: 是否找到该 EC 卷
//
// 实现细节：
// - 只填充 ShardId < len(shardFileNames) 的分片
// - 分片文件名格式：{collection}_{vid}.ec{shardId}
// - 使用读锁保护，允许并发读取
//
// 注意事项：
// - shardFileNames 数组必须预先分配足够大的空间
// - 如果数组太小，部分分片的文件名将被忽略
// - 数组的索引对应 ShardId
//
// 使用场景：
// - 检查 EC 卷的分片分布
// - 准备分片迁移操作
// - 诊断缺失的分片
func (l *DiskLocation) CollectEcShards(vid needle.VolumeId, shardFileNames []string) (ecVolume *erasure_coding.EcVolume, found bool) {
	// 获取读锁
	l.ecVolumesLock.RLock()
	defer l.ecVolumesLock.RUnlock()

	// 查找 EC 卷
	ecVolume, found = l.ecVolumes[vid]
	if !found {
		// 未找到，直接返回
		return
	}

	// 遍历该卷的所有分片
	for _, ecShard := range ecVolume.Shards {
		// 检查 ShardId 是否在数组范围内
		if ecShard.ShardId < erasure_coding.ShardId(len(shardFileNames)) {
			// 构造分片文件的完整路径
			// 格式：{directory}/{collection}_{vid}.ec{shardId}
			// 例如：/data/myfiles_1.ec00
			shardFileNames[ecShard.ShardId] = erasure_coding.EcShardFileName(ecVolume.Collection, l.Directory, int(ecVolume.VolumeId)) + erasure_coding.ToExt(int(ecShard.ShardId))
		}
	}
	return
}

// FindEcShard - 查找指定的 EC 分片
//
// 功能说明：
// 在指定的 EC 卷中查找特定的分片对象。
//
// 参数：
//   - vid: Volume ID
//   - shardId: 分片 ID（0-31，取决于 EC 配置）
//
// 返回值：
//   - *erasure_coding.EcVolumeShard: 分片对象
//   - bool: 是否找到
//
// 查找过程：
// 1. 先在 ecVolumes map 中查找 EC 卷
// 2. 然后在该卷的 Shards 列表中遍历查找匹配的 ShardId
//
// 并发安全：
// 使用读锁保护，允许并发读取
//
// 使用场景：
// - 读取特定分片的数据
// - 检查分片是否存在
// - 获取分片的元数据（如文件句柄、大小等）
func (l *DiskLocation) FindEcShard(vid needle.VolumeId, shardId erasure_coding.ShardId) (*erasure_coding.EcVolumeShard, bool) {
	// 获取读锁
	l.ecVolumesLock.RLock()
	defer l.ecVolumesLock.RUnlock()

	// 先查找 EC 卷
	ecVolume, ok := l.ecVolumes[vid]
	if !ok {
		// EC 卷不存在
		return nil, false
	}

	// 在该 EC 卷的分片列表中查找指定的分片
	for _, ecShard := range ecVolume.Shards {
		if ecShard.ShardId == shardId {
			// 找到匹配的分片
			return ecShard, true
		}
	}

	// 未找到指定的分片
	return nil, false
}

// LoadEcShard - 从磁盘加载单个 EC 分片
//
// 功能说明：
// 加载指定的 EC 分片到内存。如果 EC 卷不存在，则创建新的 EC 卷对象。
// 这个方法支持动态添加分片，常用于分布式 EC 场景。
//
// 参数：
//   - collection: 集合名称
//   - vid: Volume ID
//   - shardId: 要加载的分片 ID
//
// 返回值：
//   - *erasure_coding.EcVolume: 包含该分片的 EC 卷对象
//   - error: 加载失败的错误信息
//
// 加载流程：
// 1. 创建 EcVolumeShard 对象（打开分片文件）
// 2. 检查 EC 卷是否已存在
// 3. 如果不存在，创建新的 EC 卷
// 4. 将分片添加到 EC 卷
//
// 并发安全：
// 使用写锁保护，确保在添加分片时的原子性
//
// 错误处理：
// - os.ErrNotExist: 分片文件不存在
// - 其他错误: 文件损坏、权限问题等
//
// 使用场景：
// - 分布式 EC：从其他节点复制分片后加载
// - 动态添加分片以提高可用性
// - 恢复缺失的分片
func (l *DiskLocation) LoadEcShard(collection string, vid needle.VolumeId, shardId erasure_coding.ShardId) (*erasure_coding.EcVolume, error) {

	// 第 1 步：创建 EC 分片对象（打开分片文件）
	// 这会打开磁盘上的 .ec{shardId} 文件并读取其元数据
	ecVolumeShard, err := erasure_coding.NewEcVolumeShard(l.DiskType, l.Directory, collection, vid, shardId)
	if err != nil {
		if err == os.ErrNotExist {
			// 分片文件不存在，直接返回
			return nil, os.ErrNotExist
		}
		// 其他错误（如文件损坏、权限问题）
		return nil, fmt.Errorf("failed to create ec shard %d.%d: %v", vid, shardId, err)
	}

	// 第 2 步：获取写锁，准备修改 ecVolumes map
	l.ecVolumesLock.Lock()
	defer l.ecVolumesLock.Unlock()

	// 第 3 步：检查 EC 卷是否已存在
	ecVolume, found := l.ecVolumes[vid]
	if !found {
		// EC 卷不存在，创建新的 EC 卷对象
		// NewEcVolume 会加载 .ecx 索引文件和 .ecj 日志文件
		ecVolume, err = erasure_coding.NewEcVolume(l.DiskType, l.Directory, l.IdxDirectory, collection, vid)
		if err != nil {
			return nil, fmt.Errorf("failed to create ec volume %d: %v", vid, err)
		}
		// 将新创建的 EC 卷添加到 map
		l.ecVolumes[vid] = ecVolume
	}

	// 第 4 步：将分片添加到 EC 卷
	// 如果分片已存在，会被新的替换
	ecVolume.AddEcVolumeShard(ecVolumeShard)

	return ecVolume, nil
}

// UnloadEcShard - 卸载单个 EC 分片
//
// 功能说明：
// 从 EC 卷中移除指定的分片。如果移除后 EC 卷没有任何分片了，
// 则同时关闭并移除整个 EC 卷。
//
// 参数：
//   - vid: Volume ID
//   - shardId: 要卸载的分片 ID
//
// 返回值：
//   - bool: 总是返回 true（保持向后兼容）
//
// 卸载流程：
// 1. 查找 EC 卷
// 2. 从 EC 卷中删除指定的分片
// 3. 如果 EC 卷没有分片了，关闭并移除 EC 卷
//
// 并发安全：
// 使用写锁保护整个操作
//
// 注意事项：
// - 即使 EC 卷或分片不存在，也会返回 true
// - 这个方法不会删除磁盘上的文件，只是从内存卸载
//
// 使用场景：
// - 动态减少分片（释放内存）
// - 准备迁移分片到其他节点
// - 清理不再需要的分片
func (l *DiskLocation) UnloadEcShard(vid needle.VolumeId, shardId erasure_coding.ShardId) bool {

	// 获取写锁
	l.ecVolumesLock.Lock()
	defer l.ecVolumesLock.Unlock()

	// 查找 EC 卷
	ecVolume, found := l.ecVolumes[vid]
	if !found {
		// EC 卷不存在，返回 false
		return false
	}

	// 从 EC 卷中删除指定的分片
	// DeleteEcVolumeShard 返回 (shard, deleted)
	if _, deleted := ecVolume.DeleteEcVolumeShard(shardId); deleted {
		// 分片删除成功，检查是否还有其他分片
		if len(ecVolume.Shards) == 0 {
			// 没有分片了，从 map 中移除 EC 卷
			delete(l.ecVolumes, vid)
			// 关闭 EC 卷（关闭索引文件等）
			ecVolume.Close()
		}
		return true
	}

	// 即使分片不存在，也返回 true（保持原有行为）
	return true
}

// loadEcShards - 批量加载 EC 分片（内部方法）
//
// 功能说明：
// 从给定的分片文件名列表中解析分片 ID 并加载到内存。
// 这是一个内部辅助方法，由 loadAllEcShards 调用。
//
// 参数：
//   - shards: 分片文件名列表（如 ["myfiles_1.ec00", "myfiles_1.ec01"]）
//   - collection: 集合名称
//   - vid: Volume ID
//
// 返回值：
//   - error: 加载过程中的错误
//
// 加载流程：
// 1. 从文件名的扩展名中解析分片 ID（如 .ec00 -> 0）
// 2. 验证分片 ID 的范围（0-255）
// 3. 调用 LoadEcShard 加载每个分片
//
// 错误处理：
// - 如果任何分片加载失败，立即返回错误
// - 分片 ID 超出范围会返回错误
//
// 示例：
// 文件名 "myfiles_1.ec03" -> 分片 ID = 3
// 文件名 "myfiles_1.ec14" -> 分片 ID = 14
func (l *DiskLocation) loadEcShards(shards []string, collection string, vid needle.VolumeId) (err error) {

	// 遍历所有分片文件
	for _, shard := range shards {
		// 从文件扩展名中解析分片 ID
		// path.Ext(shard) 返回扩展名，如 ".ec03"
		// [3:] 跳过 ".ec" 前缀，获取数字部分 "03"
		shardId, err := strconv.ParseInt(path.Ext(shard)[3:], 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse ec shard name %v: %w", shard, err)
		}

		// 验证分片 ID 范围（必须能转换为 uint8）
		// ShardId 类型是 uint8，所以范围是 0-255
		if shardId < 0 || shardId > 255 {
			return fmt.Errorf("shard ID out of range: %d", shardId)
		}

		// 加载该分片到内存
		_, err = l.LoadEcShard(collection, vid, erasure_coding.ShardId(shardId))
		if err != nil {
			// 加载失败，返回错误
			return fmt.Errorf("failed to load ec shard %v: %w", shard, err)
		}
	}

	return nil
}

// loadAllEcShards - 从磁盘加载所有 EC 分片（启动时调用）
//
// 功能说明：
// 扫描数据目录和索引目录，识别并加载所有 EC 分片文件。
// 这个方法在 Volume Server 启动时调用，负责恢复所有 EC 卷的状态。
//
// 返回值：
//   - error: 扫描或加载过程中的错误
//
// 核心逻辑：
// 1. 读取数据目录和索引目录的所有文件
// 2. 按文件名排序（确保相同 Volume 的文件连续）
// 3. 将相同 VolumeId 的 EC 分片分组
// 4. 当遇到 .ecx 文件时，加载该组的所有分片
// 5. 处理孤立的分片（没有 .ecx 文件）
//
// 文件分组策略：
// - EC 分片文件: .ec00, .ec01, ..., .ec31
// - 索引文件: .ecx (Needle 索引)
// - 日志文件: .ecj (修改日志)
// - 原始文件: .dat (EC 编码前的原始 volume，可能已删除)
//
// 本地 EC vs 分布式 EC 的处理：
// - .dat 存在 + 分片不足: 未完成的 EC 编码，清理 EC 文件
// - .dat 不存在: 正常的 EC 卷（可能是分布式的）
// - .ecx 存在 + 分片存在: 尝试加载
// - 只有分片没有 .ecx: 孤立分片，如果 .dat 存在则清理
//
// 错误恢复：
// - 忽略 0 字节的分片文件（可能是创建过程中的错误文件）
// - 验证 EC 卷的完整性（分片数量、大小一致性）
// - 清理不完整或损坏的 EC 文件
func (l *DiskLocation) loadAllEcShards() (err error) {

	// 第 1 步：读取数据目录
	dirEntries, err := os.ReadDir(l.Directory)
	if err != nil {
		return fmt.Errorf("load all ec shards in dir %s: %v", l.Directory, err)
	}

	// 如果索引目录与数据目录不同，也读取索引目录
	// 这支持将索引文件存储在更快的 SSD 上
	if l.IdxDirectory != l.Directory {
		indexDirEntries, err := os.ReadDir(l.IdxDirectory)
		if err != nil {
			return fmt.Errorf("load all ec shards in dir %s: %v", l.IdxDirectory, err)
		}
		// 合并两个目录的文件列表
		dirEntries = append(dirEntries, indexDirEntries...)
	}

	// 第 2 步：按文件名排序
	// 排序确保相同 Volume 的文件（分片和索引）连续出现
	// 例如：myfiles_1.ec00, myfiles_1.ec01, myfiles_1.ecx, myfiles_2.ec00, ...
	slices.SortFunc(dirEntries, func(a, b os.DirEntry) int {
		return strings.Compare(a.Name(), b.Name())
	})

	// 第 3 步：初始化分片分组的状态变量
	var sameVolumeShards []string         // 当前 Volume 的分片文件列表
	var prevVolumeId needle.VolumeId      // 上一个处理的 Volume ID
	var prevCollection string              // 上一个处理的集合名称

	// 辅助函数：重置分组状态（开始处理新的 Volume）
	reset := func() {
		sameVolumeShards = nil
		prevVolumeId = 0
		prevCollection = ""
	}

	// 第 4 步：遍历所有文件，将分片分组
	for _, fileInfo := range dirEntries {
		// 跳过目录
		if fileInfo.IsDir() {
			continue
		}

		// 解析文件名和扩展名
		ext := path.Ext(fileInfo.Name())
		name := fileInfo.Name()
		baseName := name[:len(name)-len(ext)]

		// 从基础文件名中解析集合名和 Volume ID
		// 例如："myfiles_1" -> collection="myfiles", volumeId=1
		collection, volumeId, err := parseCollectionVolumeId(baseName)
		if err != nil {
			// 不是有效的 Volume 文件名，跳过
			continue
		}

		// 获取文件信息（用于检查文件大小）
		info, err := fileInfo.Info()
		if err != nil {
			// 无法获取文件信息，跳过
			continue
		}

		// 处理 EC 分片文件（.ec00 - .ec31）
		// 0 字节的文件应该只会错误地出现在 EC 数据文件中，所以忽略它们
		if re.MatchString(ext) && info.Size() > 0 {
			// 检查是否与上一个文件属于同一个 Volume
			// 将相同 collection 和 volumeId 的分片分组，避免混合不同集合
			if prevVolumeId == 0 || (volumeId == prevVolumeId && collection == prevCollection) {
				// 同一个 Volume，添加到当前分组
				sameVolumeShards = append(sameVolumeShards, fileInfo.Name())
			} else {
				// 不同的 Volume，先检查上一组分片是否有孤立的
				// （没有对应的 .ecx 文件）
				l.checkOrphanedShards(sameVolumeShards, prevCollection, prevVolumeId)
				// 开始新的分组
				sameVolumeShards = []string{fileInfo.Name()}
			}
			// 更新状态
			prevVolumeId = volumeId
			prevCollection = collection
			continue
		}

		// 处理 .ecx 索引文件
		// 这表示找到了完整的 EC 卷定义
		if ext == ".ecx" && volumeId == prevVolumeId && collection == prevCollection {
			// 找到了与当前分组匹配的 .ecx 文件
			// 尝试加载该组的所有分片
			l.handleFoundEcxFile(sameVolumeShards, collection, volumeId)
			// 重置状态，准备处理下一个 Volume
			reset()
			continue
		}

	}

	// 第 5 步：检查最后一组分片是否是孤立的
	// 在目录扫描结束时，检查最后一组分片是否没有对应的 .ecx 文件
	// 这处理了目录中最后一个 Volume 的情况
	l.checkOrphanedShards(sameVolumeShards, prevCollection, prevVolumeId)

	return nil
}

// deleteEcVolumeById - 根据 Volume ID 删除 EC 卷（内部方法）
//
// 功能说明：
// 从内存中删除指定的 EC 卷，并销毁其所有磁盘文件。
//
// 参数：
//   - vid: Volume ID
//
// 返回值：
//   - error: 删除过程中的错误（当前实现总是返回 nil）
//
// 删除流程：
// 1. 查找 EC 卷
// 2. 调用 Destroy() 删除所有磁盘文件
// 3. 从 ecVolumes map 中移除
//
// 并发安全：
// 使用写锁保护，因为会修改 ecVolumes map
//
// 注意事项：
// - 如果 EC 卷不存在，方法静默返回
// - 这是一个破坏性操作，会永久删除磁盘文件
func (l *DiskLocation) deleteEcVolumeById(vid needle.VolumeId) (e error) {
	// 获取写锁，因为要修改 ecVolumes map
	l.ecVolumesLock.Lock()
	defer l.ecVolumesLock.Unlock()

	// 查找 EC 卷
	ecVolume, ok := l.ecVolumes[vid]
	if !ok {
		// EC 卷不存在，直接返回
		return
	}

	// 调用 Destroy() 删除所有磁盘文件（分片、索引、日志）
	ecVolume.Destroy()

	// 从 map 中移除
	delete(l.ecVolumes, vid)

	return
}

// unmountEcVolumeByCollection - 根据集合名称卸载 EC 卷（内部方法）
//
// 功能说明：
// 从内存中卸载指定集合的所有 EC 卷，但不删除磁盘文件。
// 这通常用于集合级别的操作，如重新挂载或迁移。
//
// 参数：
//   - collectionName: 集合名称
//
// 返回值：
//   - map[needle.VolumeId]*erasure_coding.EcVolume: 被卸载的 EC 卷集合
//
// 实现细节：
// 1. 遍历所有 EC 卷，找出属于指定集合的卷
// 2. 将这些卷移到返回的 map 中
// 3. 从 ecVolumes map 中移除这些卷
//
// 注意事项：
// - 此方法没有使用锁保护，调用者需要确保线程安全
// - 不删除磁盘文件，只是从内存卸载
// - 返回的 EC 卷对象仍然有效，可以重新添加或关闭
//
// 使用场景：
// - 集合级别的重新挂载
// - 集合迁移前的准备
// - 集合配置更新
func (l *DiskLocation) unmountEcVolumeByCollection(collectionName string) map[needle.VolumeId]*erasure_coding.EcVolume {
	// 创建用于存储被卸载的 EC 卷的 map
	deltaVols := make(map[needle.VolumeId]*erasure_coding.EcVolume, 0)

	// 遍历所有 EC 卷，找出属于指定集合的卷
	for k, v := range l.ecVolumes {
		if v.Collection == collectionName {
			// 将匹配的卷添加到返回的 map
			deltaVols[k] = v
		}
	}

	// 从 ecVolumes map 中移除这些卷
	for k, _ := range deltaVols {
		delete(l.ecVolumes, k)
	}

	return deltaVols
}

// EcShardCount - 获取当前磁盘位置的 EC 分片总数
//
// 功能说明：
// 统计当前磁盘位置上所有 EC 卷的分片总数。
// 这个计数用于监控和负载均衡。
//
// 返回值：
//   - int: 分片总数
//
// 计算方式：
// 遍历所有 EC 卷，累加每个卷的分片数量
//
// 并发安全：
// 使用读锁保护，允许并发读取
//
// 使用场景：
// - 监控磁盘使用情况
// - 负载均衡决策
// - 容量规划
// - 统计报告
func (l *DiskLocation) EcShardCount() int {
	// 获取读锁
	l.ecVolumesLock.RLock()
	defer l.ecVolumesLock.RUnlock()

	// 累加所有 EC 卷的分片数量
	shardCount := 0
	for _, ecVolume := range l.ecVolumes {
		shardCount += len(ecVolume.Shards)
	}

	return shardCount
}

// handleFoundEcxFile - 处理找到 .ecx 文件时的 EC 分片组
//
// 功能说明：
// 当扫描到 .ecx 索引文件时，验证并加载对应的 EC 分片。
// 这个方法包含了完整的验证、加载和清理逻辑。
//
// 参数：
//   - shards: 该 Volume 的分片文件名列表
//   - collection: 集合名称
//   - volumeId: Volume ID
//
// 核心逻辑：
// 1. 检查 .dat 文件是否存在（区分本地 EC 和分布式 EC）
// 2. 如果 .dat 存在，验证 EC 卷的完整性
// 3. 尝试加载 EC 分片
// 4. 如果加载失败且 .dat 存在，清理 EC 文件以便使用原始 .dat
//
// 本地 EC vs 分布式 EC 的处理：
//
// 【本地 EC（.dat 文件存在）】
// - 场景：EC 编码可能未完成或失败
// - 验证：检查分片数量、大小一致性、与 .dat 文件的匹配度
// - 失败处理：清理所有 EC 文件，保留 .dat 文件继续提供服务
// - 原因：避免使用损坏的 EC 数据，确保数据可用性
//
// 【分布式 EC（.dat 文件不存在）】
// - 场景：正常的分布式 EC 卷，分片分散在多个服务器
// - 验证：跳过验证（允许部分分片）
// - 失败处理：只卸载内存中的数据，保留磁盘文件
// - 原因：等待从其他服务器获取缺失的分片
//
// 错误恢复策略：
// - 验证失败 + .dat 存在：清理 EC 文件，使用 .dat
// - 加载失败 + .dat 存在：清理 EC 文件，使用 .dat
// - 加载失败 + .dat 不存在：仅卸载，保留文件供后续恢复
func (l *DiskLocation) handleFoundEcxFile(shards []string, collection string, volumeId needle.VolumeId) {
	// 第 1 步：构造基础文件名和 .dat 文件路径
	// 基础文件名格式：{directory}/{collection}_{volumeId}
	// 例如：/data/myfiles_1
	baseFileName := erasure_coding.EcShardFileName(collection, l.Directory, int(volumeId))
	datFileName := baseFileName + ".dat"

	// 第 2 步：检查 .dat 文件是否存在
	// 这是区分本地 EC 和分布式 EC 的关键
	// 注意：意外的错误（权限、I/O）被视为"存在"，这是更安全的回退策略
	datExists := l.checkDatFileExists(datFileName)

	// 第 3 步：如果 .dat 存在，验证 EC 卷的完整性
	// 验证内容包括：
	// - 分片数量是否足够（至少需要 DataShardsCount 个数据分片）
	// - 所有分片大小是否一致（Reed-Solomon 编码要求）
	// - 分片大小是否与 .dat 文件大小匹配
	if datExists && !l.validateEcVolume(collection, volumeId) {
		// 验证失败：EC 卷不完整或损坏
		// 警告：.dat 存在但验证失败，清理 EC 文件
		glog.Warningf("Incomplete or invalid EC volume %d: .dat exists but validation failed, cleaning up EC files...", volumeId)
		// 删除所有 EC 相关文件（.ecx, .ecj, .ec00-31）
		// 保留 .dat 文件，让系统继续使用原始 volume
		l.removeEcVolumeFiles(collection, volumeId)
		return
	}

	// 第 4 步：尝试加载 EC 分片到内存
	if err := l.loadEcShards(shards, collection, volumeId); err != nil {
		// 加载失败的处理策略取决于 .dat 是否存在

		if datExists {
			// 【本地 EC 场景】.dat 存在，表示这是未完成的 EC 编码
			// 策略：清理 EC 文件，让系统回退到使用 .dat 文件
			// 理由：确保数据可用性，避免使用损坏的 EC 数据
			glog.Warningf("Failed to load EC shards for volume %d and .dat exists: %v, cleaning up EC files to use .dat...", volumeId, err)

			// 先卸载内存中的部分加载状态（释放文件句柄）
			l.unloadEcVolume(volumeId)

			// 然后删除磁盘上的 EC 文件
			l.removeEcVolumeFiles(collection, volumeId)
		} else {
			// 【分布式 EC 场景】.dat 不存在，这可能是正常情况
			// 策略：只卸载内存状态，保留磁盘文件
			// 理由：
			// 1. 分片可能在其他服务器上，等待从网络获取
			// 2. 临时的加载失败不应该删除已有的分片文件
			// 3. 保留文件供后续重试或恢复
			glog.Warningf("Failed to load EC shards for volume %d: %v (this may be normal for distributed EC volumes)", volumeId, err)

			// 清理内存中任何部分加载的状态
			// 注意：这不会删除磁盘文件，只是从内存卸载
			l.unloadEcVolume(volumeId)
		}
		return
	}

	// 加载成功！EC 卷现在可用于读取操作
}

// checkDatFileExists - 检查 .dat 文件是否存在（健壮的错误处理）
//
// 功能说明：
// 检查指定的 .dat 文件是否存在，并对意外错误进行特殊处理。
//
// 参数：
//   - datFileName: .dat 文件的完整路径
//
// 返回值：
//   - bool: true=文件存在（或无法确定），false=文件确定不存在
//
// 错误处理策略：
// - 文件存在：返回 true
// - 文件不存在（os.IsNotExist）：返回 false
// - 其他错误（权限、I/O 等）：返回 true（安全回退）
//
// 为什么意外错误返回 true？
// 这是一个保守的策略，避免将本地 EC 误判为分布式 EC：
// - 如果误判为分布式 EC，系统可能会删除 .dat 文件
// - 如果误判为本地 EC，只是多做一些验证，更安全
// - 当无法确定时，选择更安全的假设
//
// 使用场景：
// - handleFoundEcxFile: 区分本地 EC 和分布式 EC
// - checkOrphanedShards: 判断是否清理孤立分片
func (l *DiskLocation) checkDatFileExists(datFileName string) bool {
	// 尝试获取文件信息
	if _, err := os.Stat(datFileName); err == nil {
		// 文件存在
		return true
	} else if !os.IsNotExist(err) {
		// 意外错误（不是"文件不存在"）
		// 可能的原因：权限问题、I/O 错误、网络文件系统故障等
		glog.Warningf("Failed to stat .dat file %s: %v", datFileName, err)
		// 保守策略：假设文件存在，避免误删除
		return true
	}
	// 文件确实不存在
	return false
}

// checkOrphanedShards - 检查并清理孤立的 EC 分片
//
// 功能说明：
// 检查给定的分片是否是孤立的（没有对应的 .ecx 索引文件），
// 如果是孤立的且 .dat 文件存在，则清理这些分片。
//
// 参数：
//   - shards: 分片文件名列表
//   - collection: 集合名称
//   - volumeId: Volume ID
//
// 返回值：
//   - bool: true=发现并清理了孤立分片，false=没有孤立分片
//
// 什么是孤立分片？
// 孤立分片是指有 .ec* 分片文件，但没有对应的 .ecx 索引文件的情况。
// 这通常发生在 EC 编码过程被中断时：
// 1. EC 编码器创建了一些分片文件
// 2. 在创建 .ecx 文件之前进程崩溃或被终止
// 3. 留下了不完整的分片文件
//
// 清理策略：
// - .dat 存在 + 有孤立分片：清理分片（未完成的编码）
// - .dat 不存在 + 有孤立分片：保留分片（可能是分布式 EC）
//
// 为什么要清理？
// - 孤立分片没有索引，无法使用
// - 占用磁盘空间
// - 可能干扰后续的 EC 编码尝试
// - 如果 .dat 存在，应该使用 .dat 而不是不完整的分片
//
// 使用场景：
// - loadAllEcShards: 处理目录扫描时发现的孤立分片
// - 每次完成一个 Volume 的文件扫描后调用
func (l *DiskLocation) checkOrphanedShards(shards []string, collection string, volumeId needle.VolumeId) bool {
	// 检查输入有效性
	if len(shards) == 0 || volumeId == 0 {
		// 没有分片或无效的 Volume ID
		return false
	}

	// 构造 .dat 文件路径
	baseFileName := erasure_coding.EcShardFileName(collection, l.Directory, int(volumeId))
	datFileName := baseFileName + ".dat"

	// 检查 .dat 文件是否存在
	if l.checkDatFileExists(datFileName) {
		// .dat 文件存在，说明这是未完成的 EC 编码
		// 孤立分片是编码中断的遗留物，应该清理
		glog.Warningf("Found %d EC shards without .ecx file for volume %d (incomplete encoding interrupted before .ecx creation), cleaning up...",
			len(shards), volumeId)

		// 清理所有 EC 相关文件（分片、索引、日志）
		// 保留 .dat 文件，让系统继续使用原始 volume
		l.removeEcVolumeFiles(collection, volumeId)
		return true
	}

	// .dat 文件不存在，可能是分布式 EC 的部分分片
	// 不清理，保留这些分片供后续使用
	return false
}

// calculateExpectedShardSize - 根据 .dat 文件大小计算预期的分片大小
//
// 功能说明：
// 根据原始 .dat 文件的大小，计算 EC 编码后每个分片应该有的精确大小。
// 这个计算是确定性的，基于 SeaweedFS 的 EC 编码算法。
//
// 参数：
//   - datFileSize: .dat 文件的大小（字节）
//
// 返回值：
//   - int64: 预期的单个分片大小（字节）
//
// EC 编码过程（确定性）：
//
// 【第 1 阶段：处理大块数据】
// - 批次大小：LargeBlockSize * DataShardsCount = 1GB * 10 = 10GB
// - 处理方式：将数据分成 10GB 的批次
// - 每批次产生：每个分片增加 LargeBlockSize (1GB)
// - 继续直到剩余数据 < 10GB
//
// 【第 2 阶段：处理剩余小块数据】
// - 批次大小：SmallBlockSize * DataShardsCount = 1MB * 10 = 10MB
// - 处理方式：将剩余数据分成 10MB 的批次（向上取整）
// - 每批次产生：每个分片增加 SmallBlockSize (1MB)
// - 不足的部分用零填充
//
// 【Reed-Solomon 编码特性】
// - 数据分片数：DataShardsCount = 10
// - 校验分片数：ParityShardsCount = 4
// - 所有分片（数据+校验）大小必须相同
// - 零填充确保分片对齐
//
// 计算示例：
//
// 示例 1：.dat 文件大小 = 25GB
// - 大块批次：25GB / 10GB = 2 批次
// - 大块贡献：2 * 1GB = 2GB
// - 剩余数据：25GB - 20GB = 5GB
// - 小块批次：ceil(5GB / 10MB) = 512 批次
// - 小块贡献：512 * 1MB = 512MB
// - 分片大小：2GB + 512MB = 2.5GB
//
// 示例 2：.dat 文件大小 = 500MB
// - 大块批次：0（不足 10GB）
// - 剩余数据：500MB
// - 小块批次：ceil(500MB / 10MB) = 50 批次
// - 分片大小：50 * 1MB = 50MB
//
// 为什么需要这个函数？
// - 验证 EC 编码是否正确完成
// - 检测分片文件是否损坏
// - 确保本地 EC 编码的完整性
func calculateExpectedShardSize(datFileSize int64) int64 {
	var shardSize int64

	// 【阶段 1：处理大块数据】
	// 计算可以处理多少个完整的大块批次
	largeBatchSize := int64(erasure_coding.ErasureCodingLargeBlockSize) * int64(erasure_coding.DataShardsCount)
	numLargeBatches := datFileSize / largeBatchSize

	// 每个大块批次为每个分片贡献 LargeBlockSize 大小
	shardSize = numLargeBatches * int64(erasure_coding.ErasureCodingLargeBlockSize)

	// 计算处理完大块后的剩余数据
	remainingSize := datFileSize - (numLargeBatches * largeBatchSize)

	// 【阶段 2：处理剩余的小块数据】
	if remainingSize > 0 {
		// 计算需要多少个小块批次（向上取整）
		smallBatchSize := int64(erasure_coding.ErasureCodingSmallBlockSize) * int64(erasure_coding.DataShardsCount)
		numSmallBatches := (remainingSize + smallBatchSize - 1) / smallBatchSize // 向上取整

		// 每个小块批次为每个分片贡献 SmallBlockSize 大小
		shardSize += numSmallBatches * int64(erasure_coding.ErasureCodingSmallBlockSize)
	}

	return shardSize
}

// validateEcVolume - 验证 EC 卷的完整性和有效性
//
// 功能说明：
// 检查 EC 卷是否有足够的分片可以正常工作，并验证分片的一致性。
// 这个函数对本地 EC 和分布式 EC 采用不同的验证策略。
//
// 参数：
//   - collection: 集合名称
//   - vid: Volume ID
//
// 返回值：
//   - bool: true=EC 卷有效，false=EC 卷无效或不完整
//
// 验证内容：
//
// 【1. 分片大小一致性（所有场景必须）】
// - Reed-Solomon 编码要求：所有分片必须大小相同
// - 检查：遍历所有分片，确保大小完全一致
// - 失败：任何分片大小不一致都会导致验证失败
//
// 【2. 分片大小与 .dat 文件匹配（仅本地 EC）】
// - 如果 .dat 存在：计算预期分片大小并与实际对比
// - 使用 calculateExpectedShardSize 进行精确计算
// - 失败：分片大小不匹配说明编码不完整或损坏
//
// 【3. 分片数量要求（取决于场景）】
// - 分布式 EC（.dat 不存在）：任何数量的分片都有效
//   * 原因：其他分片可能在不同服务器上
//   * 策略：接受部分分片，等待网络同步
//
// - 本地 EC（.dat 存在）：至少需要 DataShardsCount 个分片
//   * 原因：需要足够的分片才能解码数据
//   * 最少要求：DataShardsCount = 10（数据分片）
//   * 失败：分片不足说明编码未完成
//
// 验证流程：
// 1. 检查 .dat 文件是否存在并计算预期大小
// 2. 遍历所有可能的分片 ID (0-31)
// 3. 检查每个分片的大小并累计数量
// 4. 验证大小一致性和数量要求
//
// 错误处理：
// - 文件不存在：跳过（正常情况）
// - 大小为 0：跳过（损坏的文件）
// - 无法 stat：验证失败（保守策略）
//
// 使用场景：
// - handleFoundEcxFile: 加载前验证 EC 卷
// - 区分完整的 EC 卷和不完整的编码尝试
func (l *DiskLocation) validateEcVolume(collection string, vid needle.VolumeId) bool {
	// 第 1 步：构造基础文件名
	baseFileName := erasure_coding.EcShardFileName(collection, l.Directory, int(vid))
	datFileName := baseFileName + ".dat"

	// 第 2 步：检查 .dat 文件并计算预期分片大小
	var expectedShardSize int64 = -1  // -1 表示未设置
	datExists := false

	// 如果 .dat 文件存在，根据其大小计算预期的分片大小
	if datFileInfo, err := os.Stat(datFileName); err == nil {
		datExists = true
		// 使用确定性算法计算预期分片大小
		expectedShardSize = calculateExpectedShardSize(datFileInfo.Size())
	} else if !os.IsNotExist(err) {
		// 如果 stat 失败（不是"文件不存在"），验证失败
		// 不将此视为"分布式 EC" - 这可能是临时错误
		glog.Warningf("Failed to stat .dat file %s: %v", datFileName, err)
		return false
	}

	// 第 3 步：遍历所有分片并验证
	shardCount := 0                    // 有效分片计数
	var actualShardSize int64 = -1     // 实际分片大小（-1 表示未设置）

	// 检查最多 MaxShardCount (32) 个分片，支持自定义 EC 配置
	// 默认配置：10 个数据分片 + 4 个校验分片 = 14 个分片
	// 但系统支持更多分片以提高容错能力
	for i := 0; i < erasure_coding.MaxShardCount; i++ {
		shardFileName := baseFileName + erasure_coding.ToExt(i)
		fi, err := os.Stat(shardFileName)

		if err == nil {
			// 分片文件存在，检查文件大小
			if fi.Size() > 0 {
				// 【验证 1：大小一致性检查】
				// Reed-Solomon 编码的关键要求：所有分片必须大小相同
				if actualShardSize == -1 {
					// 第一个有效分片，记录其大小作为参考
					actualShardSize = fi.Size()
				} else if fi.Size() != actualShardSize {
					// 发现大小不一致的分片，验证失败
					glog.Warningf("EC volume %d shard %d has size %d, expected %d (all EC shards must be same size)",
						vid, i, fi.Size(), actualShardSize)
					return false
				}
				// 大小一致，计数增加
				shardCount++
			}
			// 跳过 0 字节文件（可能是创建失败的文件）
		} else if !os.IsNotExist(err) {
			// 如果 stat 失败（不是"文件不存在"），验证失败
			// 这与 .dat 文件的错误处理保持一致
			glog.Warningf("Failed to stat shard file %s: %v", shardFileName, err)
			return false
		}
		// 文件不存在是正常情况，继续检查下一个分片
	}

	// 第 4 步：【验证 2：分片大小与 .dat 文件匹配】
	// 如果 .dat 文件存在，验证实际分片大小是否与预期匹配
	if datExists && actualShardSize > 0 && expectedShardSize > 0 {
		if actualShardSize != expectedShardSize {
			glog.Warningf("EC volume %d: shard size %d doesn't match expected size %d (based on .dat file size)",
				vid, actualShardSize, expectedShardSize)
			return false
		}
	}

	// 第 5 步：【验证 3：分片数量检查】
	// 分布式 EC：.dat 文件已删除，任何分片数量都有效
	if !datExists {
		glog.V(1).Infof("EC volume %d: distributed EC (.dat removed) with %d shards", vid, shardCount)
		return true
	}

	// 本地 EC：.dat 文件存在，需要至少 DataShardsCount 个分片
	// 否则这是一个不完整的 EC 编码，应该被清理
	if shardCount < erasure_coding.DataShardsCount {
		glog.Warningf("EC volume %d has .dat file but only %d shards (need at least %d for local EC)",
			vid, shardCount, erasure_coding.DataShardsCount)
		return false
	}

	// 验证通过：分片大小一致、数量足够、与 .dat 匹配
	return true
}

// removeEcVolumeFiles - 删除 EC 卷的所有相关文件
//
// 功能说明：
// 删除指定 EC 卷的所有相关文件，包括索引、日志和所有分片文件。
// 这是一个清理方法，用于移除不完整或损坏的 EC 卷。
//
// 参数：
//   - collection: 集合名称
//   - vid: Volume ID
//
// 删除的文件类型：
// 1. 索引文件：.ecx (Needle 索引，必须先删除)
// 2. 日志文件：.ecj (修改日志)
// 3. 分片文件：.ec00 ~ .ec31 (所有可能的分片)
//
// 删除顺序的重要性：
// 【先删除索引文件 (.ecx, .ecj)】
// - 确保启动时不会尝试加载不完整的分片
// - 如果清理被中断，下次启动不会误用损坏的数据
// - .ecx 是 EC 卷的"入口点"，删除它使卷不可见
//
// 【后删除分片文件 (.ec00-31)】
// - 即使被中断，已删除的 .ecx 也能防止加载
// - 分片文件可以安全地逐个删除
//
// 错误处理：
// - 文件不存在：静默忽略（正常情况）
// - 删除失败：记录警告但继续删除其他文件
// - 使用 glog.V(2) 记录成功的删除操作（调试级别）
//
// 注意事项：
// - 这个方法只删除 EC 相关文件
// - 不会删除 .dat 文件（原始 volume 文件）
// - 删除 .dat 文件意味着完全失去数据
// - 保留 .dat 允许系统回退到非 EC 模式
//
// 使用场景：
// - handleFoundEcxFile: 清理加载失败的 EC 卷
// - checkOrphanedShards: 清理孤立的分片
// - EC 编码失败后的回滚操作
func (l *DiskLocation) removeEcVolumeFiles(collection string, vid needle.VolumeId) {
	// 构造数据目录和索引目录的基础文件名
	baseFileName := erasure_coding.EcShardFileName(collection, l.Directory, int(vid))
	indexBaseFileName := erasure_coding.EcShardFileName(collection, l.IdxDirectory, int(vid))

	// 辅助函数：删除单个文件并处理错误
	removeFile := func(filePath, description string) {
		if err := os.Remove(filePath); err != nil {
			// 只在非"文件不存在"错误时记录警告
			if !os.IsNotExist(err) {
				glog.Warningf("Failed to remove incomplete %s %s: %v", description, filePath, err)
			}
			// 文件不存在是正常的，静默忽略
		} else {
			// 成功删除，记录到调试日志
			glog.V(2).Infof("Removed incomplete %s: %s", description, filePath)
		}
	}

	// 【阶段 1：先删除索引文件】
	// 这是关键的安全措施：删除 .ecx 后，系统不会在启动时尝试加载这个 EC 卷
	// 即使后续删除分片文件被中断，也不会误用不完整的数据
	removeFile(indexBaseFileName+".ecx", "EC index file")   // Needle 索引
	removeFile(indexBaseFileName+".ecj", "EC journal file")  // 修改日志

	// 【阶段 2：删除所有 EC 分片文件】
	// 遍历所有可能的分片 ID (0-31)
	// 使用 MaxShardCount (32) 支持自定义 EC 配置
	// 默认只使用 14 个分片（10 数据 + 4 校验），但删除所有可能的文件确保彻底清理
	for i := 0; i < erasure_coding.MaxShardCount; i++ {
		// 构造分片文件路径，例如：myfiles_1.ec00, myfiles_1.ec01, ...
		shardFilePath := baseFileName + erasure_coding.ToExt(i)
		removeFile(shardFilePath, "EC shard file")
	}

	// 注意：此方法故意不删除 .dat 文件
	// .dat 文件包含原始数据，删除它会导致数据丢失
	// 保留 .dat 文件允许系统：
	// 1. 继续使用原始 volume（非 EC 模式）
	// 2. 重新尝试 EC 编码
	// 3. 提供数据可用性保障
}
