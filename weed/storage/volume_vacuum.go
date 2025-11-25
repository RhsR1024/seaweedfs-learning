// Package storage 实现 Volume 的垃圾回收（Vacuum）和压缩（Compact）功能
package storage

import (
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	idx2 "github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// ProgressFunc 压缩进度回调函数类型
// 参数 processed: 已处理的字节偏移量
// 返回值: true 继续处理, false 中断处理
type ProgressFunc func(processed int64) bool

// garbageLevel 计算 Volume 的垃圾比例
// 垃圾比例 = 已删除数据大小 / 总文件大小
// 返回值范围: 0.0 ~ 1.0
func (v *Volume) garbageLevel() float64 {
	// 空 Volume，无垃圾
	if v.ContentSize() == 0 {
		return 0
	}

	// 获取已删除的数据大小
	deletedSize := v.DeletedSize()
	// 获取当前内容大小
	fileSize := v.ContentSize()

	// 特殊情况：有删除记录但删除大小为 0
	// 这发生在从 .sdx 转换回普通 .idx 时，删除条目的大小信息丢失
	if v.DeletedCount() > 0 && v.DeletedSize() == 0 {
		// this happens for .sdx converted back to normal .idx
		// where deleted entry size is missing

		// 通过 .dat 文件实际大小计算删除的大小
		datFileSize, _, _ := v.FileStat()
		// 删除大小 = 文件大小 - 有效内容大小 - SuperBlock 大小
		deletedSize = datFileSize - fileSize - super_block.SuperBlockSize
		fileSize = datFileSize
	}

	// 返回垃圾比例
	return float64(deletedSize) / float64(fileSize)
}

// Compact 基于 .dat 文件中的删除记录进行 Volume 压缩（方法1：扫描数据文件）
//
// 工作原理：
// 1. 扫描整个 .dat 文件，读取所有 Needle
// 2. 检查每个 Needle 是否仍然有效（未删除、未过期）
// 3. 将有效的 Needle 写入新的 .cpd 文件
// 4. 生成新的索引文件 .cpx
//
// 参数：
//   - preallocate: 新文件预分配的大小（字节）
//   - compactionBytePerSecond: 压缩速度限制（字节/秒），0 表示不限速
//
// 返回：
//   - error: 压缩过程中的错误
//
// 注意：
// - 采用 Copy-on-Write 策略，不需要加锁
// - 生成临时文件 .cpd 和 .cpx，需要调用 CommitCompact 来提交
// compact a volume based on deletions in .dat files
func (v *Volume) Compact(preallocate int64, compactionBytePerSecond int64) error {

	// 内存映射模式不支持压缩（数据在内存中，压缩无意义）
	if v.MemoryMapMaxSizeMb != 0 { //it makes no sense to compact in memory
		return nil
	}

	glog.V(3).Infof("Compacting volume %d ...", v.Id)

	//no need to lock for copy on write
	//v.accessLock.Lock()
	//defer v.accessLock.Unlock()
	//glog.V(3).Infof("Got Compaction lock...")

	// 检查是否已经在压缩中（防止并发压缩）
	if v.isCompacting || v.isCommitCompacting {
		glog.V(0).Infof("Volume %d is already compacting...", v.Id)
		return nil
	}

	// 设置压缩标志
	v.isCompacting = true
	defer func() {
		v.isCompacting = false
	}()

	// 记录压缩开始时的索引文件大小和压缩版本号
	// 用于后续 makeupDiff 检测压缩期间的新写入
	v.lastCompactIndexOffset = v.IndexFileSize()
	v.lastCompactRevision = v.SuperBlock.CompactionRevision

	glog.V(3).Infof("creating copies for volume %d ,last offset %d...", v.Id, v.lastCompactIndexOffset)

	// 同步数据文件，确保所有数据持久化到磁盘
	if err := v.DataBackend.Sync(); err != nil {
		glog.V(0).Infof("compact failed to sync volume %d", v.Id)
	}

	// 同步索引文件
	if err := v.nm.Sync(); err != nil {
		glog.V(0).Infof("compact failed to sync volume idx %d", v.Id)
	}

	// 执行实际的数据复制和索引生成
	// 生成 .cpd（压缩后的数据文件）和 .cpx（压缩后的索引文件）
	return v.copyDataAndGenerateIndexFile(v.FileName(".cpd"), v.FileName(".cpx"), preallocate, compactionBytePerSecond)
}

// Compact2 基于 .idx 索引文件进行 Volume 压缩（方法2：基于索引优化，推荐）
//
// 工作原理：
// 1. 读取 .idx 索引文件，获取所有有效 Needle 的位置
// 2. 按索引顺序从 .dat 文件读取有效的 Needle 数据
// 3. 将有效数据写入新的 .cpd 文件
// 4. 生成新的索引文件 .cpx
//
// 优势：
// - 相比 Compact()，只读取有效数据，不需要扫描整个 .dat 文件
// - 性能更好，适合垃圾比例较高的场景
// - 支持进度回调，可以实时监控压缩进度
//
// 参数：
//   - preallocate: 新文件预分配的大小（字节）
//   - compactionBytePerSecond: 压缩速度限制（字节/秒），0 表示不限速
//   - progressFn: 进度回调函数，可以为 nil
//
// 返回：
//   - error: 压缩过程中的错误
//
// 注意：
// - 生成临时文件 .cpd 和 .cpx，需要调用 CommitCompact 来提交
// compact a volume based on deletions in .idx files
func (v *Volume) Compact2(preallocate int64, compactionBytePerSecond int64, progressFn ProgressFunc) error {

	// 内存映射模式不支持压缩
	if v.MemoryMapMaxSizeMb != 0 { //it makes no sense to compact in memory
		return nil
	}

	glog.V(3).Infof("Compact2 volume %d ...", v.Id)

	// 检查是否已经在压缩中
	if v.isCompacting || v.isCommitCompacting {
		glog.V(0).Infof("Volume %d is already compacting2 ...", v.Id)
		return nil
	}

	// 设置压缩标志
	v.isCompacting = true
	defer func() {
		v.isCompacting = false
	}()

	// 记录压缩开始时的状态
	v.lastCompactIndexOffset = v.IndexFileSize()
	v.lastCompactRevision = v.SuperBlock.CompactionRevision

	glog.V(3).Infof("creating copies for volume %d ...", v.Id)

	// 检查 DataBackend 是否可用
	if v.DataBackend == nil {
		return fmt.Errorf("volume %d backend is empty remote:%v", v.Id, v.HasRemoteFile())
	}

	// 同步数据文件到磁盘
	if err := v.DataBackend.Sync(); err != nil {
		glog.V(0).Infof("compact2 failed to sync volume dat %d: %v", v.Id, err)
	}

	// 同步索引文件到磁盘
	if err := v.nm.Sync(); err != nil {
		glog.V(0).Infof("compact2 failed to sync volume idx %d: %v", v.Id, err)
	}

	// 执行基于索引的数据复制
	// 从原始文件（.dat/.idx）读取，写入到压缩文件（.cpd/.cpx）
	return v.copyDataBasedOnIndexFile(
		v.FileName(".dat"), v.FileName(".idx"),
		v.FileName(".cpd"), v.FileName(".cpx"),
		v.SuperBlock,
		v.Version(),
		preallocate,
		compactionBytePerSecond,
		progressFn,
	)
}

// CommitCompact 提交压缩结果，用压缩后的文件替换原文件
//
// 工作流程：
// 1. 关闭当前 Volume 的数据和索引文件
// 2. 应用压缩期间的增量更新（makeupDiff）
// 3. 将 .cpd 重命名为 .dat，.cpx 重命名为 .idx
// 4. 删除旧的 LevelDB 索引文件
// 5. 重新加载 Volume
//
// 返回：
//   - error: 提交过程中的错误
//
// 注意：
// - 这是一个关键操作，必须确保原子性
// - 在 Windows 平台需要先删除原文件才能重命名
// - 提交失败会保留原文件，删除压缩文件
func (v *Volume) CommitCompact() error {
	// 内存映射模式不支持压缩
	if v.MemoryMapMaxSizeMb != 0 { //it makes no sense to compact in memory
		return nil
	}

	glog.V(0).Infof("Committing volume %d vacuuming...", v.Id)

	// 检查是否已经在提交中
	if v.isCommitCompacting {
		glog.V(0).Infof("Volume %d is already commit compacting ...", v.Id)
		return nil
	}

	// 设置提交标志
	v.isCommitCompacting = true
	defer func() {
		v.isCommitCompacting = false
	}()

	// 获取数据文件访问锁，阻止新的读写操作
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()

	glog.V(3).Infof("Got volume %d committing lock...", v.Id)

	// 关闭索引文件
	if v.nm != nil {
		v.nm.Close()
		v.nm = nil
	}

	// 关闭数据文件
	if v.DataBackend != nil {
		if err := v.DataBackend.Close(); err != nil {
			glog.V(0).Infof("failed to close volume %d", v.Id)
		}
	}
	v.DataBackend = nil

	// 减少 Prometheus 指标计数
	stats.VolumeServerVolumeGauge.WithLabelValues(v.Collection, "volume").Dec()

	var e error

	// 应用压缩期间的增量更新
	// makeupDiff 会检测压缩期间新写入的数据，并将其追加到压缩后的文件中
	if e = v.makeupDiff(v.FileName(".cpd"), v.FileName(".cpx"), v.FileName(".dat"), v.FileName(".idx")); e != nil {
		// 增量更新失败，删除压缩文件，保留原文件
		glog.V(0).Infof("makeupDiff in CommitCompact volume %d failed %v", v.Id, e)

		// 清理压缩文件
		e = os.Remove(v.FileName(".cpd"))
		if e != nil {
			return e
		}
		e = os.Remove(v.FileName(".cpx"))
		if e != nil {
			return e
		}
	} else {
		// 增量更新成功，替换原文件

		// Windows 平台特殊处理：必须先删除原文件才能重命名
		if runtime.GOOS == "windows" {
			e = os.RemoveAll(v.FileName(".dat"))
			if e != nil {
				return e
			}
			e = os.RemoveAll(v.FileName(".idx"))
			if e != nil {
				return e
			}
		}

		// 将压缩文件重命名为正式文件
		var e error
		if e = os.Rename(v.FileName(".cpd"), v.FileName(".dat")); e != nil {
			return fmt.Errorf("rename %s: %v", v.FileName(".cpd"), e)
		}
		if e = os.Rename(v.FileName(".cpx"), v.FileName(".idx")); e != nil {
			return fmt.Errorf("rename %s: %v", v.FileName(".cpx"), e)
		}
	}

	//glog.V(3).Infof("Pretending to be vacuuming...")
	//time.Sleep(20 * time.Second)

	// 删除旧的 LevelDB 索引文件（压缩后需要重建）
	os.RemoveAll(v.FileName(".ldb"))

	glog.V(3).Infof("Loading volume %d commit file...", v.Id)

	// 重新加载 Volume（读取新的 .dat 和 .idx 文件）
	if e = v.load(true, false, v.needleMapKind, 0, v.Version()); e != nil {
		return e
	}

	glog.V(3).Infof("Finish committing volume %d", v.Id)
	return nil
}

// cleanupCompact 清理压缩临时文件
//
// 清理以下文件：
// - .cpd (压缩后的数据文件)
// - .cpx (压缩后的索引文件)
// - .cpldb (压缩过程中的 LevelDB 索引)
//
// 使用场景：
// - 压缩失败后清理临时文件
// - Volume 启动时清理上次遗留的临时文件
//
// 返回：
//   - error: 清理过程中的错误（文件不存在不算错误）
func (v *Volume) cleanupCompact() error {
	glog.V(0).Infof("Cleaning up volume %d vacuuming...", v.Id)

	// 删除压缩数据文件
	e1 := os.Remove(v.FileName(".cpd"))
	// 删除压缩索引文件
	e2 := os.Remove(v.FileName(".cpx"))
	// 删除压缩过程的 LevelDB 索引
	e3 := os.RemoveAll(v.FileName(".cpldb"))

	// 只有在文件存在但删除失败时才返回错误
	if e1 != nil && !os.IsNotExist(e1) {
		return e1
	}
	if e2 != nil && !os.IsNotExist(e2) {
		return e2
	}
	if e3 != nil && !os.IsNotExist(e3) {
		return e3
	}
	return nil
}

// fetchCompactRevisionFromDatFile 从 .dat 文件的 SuperBlock 读取压缩版本号
//
// 压缩版本号用于检测 Volume 是否被压缩过：
// - 每次压缩成功，版本号 +1
// - 用于确保压缩文件和原文件的一致性
//
// 参数：
//   - datBackend: .dat 文件的 Backend 接口
//
// 返回：
//   - compactRevision: 压缩版本号
//   - err: 读取错误
func fetchCompactRevisionFromDatFile(datBackend backend.BackendStorageFile) (compactRevision uint16, err error) {
	// 读取 SuperBlock（Volume 元数据）
	superBlock, err := super_block.ReadSuperBlock(datBackend)
	if err != nil {
		return 0, err
	}
	// 返回压缩版本号
	return superBlock.CompactionRevision, nil
}

// makeupDiff 应用压缩期间的增量更新到新文件
//
// 压缩场景：
// Volume 压缩是一个耗时的过程（可能几分钟到几小时）。
// 在压缩期间，Volume 仍然可以接受新的写入和删除操作。
// makeupDiff 负责将这些增量操作应用到压缩后的文件上。
//
// 工作流程：
// 1. 检查原索引文件是否有新增条目（对比 lastCompactIndexOffset）
// 2. 检查原数据文件的压缩版本号是否改变
// 3. 读取压缩期间新增的索引条目
// 4. 将对应的 Needle 数据追加到新数据文件
// 5. 更新新索引文件
//
// 参数：
//   - newDatFileName: 压缩后的数据文件 (.cpd)
//   - newIdxFileName: 压缩后的索引文件 (.cpx)
//   - oldDatFileName: 原始数据文件 (.dat)
//   - oldIdxFileName: 原始索引文件 (.idx)
//
// 返回：
//   - err: 处理过程中的错误
//
// 注意：
// - 如果原文件的压缩版本号改变，说明有其他压缩操作，直接失败
// - 增量更新必须保持与原操作顺序一致
// if old .dat and .idx files are updated, this func tries to apply the same changes to new files accordingly
func (v *Volume) makeupDiff(newDatFileName, newIdxFileName, oldDatFileName, oldIdxFileName string) (err error) {
	var indexSize int64

	// 打开原始索引文件（只读）
	oldIdxFile, err := os.Open(oldIdxFileName)
	if err != nil {
		return fmt.Errorf("makeupDiff open %s failed: %v", oldIdxFileName, err)
	}
	defer oldIdxFile.Close()

	// 打开原始数据文件（只读）
	oldDatFile, err := os.Open(oldDatFileName)
	if err != nil {
		return fmt.Errorf("makeupDiff open %s failed: %v", oldDatFileName, err)
	}
	oldDatBackend := backend.NewDiskFile(oldDatFile)
	defer oldDatBackend.Close()

	// skip if the old .idx file has not changed
	// 验证原索引文件的完整性，并获取其大小
	if indexSize, err = verifyIndexFileIntegrity(oldIdxFile); err != nil {
		return fmt.Errorf("verifyIndexFileIntegrity %s failed: %v", oldIdxFileName, err)
	}

	// 如果索引文件没有新增内容，直接返回（无增量更新）
	if indexSize == 0 || uint64(indexSize) <= v.lastCompactIndexOffset {
		return nil
	}

	// fail if the old .dat file has changed to a new revision
	// 获取原数据文件的压缩版本号
	oldDatCompactRevision, err := fetchCompactRevisionFromDatFile(oldDatBackend)
	if err != nil {
		return fmt.Errorf("fetchCompactRevisionFromDatFile src %s failed: %v", oldDatFile.Name(), err)
	}

	// 检查压缩版本号是否匹配（确保没有其他压缩操作）
	if oldDatCompactRevision != v.lastCompactRevision {
		return fmt.Errorf("current old dat file's compact revision %d is not the expected one %d", oldDatCompactRevision, v.lastCompactRevision)
	}

	// keyField 存储 Needle 的位置和大小
	type keyField struct {
		offset Offset // Needle 在文件中的偏移量
		size   Size   // Needle 的大小
	}

	// 存储压缩期间更新的索引条目
	// key: NeedleId, value: 最新的 offset 和 size
	// 使用 map 去重，同一个 NeedleId 只保留最新的更新
	incrementedHasUpdatedIndexEntry := make(map[NeedleId]keyField)

	// 从后向前扫描新增的索引条目
	// 为什么从后向前？因为同一个 key 可能有多次更新，最后一次是最新的
	for idxOffset := indexSize - NeedleMapEntrySize; uint64(idxOffset) >= v.lastCompactIndexOffset; idxOffset -= NeedleMapEntrySize {
		var IdxEntry []byte

		// 读取索引条目（16 字节）
		if IdxEntry, err = readIndexEntryAtOffset(oldIdxFile, idxOffset); err != nil {
			return fmt.Errorf("readIndexEntry %s at offset %d failed: %v", oldIdxFileName, idxOffset, err)
		}

		// 解析索引条目：key(8字节) + offset(4字节) + size(4字节)
		key, offset, size := idx2.IdxFileEntry(IdxEntry)
		glog.V(4).Infof("key %d offset %d size %d", key, offset, size)

		// 只记录第一次遇到的条目（因为是从后向前扫描，所以第一次遇到的是最新的）
		if _, found := incrementedHasUpdatedIndexEntry[key]; !found {
			incrementedHasUpdatedIndexEntry[key] = keyField{
				offset: offset,
				size:   size,
			}
		}
	}

	// no updates during commit step
	// 没有增量更新，直接返回
	if len(incrementedHasUpdatedIndexEntry) == 0 {
		return nil
	}

	// deal with updates during commit step
	// 处理增量更新：将变更应用到新文件
	var (
		dst, idx *os.File
	)

	// 打开新数据文件（读写模式）
	if dst, err = os.OpenFile(newDatFileName, os.O_RDWR, 0644); err != nil {
		return fmt.Errorf("open dat file %s failed: %v", newDatFileName, err)
	}
	dstDatBackend := backend.NewDiskFile(dst)
	defer dstDatBackend.Close()

	// 打开新索引文件（读写模式）
	if idx, err = os.OpenFile(newIdxFileName, os.O_RDWR, 0644); err != nil {
		return fmt.Errorf("open idx file %s failed: %v", newIdxFileName, err)
	}

	defer func() {
		idx.Sync() // 同步索引文件到磁盘
		idx.Close()
	}()

	// 获取新索引文件的当前大小
	stat, err := idx.Stat()
	if err != nil {
		return fmt.Errorf("stat file %s: %v", idx.Name(), err)
	}
	idxSize := stat.Size()

	// 验证新数据文件的压缩版本号
	var newDatCompactRevision uint16
	newDatCompactRevision, err = fetchCompactRevisionFromDatFile(dstDatBackend)
	if err != nil {
		return fmt.Errorf("fetchCompactRevisionFromDatFile dst %s failed: %v", dst.Name(), err)
	}

	// 新文件的版本号应该比旧文件大 1
	if oldDatCompactRevision+1 != newDatCompactRevision {
		return fmt.Errorf("oldDatFile %s 's compact revision is %d while newDatFile %s 's compact revision is %d", oldDatFileName, oldDatCompactRevision, newDatFileName, newDatCompactRevision)
	}

	// 遍历所有需要更新的条目
	for key, increIdxEntry := range incrementedHasUpdatedIndexEntry {

		// 将索引信息转换为字节数组（16 字节）
		idxEntryBytes := needle_map.ToBytes(key, increIdxEntry.offset, increIdxEntry.size)

		var offset int64

		// 定位到文件末尾
		if offset, err = dst.Seek(0, 2); err != nil {
			glog.V(0).Infof("failed to seek the end of file: %v", err)
			return
		}

		//ensure file writing starting from aligned positions
		// 确保写入位置是 8 字节对齐的
		if offset%NeedlePaddingSize != 0 {
			offset = offset + (NeedlePaddingSize - offset%NeedlePaddingSize)
			if offset, err = dst.Seek(offset, 0); err != nil {
				glog.V(0).Infof("failed to align in datafile %s: %v", dst.Name(), err)
				return
			}
		}

		//updated needle
		// 如果是更新操作（offset 和 size 都有效）
		if !increIdxEntry.offset.IsZero() && increIdxEntry.size != 0 && increIdxEntry.size.IsValid() {
			//even the needle cache in memory is hit, the need_bytes is correct
			glog.V(4).Infof("file %d offset %d size %d", key, increIdxEntry.offset.ToActualOffset(), increIdxEntry.size)

			var needleBytes []byte
			// 从原数据文件读取 Needle 数据
			needleBytes, err = needle.ReadNeedleBlob(oldDatBackend, increIdxEntry.offset.ToActualOffset(), increIdxEntry.size, v.Version())
			if err != nil {
				return fmt.Errorf("ReadNeedleBlob %s key %d offset %d size %d failed: %v", oldDatFile.Name(), key, increIdxEntry.offset.ToActualOffset(), increIdxEntry.size, err)
			}

			// 将 Needle 数据写入新数据文件
			dstDatBackend.Write(needleBytes)
			if err := dstDatBackend.Sync(); err != nil {
				return fmt.Errorf("cannot sync needle %s: %v", dstDatBackend.File.Name(), err)
			}

			// 更新索引条目中的 offset（新文件中的位置）
			util.Uint32toBytes(idxEntryBytes[8:12], uint32(offset/NeedlePaddingSize))
		} else { //deleted needle
			// 如果是删除操作（offset 为 0 或 size 无效）
			// 创建一个假的删除标记 Needle
			//fakeDelNeedle's default Data field is nil
			fakeDelNeedle := new(needle.Needle)
			fakeDelNeedle.Id = key
			fakeDelNeedle.Cookie = 0x12345678
			fakeDelNeedle.AppendAtNs = uint64(time.Now().UnixNano())

			// 追加删除标记到数据文件
			_, _, _, err = fakeDelNeedle.Append(dstDatBackend, v.Version())
			if err != nil {
				return fmt.Errorf("append deleted %d failed: %v", key, err)
			}

			// 索引中将 offset 设置为 0（表示已删除）
			util.Uint32toBytes(idxEntryBytes[8:12], uint32(0))
		}

		// 将更新后的索引条目追加到新索引文件末尾
		if _, err := idx.Seek(0, 2); err != nil {
			return fmt.Errorf("cannot seek end of indexfile %s: %v",
				newIdxFileName, err)
		}
		_, err = idx.Write(idxEntryBytes)
		if err != nil {
			return fmt.Errorf("cannot write indexfile %s: %v", newIdxFileName, err)
		}
	}

	// 将新增的索引条目加载到临时的 NeedleMap 中
	return v.tmpNm.DoOffsetLoading(v, idx, uint64(idxSize)/NeedleMapEntrySize)
}

// VolumeFileScanner4Vacuum 扫描 Volume 文件用于压缩的扫描器
//
// 实现了 VolumeFileScanner 接口，用于 Compact() 方法（基于扫描数据文件的压缩）
//
// 工作流程：
// 1. VisitSuperBlock: 处理 SuperBlock，增加压缩版本号
// 2. VisitNeedle: 遍历每个 Needle，检查是否有效，写入新文件
//
// 字段说明：
// - version: Needle 版本号（从 SuperBlock 读取）
// - v: 正在压缩的 Volume
// - dstBackend: 目标数据文件（.cpd）
// - nm: 新的内存索引（用于构建 .cpx）
// - newOffset: 新文件中的当前写入位置
// - now: 当前时间戳（用于 TTL 检查）
// - writeThrottler: 写入限速器
type VolumeFileScanner4Vacuum struct {
	version        needle.Version             // Needle 版本
	v              *Volume                    // 正在压缩的 Volume
	dstBackend     backend.BackendStorageFile // 目标数据文件
	nm             *needle_map.MemDb          // 新索引（内存）
	newOffset      int64                      // 新文件写入位置
	now            uint64                     // 当前时间戳
	writeThrottler *util.WriteThrottler       // 写入限速器
}

// VisitSuperBlock 处理 SuperBlock（Volume 元数据）
//
// 操作：
// 1. 记录 Needle 版本号
// 2. 增加压缩版本号（CompactionRevision++）
// 3. 将更新后的 SuperBlock 写入目标文件
// 4. 更新写入位置
//
// 参数：
//   - superBlock: 从原文件读取的 SuperBlock
//
// 返回：
//   - error: 写入错误
func (scanner *VolumeFileScanner4Vacuum) VisitSuperBlock(superBlock super_block.SuperBlock) error {
	// 记录 Needle 版本号（用于后续读取 Needle）
	scanner.version = superBlock.Version

	// 压缩版本号 +1（标记这是一次新的压缩）
	superBlock.CompactionRevision++

	// 将 SuperBlock 写入新文件的开头（偏移量 0）
	_, err := scanner.dstBackend.WriteAt(superBlock.Bytes(), 0)

	// 更新写入位置（跳过 SuperBlock）
	scanner.newOffset = int64(superBlock.BlockSize())

	return err

}

// ReadNeedleBody 是否读取 Needle 的 Body 部分
//
// 返回 true 表示需要读取完整的 Needle 数据（Header + Body）
// 这样才能将其写入到新文件
func (scanner *VolumeFileScanner4Vacuum) ReadNeedleBody() bool {
	return true
}

// VisitNeedle 访问每个 Needle，决定是否保留到新文件
//
// 过滤条件：
// 1. 检查 TTL 是否过期
// 2. 检查索引中是否有效（未删除）
// 3. 检查偏移量是否匹配（确保是最新版本）
//
// 参数：
//   - n: 当前 Needle
//   - offset: Needle 在原文件中的偏移量
//   - needleHeader: Needle 头部字节（未使用）
//   - needleBody: Needle 主体字节（未使用）
//
// 返回：
//   - error: 处理错误
func (scanner *VolumeFileScanner4Vacuum) VisitNeedle(n *needle.Needle, offset int64, needleHeader, needleBody []byte) error {
	// 检查 TTL 是否过期
	// 如果 Needle 有 TTL 且已过期，跳过（不写入新文件）
	if n.HasTtl() && scanner.now >= n.LastModified+uint64(scanner.v.Ttl.Minutes()*60) {
		return nil
	}

	// 从索引中查找这个 Needle
	nv, ok := scanner.v.nm.Get(n.Id)
	glog.V(4).Infoln("needle expected offset ", offset, "ok", ok, "nv", nv)

	// 检查 Needle 是否有效：
	// 1. ok: 在索引中存在
	// 2. nv.Offset == offset: 偏移量匹配（确保是最新版本，不是被覆盖的旧版本）
	// 3. nv.Size > 0 && nv.Size.IsValid(): 大小有效（未删除）
	if ok && nv.Offset.ToActualOffset() == offset && nv.Size > 0 && nv.Size.IsValid() {
		// 将 Needle 添加到新索引中（记录新的偏移量）
		if err := scanner.nm.Set(n.Id, ToOffset(scanner.newOffset), n.Size); err != nil {
			return fmt.Errorf("cannot put needle: %s", err)
		}

		// 将 Needle 追加到新数据文件
		if _, _, _, err := n.Append(scanner.dstBackend, scanner.v.Version()); err != nil {
			return fmt.Errorf("cannot append needle: %s", err)
		}

		// 计算写入的字节数（包括对齐）
		delta := n.DiskSize(scanner.version)
		// 更新写入位置
		scanner.newOffset += delta
		// 限速（如果配置了写入速度限制）
		scanner.writeThrottler.MaybeSlowdown(delta)

		glog.V(4).Infoln("saving key", n.Id, "volume offset", offset, "=>", scanner.newOffset, "data_size", n.Size)
	}

	return nil
}

// copyDataAndGenerateIndexFile 通过扫描数据文件进行压缩（Compact 方法使用）
//
// 工作流程：
// 1. 创建新的数据文件 (.cpd) 和内存索引
// 2. 使用 VolumeFileScanner4Vacuum 扫描原数据文件
// 3. 将有效的 Needle 写入新文件
// 4. 将内存索引保存为 .cpx 文件
//
// 参数：
//   - dstName: 目标数据文件名 (.cpd)
//   - idxName: 目标索引文件名 (.cpx)
//   - preallocate: 预分配空间大小（字节）
//   - compactionBytePerSecond: 写入速度限制（字节/秒）
//
// 返回：
//   - err: 压缩过程中的错误
func (v *Volume) copyDataAndGenerateIndexFile(dstName, idxName string, preallocate int64, compactionBytePerSecond int64) (err error) {
	var dst backend.BackendStorageFile

	// 创建新的数据文件（带预分配）
	if dst, err = backend.CreateVolumeFile(dstName, preallocate, 0); err != nil {
		return err
	}
	defer dst.Close()

	// 创建内存索引（用于构建 .cpx）
	nm := needle_map.NewMemDb()
	defer nm.Close()

	// 创建扫描器
	scanner := &VolumeFileScanner4Vacuum{
		v:              v,                                               // Volume 引用
		now:            uint64(time.Now().Unix()),                       // 当前时间戳（TTL 检查）
		nm:             nm,                                              // 新索引
		dstBackend:     dst,                                             // 新数据文件
		writeThrottler: util.NewWriteThrottler(compactionBytePerSecond), // 限速器
	}

	// 扫描原数据文件，调用 scanner.VisitSuperBlock 和 scanner.VisitNeedle
	err = ScanVolumeFile(v.dir, v.Collection, v.Id, v.needleMapKind, scanner)
	if err != nil {
		return err
	}

	// 将内存索引保存为 .cpx 文件
	return nm.SaveToIdx(idxName)
}

// copyDataBasedOnIndexFile 基于索引文件进行压缩（Compact2 方法使用，推荐）
//
// 工作流程：
// 1. 创建新的数据文件和内存索引
// 2. 加载原索引文件到内存
// 3. 遍历索引中的所有有效条目
// 4. 从原数据文件读取对应的 Needle，写入新文件
// 5. 构建新索引并保存
// 6. 创建临时 NeedleMap 用于 CommitCompact
//
// 优势：
// - 只读取有效数据，避免扫描整个 .dat 文件
// - 性能优于 copyDataAndGenerateIndexFile
// - 支持进度回调
//
// 参数：
//   - srcDatName: 原数据文件 (.dat)
//   - srcIdxName: 原索引文件 (.idx)
//   - dstDatName: 新数据文件 (.cpd)
//   - datIdxName: 新索引文件 (.cpx)
//   - sb: SuperBlock（Volume 元数据）
//   - version: Needle 版本号
//   - preallocate: 预分配空间大小
//   - compactionBytePerSecond: 写入速度限制
//   - progressFn: 进度回调函数（可选）
//
// 返回：
//   - err: 压缩过程中的错误
func (v *Volume) copyDataBasedOnIndexFile(srcDatName, srcIdxName, dstDatName, datIdxName string, sb super_block.SuperBlock, version needle.Version, preallocate, compactionBytePerSecond int64, progressFn ProgressFunc) (err error) {
	var (
		srcDatBackend, dstDatBackend backend.BackendStorageFile
		dataFile                     *os.File
	)

	// 创建新数据文件（带预分配）
	if dstDatBackend, err = backend.CreateVolumeFile(dstDatName, preallocate, 0); err != nil {
		return err
	}
	defer func() {
		dstDatBackend.Sync() // 同步到磁盘
		dstDatBackend.Close()
	}()

	// 创建两个内存索引
	oldNm := needle_map.NewMemDb() // 原索引（从 .idx 加载）
	defer oldNm.Close()
	newNm := needle_map.NewMemDb() // 新索引（压缩后的）
	defer newNm.Close()

	// 加载原索引文件到内存
	if err = oldNm.LoadFromIdx(srcIdxName); err != nil {
		return err
	}

	// 打开原数据文件（只读）
	if dataFile, err = os.Open(srcDatName); err != nil {
		return err
	}
	srcDatBackend = backend.NewDiskFile(dataFile)
	defer srcDatBackend.Close()

	// 当前时间戳（用于 TTL 检查）
	now := uint64(time.Now().Unix())

	// 更新 SuperBlock 的压缩版本号
	sb.CompactionRevision++
	// 写入新文件的 SuperBlock
	dstDatBackend.WriteAt(sb.Bytes(), 0)
	// 新文件的写入位置（跳过 SuperBlock）
	newOffset := int64(sb.BlockSize())

	// 创建写入限速器
	writeThrottler := util.NewWriteThrottler(compactionBytePerSecond)

	// 遍历原索引中的所有条目（升序遍历）
	err = oldNm.AscendingVisit(func(value needle_map.NeedleValue) error {

		offset, size := value.Offset, value.Size

		// 跳过已删除的条目（offset 为 0 或 size 标记为删除）
		if offset.IsZero() || size.IsDeleted() {
			return nil
		}

		// 调用进度回调函数
		if progressFn != nil {
			if !progressFn(offset.ToActualOffset()) {
				return fmt.Errorf("interrupted") // 用户中断
			}
		}

		// 创建 Needle 对象并从原文件读取数据
		n := new(needle.Needle)
		if err := n.ReadData(srcDatBackend, offset.ToActualOffset(), size, version); err != nil {
			return fmt.Errorf("cannot hydrate needle from file: %s", err)
		}

		// 检查 TTL 是否过期
		if n.HasTtl() && now >= n.LastModified+uint64(sb.Ttl.Minutes()*60) {
			return nil // 跳过过期的 Needle
		}

		// 将 Needle 添加到新索引（记录新位置）
		if err = newNm.Set(n.Id, ToOffset(newOffset), n.Size); err != nil {
			return fmt.Errorf("cannot put needle: %s", err)
		}

		// 将 Needle 追加到新数据文件
		if _, _, _, err = n.Append(dstDatBackend, sb.Version); err != nil {
			return fmt.Errorf("cannot append needle: %s", err)
		}

		// 计算写入的字节数（包括对齐）
		delta := n.DiskSize(version)
		// 更新写入位置
		newOffset += delta
		// 限速
		writeThrottler.MaybeSlowdown(delta)

		glog.V(4).Infoln("saving key", n.Id, "volume offset", offset, "=>", newOffset, "data_size", n.Size)

		return nil
	})
	if err != nil {
		return err
	}

	// 对于没有 TTL 的 Volume，验证压缩后的文件大小
	if v.Ttl.String() == "" {
		dstDatSize, _, err := dstDatBackend.GetStat()
		if err != nil {
			return err
		}

		// 检查大小是否合理
		if v.nm.ContentSize() > v.nm.DeletedSize() {
			// 期望大小 = 内容大小 - 删除大小
			expectedContentSize := v.nm.ContentSize() - v.nm.DeletedSize()
			if expectedContentSize > uint64(dstDatSize) {
				return fmt.Errorf("volume %s unexpected new data size: %d does not match size of content minus deleted: %d",
					v.Id.String(), dstDatSize, expectedContentSize)
			}
		} else if v.nm.DeletedSize() > v.nm.ContentSize() {
			// 异常情况：删除大小大于内容大小（可能索引不一致）
			glog.Warningf("volume %s content size: %d less deleted size: %d, new size: %d",
				v.Id.String(), v.nm.ContentSize(), v.nm.DeletedSize(), dstDatSize)
		}
	}

	// 保存新索引到 .cpx 文件
	err = newNm.SaveToIdx(datIdxName)
	if err != nil {
		return err
	}

	// 打开新索引文件（用于后续加载）
	indexFile, err := os.OpenFile(datIdxName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		glog.Errorf("cannot open Volume Index %s: %v", datIdxName, err)
		return err
	}
	defer func() {
		indexFile.Sync()
		indexFile.Close()
	}()

	// 清理旧的临时 NeedleMap
	if v.tmpNm != nil {
		v.tmpNm.Close()
		v.tmpNm = nil
	}

	// 根据索引类型创建临时 NeedleMap
	// tmpNm 用于 makeupDiff 和 CommitCompact
	if v.needleMapKind == NeedleMapInMemory {
		// 内存索引
		nm := &NeedleMap{
			m: needle_map.NewCompactMap(),
		}
		v.tmpNm = nm
		//can be optimized, filling nm in oldNm.AscendingVisit
		// 加载索引文件到内存（可以优化：在 AscendingVisit 中直接填充）
		err = v.tmpNm.DoOffsetLoading(nil, indexFile, 0)
		return err
	} else {
		// LevelDB 索引
		dbFileName := v.FileName(".ldb")
		m := &LevelDbNeedleMap{dbFileName: dbFileName}
		m.dbFileName = dbFileName
		mm := &mapMetric{}
		m.mapMetric = *mm
		v.tmpNm = m
		err = v.tmpNm.DoOffsetLoading(v, indexFile, 0)
		if err != nil {
			return err
		}
	}
	return
}
