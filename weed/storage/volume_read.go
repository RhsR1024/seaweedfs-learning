// Package storage 实现 SeaweedFS 的 Volume 读取操作
// 本文件包含 Volume 的读取核心逻辑，支持 Needle 数据读取、元数据读取和完整卷扫描
//
// 主要功能:
//  1. Needle 数据读取（支持 TTL、删除标记、元数据优先）
//  2. 流式数据读取（支持分块读取、CRC 校验）
//  3. Volume 文件扫描（用于修复、备份和数据迁移）
//  4. 压缩感知读取（支持卷压缩期间的读取）
//
// 读取模型:
//   SeaweedFS 的读取路径分为两个阶段：
//   1. 索引查找：通过 NeedleMapper 查找 Needle 的物理位置（offset, size）
//   2. 数据读取：根据物理位置从 .dat 文件读取数据
//
// 性能优化:
//   - 元数据优先读取：对于大文件（>1MB），可以先只读元数据
//   - 流式分块读取：避免大文件一次性加载到内存
//   - 读写锁保护：允许并发读取，但与写入/压缩互斥
//   - CRC 增量校验：分块读取时增量计算 CRC，避免重复计算
package storage

import (
	"fmt"
	"io"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util/mem"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// PagedReadLimit 分页读取阈值（1MB）
// 对于大于此阈值的 Needle，如果启用 AttemptMetaOnly，会先尝试只读取元数据
// 这样可以避免不必要的大数据读取（例如只需要获取文件属性时）
const PagedReadLimit = 1024 * 1024

// readNeedle 读取 Needle 的完整数据（通过 NeedleMapper 索引查找）
// 这是 Volume 读取的核心函数，支持多种高级特性
//
// 参数:
//   - n: Needle 对象（需要设置 Id 字段）
//   - readOption: 读取选项（可选）
//     * AttemptMetaOnly: 对于大文件（>1MB），尝试只读取元数据
//     * ReadDeleted: 是否允许读取已删除的 Needle
//     * VolumeRevision: 用于检测卷压缩
//   - onReadSizeFn: 读取大小回调函数（用于统计和监控）
//
// 返回值:
//   - count: 实际读取的字节数（DataSize）
//   - err: 错误（ErrorNotFound、ErrorDeleted 等）
//
// 读取流程:
//  1. 【索引查找】通过 NeedleMapper 查找 Needle 的物理位置和大小
//  2. 【删除检查】检查 Needle 是否被删除（负数 size）
//  3. 【元数据优先】对于大文件，先尝试只读取元数据（节省带宽）
//  4. 【完整数据读取】如果需要，读取完整的 Needle 数据
//  5. 【TTL 检查】检查 Needle 是否已过期
//
// 特殊处理:
//   - 4字节偏移溢出处理：如果 Size 不匹配，尝试 +MaxPossibleVolumeSize
//   - 元数据优先优化：对于非压缩、非分块的大文件，可以只返回元数据
//   - 已删除数据读取：支持读取墓碑数据（用于数据恢复）
//
// 锁机制:
//   使用读锁（RLock），允许并发读取，但与写入/压缩互斥
func (v *Volume) readNeedle(n *needle.Needle, readOption *ReadOption, onReadSizeFn func(size Size)) (count int, err error) {
	v.dataFileAccessLock.RLock()
	defer v.dataFileAccessLock.RUnlock()

	// 步骤 1: 从索引中查找 Needle 的物理位置
	nv, ok := v.nm.Get(n.Id)
	if !ok || nv.Offset.IsZero() {
		return -1, ErrorNotFound // Needle 不存在
	}
	readSize := nv.Size

	// 步骤 2: 检查 Needle 是否被删除
	// 已删除的 Needle 在索引中大小为负数
	if readSize.IsDeleted() {
		if readOption != nil && readOption.ReadDeleted && readSize != TombstoneFileSize {
			// 允许读取已删除的数据（用于数据恢复）
			glog.V(3).Infof("reading deleted %s", n.String())
			readSize = -readSize // 转换为正数以读取
		} else {
			return -1, ErrorDeleted // 默认拒绝读取已删除的数据
		}
	}
	if readSize == 0 {
		return 0, nil // 空 Needle
	}

	// 步骤 3: 调用回调函数（用于统计和流量控制）
	if onReadSizeFn != nil {
		onReadSizeFn(readSize)
	}

	// 步骤 4: 元数据优先读取（性能优化）
	// 对于大文件（>1MB），如果只需要元数据，可以避免读取整个文件
	if readOption != nil && readOption.AttemptMetaOnly && readSize > PagedReadLimit {
		readOption.VolumeRevision = v.SuperBlock.CompactionRevision
		err = n.ReadNeedleMeta(v.DataBackend, nv.Offset.ToActualOffset(), readSize, v.Version())
		// 处理 4 字节偏移溢出情况（32 位偏移最大支持 4GB）
		if err == needle.ErrorSizeMismatch && OffsetSize == 4 {
			readOption.IsOutOfRange = true
			// 尝试从 +4GB 偏移处读取
			err = n.ReadNeedleMeta(v.DataBackend, nv.Offset.ToActualOffset()+int64(MaxPossibleVolumeSize), readSize, v.Version())
		}
		if err != nil {
			return 0, err
		}
		// 如果是非压缩、非分块的文件，可以只返回元数据
		if !n.IsCompressed() && !n.IsChunkedManifest() {
			readOption.IsMetaOnly = true
		}
	}

	// 步骤 5: 读取完整的 Needle 数据（如果需要）
	if readOption == nil || !readOption.IsMetaOnly {
		err = n.ReadData(v.DataBackend, nv.Offset.ToActualOffset(), readSize, v.Version())
		v.checkReadWriteError(err) // 检查是否需要标记磁盘为只读
		if err != nil {
			return 0, err
		}
	}
	count = int(n.DataSize)

	// 步骤 6: TTL（Time To Live）检查
	if !n.HasTtl() {
		return // 没有 TTL，直接返回
	}
	ttlMinutes := n.Ttl.Minutes()
	if ttlMinutes == 0 {
		return // TTL 为 0，永不过期
	}
	if !n.HasLastModifiedDate() {
		return // 没有修改时间，无法判断过期
	}
	// 检查是否已过期
	if time.Now().Before(time.Unix(0, int64(n.AppendAtNs)).Add(time.Duration(ttlMinutes) * time.Minute)) {
		return // 未过期
	}
	// 已过期，返回 ErrorNotFound
	return -1, ErrorNotFound
}

// readNeedleMetaAt 在指定物理偏移处读取 Needle 的元数据
// 与 readNeedle 不同，此函数不通过索引查找，而是直接从指定位置读取
//
// 参数:
//   - n: Needle 对象（用于存储读取的元数据）
//   - offset: 物理偏移量（.dat 文件中的绝对位置）
//   - size: Needle 大小（可以为负数，表示已删除）
//
// 返回值:
//   - err: 读取错误
//
// 用途:
//   - Volume 修复工具：直接从文件偏移读取损坏的 Needle
//   - 数据恢复：读取已删除的 Needle 元数据
//   - 索引重建：扫描整个 .dat 文件重建索引
//
// 特殊处理:
//   - 已删除 Needle：size < 0 会被转换为 0（元数据部分不包含数据）
//   - 4字节偏移溢出：自动尝试 +MaxPossibleVolumeSize
//
// 注意:
//   此函数不检查 TTL、不验证索引，仅用于低级别操作
func (v *Volume) readNeedleMetaAt(n *needle.Needle, offset int64, size int32) (err error) {
	v.dataFileAccessLock.RLock()
	defer v.dataFileAccessLock.RUnlock()

	// 处理已删除 Needle 的负数大小
	if size < 0 {
		size = 0
	}
	// 读取 Needle 元数据（不包含实际数据）
	err = n.ReadNeedleMeta(v.DataBackend, offset, Size(size), v.Version())
	// 处理 4 字节偏移溢出（32 位偏移最大 4GB）
	if err == needle.ErrorSizeMismatch && OffsetSize == 4 {
		err = n.ReadNeedleMeta(v.DataBackend, offset+int64(MaxPossibleVolumeSize), Size(size), v.Version())
	}
	if err != nil {
		return err
	}
	return nil
}

// readNeedleDataInto 将 Needle 数据流式写入到 Writer（支持范围读取和 CRC 校验）
// 这是一个高级流式读取函数，专为大文件设计，支持分块读取、压缩感知和并发压缩
//
// 参数:
//   - n: Needle 对象（需要设置 Id 字段）
//   - readOption: 读取选项
//     * HasSlowRead: 慢速读取模式（每次读取前获取锁，适用于长时间读取）
//     * ReadBufferSize: 读取缓冲区大小（默认值）
//     * VolumeRevision: 卷压缩版本号（用于检测压缩）
//     * IsOutOfRange: 是否使用 +4GB 偏移（4字节偏移溢出）
//   - writer: 数据输出目标（io.Writer）
//   - offset: 数据内偏移（相对于 Needle 数据开始位置，用于范围读取）
//   - size: 读取大小（字节数）
//
// 返回值:
//   - err: 读取或写入错误
//
// 核心功能:
//  1. 【流式读取】分块读取，避免大文件占用大量内存
//  2. 【CRC 校验】增量计算 CRC32，确保数据完整性
//  3. 【压缩感知】在读取期间检测卷压缩，自动重新定位
//  4. 【范围读取】支持 HTTP Range 请求（offset, size）
//
// 锁策略:
//   - 普通模式：整个读取期间持有读锁（适用于小文件）
//   - 慢速模式：每次分块读取前临时获取锁（适用于大文件、长时间读取）
//
// CRC 校验:
//   - 仅在读取完整数据时校验（offset=0, size=DataSize）
//   - 使用增量 CRC 计算（crc.Update）
//   - 兼容旧版本 CRC 算法（crc.Value() vs uint32(crc)）
//
// 压缩感知:
//   在长时间读取期间，Volume 可能被压缩，此函数会：
//   1. 检测 VolumeRevision 变化
//   2. 重新从索引查找 Needle 位置
//   3. 继续从新位置读取
//
// 示例:
//   读取完整文件: readNeedleDataInto(n, opt, writer, 0, n.DataSize)
//   读取范围: readNeedleDataInto(n, opt, writer, 1024, 4096) // 读取 1KB-5KB
func (v *Volume) readNeedleDataInto(n *needle.Needle, readOption *ReadOption, writer io.Writer, offset int64, size int64) (err error) {

	// 锁策略 1: 普通模式 - 整个读取期间持有读锁
	if !readOption.HasSlowRead {
		v.dataFileAccessLock.RLock()
		defer v.dataFileAccessLock.RUnlock()
	}

	// 步骤 1: 从索引查找 Needle 位置（可能在慢速模式下临时加锁）
	if readOption.HasSlowRead {
		v.dataFileAccessLock.RLock()
	}
	nv, ok := v.nm.Get(n.Id)
	if readOption.HasSlowRead {
		v.dataFileAccessLock.RUnlock()
	}

	if !ok || nv.Offset.IsZero() {
		return ErrorNotFound
	}
	readSize := nv.Size

	// 步骤 2: 检查删除状态
	if readSize.IsDeleted() {
		if readOption != nil && readOption.ReadDeleted && readSize != TombstoneFileSize {
			glog.V(3).Infof("reading deleted %s", n.String())
			readSize = -readSize
		} else {
			return ErrorDeleted
		}
	}
	if readSize == 0 {
		return nil
	}

	// 步骤 3: 计算实际物理偏移
	actualOffset := nv.Offset.ToActualOffset()
	if readOption.IsOutOfRange {
		actualOffset += int64(MaxPossibleVolumeSize) // 处理 4 字节偏移溢出
	}

	// 步骤 4: 分配读取缓冲区（从内存池）
	buf := mem.Allocate(min(readOption.ReadBufferSize, int(size)))
	defer mem.Free(buf)

	// 步骤 5: 分块流式读取和 CRC 校验
	crc := needle.CRC(0) // 初始化 CRC 校验
	for x := offset; x < offset+size; x += int64(len(buf)) {

		// 慢速模式：每次读取前临时加锁
		if readOption.HasSlowRead {
			v.dataFileAccessLock.RLock()
		}

		// 检测卷压缩：如果压缩版本号变化，需要重新定位
		if readOption.VolumeRevision != v.SuperBlock.CompactionRevision {
			// 卷已被压缩，重新查找 Needle 位置
			nv, ok = v.nm.Get(n.Id)
			if !ok || nv.Offset.IsZero() {
				if readOption.HasSlowRead {
					v.dataFileAccessLock.RUnlock()
				}
				return ErrorNotFound
			}
			actualOffset = nv.Offset.ToActualOffset()
			readOption.VolumeRevision = v.SuperBlock.CompactionRevision
		}

		// 读取当前分块
		count, err := n.ReadNeedleData(v.DataBackend, actualOffset, buf, x)
		if readOption.HasSlowRead {
			v.dataFileAccessLock.RUnlock()
		}

		// 步骤 6: 写入数据到 Writer 并更新 CRC
		toWrite := min(count, int(offset+size-x))
		if toWrite > 0 {
			crc = crc.Update(buf[0:toWrite]) // 增量更新 CRC
			// CRC 校验（仅在读取完整数据时）
			// 兼容旧版本：同时检查 crc 和 crc.Value()
			if offset == 0 && size == int64(n.DataSize) && int64(count) == size && (n.Checksum != crc && uint32(n.Checksum) != crc.Value()) {
				// 此检查仅在满足以下条件时有效：
				// 1. 缓冲区足够大（读取完整数据）
				// 2. 请求读取所有数据（offset=0, size=DataSize）
				// 否则无法校验部分数据的有效性
				stats.VolumeServerHandlerCounter.WithLabelValues(stats.ErrorCRC).Inc()
				return fmt.Errorf("ReadNeedleData checksum %v expected %v for Needle: %v,%v", crc, n.Checksum, v.Id, n)
			}
			if _, err = writer.Write(buf[0:toWrite]); err != nil {
				return fmt.Errorf("ReadNeedleData write: %w", err)
			}
		}

		// 步骤 7: 错误处理
		if err != nil {
			if err == io.EOF {
				err = nil
				break
			}
			return fmt.Errorf("ReadNeedleData: %w", err)
		}
		if count <= 0 {
			break
		}
	}

	// 最终 CRC 校验（适用于小缓冲区多次读取的情况）
	if offset == 0 && size == int64(n.DataSize) && (n.Checksum != crc && uint32(n.Checksum) != crc.Value()) {
		stats.VolumeServerHandlerCounter.WithLabelValues(stats.ErrorCRC).Inc()
		return fmt.Errorf("ReadNeedleData checksum %v expected %v for Needle: %v,%v", crc, n.Checksum, v.Id, n)
	}
	return nil

}

// min 返回两个整数中的较小值
// 这是一个工具函数，用于计算每次读取的字节数
func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

// ReadNeedleBlob 读取 Needle 的原始二进制数据（低级别接口）
// 此函数直接从物理偏移读取原始字节，不解析 Needle 结构
//
// 参数:
//   - offset: 物理偏移量（.dat 文件中的绝对位置）
//   - size: 读取大小
//
// 返回值:
//   - []byte: 原始二进制数据（包含 Needle Header + Data + Padding + Footer）
//   - error: 读取错误
//
// 用途:
//   - 副本同步：直接复制原始 Needle 数据到副本
//   - 数据导出：不解析直接导出原始数据
//   - 调试工具：查看 Needle 的二进制结构
//
// 注意:
//   此函数不进行任何验证或解析，调用者需要自行处理数据格式
func (v *Volume) ReadNeedleBlob(offset int64, size Size) ([]byte, error) {
	v.dataFileAccessLock.RLock()
	defer v.dataFileAccessLock.RUnlock()

	return needle.ReadNeedleBlob(v.DataBackend, offset, size, v.Version())
}

// VolumeFileScanner Volume 文件扫描器接口
// 用于遍历整个 Volume 文件的所有 Needle，支持数据恢复、索引重建、备份等场景
//
// 实现此接口的类型可以逐个访问 Volume 中的每个 Needle
//
// 使用场景:
//   - 索引重建：扫描 .dat 文件重建 .idx 索引
//   - 数据修复：检查并修复损坏的 Needle
//   - 数据导出：将 Volume 导出为其他格式
//   - 数据统计：统计 Volume 的使用情况
//
// 方法说明:
//   - VisitSuperBlock: 访问 Volume 的超级块（包含版本、副本策略等元数据）
//   - ReadNeedleBody: 是否读取 Needle 的完整数据（如果只需要元数据，可返回 false）
//   - VisitNeedle: 访问每个 Needle（按物理顺序）
type VolumeFileScanner interface {
	// VisitSuperBlock 访问 Volume 的超级块
	// 在扫描开始时调用一次
	VisitSuperBlock(super_block.SuperBlock) error

	// ReadNeedleBody 是否需要读取 Needle 的完整数据
	// 返回 false 时，VisitNeedle 的 needleBody 参数为 nil
	ReadNeedleBody() bool

	// VisitNeedle 访问单个 Needle
	// 参数:
	//   - n: Needle 对象（已解析元数据）
	//   - offset: 物理偏移量（在 .dat 文件中的位置）
	//   - needleHeader: Needle 头部原始字节
	//   - needleBody: Needle 数据原始字节（如果 ReadNeedleBody() 返回 true）
	// 返回 io.EOF 可以提前终止扫描
	VisitNeedle(n *needle.Needle, offset int64, needleHeader, needleBody []byte) error
}

// ScanVolumeFile 扫描整个 Volume 文件，遍历所有 Needle
// 这是一个高级扫描接口，自动加载 Volume 并按顺序访问每个 Needle
//
// 参数:
//   - dirname: Volume 文件所在目录
//   - collection: Collection 名称
//   - id: Volume ID
//   - needleMapKind: 索引类型（NeedleMapInMemory、NeedleMapLevelDb 等）
//   - volumeFileScanner: 扫描器实现（实现 VolumeFileScanner 接口）
//
// 返回值:
//   - err: 扫描错误
//
// 工作流程:
//  1. 加载 Volume（不加载索引，节省内存）
//  2. 调用 VisitSuperBlock 访问超级块
//  3. 从超级块后开始逐个读取和访问 Needle
//  4. 自动关闭 Volume
//
// 典型用法:
//
//	type MyScanner struct{}
//	func (s *MyScanner) VisitSuperBlock(sb SuperBlock) error { return nil }
//	func (s *MyScanner) ReadNeedleBody() bool { return true }
//	func (s *MyScanner) VisitNeedle(n *Needle, offset int64, header, body []byte) error {
//	    fmt.Printf("Needle %d at %d\n", n.Id, offset)
//	    return nil
//	}
//	ScanVolumeFile("/data", "mycol", 3, NeedleMapInMemory, &MyScanner{})
//
// 注意:
//   - 此函数会按物理顺序访问 Needle，不是按 Needle ID 顺序
//   - 扫描期间不持有锁，适合离线处理
func ScanVolumeFile(dirname string, collection string, id needle.VolumeId,
	needleMapKind NeedleMapKind,
	volumeFileScanner VolumeFileScanner) (err error) {
	var v *Volume
	// 步骤 1: 加载 Volume（不加载索引）
	if v, err = loadVolumeWithoutIndex(dirname, collection, id, needleMapKind, needle.GetCurrentVersion()); err != nil {
		return fmt.Errorf("failed to load volume %d: %v", id, err)
	}
	// 步骤 2: 访问超级块
	if err = volumeFileScanner.VisitSuperBlock(v.SuperBlock); err != nil {
		return fmt.Errorf("failed to process volume %d super block: %v", id, err)
	}
	defer v.Close()

	version := v.Version()

	// 步骤 3: 从超级块后开始扫描
	offset := int64(v.SuperBlock.BlockSize())

	return ScanVolumeFileFrom(version, v.DataBackend, offset, volumeFileScanner)
}

// ScanVolumeFileFrom 从指定偏移开始扫描 Volume 文件
// 这是一个低级别扫描函数，不需要完整的 Volume 对象
//
// 参数:
//   - version: Needle 版本号（影响 Needle 格式）
//   - datBackend: 数据存储后端（.dat 文件）
//   - offset: 开始扫描的偏移量（通常是超级块后）
//   - volumeFileScanner: 扫描器实现
//
// 返回值:
//   - err: 扫描错误（io.EOF 表示正常结束）
//
// 工作原理:
//  1. 从 offset 读取 Needle Header
//  2. 根据 Header 信息读取 Needle Body（如果需要）
//  3. 调用 volumeFileScanner.VisitNeedle
//  4. 移动到下一个 Needle（offset += HeaderSize + BodySize）
//  5. 重复直到文件结束或遇到错误
//
// Needle 布局:
//   [Needle Header (NeedleHeaderSize)] [Needle Body (rest)] [Needle Footer]
//
// 错误处理:
//   - io.EOF: 正常结束，返回 nil
//   - 其他错误: 记录日志并返回错误
//
// 用途:
//   - 直接扫描 .dat 文件（不需要加载 Volume）
//   - 从特定位置恢复扫描
//   - 并行扫描多个 Volume
func ScanVolumeFileFrom(version needle.Version, datBackend backend.BackendStorageFile, offset int64, volumeFileScanner VolumeFileScanner) (err error) {
	// 步骤 1: 读取第一个 Needle Header
	n, nh, rest, e := needle.ReadNeedleHeader(datBackend, version, offset)
	if e != nil {
		if e == io.EOF {
			return nil // 空文件或已到达文件末尾
		}
		return fmt.Errorf("cannot read %s at offset %d: %v", datBackend.Name(), offset, e)
	}
	// 步骤 2: 循环处理每个 Needle
	for n != nil {
		var needleBody []byte
		// 读取 Needle Body（如果需要）
		if volumeFileScanner.ReadNeedleBody() {
			if needleBody, err = n.ReadNeedleBody(datBackend, version, offset+NeedleHeaderSize, rest); err != nil {
				glog.V(0).Infof("cannot read needle head [%d, %d) body [%d, %d) body length %d: %v", offset, offset+NeedleHeaderSize, offset+NeedleHeaderSize, offset+NeedleHeaderSize+rest, rest, err)
				// 注意：读取 Body 失败不中断扫描，继续下一个 Needle
			}
		}
		// 步骤 3: 访问 Needle
		err := volumeFileScanner.VisitNeedle(n, offset, nh, needleBody)
		if err == io.EOF {
			return nil // 扫描器请求提前终止
		}
		if err != nil {
			glog.V(0).Infof("visit needle error: %v", err)
			return fmt.Errorf("visit needle error: %w", err)
		}
		// 步骤 4: 移动到下一个 Needle
		offset += NeedleHeaderSize + rest
		glog.V(4).Infof("==> new entry offset %d", offset)
		// 步骤 5: 读取下一个 Needle Header
		if n, nh, rest, err = needle.ReadNeedleHeader(datBackend, version, offset); err != nil {
			if err == io.EOF {
				return nil // 正常到达文件末尾
			}
			return fmt.Errorf("cannot read needle header at offset %d: %v", offset, err)
		}
		glog.V(4).Infof("new entry needle size:%d rest:%d", n.Size, rest)
	}
	return nil
}
