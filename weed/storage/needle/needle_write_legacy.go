// Package needle 提供 SeaweedFS 的 Needle 存储结构和相关操作
// 本文件实现 Needle 的旧版写入逻辑（Legacy Write）
//
// ============================================================================
// 旧版写入与新版写入
// ============================================================================
//
// 这个文件包含"旧版"（Legacy）的 Needle 写入实现。
// 新版实现在 needle_write.go 中，使用优化的内存分配策略。
//
// 保留旧版实现的原因：
// 1. 向后兼容性测试
// 2. 作为新实现的参考基准
// 3. 某些场景下可能仍需要使用
//
// ============================================================================
// Needle 版本差异
// ============================================================================
//
// Version1 (最简单):
//   Header(16) + Data(N) + Checksum(4) + Padding
//
// Version2 (增加元数据):
//   Header(16) + DataSize(4) + Data(N) + Flags(1) +
//   [NameSize(1) + Name] + [MimeSize(1) + Mime] +
//   [LastModified(5)] + [Ttl(2)] + [PairsSize(2) + Pairs] +
//   Checksum(4) + Padding
//
// Version3 (增加时间戳):
//   与 Version2 相同，但在 Checksum 后增加 AppendAtNs(8)
//
// ============================================================================
package needle

import (
	"bytes"
	"fmt"
	"math"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/buffer_pool"
)

// LegacyPrepareWriteBuffer 准备 Needle 的写入缓冲区（旧版实现）
//
// 这个函数将 Needle 序列化为二进制格式，写入到提供的 bytes.Buffer 中。
// 支持 Version1、Version2、Version3 三种格式。
//
// 参数:
//   - version: Needle 版本（Version1/Version2/Version3）
//   - writeBytes: 输出缓冲区，序列化后的数据写入此处
//
// 返回:
//   - Size: 数据大小（Version1 为 n.Size，Version2/3 为 n.DataSize）
//   - int64: 实际写入的总字节数（包含 Header、Padding 等）
//   - error: 序列化错误
//
// Version1 布局：
//
//	+--------+----------+------+------+----------+---------+
//	| Cookie | NeedleId | Size | Data | Checksum | Padding |
//	| 4 bytes| 8 bytes  | 4 B  | N B  | 4 bytes  | 0-7 B   |
//	+--------+----------+------+------+----------+---------+
//
// Version2/3 布局：
//
//	+--------+----------+------+----------+------+-------+...+----------+---------+
//	| Cookie | NeedleId | Size | DataSize | Data | Flags |...| Checksum | Padding |
//	| 4 B    | 8 B      | 4 B  | 4 B      | N B  | 1 B   |...| 4 B      | 0-7 B   |
//	+--------+----------+------+----------+------+-------+...+----------+---------+
func (n *Needle) LegacyPrepareWriteBuffer(version Version, writeBytes *bytes.Buffer) (Size, int64, error) {
	// 重置缓冲区，准备写入新数据
	writeBytes.Reset()

	switch version {
	case Version1:
		// ========== Version1: 最简单的格式 ==========
		// 只包含基本的 Header、Data、Checksum

		// 创建 Header 缓冲区（16 字节）
		header := make([]byte, NeedleHeaderSize)

		// 写入 Cookie（前 4 字节）
		CookieToBytes(header[0:CookieSize], n.Cookie)

		// 写入 NeedleId（接下来 8 字节）
		NeedleIdToBytes(header[CookieSize:CookieSize+NeedleIdSize], n.Id)

		// 计算并写入 Size（最后 4 字节）
		// Version1 的 Size 就是 Data 的长度
		n.Size = Size(len(n.Data))
		SizeToBytes(header[CookieSize+NeedleIdSize:CookieSize+NeedleIdSize+SizeSize], n.Size)

		// 保存 size 和计算 actualSize
		size := n.Size
		actualSize := NeedleHeaderSize + int64(n.Size)

		// 写入 Header
		writeBytes.Write(header)

		// 写入 Data
		writeBytes.Write(n.Data)

		// 计算 Padding 长度（对齐到 8 字节边界）
		padding := PaddingLength(n.Size, version)

		// 写入 Checksum（复用 header 缓冲区的前 4 字节）
		util.Uint32toBytes(header[0:NeedleChecksumSize], uint32(n.Checksum))

		// 写入 Checksum + Padding
		writeBytes.Write(header[0 : NeedleChecksumSize+padding])

		return size, actualSize, nil

	case Version2, Version3:
		// ========== Version2/Version3: 完整的元数据格式 ==========

		// 创建扩展的 Header 缓冲区（包含 TimestampSize 以便复用）
		header := make([]byte, NeedleHeaderSize+TimestampSize)

		// 写入 Cookie
		CookieToBytes(header[0:CookieSize], n.Cookie)

		// 写入 NeedleId
		NeedleIdToBytes(header[CookieSize:CookieSize+NeedleIdSize], n.Id)

		// 处理文件名长度（最大 255 字节）
		if len(n.Name) >= math.MaxUint8 {
			n.NameSize = math.MaxUint8
		} else {
			n.NameSize = uint8(len(n.Name))
		}

		// 设置 DataSize 和 MimeSize
		n.DataSize, n.MimeSize = uint32(len(n.Data)), uint8(len(n.Mime))

		// 计算 Size（所有可选字段的总大小）
		if n.DataSize > 0 {
			// 基础大小：DataSize(4) + Data(N) + Flags(1)
			n.Size = 4 + Size(n.DataSize) + 1

			// 可选：Name
			if n.HasName() {
				n.Size = n.Size + 1 + Size(n.NameSize) // NameSize(1) + Name(N)
			}

			// 可选：Mime
			if n.HasMime() {
				n.Size = n.Size + 1 + Size(n.MimeSize) // MimeSize(1) + Mime(N)
			}

			// 可选：LastModified（固定 5 字节）
			if n.HasLastModifiedDate() {
				n.Size = n.Size + LastModifiedBytesLength
			}

			// 可选：Ttl（固定 2 字节）
			if n.HasTtl() {
				n.Size = n.Size + TtlBytesLength
			}

			// 可选：Pairs
			if n.HasPairs() {
				n.Size += 2 + Size(n.PairsSize) // PairsSize(2) + Pairs(N)
			}
		} else {
			// 空数据（删除标记）
			n.Size = 0
		}

		// 写入 Size 到 Header
		SizeToBytes(header[CookieSize+NeedleIdSize:CookieSize+NeedleIdSize+SizeSize], n.Size)

		// 写入 Header（前 16 字节）
		writeBytes.Write(header[0:NeedleHeaderSize])

		// 如果有数据，写入数据和元数据
		if n.DataSize > 0 {
			// 写入 DataSize（4 字节）
			util.Uint32toBytes(header[0:4], n.DataSize)
			writeBytes.Write(header[0:4])

			// 写入 Data
			writeBytes.Write(n.Data)

			// 写入 Flags（1 字节）
			util.Uint8toBytes(header[0:1], n.Flags)
			writeBytes.Write(header[0:1])

			// 写入可选的 Name
			if n.HasName() {
				util.Uint8toBytes(header[0:1], n.NameSize)
				writeBytes.Write(header[0:1])
				writeBytes.Write(n.Name[:n.NameSize])
			}

			// 写入可选的 Mime
			if n.HasMime() {
				util.Uint8toBytes(header[0:1], n.MimeSize)
				writeBytes.Write(header[0:1])
				writeBytes.Write(n.Mime)
			}

			// 写入可选的 LastModified（5 字节，从 8 字节 uint64 中取后 5 字节）
			if n.HasLastModifiedDate() {
				util.Uint64toBytes(header[0:8], n.LastModified)
				writeBytes.Write(header[8-LastModifiedBytesLength : 8])
			}

			// 写入可选的 Ttl（2 字节）
			if n.HasTtl() && n.Ttl != nil {
				n.Ttl.ToBytes(header[0:TtlBytesLength])
				writeBytes.Write(header[0:TtlBytesLength])
			}

			// 写入可选的 Pairs
			if n.HasPairs() {
				util.Uint16toBytes(header[0:2], n.PairsSize)
				writeBytes.Write(header[0:2])
				writeBytes.Write(n.Pairs)
			}
		}

		// 计算 Padding
		padding := PaddingLength(n.Size, version)

		// 写入 Checksum
		util.Uint32toBytes(header[0:NeedleChecksumSize], uint32(n.Checksum))

		if version == Version2 {
			// Version2: 只有 Checksum + Padding
			writeBytes.Write(header[0 : NeedleChecksumSize+padding])
		} else {
			// Version3: Checksum + Timestamp + Padding
			util.Uint64toBytes(header[NeedleChecksumSize:NeedleChecksumSize+TimestampSize], n.AppendAtNs)
			writeBytes.Write(header[0 : NeedleChecksumSize+TimestampSize+padding])
		}

		return Size(n.DataSize), GetActualSize(n.Size, version), nil
	}

	return 0, 0, fmt.Errorf("Unsupported Version! (%d)", version)
}

// LegacyAppend 将 Needle 追加到 Volume 文件（旧版实现）
//
// 这个函数将 Needle 写入到 Volume 数据文件的末尾。
// 使用文件锁和事务语义，确保写入的原子性。
//
// 事务语义：
// - 写入前记录文件当前大小
// - 写入失败时截断文件到原始大小（回滚）
// - 这确保了即使写入中途失败，文件也保持一致状态
//
// 参数:
//   - w: 后端存储文件接口
//   - version: Needle 版本
//
// 返回:
//   - offset: Needle 在文件中的偏移量（uint64）
//   - size: 数据大小
//   - actualSize: 实际写入的字节数（包含 Header、Padding 等）
//   - err: 写入错误
func (n *Needle) LegacyAppend(w backend.BackendStorageFile, version Version) (offset uint64, size Size, actualSize int64, err error) {
	// 获取当前文件大小（即新 Needle 的偏移量）
	if end, _, e := w.GetStat(); e == nil {
		// 设置 defer 处理写入失败的回滚
		// 如果 err != nil，将文件截断到原始大小
		defer func(w backend.BackendStorageFile, off int64) {
			if err != nil {
				// 尝试回滚：截断文件到写入前的大小
				if te := w.Truncate(end); te != nil {
					glog.V(0).Infof("Failed to truncate %s back to %d with error: %v", w.Name(), end, te)
				}
			}
		}(w, end)
		offset = uint64(end)
	} else {
		err = fmt.Errorf("Cannot Read Current Volume Position: %w", e)
		return
	}

	// 检查 Volume 大小限制
	// MaxPossibleVolumeSize 是 32GB（使用 32 位偏移时）或更大（64 位偏移）
	if offset >= MaxPossibleVolumeSize && len(n.Data) != 0 {
		err = fmt.Errorf("Volume Size %d Exceeded %d", offset, MaxPossibleVolumeSize)
		return
	}

	// 从缓冲池获取写入缓冲区
	// 使用缓冲池减少内存分配和 GC 压力
	bytesBuffer := buffer_pool.SyncPoolGetBuffer()
	defer buffer_pool.SyncPoolPutBuffer(bytesBuffer)

	// 序列化 Needle 到缓冲区
	size, actualSize, err = n.LegacyPrepareWriteBuffer(version, bytesBuffer)

	// 执行写入
	if err == nil {
		_, err = w.WriteAt(bytesBuffer.Bytes(), int64(offset))
		if err != nil {
			err = fmt.Errorf("failed to write %d bytes to %s at offset %d: %w",
				actualSize, w.Name(), offset, err)
		}
	}

	return offset, size, actualSize, err
}
