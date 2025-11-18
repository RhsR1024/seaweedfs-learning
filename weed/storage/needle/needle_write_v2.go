// Package needle 实现 Version2 格式的 Needle 序列化
// Version2 增加了丰富的元数据支持，是生产环境的主流版本
package needle

import (
	"bytes"
	"math"

	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// writeNeedleV2 将 Needle 序列化为 Version2 格式的二进制数据
// Version2 在 V1 基础上增加了元数据（文件名、MIME、TTL 等）
//
// 参数:
//   - n: 要序列化的 Needle 对象
//   - offset: 写入偏移量（传递给通用函数）
//   - bytesBuffer: 输出缓冲区
//
// 返回值:
//   - size: 数据大小（n.DataSize）
//   - actualSize: 实际序列化后的总字节数
//   - err: 错误信息
//
// Version2 格式特点:
//   - 支持文件名（Name）、MIME 类型
//   - 支持 Last-Modified 时间戳
//   - 支持 TTL 过期时间
//   - 支持自定义键值对（Pairs）
//   - Footer 只包含 Checksum + Padding
//
// 实现方式:
//   - 调用 writeNeedleCommon 处理主体部分
//   - 使用自定义的 writeFooter 函数写入尾部
func writeNeedleV2(n *Needle, offset uint64, bytesBuffer *bytes.Buffer) (size Size, actualSize int64, err error) {
	// 调用通用序列化函数，传入 V2 的 footer 写入逻辑
	return writeNeedleCommon(n, offset, bytesBuffer, Version2, func(n *Needle, header []byte, bytesBuffer *bytes.Buffer, padding int) {
		// Version2 Footer: Checksum(4) + Padding(0-7)

		// 写入 Checksum（4 字节）
		util.Uint32toBytes(header[0:NeedleChecksumSize], uint32(n.Checksum))

		// 写入 Checksum + Padding
		bytesBuffer.Write(header[0 : NeedleChecksumSize+padding])
	})
}

// writeNeedleCommon 是 Version2/Version3 的通用序列化函数
// 处理复杂的元数据字段和动态大小计算
//
// 参数:
//   - n: 要序列化的 Needle 对象
//   - offset: 写入偏移量（未使用，预留）
//   - bytesBuffer: 输出缓冲区
//   - version: 版本号（影响 padding 计算和 footer 格式）
//   - writeFooter: 自定义的 footer 写入函数（V2/V3 不同）
//
// 返回值:
//   - size: 数据大小（n.DataSize）
//   - actualSize: 实际序列化后的总字节数
//   - err: 错误信息
//
// Version2+ 格式布局（可变字段）:
//
//	+------------------+--------+----------------------------------+
//	| Cookie (4 bytes) | 随机值  | 验证 Needle 有效性                |
//	+------------------+--------+----------------------------------+
//	| NeedleId (8)     | ID     | 全局唯一标识符                    |
//	+------------------+--------+----------------------------------+
//	| Size (4)         | 大小    | 元数据区总大小（不含 Data 本身）   |
//	+------------------+--------+----------------------------------+
//	| DataSize (4)     | 长度    | 实际数据的字节数                  |
//	+------------------+--------+----------------------------------+
//	| Data (N)         | 数据    | 实际的文件内容                    |
//	+------------------+--------+----------------------------------+
//	| Flags (1)        | 标志位  | 指示后续字段的存在性              |
//	+------------------+--------+----------------------------------+
//	| [Name]           | 可选    | 文件名（如果 Flags 指示）         |
//	+------------------+--------+----------------------------------+
//	| [Mime]           | 可选    | MIME 类型（如果 Flags 指示）      |
//	+------------------+--------+----------------------------------+
//	| [LastModified]   | 可选    | 最后修改时间（如果 Flags 指示）    |
//	+------------------+--------+----------------------------------+
//	| [TTL]            | 可选    | 过期时间（如果 Flags 指示）        |
//	+------------------+--------+----------------------------------+
//	| [Pairs]          | 可选    | 自定义键值对（如果 Flags 指示）    |
//	+------------------+--------+----------------------------------+
//	| Checksum (4)     | 校验和  | CRC32 校验（所有数据）            |
//	+------------------+--------+----------------------------------+
//	| [AppendAtNs (8)] | V3 专属 | 追加时间戳（纳秒）                |
//	+------------------+--------+----------------------------------+
//	| Padding (0-7)    | 填充    | 对齐到 8 字节边界                 |
//	+------------------+--------+----------------------------------+
//
// Size 字段计算规则:
//   - DataSize(4) + Data 长度 + Flags(1) = 基础部分
//   - 如果有 Name: + NameSize(1) + Name 内容
//   - 如果有 Mime: + MimeSize(1) + Mime 内容
//   - 如果有 LastModified: + 5 字节时间戳
//   - 如果有 TTL: + 2 字节 TTL
//   - 如果有 Pairs: + PairsSize(2) + Pairs 内容
//
// 注意:
//   - Size 不包含 Header、Checksum、AppendAtNs、Padding
//   - 文件名长度限制为 255 字节（uint8）
//   - 使用 Flags 按需存储字段，节省空间
func writeNeedleCommon(n *Needle, offset uint64, bytesBuffer *bytes.Buffer, version Version, writeFooter func(n *Needle, header []byte, bytesBuffer *bytes.Buffer, padding int)) (size Size, actualSize int64, err error) {
	// 重置缓冲区
	bytesBuffer.Reset()

	// 分配 header 数组（最大需要 NeedleHeaderSize + TimestampSize）
	header := make([]byte, NeedleHeaderSize+TimestampSize)

	// === 写入 Header 部分（16 字节）===

	// Cookie: 前 4 字节
	CookieToBytes(header[0:CookieSize], n.Cookie)

	// NeedleId: 接下来 8 字节
	NeedleIdToBytes(header[CookieSize:CookieSize+NeedleIdSize], n.Id)

	// === 计算和设置字段大小 ===

	// 文件名长度限制为 255（uint8 最大值）
	if len(n.Name) >= math.MaxUint8 {
		n.NameSize = math.MaxUint8
	} else {
		n.NameSize = uint8(len(n.Name))
	}

	// 设置数据和 MIME 大小
	n.DataSize, n.MimeSize = uint32(len(n.Data)), uint8(len(n.Mime))

	// === 计算 Size 字段（元数据区总大小）===
	if n.DataSize > 0 {
		// 基础部分: DataSize(4) + Data 长度 + Flags(1)
		n.Size = 4 + Size(n.DataSize) + 1

		// 可选字段（根据 Flags 标志位判断）
		if n.HasName() {
			n.Size = n.Size + 1 + Size(n.NameSize) // NameSize(1) + Name 内容
		}
		if n.HasMime() {
			n.Size = n.Size + 1 + Size(n.MimeSize) // MimeSize(1) + Mime 内容
		}
		if n.HasLastModifiedDate() {
			n.Size = n.Size + LastModifiedBytesLength // 5 字节时间戳
		}
		if n.HasTtl() {
			n.Size = n.Size + TtlBytesLength // 2 字节 TTL
		}
		if n.HasPairs() {
			n.Size += 2 + Size(n.PairsSize) // PairsSize(2) + Pairs 内容
		}
	} else {
		// 如果没有数据（删除标记），Size 为 0
		n.Size = 0
	}

	// Size: Header 最后 4 字节
	SizeToBytes(header[CookieSize+NeedleIdSize:CookieSize+NeedleIdSize+SizeSize], n.Size)

	// 写入 Header 到缓冲区
	bytesBuffer.Write(header[0:NeedleHeaderSize])

	// === 写入 Body 部分（仅当有数据时）===
	if n.DataSize > 0 {
		// 1. DataSize (4 字节)
		util.Uint32toBytes(header[0:4], n.DataSize)
		bytesBuffer.Write(header[0:4])

		// 2. Data (实际文件内容)
		bytesBuffer.Write(n.Data)

		// 3. Flags (1 字节，标识后续字段的存在性)
		util.Uint8toBytes(header[0:1], n.Flags)
		bytesBuffer.Write(header[0:1])

		// 4. 可选字段（根据 Flags 决定）

		// Name（文件名）
		if n.HasName() {
			util.Uint8toBytes(header[0:1], n.NameSize)
			bytesBuffer.Write(header[0:1])
			bytesBuffer.Write(n.Name[:n.NameSize])
		}

		// Mime（MIME 类型）
		if n.HasMime() {
			util.Uint8toBytes(header[0:1], n.MimeSize)
			bytesBuffer.Write(header[0:1])
			bytesBuffer.Write(n.Mime)
		}

		// LastModified（最后修改时间，5 字节）
		if n.HasLastModifiedDate() {
			util.Uint64toBytes(header[0:8], n.LastModified)
			bytesBuffer.Write(header[8-LastModifiedBytesLength : 8]) // 取低 5 字节
		}

		// TTL（过期时间，2 字节）
		if n.HasTtl() && n.Ttl != nil {
			n.Ttl.ToBytes(header[0:TtlBytesLength])
			bytesBuffer.Write(header[0:TtlBytesLength])
		}

		// Pairs（自定义键值对）
		if n.HasPairs() {
			util.Uint16toBytes(header[0:2], n.PairsSize)
			bytesBuffer.Write(header[0:2])
			bytesBuffer.Write(n.Pairs)
		}
	}

	// === 写入 Footer 部分（Checksum + [AppendAtNs] + Padding）===

	// 计算 padding 字节数（对齐到 8 字节）
	padding := PaddingLength(n.Size, version)

	// 调用版本特定的 footer 写入函数
	writeFooter(n, header, bytesBuffer, int(padding))

	// === 返回值计算 ===
	size = Size(n.DataSize)                  // 数据大小
	actualSize = GetActualSize(n.Size, version) // 实际总大小

	return size, actualSize, nil
}
