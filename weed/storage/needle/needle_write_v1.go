// Package needle 实现 Version1 格式的 Needle 序列化
// Version1 是最早期的基础格式，结构简单但功能受限
package needle

import (
	"bytes"

	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// writeNeedleV1 将 Needle 序列化为 Version1 格式的二进制数据
// Version1 是 SeaweedFS 的初始版本，采用最简化的存储格式
//
// 参数:
//   - n: 要序列化的 Needle 对象
//   - offset: 写入偏移量（本版本未使用，用于接口统一）
//   - bytesBuffer: 输出缓冲区
//
// 返回值:
//   - size: 数据大小（等于 n.Data 的长度）
//   - actualSize: 实际序列化后的总字节数（Header + Data）
//   - err: 错误信息（V1 格式不会失败，始终为 nil）
//
// Version1 格式布局（固定字段）:
//
//	+------------------+--------+----------------------------------+
//	| Cookie (4 bytes) | 随机值  | 用于验证 Needle 有效性             |
//	+------------------+--------+----------------------------------+
//	| NeedleId (8)     | ID     | 全局唯一的 Needle 标识符           |
//	+------------------+--------+----------------------------------+
//	| Size (4)         | 大小    | 数据部分的字节数                  |
//	+------------------+--------+----------------------------------+
//	| Data (N)         | 数据    | 实际的文件内容（原始字节）         |
//	+------------------+--------+----------------------------------+
//	| Checksum (4)     | 校验和  | CRC32 校验（仅 Data 部分）        |
//	+------------------+--------+----------------------------------+
//	| Padding (0-7)    | 填充    | 对齐到 8 字节边界                 |
//	+------------------+--------+----------------------------------+
//
// 特点:
//   - 不支持元数据（文件名、MIME 类型等）
//   - 不支持时间戳（Last-Modified、AppendAt）
//   - 不支持 TTL 过期时间
//   - 不支持自定义键值对
//   - 结构简单，性能最高
//
// 注意:
//   - Size 字段只记录 Data 长度，不包含其他字段
//   - actualSize 计算不包括 Checksum 和 Padding
//   - 使用 8 字节对齐以优化磁盘访问性能
func writeNeedleV1(n *Needle, offset uint64, bytesBuffer *bytes.Buffer) (size Size, actualSize int64, err error) {
	// 重置缓冲区，清空之前的数据
	bytesBuffer.Reset()

	// 分配 16 字节的 header（复用于多处以减少分配）
	header := make([]byte, NeedleHeaderSize)

	// === 写入 Header 部分（16 字节）===

	// Cookie: 前 4 字节，随机魔数用于验证完整性
	CookieToBytes(header[0:CookieSize], n.Cookie)

	// NeedleId: 接下来 8 字节，全局唯一标识符
	NeedleIdToBytes(header[CookieSize:CookieSize+NeedleIdSize], n.Id)

	// Size: 最后 4 字节，数据长度
	n.Size = Size(len(n.Data))
	SizeToBytes(header[CookieSize+NeedleIdSize:CookieSize+NeedleIdSize+SizeSize], n.Size)

	// 记录返回值
	size = n.Size
	actualSize = NeedleHeaderSize + int64(n.Size)

	// 写入 Header 到缓冲区
	bytesBuffer.Write(header)

	// === 写入 Data 部分 ===
	bytesBuffer.Write(n.Data)

	// === 写入 Footer 部分（Checksum + Padding）===

	// 计算需要的 padding 字节数（对齐到 8 字节）
	padding := PaddingLength(n.Size, Version1)

	// 复用 header 数组写入 Checksum（4 字节）
	util.Uint32toBytes(header[0:NeedleChecksumSize], uint32(n.Checksum))

	// 写入 Checksum + Padding（padding 部分自动为 0）
	bytesBuffer.Write(header[0 : NeedleChecksumSize+padding])

	return size, actualSize, nil
}
