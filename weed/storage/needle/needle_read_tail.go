// Package needle 提供 SeaweedFS 的核心数据结构和操作
//
// 本文件实现 Needle 尾部数据的读取和长度计算功能。
// Needle 尾部包含校验和（Checksum）和可选的时间戳（Version 3+）。
package needle

import (
	"errors"

	"github.com/seaweedfs/seaweedfs/weed/stats"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// readNeedleTail 读取 Needle 的尾部数据
//
// Needle 的尾部数据包含：
// 1. Checksum（4 字节）：数据完整性校验和，所有版本都有
// 2. AppendAtNs（8 字节）：追加时间戳（纳秒），仅 Version 3 有
//
// 尾部数据布局：
//   Version 1/2: [Checksum(4)]
//   Version 3:   [Checksum(4)][AppendAtNs(8)]
//
// 功能说明：
// - 从 needleBody 读取校验和，与数据的实际校验和比较
// - 如果校验失败，返回 CRC 错误
// - 如果是 Version 3，额外读取追加时间戳
// - 如果数据为空（被跳过），只读取校验和不进行校验
//
// 参数：
//   needleBody: Needle 尾部数据的字节数组
//   version: Volume 版本号
//
// 返回：
//   error: 如果校验和不匹配，返回 CRC 错误
//
// 使用场景：
//   - Volume 读取 Needle 数据时验证完整性
//   - 检测磁盘数据损坏
//   - 读取文件的追加时间信息
//
// 向后兼容性说明：
//   代码中保留了对旧版本 CRC 计算方式的兼容性处理。
//   在 commit 056c480eb 中，CRC 计算从 crc.Value() 改为 uint32(crc)，
//   该改动出现在 version 3.09 中。
func (n *Needle) readNeedleTail(needleBody []byte, version Version) error {

	// 所有版本都需要读取和验证校验和
	if len(n.Data) > 0 {
		// 从 needleBody 读取存储的校验和（前 4 字节）
		expectedChecksum := CRC(util.BytesToUint32(needleBody[0:NeedleChecksumSize]))
		// 计算数据的实际校验和
		dataChecksum := NewCRC(n.Data)
		if expectedChecksum != dataChecksum {
			// 校验和不匹配，数据损坏
			// 注意：crc.Value() 函数已被弃用，此处的双重检查是为了向后兼容
			// 旧版本使用 crc.Value() 而不是 uint32(crc)，该改动出现在 commit 056c480eb
			// 版本切换发生在 version 3.09
			stats.VolumeServerHandlerCounter.WithLabelValues(stats.ErrorCRC).Inc()
			return errors.New("CRC error! Data On Disk Corrupted")
		}
		n.Checksum = dataChecksum
	} else {
		// 当数据被跳过读取时（如只需要元数据），仅读取校验和，不进行校验
		n.Checksum = CRC(util.BytesToUint32(needleBody[0:NeedleChecksumSize]))
	}

	// Version 3 增加了追加时间戳字段
	if version == Version3 {
		// 时间戳位于校验和之后的 8 字节
		tsOffset := NeedleChecksumSize
		n.AppendAtNs = util.BytesToUint64(needleBody[tsOffset : tsOffset+TimestampSize])
	}
	return nil
}

// PaddingLength 计算 Needle 的填充长度
//
// 为了优化磁盘 I/O 性能，Needle 需要对齐到 8 字节边界。
// 填充长度确保整个 Needle（Header + Data + Tail + Padding）的总长度是 8 的倍数。
//
// Needle 总长度计算：
//   Version 1/2: NeedleHeaderSize + needleSize + NeedleChecksumSize + Padding
//   Version 3:   NeedleHeaderSize + needleSize + NeedleChecksumSize + TimestampSize + Padding
//
// 参数：
//   needleSize: Needle 数据部分的大小（字节）
//   version: Volume 版本号
//
// 返回：需要填充的字节数（0-7）
//
// 示例：
//   假设 NeedleHeaderSize=16, NeedleChecksumSize=4, TimestampSize=8, NeedlePaddingSize=8
//   Version 2, needleSize=10:
//     totalSize = 16 + 10 + 4 = 30
//     padding = 8 - (30 % 8) = 8 - 6 = 2 字节
//
//   Version 3, needleSize=10:
//     totalSize = 16 + 10 + 4 + 8 = 38
//     padding = 8 - (38 % 8) = 8 - 6 = 2 字节
//
// 使用场景：
//   - 写入 Needle 时计算需要填充多少字节
//   - 读取 Needle 时定位下一个 Needle 的位置
//   - 磁盘空间使用量统计
func PaddingLength(needleSize Size, version Version) Size {
	if version == Version3 {
		// Version 3 包含时间戳字段
		// 注意：虽然代码注释说与 Version 2 相同，但实际上多了 TimestampSize
		return NeedlePaddingSize - ((NeedleHeaderSize + needleSize + NeedleChecksumSize + TimestampSize) % NeedlePaddingSize)
	}
	// Version 1/2 不包含时间戳字段
	return NeedlePaddingSize - ((NeedleHeaderSize + needleSize + NeedleChecksumSize) % NeedlePaddingSize)
}

// NeedleBodyLength 计算 Needle 尾部（Body）的总长度
//
// Needle Body 包含：
//   Version 1/2: Data + Checksum + Padding
//   Version 3:   Data + Checksum + Timestamp + Padding
//
// 注意：这里的 "Body" 指的是数据部分及其尾部，不包括 Header。
//
// 参数：
//   needleSize: Needle 数据部分的大小（字节）
//   version: Volume 版本号
//
// 返回：Needle Body 的总长度（字节）
//
// 计算公式：
//   Version 1/2: needleSize + NeedleChecksumSize + PaddingLength
//   Version 3:   needleSize + NeedleChecksumSize + TimestampSize + PaddingLength
//
// 示例：
//   假设 needleSize=100, NeedleChecksumSize=4, TimestampSize=8
//   Version 2:
//     padding = PaddingLength(100, Version2) = 假设为 4
//     bodyLength = 100 + 4 + 4 = 108 字节
//
//   Version 3:
//     padding = PaddingLength(100, Version3) = 假设为 4
//     bodyLength = 100 + 4 + 8 + 4 = 116 字节
//
// 使用场景：
//   - 文件 I/O 操作时确定读取长度
//   - 计算 Needle 在 Volume 文件中的偏移量
//   - 磁盘空间分配和管理
func NeedleBodyLength(needleSize Size, version Version) int64 {
	if version == Version3 {
		// Version 3: Data + Checksum + Timestamp + Padding
		return int64(needleSize) + NeedleChecksumSize + TimestampSize + int64(PaddingLength(needleSize, version))
	}
	// Version 1/2: Data + Checksum + Padding
	return int64(needleSize) + NeedleChecksumSize + int64(PaddingLength(needleSize, version))
}
