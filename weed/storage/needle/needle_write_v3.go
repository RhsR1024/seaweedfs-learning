// Package needle 实现 Version3 格式的 Needle 序列化
// Version3 在 V2 基础上增加了纳秒级追加时间戳，支持更精细的时间追踪
package needle

import (
	"bytes"

	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// writeNeedleV3 将 Needle 序列化为 Version3 格式的二进制数据
// Version3 是 SeaweedFS 的最新版本，增加了 AppendAtNs 追加时间戳
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
// Version3 格式特点:
//   - 继承 Version2 的所有元数据支持
//   - 新增 AppendAtNs 字段（8 字节纳秒级时间戳）
//   - Footer 包含 Checksum + AppendAtNs + Padding
//
// Version3 vs Version2:
//   - V2 Footer: Checksum(4) + Padding(0-7)
//   - V3 Footer: Checksum(4) + AppendAtNs(8) + Padding(0-7)
//
// AppendAtNs 用途:
//   - 记录 Needle 实际写入 Volume 的精确时间（纳秒）
//   - 用于 Replication 同步延迟监控
//   - 用于 Compaction 时判断数据新旧
//   - 用于时间序列数据的精确排序
//
// 实现方式:
//   - 调用 writeNeedleCommon 处理主体部分
//   - 使用自定义的 writeFooter 函数写入 V3 特有的尾部
//
// 注意:
//   - AppendAtNs 在 WriteNeedleBlob 中可能被外部更新
//   - 时间戳位置固定：Checksum 之后，Padding 之前
func writeNeedleV3(n *Needle, offset uint64, bytesBuffer *bytes.Buffer) (size Size, actualSize int64, err error) {
	// 调用通用序列化函数，传入 V3 的 footer 写入逻辑
	return writeNeedleCommon(n, offset, bytesBuffer, Version3, func(n *Needle, header []byte, bytesBuffer *bytes.Buffer, padding int) {
		// Version3 Footer: Checksum(4) + AppendAtNs(8) + Padding(0-7)

		// 1. 写入 Checksum（4 字节）
		util.Uint32toBytes(header[0:NeedleChecksumSize], uint32(n.Checksum))

		// 2. 写入 AppendAtNs（8 字节纳秒级时间戳）
		util.Uint64toBytes(header[NeedleChecksumSize:NeedleChecksumSize+TimestampSize], n.AppendAtNs)

		// 3. 写入 Checksum + AppendAtNs + Padding
		// 注意：padding 部分的字节自动为 0（header 初始化时已清零）
		bytesBuffer.Write(header[0 : NeedleChecksumSize+TimestampSize+padding])
	})
}
