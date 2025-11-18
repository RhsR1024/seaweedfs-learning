// 构建标签: 使用 5 字节 Offset 模式
// 编译时需要指定: go build -tags 5BytesOffset
//go:build 5BytesOffset
// +build 5BytesOffset

package types

import (
	"fmt"
)

// OffsetHigher 表示 Offset 的高位字节部分
// 在 5 字节模式下包含额外的第 5 字节,扩展可寻址范围
//
// 设计说明:
//   - 4 字节模式: 空结构体,不包含 b4 字段
//   - 5 字节模式: 包含 b4 字段,提供额外 8 位地址空间
//   - 通过编译标签在两种模式间切换
type OffsetHigher struct {
	b4 byte // 最高字节,仅在 5 字节模式存在
}

// 5 字节 Offset 模式的常量定义
const (
	OffsetSize                   = 4 + 1                                        // Offset 字段的字节数: 5 字节
	MaxPossibleVolumeSize uint64 = 4 * 1024 * 1024 * 1024 * 8 * 256            // 最大 Volume 大小: 8TB
	                                                                            // 计算: 4GB * 8 (NeedlePaddingSize) * 256 (额外字节) = 8TB
)

// OffsetToBytes 将 Offset 序列化为字节数组
// 使用大端序编码(高位在前)
//
// 参数:
//   - bytes: 目标字节数组,至少 5 字节
//   - offset: 要序列化的 Offset
//
// 字节布局 (大端序):
//   bytes[0] = b3 (第 4 高字节)
//   bytes[1] = b2
//   bytes[2] = b1
//   bytes[3] = b0 (最低字节)
//   bytes[4] = b4 (最高字节)
//
// 注意:
//   - 5 字节模式的布局与 4 字节模式兼容(前 4 字节)
//   - b4 字节放在最后,便于向前兼容
//
// 使用场景:
//   - 大容量 Volume (>32GB) 的索引存储
//   - 写入索引文件和 LevelDB
func OffsetToBytes(bytes []byte, offset Offset) {
	bytes[4] = offset.b4
	bytes[3] = offset.b0
	bytes[2] = offset.b1
	bytes[1] = offset.b2
	bytes[0] = offset.b3
}

// Uint32ToOffset 将 uint32 转换为 Offset
// 仅用于测试,未来可能会被移除
//
// 参数:
//   - offset: uint32 类型的偏移量值
//
// 返回值:
//   - Offset: 转换后的 Offset 结构
//
// 注意:
//   - 这个值是经过 8 字节对齐后的偏移量单位数
//   - 实际字节偏移 = offset * 8
//   - 5 字节模式下,b4 从 offset 的高 32 位提取(但 uint32 没有高位,实际为 0)
func Uint32ToOffset(offset uint32) Offset {
	return Offset{
		OffsetHigher: OffsetHigher{
			b4: byte(offset >> 32),  // uint32 >> 32 = 0,仅为类型兼容
		},
		OffsetLower: OffsetLower{
			b0: byte(offset),
			b1: byte(offset >> 8),
			b2: byte(offset >> 16),
			b3: byte(offset >> 24),
		},
	}
}

// BytesToOffset 从字节数组反序列化 Offset
// 使用大端序解码
//
// 参数:
//   - bytes: 源字节数组,至少 5 字节
//
// 返回值:
//   - Offset: 解析出的 Offset 结构
//
// 字节布局 (大端序):
//   bytes[0] = b3 (第 4 高字节)
//   bytes[1] = b2
//   bytes[2] = b1
//   bytes[3] = b0 (最低字节)
//   bytes[4] = b4 (最高字节)
//
// 使用场景:
//   - 从索引文件加载 (大容量 Volume)
//   - 从 LevelDB 读取
//   - 解析网络数据
func BytesToOffset(bytes []byte) Offset {
	return Offset{
		OffsetHigher: OffsetHigher{
			b4: bytes[4],
		},
		OffsetLower: OffsetLower{
			b0: bytes[3],
			b1: bytes[2],
			b2: bytes[1],
			b3: bytes[0],
		},
	}
}

// IsZero 判断 Offset 是否为零值
//
// 返回值:
//   - true: 所有 5 个字节都为 0
//   - false: 至少有一个字节不为 0
//
// 使用场景:
//   - 验证 Offset 是否已初始化
//   - 检查索引条目是否有效
func (offset Offset) IsZero() bool {
	return offset.b0 == 0 && offset.b1 == 0 && offset.b2 == 0 && offset.b3 == 0 && offset.b4 == 0
}

// ToOffset 将实际字节偏移量转换为 Offset 结构
// 自动处理 8 字节对齐,支持大容量 Volume
//
// 参数:
//   - offset: 实际的字节偏移量 (int64)
//
// 返回值:
//   - Offset: 对齐后的 Offset 结构
//
// 转换公式:
//   Offset 值 = 实际偏移量 / NeedlePaddingSize (8)
//
// 示例:
//   actualOffset := int64(1099511627776)  // 1TB
//   offset := ToOffset(actualOffset)  // offset 内部值为 137438953472
//
// 注意:
//   - 输入的 offset 必须是 8 的倍数
//   - 5 字节模式可以表示高达 8TB 的地址空间
//   - b4 字节存储最高 8 位
func ToOffset(offset int64) Offset {
	smaller := offset / int64(NeedlePaddingSize)
	return Offset{
		OffsetHigher: OffsetHigher{
			b4: byte(smaller >> 32),  // 提取第 32-39 位
		},
		OffsetLower: OffsetLower{
			b0: byte(smaller),        // 提取第 0-7 位
			b1: byte(smaller >> 8),   // 提取第 8-15 位
			b2: byte(smaller >> 16),  // 提取第 16-23 位
			b3: byte(smaller >> 24),  // 提取第 24-31 位
		},
	}
}

// ToActualOffset 将 Offset 转换为实际的字节偏移量
// 恢复 8 字节对齐前的真实地址,支持 8TB 范围
//
// 返回值:
//   - actualOffset: 实际的文件字节偏移量
//
// 转换公式:
//   实际偏移量 = (b0 + b1<<8 + b2<<16 + b3<<24 + b4<<32) * NeedlePaddingSize (8)
//
// 示例:
//   offset := Offset{...}  // 内部值为 137438953472 (1TB/8)
//   actual := offset.ToActualOffset()  // 返回 1099511627776 (1TB)
//
// 使用场景:
//   - 定位大文件读写位置
//   - 大容量 Volume 的磁盘 I/O 操作
//   - 计算实际存储位置
func (offset Offset) ToActualOffset() (actualOffset int64) {
	return (int64(offset.b0) + int64(offset.b1)<<8 + int64(offset.b2)<<16 + int64(offset.b3)<<24 + int64(offset.b4)<<32) * int64(NeedlePaddingSize)
}

// String 将 Offset 转换为字符串表示
// 返回 Offset 的逻辑值(非实际字节偏移)
//
// 返回值:
//   - string: Offset 的十进制字符串表示
//
// 注意:
//   - 返回的是 Offset 的内部值,不是实际字节偏移
//   - 要获取实际偏移,使用 ToActualOffset()
//   - 包含所有 5 字节的值
//
// 示例:
//   offset := ToOffset(1099511627776)  // 1TB
//   fmt.Println(offset.String())  // 输出: "137438953472"
//   fmt.Println(offset.ToActualOffset())  // 输出: 1099511627776
func (offset Offset) String() string {
	return fmt.Sprintf("%d", int64(offset.b0)+int64(offset.b1)<<8+int64(offset.b2)<<16+int64(offset.b3)<<24+int64(offset.b4)<<32)
}
