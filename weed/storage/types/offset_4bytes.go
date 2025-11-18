// 构建标签: 默认使用 4 字节 Offset 模式
// 如果需要 5 字节模式,需要在编译时指定 -tags 5BytesOffset
//go:build !5BytesOffset
// +build !5BytesOffset

package types

import (
	"fmt"
)

// OffsetHigher 表示 Offset 的高位字节部分
// 在 4 字节模式下为空结构体,不占用额外空间
//
// 设计说明:
//   - 4 字节模式: 空结构体,零内存开销
//   - 5 字节模式: 包含 b4 字段 (在 offset_5bytes.go 中定义)
//   - 通过组合 OffsetHigher + OffsetLower 实现灵活的 Offset 大小
type OffsetHigher struct {
	// b4 byte  // 在 5 字节模式中启用,4 字节模式中不存在
}

// 4 字节 Offset 模式的常量定义
const (
	OffsetSize                   = 4                                  // Offset 字段的字节数
	MaxPossibleVolumeSize uint64 = 4 * 1024 * 1024 * 1024 * 8         // 最大 Volume 大小: 32GB
	                                                                   // 计算: 4GB (2^32) * 8 (NeedlePaddingSize) = 32GB
)

// OffsetToBytes 将 Offset 序列化为字节数组
// 使用大端序编码(高位在前)
//
// 参数:
//   - bytes: 目标字节数组,至少 4 字节
//   - offset: 要序列化的 Offset
//
// 字节布局 (大端序):
//   bytes[0] = b3 (最高字节)
//   bytes[1] = b2
//   bytes[2] = b1
//   bytes[3] = b0 (最低字节)
//
// 使用场景:
//   - 写入索引文件 (.idx)
//   - 存储到 LevelDB
//   - 序列化到网络包
func OffsetToBytes(bytes []byte, offset Offset) {
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
func Uint32ToOffset(offset uint32) Offset {
	return Offset{
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
//   - bytes: 源字节数组,至少 4 字节
//
// 返回值:
//   - Offset: 解析出的 Offset 结构
//
// 字节布局 (大端序):
//   bytes[0] = b3 (最高字节)
//   bytes[1] = b2
//   bytes[2] = b1
//   bytes[3] = b0 (最低字节)
//
// 使用场景:
//   - 从索引文件加载
//   - 从 LevelDB 读取
//   - 解析网络数据
func BytesToOffset(bytes []byte) Offset {
	return Offset{
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
//   - true: 所有字节都为 0
//   - false: 至少有一个字节不为 0
//
// 使用场景:
//   - 验证 Offset 是否已初始化
//   - 检查索引条目是否有效
func (offset Offset) IsZero() bool {
	return offset.b0 == 0 && offset.b1 == 0 && offset.b2 == 0 && offset.b3 == 0
}

// ToOffset 将实际字节偏移量转换为 Offset 结构
// 自动处理 8 字节对齐
//
// 参数:
//   - offset: 实际的字节偏移量
//
// 返回值:
//   - Offset: 对齐后的 Offset 结构
//
// 转换公式:
//   Offset 值 = 实际偏移量 / NeedlePaddingSize (8)
//
// 示例:
//   actualOffset := int64(8192)  // 实际偏移 8KB
//   offset := ToOffset(actualOffset)  // offset 内部值为 1024
//
// 注意:
//   - 输入的 offset 必须是 8 的倍数
//   - 这种设计可以用 4 字节表示 32GB 的地址空间
func ToOffset(offset int64) Offset {
	smaller := uint32(offset / int64(NeedlePaddingSize))
	return Uint32ToOffset(smaller)
}

// ToActualOffset 将 Offset 转换为实际的字节偏移量
// 恢复 8 字节对齐前的真实地址
//
// 返回值:
//   - actualOffset: 实际的文件字节偏移量
//
// 转换公式:
//   实际偏移量 = Offset 值 * NeedlePaddingSize (8)
//
// 示例:
//   offset := Offset{...}  // 内部值为 1024
//   actual := offset.ToActualOffset()  // 返回 8192
//
// 使用场景:
//   - 定位文件读写位置
//   - 磁盘 I/O 操作
//   - 计算实际存储位置
func (offset Offset) ToActualOffset() (actualOffset int64) {
	return (int64(offset.b0) + int64(offset.b1)<<8 + int64(offset.b2)<<16 + int64(offset.b3)<<24) * int64(NeedlePaddingSize)
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
//
// 示例:
//   offset := Uint32ToOffset(1024)
//   fmt.Println(offset.String())  // 输出: "1024"
//   fmt.Println(offset.ToActualOffset())  // 输出: 8192
func (offset Offset) String() string {
	return fmt.Sprintf("%d", int64(offset.b0)+int64(offset.b1)<<8+int64(offset.b2)<<16+int64(offset.b3)<<24)
}
