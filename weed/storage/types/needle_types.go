// Package types 定义 SeaweedFS 存储层的核心类型
// 包括 Needle 的偏移量、大小、Cookie 等基础数据结构
package types

import (
	"fmt"
	"strconv"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

// Offset 表示 Needle 在 Volume 文件中的偏移量
// 支持 4 字节或 5 字节两种模式,通过编译标签选择
//
// 结构说明:
//   - OffsetHigher: 高位字节(4 字节模式为空,5 字节模式为 1 字节)
//   - OffsetLower: 低 4 字节
//
// 对齐机制:
//   - 实际偏移量 = Offset * NeedlePaddingSize (8 字节)
//   - 4 字节模式: 最大支持 32GB Volume (4GB * 8)
//   - 5 字节模式: 最大支持 8TB Volume (1TB * 8)
//
// 设计目的:
//   - 通过 8 字节对齐减少索引大小
//   - 提升磁盘 I/O 性能(对齐到扇区边界)
type Offset struct {
	OffsetHigher // 高位字节,根据编译标签决定是否存在
	OffsetLower  // 低 4 字节,所有模式都包含
}

// Size 表示 Needle 数据部分的大小(字节数)
// 使用 int32 有符号类型来支持删除标记
//
// 特殊值:
//   - > 0: 正常的数据大小
//   - < 0: 表示已删除的 Needle
//   - TombstoneFileSize (-1): 墓碑标记,表示文件已被删除
//
// 注意:
//   - Size 不包含 Needle 头部的大小
//   - 实际磁盘占用 = NeedleHeaderSize + Size + ChecksumSize
type Size int32

// IsDeleted 判断 Needle 是否已被删除
//
// 返回值:
//   - true: Size < 0 或等于墓碑标记
//   - false: 正常的有效数据
//
// 删除机制:
//   - SeaweedFS 不会立即删除文件,而是标记为删除
//   - 通过压缩(Compaction)操作真正回收空间
func (s Size) IsDeleted() bool {
	return s < 0 || s == TombstoneFileSize
}

// IsValid 判断 Needle 数据是否有效
//
// 返回值:
//   - true: Size > 0 且不是墓碑标记
//   - false: 已删除或无效数据
//
// 使用场景:
//   - 读取操作前验证数据有效性
//   - 统计有效文件数量和大小
func (s Size) IsValid() bool {
	return s > 0 && s != TombstoneFileSize
}

// OffsetLower 表示 Offset 的低 4 字节
// 使用小端序存储,便于在不同架构间转换
//
// 字节布局 (从低到高):
//   - b0: 最低字节 (bit 0-7)
//   - b1: 第二字节 (bit 8-15)
//   - b2: 第三字节 (bit 16-23)
//   - b3: 最高字节 (bit 24-31)
//
// 设计说明:
//   - 所有 Offset 模式都包含这 4 个字节
//   - 使用独立字节而非 uint32 避免对齐问题
//   - 方便序列化和反序列化操作
type OffsetLower struct {
	b3 byte // 最高字节
	b2 byte // 第三字节
	b1 byte // 第二字节
	b0 byte // 最低字节
}

// Cookie 是 Needle 的安全令牌
// 用于验证客户端请求的合法性,防止文件 ID 被猜测
//
// 生成机制:
//   - Master 分配 fid 时生成随机 Cookie
//   - 客户端上传时必须提供正确的 Cookie
//   - 读取时可选择性验证 Cookie
//
// 格式:
//   - uint32 类型,占 4 字节
//   - 通常以 16 进制字符串形式传输
//
// 安全性:
//   - 防止恶意用户枚举文件 ID
//   - 不提供加密功能,只是访问令牌
type Cookie uint32

// 常量定义 - Needle 和索引相关的固定大小
const (
	SizeSize           = 4 // Size 字段的字节数 (uint32)
	NeedleHeaderSize   = CookieSize + NeedleIdSize + SizeSize // Needle 头部总大小: 4 + 8 + 4 = 16 字节
	DataSizeSize       = 4 // 数据大小字段的字节数
	NeedleMapEntrySize = NeedleIdSize + OffsetSize + SizeSize // 索引条目大小: 8 + 4/5 + 4 = 16/17 字节
	TimestampSize      = 8 // 时间戳字段的字节数 (int64)
	NeedlePaddingSize  = 8 // Needle 对齐填充大小,所有 Needle 按 8 字节对齐
	TombstoneFileSize  = Size(-1) // 墓碑标记,表示文件已被删除
	CookieSize         = 4 // Cookie 字段的字节数 (uint32)
)

// CookieToBytes 将 Cookie 序列化为字节数组
// 使用大端序编码
//
// 参数:
//   - bytes: 目标字节数组,至少 4 字节
//   - cookie: 要序列化的 Cookie 值
//
// 使用场景:
//   - 写入 Needle 头部
//   - 网络传输 Cookie
func CookieToBytes(bytes []byte, cookie Cookie) {
	util.Uint32toBytes(bytes, uint32(cookie))
}

// Uint32ToCookie 将 uint32 转换为 Cookie 类型
//
// 参数:
//   - cookie: uint32 类型的 Cookie 值
//
// 返回值:
//   - Cookie: 转换后的 Cookie 类型
func Uint32ToCookie(cookie uint32) Cookie {
	return Cookie(cookie)
}

// BytesToCookie 从字节数组反序列化 Cookie
// 使用大端序解码
//
// 参数:
//   - bytes: 源字节数组,至少 4 字节
//
// 返回值:
//   - Cookie: 解析出的 Cookie 值
//
// 使用场景:
//   - 读取 Needle 头部
//   - 从网络数据包解析 Cookie
func BytesToCookie(bytes []byte) Cookie {
	return Cookie(util.BytesToUint32(bytes[0:4]))
}

// ParseCookie 从 16 进制字符串解析 Cookie
// Cookie 在 URL 和 API 中通常以 16 进制字符串形式传输
//
// 参数:
//   - cookieString: 16 进制格式的 Cookie 字符串 (如 "a1b2c3d4")
//
// 返回值:
//   - Cookie: 解析出的 Cookie 值
//   - error: 解析失败时返回错误
//
// 示例:
//   cookie, err := ParseCookie("a1b2c3d4")
//   if err != nil {
//       // 处理解析错误
//   }
func ParseCookie(cookieString string) (Cookie, error) {
	cookie, err := strconv.ParseUint(cookieString, 16, 32)
	if err != nil {
		return 0, fmt.Errorf("needle cookie %s format error: %v", cookieString, err)
	}
	return Cookie(cookie), nil
}

// BytesToSize 从字节数组反序列化 Size
// 使用大端序解码
//
// 参数:
//   - bytes: 源字节数组,至少 4 字节
//
// 返回值:
//   - Size: 解析出的 Size 值
//
// 使用场景:
//   - 读取 Needle 头部的 Size 字段
//   - 从索引文件加载 Size
func BytesToSize(bytes []byte) Size {
	return Size(util.BytesToUint32(bytes))
}

// SizeToBytes 将 Size 序列化为字节数组
// 使用大端序编码
//
// 参数:
//   - bytes: 目标字节数组,至少 4 字节
//   - size: 要序列化的 Size 值
//
// 使用场景:
//   - 写入 Needle 头部的 Size 字段
//   - 写入索引文件
func SizeToBytes(bytes []byte, size Size) {
	util.Uint32toBytes(bytes, uint32(size))
}
