// Package needle 提供 SeaweedFS 的 Needle 存储结构和相关操作
// 本文件定义 FileId 结构体及其序列化/反序列化方法
//
// ============================================================================
// FileId 概述
// ============================================================================
//
// FileId 是 SeaweedFS 中文件的唯一标识符，用于定位存储的文件。
// 它由三部分组成：
//   1. VolumeId - 卷 ID，标识文件存储在哪个 Volume 中
//   2. NeedleId (Key) - Needle ID，Volume 内的唯一标识
//   3. Cookie - 随机数，用于防止 URL 猜测攻击
//
// 字符串格式：
//   <VolumeId>,<NeedleId><Cookie>
//
// 示例：
//   - "3,01637037d6" -> VolumeId=3, Key=0x01, Cookie=0x637037d6
//   - "100,abcd1234abcd1234" -> VolumeId=100, Key=0xabcd1234, Cookie=0xabcd1234
//
// ============================================================================
// 安全性设计
// ============================================================================
//
// Cookie 的作用是防止暴力枚举攻击：
// - 即使攻击者知道 VolumeId 和 NeedleId，没有正确的 Cookie 也无法访问文件
// - Cookie 是在文件上传时随机生成的 32 位整数
// - 这提供了 2^32 种可能性，使暴力猜测变得不切实际
//
// ============================================================================
package needle

import (
	"encoding/hex"
	"fmt"
	"strings"

	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// FileId 文件标识符结构体
//
// 这是 SeaweedFS 中定位文件的核心数据结构。
// 每个上传的文件都会获得一个唯一的 FileId。
//
// 字段说明：
//   - VolumeId: 卷 ID（32 位无符号整数），标识文件所在的 Volume
//   - Key: Needle ID（64 位无符号整数），Volume 内的唯一标识符
//   - Cookie: 安全令牌（32 位无符号整数），防止 URL 猜测
type FileId struct {
	VolumeId VolumeId // 卷 ID
	Key      NeedleId // Needle ID（文件在卷中的唯一标识）
	Cookie   Cookie   // Cookie（安全令牌，防止暴力猜测）
}

// NewFileIdFromNeedle 从 Needle 对象创建 FileId
//
// 在文件上传完成后，使用此函数生成返回给客户端的 FileId。
//
// 参数:
//   - VolumeId: 文件存储的卷 ID
//   - n: 包含 Id 和 Cookie 的 Needle 对象
//
// 返回:
//   - *FileId: 新创建的文件标识符
func NewFileIdFromNeedle(VolumeId VolumeId, n *Needle) *FileId {
	return &FileId{VolumeId: VolumeId, Key: n.Id, Cookie: n.Cookie}
}

// NewFileId 从原始值创建 FileId
//
// 参数:
//   - VolumeId: 卷 ID
//   - key: Needle ID（uint64）
//   - cookie: Cookie 值（uint32）
//
// 返回:
//   - *FileId: 新创建的文件标识符
func NewFileId(VolumeId VolumeId, key uint64, cookie uint32) *FileId {
	return &FileId{VolumeId: VolumeId, Key: Uint64ToNeedleId(key), Cookie: Uint32ToCookie(cookie)}
}

// ParseFileIdFromString 从字符串反序列化 FileId
//
// 解析格式：<VolumeId>,<NeedleIdCookie>
// 其中 NeedleIdCookie 是 NeedleId 和 Cookie 的十六进制拼接
//
// 解析步骤：
// 1. 按逗号分割，获取 VolumeId 和 NeedleIdCookie
// 2. 解析 VolumeId（十进制数字）
// 3. 解析 NeedleIdCookie（十六进制字符串）
//
// 参数:
//   - fid: FileId 字符串，如 "3,01637037d6"
//
// 返回:
//   - *FileId: 解析后的文件标识符
//   - error: 解析错误（格式不正确时）
//
// 示例：
//
//	fid, err := ParseFileIdFromString("100,abcd12345678")
//	// fid.VolumeId = 100
//	// fid.Key = 0xabcd
//	// fid.Cookie = 0x12345678
func ParseFileIdFromString(fid string) (*FileId, error) {
	// 步骤 1：分割 VolumeId 和 NeedleIdCookie
	vid, needleKeyCookie, err := splitVolumeId(fid)
	if err != nil {
		return nil, err
	}

	// 步骤 2：解析 VolumeId（十进制字符串 -> VolumeId）
	volumeId, err := NewVolumeId(vid)
	if err != nil {
		return nil, err
	}

	// 步骤 3：解析 NeedleId 和 Cookie（十六进制字符串）
	nid, cookie, err := ParseNeedleIdCookie(needleKeyCookie)
	if err != nil {
		return nil, err
	}

	// 构建并返回 FileId
	fileId := &FileId{VolumeId: volumeId, Key: nid, Cookie: cookie}
	return fileId, nil
}

// GetVolumeId 获取卷 ID
func (n *FileId) GetVolumeId() VolumeId {
	return n.VolumeId
}

// GetNeedleId 获取 Needle ID
func (n *FileId) GetNeedleId() NeedleId {
	return n.Key
}

// GetCookie 获取 Cookie
func (n *FileId) GetCookie() Cookie {
	return n.Cookie
}

// GetNeedleIdCookie 获取 NeedleId 和 Cookie 的组合字符串
//
// 返回格式：十六进制字符串，NeedleId 和 Cookie 连接
// 用于构建完整的 FileId 字符串或 URL 路径
func (n *FileId) GetNeedleIdCookie() string {
	return formatNeedleIdCookie(n.Key, n.Cookie)
}

// String 将 FileId 序列化为字符串
//
// 返回格式：<VolumeId>,<NeedleIdCookie>
// 这是 FileId 的标准字符串表示形式
//
// 示例：
//
//	fid := &FileId{VolumeId: 3, Key: 0x01, Cookie: 0x637037d6}
//	str := fid.String() // "3,01637037d6"
func (n *FileId) String() string {
	return n.VolumeId.String() + "," + formatNeedleIdCookie(n.Key, n.Cookie)
}

// formatNeedleIdCookie 格式化 NeedleId 和 Cookie 为十六进制字符串
//
// 格式化规则：
// 1. 将 NeedleId（8 字节）和 Cookie（4 字节）合并为 12 字节
// 2. 跳过 NeedleId 部分的前导零
// 3. 转换为十六进制字符串
//
// 这种格式化方式可以减少字符串长度，同时保持可逆性：
// - 最短：9 个字符（1 字节 NeedleId + 4 字节 Cookie）
// - 最长：24 个字符（8 字节 NeedleId + 4 字节 Cookie）
//
// 参数:
//   - key: NeedleId（64 位）
//   - cookie: Cookie（32 位）
//
// 返回:
//   - string: 格式化后的十六进制字符串
func formatNeedleIdCookie(key NeedleId, cookie Cookie) string {
	// 创建 12 字节缓冲区（NeedleId 8 字节 + Cookie 4 字节）
	bytes := make([]byte, NeedleIdSize+CookieSize)

	// 将 NeedleId 写入前 8 字节（大端序）
	NeedleIdToBytes(bytes[0:NeedleIdSize], key)

	// 将 Cookie 写入后 4 字节（大端序）
	CookieToBytes(bytes[NeedleIdSize:NeedleIdSize+CookieSize], cookie)

	// 找到第一个非零字节的位置
	// 目的是跳过 NeedleId 的前导零，减少字符串长度
	// 注意：只跳过 NeedleId 部分的零，不跳过 Cookie 部分
	nonzero_index := 0
	for ; bytes[nonzero_index] == 0 && nonzero_index < NeedleIdSize; nonzero_index++ {
	}

	// 从第一个非零字节开始编码为十六进制字符串
	return hex.EncodeToString(bytes[nonzero_index:])
}

// splitVolumeId 分割 FileId 字符串为 VolumeId 和 KeyCookie 部分
//
// 输入格式：<VolumeId>,<KeyCookie>
// 分割点：第一个逗号
//
// 参数:
//   - fid: 完整的 FileId 字符串
//
// 返回:
//   - vid: VolumeId 部分（逗号前）
//   - key_cookie: NeedleId + Cookie 部分（逗号后）
//   - err: 格式错误时返回
//
// 注意：此函数从 operation/delete_content.go 复制，避免循环依赖
func splitVolumeId(fid string) (vid string, key_cookie string, err error) {
	// 找到第一个逗号的位置
	commaIndex := strings.Index(fid, ",")

	// 验证格式：逗号必须存在且不在开头
	if commaIndex <= 0 {
		return "", "", fmt.Errorf("wrong fid format")
	}

	// 分割并返回
	return fid[:commaIndex], fid[commaIndex+1:], nil
}
