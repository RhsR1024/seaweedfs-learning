// Package util 提供 SeaweedFS 的通用工具函数
//
// 本文件包含字节处理相关的工具函数：
// - 字节数组与整数类型的转换（大端序）
// - 哈希计算（MD5）和编码（Base64）
// - 随机数生成
// - 字节单位解析（KB/KiB、MB/MiB 等）
// - 人类可读格式转换
package util

import (
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"unicode"
)

// BytesToHumanReadable 将字节数转换为人类可读的格式
//
// 使用 IEC 标准（1024 进制），转换为带单位的字符串表示。
// 单位序列：B（字节）、KiB、MiB、GiB、TiB、PiB、EiB
//
// 参数：
//   b: 字节数（uint64）
//
// 返回：人类可读的字符串，保留两位小数
//
// 示例：
//   BytesToHumanReadable(1024)        -> "1.00 KiB"
//   BytesToHumanReadable(1536)        -> "1.50 KiB"
//   BytesToHumanReadable(1048576)     -> "1.00 MiB"
//   BytesToHumanReadable(1073741824)  -> "1.00 GiB"
//   BytesToHumanReadable(500)         -> "500 B"
//
// 使用场景：
//   - 日志输出文件大小
//   - Web 界面显示存储容量
//   - 监控报表生成
func BytesToHumanReadable(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}

	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}

	return fmt.Sprintf("%.2f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

// ==================== 字节数组与整数类型转换（大端序） ====================
//
// 以下函数实现字节数组与整数类型之间的转换，统一使用大端序（Big Endian）。
// 大端序：高位字节存储在低地址，低位字节存储在高地址。
//
// 例如：uint32 值 0x12345678 在大端序中的字节序列为 [0x12, 0x34, 0x56, 0x78]
//
// 这些函数广泛用于 SeaweedFS 的数据序列化和反序列化，包括：
// - Needle 数据结构的读写
// - Volume 索引文件的处理
// - 网络协议的数据传输

// BytesToUint64 将字节数组转换为 uint64（大端序）
//
// 从字节数组的第一个字节开始，依次读取并组合成 64 位无符号整数。
// 支持任意长度的字节数组（通常为 8 字节）。
//
// 参数：
//   b: 字节数组（建议长度为 8）
//
// 返回：转换后的 uint64 值
//
// 示例：
//   BytesToUint64([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00}) -> 256
//   BytesToUint64([]byte{0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0}) -> 0x123456789ABCDEF0
func BytesToUint64(b []byte) (v uint64) {
	length := uint(len(b))
	for i := uint(0); i < length-1; i++ {
		v += uint64(b[i])
		v <<= 8
	}
	v += uint64(b[length-1])
	return
}

// BytesToUint32 将字节数组转换为 uint32（大端序）
//
// 从字节数组的第一个字节开始，依次读取并组合成 32 位无符号整数。
// 支持任意长度的字节数组（通常为 4 字节）。
//
// 参数：
//   b: 字节数组（建议长度为 4）
//
// 返回：转换后的 uint32 值
//
// 示例：
//   BytesToUint32([]byte{0x00, 0x00, 0x01, 0x00}) -> 256
//   BytesToUint32([]byte{0x12, 0x34, 0x56, 0x78}) -> 0x12345678
func BytesToUint32(b []byte) (v uint32) {
	length := uint(len(b))
	for i := uint(0); i < length-1; i++ {
		v += uint32(b[i])
		v <<= 8
	}
	v += uint32(b[length-1])
	return
}

// BytesToUint16 将字节数组转换为 uint16（大端序）
//
// 从 2 字节数组中读取并组合成 16 位无符号整数。
//
// 参数：
//   b: 字节数组（必须为 2 字节）
//
// 返回：转换后的 uint16 值
//
// 示例：
//   BytesToUint16([]byte{0x01, 0x00}) -> 256
//   BytesToUint16([]byte{0x12, 0x34}) -> 0x1234
func BytesToUint16(b []byte) (v uint16) {
	v += uint16(b[0])
	v <<= 8
	v += uint16(b[1])
	return
}

// Uint64toBytes 将 uint64 转换为字节数组（大端序）
//
// 将 64 位无符号整数按大端序写入 8 字节数组。
// 注意：函数不检查数组长度，调用者需确保 b 长度至少为 8。
//
// 参数：
//   b: 目标字节数组（长度必须 >= 8）
//   v: 要转换的 uint64 值
//
// 示例：
//   var buf [8]byte
//   Uint64toBytes(buf[:], 0x123456789ABCDEF0)
//   // buf = [0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0]
func Uint64toBytes(b []byte, v uint64) {
	for i := uint(0); i < 8; i++ {
		b[7-i] = byte(v >> (i * 8))
	}
}

// Uint32toBytes 将 uint32 转换为字节数组（大端序）
//
// 将 32 位无符号整数按大端序写入 4 字节数组。
// 注意：函数不检查数组长度，调用者需确保 b 长度至少为 4。
//
// 参数：
//   b: 目标字节数组（长度必须 >= 4）
//   v: 要转换的 uint32 值
//
// 示例：
//   var buf [4]byte
//   Uint32toBytes(buf[:], 0x12345678)
//   // buf = [0x12, 0x34, 0x56, 0x78]
func Uint32toBytes(b []byte, v uint32) {
	for i := uint(0); i < 4; i++ {
		b[3-i] = byte(v >> (i * 8))
	}
}

// Uint16toBytes 将 uint16 转换为字节数组（大端序）
//
// 将 16 位无符号整数按大端序写入 2 字节数组。
// 注意：函数不检查数组长度，调用者需确保 b 长度至少为 2。
//
// 参数：
//   b: 目标字节数组（长度必须 >= 2）
//   v: 要转换的 uint16 值
//
// 示例：
//   var buf [2]byte
//   Uint16toBytes(buf[:], 0x1234)
//   // buf = [0x12, 0x34]
func Uint16toBytes(b []byte, v uint16) {
	b[0] = byte(v >> 8)
	b[1] = byte(v)
}

// Uint8toBytes 将 uint8 转换为字节数组
//
// 将 8 位无符号整数写入单字节数组。
// 注意：函数不检查数组长度，调用者需确保 b 长度至少为 1。
//
// 参数：
//   b: 目标字节数组（长度必须 >= 1）
//   v: 要转换的 uint8 值
func Uint8toBytes(b []byte, v uint8) {
	b[0] = byte(v)
}

// ==================== 哈希计算和编码函数 ====================
//
// 以下函数提供 MD5 哈希计算和 Base64 编码功能，
// 用于数据完整性验证、负载均衡和数据分布。

// HashStringToLong 将字符串哈希为 64 位整数
//
// 使用 MD5 哈希算法将字符串转换为 64 位整数，取 MD5 结果的前 8 字节。
// 该函数常用于一致性哈希和数据分片场景。
//
// 参数：
//   dir: 要哈希的字符串
//
// 返回：64 位有符号整数（取 MD5 哈希的前 8 字节）
//
// 使用场景：
//   - 数据分片：根据文件路径计算所属分片
//   - 负载均衡：根据键名分配到不同的服务器
//   - 一致性哈希环的节点定位
//
// 示例：
//   hash := HashStringToLong("/path/to/file")
//   serverId := hash % int64(serverCount)
func HashStringToLong(dir string) (v int64) {
	h := md5.New()
	io.WriteString(h, dir)

	b := h.Sum(nil)

	v += int64(b[0])
	v <<= 8
	v += int64(b[1])
	v <<= 8
	v += int64(b[2])
	v <<= 8
	v += int64(b[3])
	v <<= 8
	v += int64(b[4])
	v <<= 8
	v += int64(b[5])
	v <<= 8
	v += int64(b[6])
	v <<= 8
	v += int64(b[7])

	return
}

// HashToInt32 将字节数据哈希为 32 位整数
//
// 使用 MD5 哈希算法将字节数组转换为 32 位整数，取 MD5 结果的前 4 字节。
//
// 参数：
//   data: 要哈希的字节数组
//
// 返回：32 位有符号整数（取 MD5 哈希的前 4 字节）
//
// 使用场景：
//   - 生成较短的哈希标识
//   - 数据校验和计算
//   - 快速数据分组
func HashToInt32(data []byte) (v int32) {
	h := md5.New()
	h.Write(data)

	b := h.Sum(nil)

	v += int32(b[0])
	v <<= 8
	v += int32(b[1])
	v <<= 8
	v += int32(b[2])
	v <<= 8
	v += int32(b[3])

	return
}

// Base64Encode 将字节数据编码为 Base64 字符串
//
// 使用标准 Base64 编码（RFC 4648）。
//
// 参数：
//   data: 要编码的字节数组
//
// 返回：Base64 编码的字符串
//
// 示例：
//   Base64Encode([]byte{0x48, 0x65, 0x6c, 0x6c, 0x6f}) -> "SGVsbG8="
func Base64Encode(data []byte) string {
	return base64.StdEncoding.EncodeToString(data)
}

// Base64Md5 计算数据的 MD5 并返回 Base64 编码结果
//
// 先计算数据的 MD5 哈希（16 字节），然后进行 Base64 编码。
// 常用于 S3 API 兼容接口的 Content-MD5 头部。
//
// 参数：
//   data: 要哈希的字节数组
//
// 返回：MD5 哈希的 Base64 编码字符串（24 字符）
//
// 示例：
//   Base64Md5([]byte("hello")) -> "XUFAKrxLKna5cZ2REBfFkg=="
//
// 使用场景：
//   - S3 API 的 Content-MD5 头部
//   - 数据完整性验证
//   - ETag 生成
func Base64Md5(data []byte) string {
	return Base64Encode(Md5(data))
}

// Md5 计算数据的 MD5 哈希值
//
// 返回 16 字节的 MD5 哈希结果。
//
// 参数：
//   data: 要哈希的字节数组
//
// 返回：16 字节的 MD5 哈希值
//
// 示例：
//   md5Hash := Md5([]byte("hello"))
//   // md5Hash = [16]byte{0x5d, 0x41, 0x40, 0x2a, ...}
func Md5(data []byte) []byte {
	hash := md5.New()
	hash.Write(data)
	return hash.Sum(nil)
}

// Md5String 计算数据的 MD5 并返回十六进制字符串
//
// 返回 32 字符的十六进制表示（小写）。
//
// 参数：
//   data: 要哈希的字节数组
//
// 返回：32 字符的十六进制 MD5 字符串
//
// 示例：
//   Md5String([]byte("hello")) -> "5d41402abc4b2a76b9719d911017c592"
//
// 使用场景：
//   - 文件指纹计算
//   - 数据去重标识
//   - 日志记录和调试
func Md5String(data []byte) string {
	return fmt.Sprintf("%x", Md5(data))
}

// Base64Md5ToBytes 将 Base64 编码的 MD5 字符串解码为字节数组
//
// 解码 Base64 编码的 MD5 哈希值（通常为 16 字节）。
// 这是 Base64Md5 函数的逆操作。
//
// 参数：
//   contentMd5: Base64 编码的 MD5 字符串
//
// 返回：解码后的字节数组，解码失败返回 nil
//
// 示例：
//   bytes := Base64Md5ToBytes("XUFAKrxLKna5cZ2REBfFkg==")
//   // bytes = [16]byte{0x5d, 0x41, 0x40, 0x2a, ...}
func Base64Md5ToBytes(contentMd5 string) []byte {
	data, err := base64.StdEncoding.DecodeString(contentMd5)
	if err != nil {
		return nil
	}
	return data
}

// ==================== 随机数生成函数 ====================
//
// 以下函数使用加密安全的随机数生成器生成随机数据。

// RandomInt32 生成随机的 32 位整数
//
// 使用加密安全的随机数生成器（crypto/rand）生成随机整数。
//
// 返回：随机的 32 位有符号整数
//
// 使用场景：
//   - 生成随机 Volume ID
//   - 随机会话标识
//   - 测试数据生成
func RandomInt32() int32 {
	buf := make([]byte, 4)
	rand.Read(buf)
	return int32(BytesToUint32(buf))
}

// RandomUint64 生成随机的 64 位整数
//
// 注意：函数名为 RandomUint64，但返回类型为 int32（可能是历史遗留问题）。
// 实际读取 8 字节随机数据，但只返回 32 位整数。
//
// 返回：随机的 32 位有符号整数（注意不是 64 位）
func RandomUint64() int32 {
	buf := make([]byte, 8)
	rand.Read(buf)
	return int32(BytesToUint64(buf))
}

// RandomBytes 生成指定长度的随机字节数组
//
// 使用加密安全的随机数生成器生成指定长度的随机数据。
//
// 参数：
//   byteCount: 要生成的字节数
//
// 返回：随机字节数组
//
// 使用场景：
//   - 生成随机密钥
//   - 生成随机文件内容（测试）
//   - 生成随机标识符
//
// 示例：
//   randomData := RandomBytes(16)  // 生成 16 字节随机数据
func RandomBytes(byteCount int) []byte {
	buf := make([]byte, byteCount)
	rand.Read(buf)
	return buf
}

// ==================== 辅助类型和工具函数 ====================

// BytesReader 字节数组读取器
//
// 封装字节数组和对应的 Reader，提供便捷的读取接口。
// 同时保留原始字节数组的引用，方便需要时直接访问。
type BytesReader struct {
	Bytes  []byte        // 原始字节数组
	*bytes.Reader          // 嵌入的字节读取器
}

// NewBytesReader 创建新的字节数组读取器
//
// 创建一个 BytesReader 实例，同时保留字节数组引用和创建 Reader。
//
// 参数：
//   b: 字节数组
//
// 返回：BytesReader 实例指针
//
// 使用场景：
//   - 需要多次读取同一字节数组
//   - 需要同时访问原始数据和 Reader 接口
func NewBytesReader(b []byte) *BytesReader {
	return &BytesReader{
		Bytes:  b,
		Reader: bytes.NewReader(b),
	}
}

// EmptyTo 如果字符串为空则返回默认值
//
// 简单的空值处理函数，当字符串为空时返回指定的默认值。
//
// 参数：
//   s: 原始字符串
//   to: 默认值（当 s 为空时返回）
//
// 返回：如果 s 为空返回 to，否则返回 s
//
// 示例：
//   EmptyTo("", "default")    -> "default"
//   EmptyTo("value", "default") -> "value"
func EmptyTo(s, to string) string {
	if s == "" {
		return to
	}

	return s
}

// IfElse 三元运算符的替代实现
//
// 根据布尔条件返回两个字符串之一，类似于三元运算符 b ? this : that。
//
// 参数：
//   b: 布尔条件
//   this: 条件为 true 时返回的值
//   that: 条件为 false 时返回的值
//
// 返回：根据条件返回 this 或 that
//
// 示例：
//   IfElse(true, "yes", "no")   -> "yes"
//   IfElse(false, "yes", "no")  -> "no"
func IfElse(b bool, this, that string) string {
	if b {
		return this
	}
	return that
}

// ==================== 字节单位解析 ====================
//
// 以下函数和常量用于解析和定义字节单位（KB、MB、GB 等）。

// ParseBytes 解析字节单位字符串为字节数
//
// 支持两种单位体系：
// 1. IEC 标准（二进制，1024 进制）：KiB, MiB, GiB, TiB, PiB, EiB
// 2. SI 标准（十进制，1000 进制）：KB, MB, GB, TB, PB, EB
//
// 支持的格式：
// - 带空格："42 MB", "42 MiB"
// - 不带空格："42MB", "42MiB"
// - 带逗号分隔："1,024 KB"
// - 简写形式："42M", "42Mi"
// - 大小写不敏感："42mb", "42MB", "42Mb"
//
// 参数：
//   s: 字节单位字符串
//
// 返回：
//   uint64: 解析后的字节数
//   error: 解析错误
//
// 示例：
//   ParseBytes("42MB")      -> 42000000, nil       // SI 标准
//   ParseBytes("42 MB")     -> 42000000, nil
//   ParseBytes("42 MiB")    -> 44040192, nil       // IEC 标准 (42 * 1024 * 1024)
//   ParseBytes("1,024 KB")  -> 1024000, nil
//   ParseBytes("1.5GB")     -> 1500000000, nil
//   ParseBytes("invalid")   -> 0, error
//
// 使用场景：
//   - 配置文件解析（如 max_volume_size = "30GB"）
//   - 命令行参数解析
//   - API 请求参数解析
func ParseBytes(s string) (uint64, error) {
	lastDigit := 0
	hasComma := false
	// 找到数字部分的结束位置
	for _, r := range s {
		if !(unicode.IsDigit(r) || r == '.' || r == ',') {
			break
		}
		if r == ',' {
			hasComma = true
		}
		lastDigit++
	}

	num := s[:lastDigit]
	// 移除千位分隔符
	if hasComma {
		num = strings.Replace(num, ",", "", -1)
	}

	// 解析数字部分
	f, err := strconv.ParseFloat(num, 64)
	if err != nil {
		return 0, err
	}

	// 解析单位部分
	extra := strings.ToLower(strings.TrimSpace(s[lastDigit:]))
	if m, ok := bytesSizeTable[extra]; ok {
		f *= float64(m)
		if f >= math.MaxUint64 {
			return 0, fmt.Errorf("too large: %v", s)
		}
		return uint64(f), nil
	}

	return 0, fmt.Errorf("unhandled size name: %v", extra)
}

// bytesSizeTable 字节单位映射表
//
// 支持完整形式和简写形式，以及大小写不敏感。
// 包含 IEC 标准（KiB、MiB 等）和 SI 标准（KB、MB 等）。
var bytesSizeTable = map[string]uint64{
	"b": Byte, "kib": KiByte, "kb": KByte, "mib": MiByte, "mb": MByte, "gib": GiByte, "gb": GByte,
	"tib": TiByte, "tb": TByte, "pib": PiByte, "pb": PByte, "eib": EiByte, "eb": EByte,
	// 简写形式（不带 "B" 后缀）
	"": Byte, "ki": KiByte, "k": KByte, "mi": MiByte, "m": MByte, "gi": GiByte, "g": GByte,
	"ti": TiByte, "t": TByte, "pi": PiByte, "p": PByte, "ei": EiByte, "e": EByte,
}

// ==================== IEC 标准字节单位（二进制，1024 进制） ====================
//
// IEC（International Electrotechnical Commission）标准使用二进制倍数。
// 这是计算机领域最常用的单位体系，因为计算机使用二进制。
//
// 命名规则：
// - KiB = Kibibyte (Kibi = Kilo Binary)
// - MiB = Mebibyte (Mebi = Mega Binary)
// - GiB = Gibibyte (Gibi = Giga Binary)
// - TiB = Tebibyte (Tebi = Tera Binary)
// - PiB = Pebibyte (Pebi = Peta Binary)
// - EiB = Exbibyte (Exbi = Exa Binary)
const (
	Byte = 1 << (iota * 10) // 1 字节
	KiByte                   // 1024 字节 (2^10)
	MiByte                   // 1,048,576 字节 (2^20)
	GiByte                   // 1,073,741,824 字节 (2^30)
	TiByte                   // 1,099,511,627,776 字节 (2^40)
	PiByte                   // 1,125,899,906,842,624 字节 (2^50)
	EiByte                   // 1,152,921,504,606,846,976 字节 (2^60)
)

// ==================== SI 标准字节单位（十进制，1000 进制） ====================
//
// SI（International System of Units）标准使用十进制倍数。
// 这是国际标准单位制，硬盘厂商通常使用这种单位。
//
// 命名规则：
// - KB = Kilobyte (1000 字节)
// - MB = Megabyte (1000 KB)
// - GB = Gigabyte (1000 MB)
// - TB = Terabyte (1000 GB)
// - PB = Petabyte (1000 TB)
// - EB = Exabyte  (1000 PB)
//
// 注意：SI 单位与 IEC 单位的差异会随着单位增大而显著增加。
// 例如：1 GB (SI) = 1,000,000,000 字节
//      1 GiB (IEC) = 1,073,741,824 字节
//      差异约为 7.4%
const (
	IByte = 1                // 1 字节
	KByte = IByte * 1000     // 1,000 字节
	MByte = KByte * 1000     // 1,000,000 字节
	GByte = MByte * 1000     // 1,000,000,000 字节
	TByte = GByte * 1000     // 1,000,000,000,000 字节
	PByte = TByte * 1000     // 1,000,000,000,000,000 字节
	EByte = PByte * 1000     // 1,000,000,000,000,000,000 字节
)
