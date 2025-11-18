// Package needle 实现 SeaweedFS 中 Needle 数据结构的 TTL (Time To Live) 功能
// TTL 用于控制文件的生存时间,过期后可以被自动清理
package needle

import (
	"fmt"
	"strconv"
)

const (
	// stored unit types
	// 存储单位类型常量,用于表示 TTL 的时间单位
	Empty  byte = iota // 0: 空值,表示没有设置 TTL
	Minute             // 1: 分钟
	Hour               // 2: 小时
	Day                // 3: 天
	Week               // 4: 周
	Month              // 5: 月(按 30 天计算)
	Year               // 6: 年(按 365 天计算)
)

// TTL 表示文件的生存时间(Time To Live)
// 用于控制文件在系统中的保留时长,过期后可被清理
//
// 存储格式:
// - 使用 2 个字节存储: [Count][Unit]
// - Count: 时间数量(0-255)
// - Unit: 时间单位(Minute/Hour/Day/Week/Month/Year)
//
// 示例:
// - {Count: 3, Unit: Minute} 表示 3 分钟
// - {Count: 24, Unit: Hour} 表示 24 小时
// - {Count: 7, Unit: Day} 表示 7 天
type TTL struct {
	Count byte // 时间数量,范围 0-255
	Unit  byte // 时间单位,使用上面定义的常量
}

// EMPTY_TTL 表示空 TTL,即永久保存,不会过期
var EMPTY_TTL = &TTL{}

// ReadTTL translate a readable ttl to internal ttl
// Supports format example:
// 3m: 3 minutes
// 4h: 4 hours
// 5d: 5 days
// 6w: 6 weeks
// 7M: 7 months
// 8y: 8 years
//
// # ReadTTL 将可读的 TTL 字符串转换为内部 TTL 结构
//
// 参数:
//
//	ttlString: TTL 字符串,格式为 "数字+单位"
//
// 返回值:
//
//	*TTL: 转换后的 TTL 结构
//	error: 解析错误(数字格式错误)
//
// 支持的格式:
// - "3m": 3 分钟 (minutes)
// - "4h": 4 小时 (hours)
// - "5d": 5 天 (days)
// - "6w": 6 周 (weeks)
// - "7M": 7 月 (Months,注意大写 M)
// - "8y": 8 年 (years)
// - "180": 默认为 180 分钟(纯数字默认单位为分钟)
// - "": 空字符串表示永久保存
//
// 优化逻辑:
// - 自动选择最合适的时间单位
// - 例如 60 分钟会转换为 1 小时
// - 保证 Count 值不超过 255
func ReadTTL(ttlString string) (*TTL, error) {
	if ttlString == "" {
		return EMPTY_TTL, nil
	}
	ttlBytes := []byte(ttlString)
	unitByte := ttlBytes[len(ttlBytes)-1]
	countBytes := ttlBytes[0 : len(ttlBytes)-1]
	if '0' <= unitByte && unitByte <= '9' {
		countBytes = ttlBytes
		unitByte = 'm'
	}
	count, err := strconv.Atoi(string(countBytes))
	unit := toStoredByte(unitByte)
	return fitTtlCount(count, unit), err
}

// fitTtlCount 将 TTL 转换为最合适的存储格式
//
// 参数:
//
//	count: 时间数量
//	unit: 时间单位
//
// 返回值:
//
//	*TTL: 优化后的 TTL 结构
//
// 优化策略:
// 1. 首先尝试使用更大的精确单位(年/月/周/天/小时)
//   - 如果秒数能被该单位整除且 Count < 256,使用该单位
//
// 2. 如果所有精确单位都不适用,尝试向上取整
//   - 分钟(向上取整)
//   - 小时(向上取整)
//   - 天(向上取整)
//   - 周(向上取整)
//   - 月(向上取整)
//   - 年(向上取整)
//
// 3. 如果仍然超过 255,返回 EMPTY_TTL(永久保存)
//
// 目标:
// - 保证 Count 值在 0-255 范围内(byte 类型限制)
// - 尽可能使用精确的时间单位
// - 避免数据损失
func fitTtlCount(count int, unit byte) *TTL {
	seconds := ToSeconds(count, unit)
	if seconds == 0 {
		return EMPTY_TTL
	}
	if seconds%(3600*24*365) == 0 && seconds/(3600*24*365) < 256 {
		return &TTL{Count: byte(seconds / (3600 * 24 * 365)), Unit: Year}
	}
	if seconds%(3600*24*30) == 0 && seconds/(3600*24*30) < 256 {
		return &TTL{Count: byte(seconds / (3600 * 24 * 30)), Unit: Month}
	}
	if seconds%(3600*24*7) == 0 && seconds/(3600*24*7) < 256 {
		return &TTL{Count: byte(seconds / (3600 * 24 * 7)), Unit: Week}
	}
	if seconds%(3600*24) == 0 && seconds/(3600*24) < 256 {
		return &TTL{Count: byte(seconds / (3600 * 24)), Unit: Day}
	}
	if seconds%(3600) == 0 && seconds/(3600) < 256 {
		return &TTL{Count: byte(seconds / (3600)), Unit: Hour}
	}
	if seconds/60 < 256 {
		return &TTL{Count: byte(seconds / 60), Unit: Minute}
	}
	if seconds/(3600) < 256 {
		return &TTL{Count: byte(seconds / (3600)), Unit: Hour}
	}
	if seconds/(3600*24) < 256 {
		return &TTL{Count: byte(seconds / (3600 * 24)), Unit: Day}
	}
	if seconds/(3600*24*7) < 256 {
		return &TTL{Count: byte(seconds / (3600 * 24 * 7)), Unit: Week}
	}
	if seconds/(3600*24*30) < 256 {
		return &TTL{Count: byte(seconds / (3600 * 24 * 30)), Unit: Month}
	}
	if seconds/(3600*24*365) < 256 {
		return &TTL{Count: byte(seconds / (3600 * 24 * 365)), Unit: Year}
	}
	return EMPTY_TTL
}

// read stored bytes to a ttl
//
// # LoadTTLFromBytes 从字节数组加载 TTL
//
// 参数:
//
//	input: 2 字节的数组,格式为 [Count, Unit]
//
// 返回值:
//
//	t: 加载的 TTL 结构
//
// 存储格式:
// - input[0]: Count 字节(时间数量)
// - input[1]: Unit 字节(时间单位)
// - [0, 0]: 表示 EMPTY_TTL(永久保存)
//
// 使用场景:
// - 从磁盘读取 Volume 或 Needle 的 TTL 信息
// - 从网络协议中解析 TTL 数据
func LoadTTLFromBytes(input []byte) (t *TTL) {
	if input[0] == 0 && input[1] == 0 {
		return EMPTY_TTL
	}
	return &TTL{Count: input[0], Unit: input[1]}
}

// read stored bytes to a ttl
//
// # LoadTTLFromUint32 从 uint32 整数加载 TTL
//
// 参数:
//
//	ttl: 32 位整数,高 8 位为 Count,低 8 位为 Unit
//
// 返回值:
//
//	t: 加载的 TTL 结构
//
// 编码格式:
// - 位 8-15(高字节): Count
// - 位 0-7(低字节): Unit
// - 位 16-31: 未使用
//
// 转换示例:
// - 0x00000301: Count=3, Unit=1(Minute) -> 3 分钟
// - 0x00001802: Count=24, Unit=2(Hour) -> 24 小时
//
// 使用场景:
// - 从紧凑的整数格式中恢复 TTL
// - 用于网络传输或数据库存储
func LoadTTLFromUint32(ttl uint32) (t *TTL) {
	input := make([]byte, 2)
	input[1] = byte(ttl)
	input[0] = byte(ttl >> 8)
	return LoadTTLFromBytes(input)
}

// save stored bytes to an output with 2 bytes
//
// # ToBytes 将 TTL 序列化为 2 字节数组
//
// 参数:
//
//	output: 至少 2 字节的输出数组
//
// 输出格式:
// - output[0]: Count(时间数量)
// - output[1]: Unit(时间单位)
//
// 使用场景:
// - 将 TTL 写入磁盘(Volume 或 Needle 头部)
// - 序列化到网络协议中
//
// 注意:
// - 调用者需要保证 output 数组至少有 2 字节
// - 与 LoadTTLFromBytes 对应,可互相转换
func (t *TTL) ToBytes(output []byte) {
	output[0] = t.Count
	output[1] = t.Unit
}

// ToUint32 将 TTL 转换为 uint32 整数格式
//
// 返回值:
//
//	output: 32 位整数编码,格式为 (Count << 8) | Unit
//
// 编码格式:
// - 位 8-15: Count(时间数量)
// - 位 0-7: Unit(时间单位)
// - 位 16-31: 保留为 0
//
// 特殊情况:
// - 如果 TTL 为 nil 或 Count 为 0,返回 0(表示永久保存)
//
// 转换示例:
// - {Count: 3, Unit: Minute(1)} -> 0x00000301 (769)
// - {Count: 24, Unit: Hour(2)} -> 0x00001802 (6146)
//
// 使用场景:
// - 紧凑的整数存储
// - 网络传输
// - 与 LoadTTLFromUint32 对应,可互相转换
func (t *TTL) ToUint32() (output uint32) {
	if t == nil || t.Count == 0 {
		return 0
	}
	output = uint32(t.Count) << 8
	output += uint32(t.Unit)
	return output
}

// String 将 TTL 转换为可读的字符串格式
//
// 返回值:
//
//	string: TTL 字符串,格式为 "数字+单位"
//
// 输出格式:
// - "3m": 3 分钟
// - "4h": 4 小时
// - "5d": 5 天
// - "6w": 6 周
// - "7M": 7 月(注意大写 M)
// - "8y": 8 年
// - "": 空字符串表示永久保存
//
// 特殊情况:
// - TTL 为 nil -> ""
// - Count 为 0 -> ""
// - Unit 为 Empty -> ""
//
// 使用场景:
// - API 响应中显示 TTL
// - 日志输出
// - 配置文件中的 TTL 表示
func (t *TTL) String() string {
	if t == nil || t.Count == 0 {
		return ""
	}
	if t.Unit == Empty {
		return ""
	}
	countString := strconv.Itoa(int(t.Count))
	switch t.Unit {
	case Minute:
		return countString + "m"
	case Hour:
		return countString + "h"
	case Day:
		return countString + "d"
	case Week:
		return countString + "w"
	case Month:
		return countString + "M"
	case Year:
		return countString + "y"
	}
	return ""
}

// ToSeconds 将 TTL 转换为秒数
//
// 返回值:
//
//	uint64: TTL 对应的秒数
//
// 使用场景:
// - 计算文件过期时间
// - TTL 比较和排序
// - 与 Unix 时间戳配合使用
func (t *TTL) ToSeconds() uint64 {
	return ToSeconds(int(t.Count), t.Unit)
}

// ToSeconds 将时间数量和单位转换为秒数
//
// 参数:
//
//	count: 时间数量
//	unit: 时间单位(Minute/Hour/Day/Week/Month/Year)
//
// 返回值:
//
//	uint64: 对应的秒数
//
// 转换关系:
// - Minute: count * 60 秒
// - Hour: count * 3600 秒
// - Day: count * 86400 秒
// - Week: count * 604800 秒
// - Month: count * 2592000 秒 (按 30 天计算)
// - Year: count * 31536000 秒 (按 365 天计算)
// - Empty: 0 秒(永久保存)
//
// 注意:
// - 月份按 30 天计算,不是精确的日历月
// - 年份按 365 天计算,不考虑闰年
func ToSeconds(count int, unit byte) uint64 {
	switch unit {
	case Empty:
		return 0
	case Minute:
		return uint64(count) * 60
	case Hour:
		return uint64(count) * 60 * 60
	case Day:
		return uint64(count) * 60 * 24 * 60
	case Week:
		return uint64(count) * 60 * 24 * 7 * 60
	case Month:
		return uint64(count) * 60 * 24 * 30 * 60
	case Year:
		return uint64(count) * 60 * 24 * 365 * 60
	}
	return 0
}

// toStoredByte 将可读的单位字符转换为存储单位常量
//
// 参数:
//
//	readableUnitByte: 可读的单位字符
//
// 返回值:
//
//	byte: 对应的存储单位常量
//
// 映射关系:
// - 'm' -> Minute (1)
// - 'h' -> Hour (2)
// - 'd' -> Day (3)
// - 'w' -> Week (4)
// - 'M' -> Month (5) (注意大写)
// - 'y' -> Year (6)
// - 其他 -> 0 (Empty)
//
// 注意:
// - Month 使用大写 'M',与 Minute 的小写 'm' 区分
// - 未知字符返回 0(Empty)
func toStoredByte(readableUnitByte byte) byte {
	switch readableUnitByte {
	case 'm':
		return Minute
	case 'h':
		return Hour
	case 'd':
		return Day
	case 'w':
		return Week
	case 'M':
		return Month
	case 'y':
		return Year
	}
	return 0
}

// Minutes 将 TTL 转换为分钟数
//
// 返回值:
//
//	uint32: TTL 对应的分钟数
//
// 转换关系:
// - Minute: count 分钟
// - Hour: count * 60 分钟
// - Day: count * 1440 分钟
// - Week: count * 10080 分钟
// - Month: count * 43200 分钟 (按 30 天计算)
// - Year: count * 525600 分钟 (按 365 天计算)
// - Empty: 0 分钟
//
// 使用场景:
// - 计算以分钟为单位的过期时间
// - TTL 比较和显示
func (t TTL) Minutes() uint32 {
	switch t.Unit {
	case Empty:
		return 0
	case Minute:
		return uint32(t.Count)
	case Hour:
		return uint32(t.Count) * 60
	case Day:
		return uint32(t.Count) * 60 * 24
	case Week:
		return uint32(t.Count) * 60 * 24 * 7
	case Month:
		return uint32(t.Count) * 60 * 24 * 30
	case Year:
		return uint32(t.Count) * 60 * 24 * 365
	}
	return 0
}

// SecondsToTTL 将秒数转换为可读的 TTL 字符串
//
// 参数:
//
//	seconds: 秒数
//
// 返回值:
//
//	string: TTL 字符串,格式为 "数字+单位"
//
// 转换策略:
// 1. 优先使用更大的精确单位(年/月/周/天/小时/分钟)
//   - 如果秒数能被该单位整除且 Count < 256,使用该单位
//
// 2. 如果所有精确单位都不适用,尝试向下取整
//   - 按小时/天/周/月/年依次尝试
//
// 3. 如果仍然不适用,返回空字符串
//
// 输出示例:
// - 180 -> "3m" (3 分钟)
// - 3600 -> "1h" (1 小时)
// - 86400 -> "1d" (1 天)
// - 604800 -> "1w" (1 周)
// - 2592000 -> "1M" (1 月)
// - 31536000 -> "1y" (1 年)
// - 0 -> "" (永久保存)
//
// 限制:
// - Count 必须 < 256(byte 类型限制)
// - 如果秒数太大无法用单位表示,返回空字符串
//
// 使用场景:
// - 将秒数转换为友好的 TTL 表示
// - API 响应格式化
func SecondsToTTL(seconds int32) string {
	if seconds == 0 {
		return ""
	}
	if seconds%(3600*24*365) == 0 && seconds/(3600*24*365) < 256 {
		return fmt.Sprintf("%dy", seconds/(3600*24*365))
	}
	if seconds%(3600*24*30) == 0 && seconds/(3600*24*30) < 256 {
		return fmt.Sprintf("%dM", seconds/(3600*24*30))
	}
	if seconds%(3600*24*7) == 0 && seconds/(3600*24*7) < 256 {
		return fmt.Sprintf("%dw", seconds/(3600*24*7))
	}
	if seconds%(3600*24) == 0 && seconds/(3600*24) < 256 {
		return fmt.Sprintf("%dd", seconds/(3600*24))
	}
	if seconds%(3600) == 0 && seconds/(3600) < 256 {
		return fmt.Sprintf("%dh", seconds/(3600))
	}
	if seconds/60 < 256 {
		return fmt.Sprintf("%dm", seconds/60)
	}
	if seconds/(3600) < 256 {
		return fmt.Sprintf("%dh", seconds/(3600))
	}
	if seconds/(3600*24) < 256 {
		return fmt.Sprintf("%dd", seconds/(3600*24))
	}
	if seconds/(3600*24*7) < 256 {
		return fmt.Sprintf("%dw", seconds/(3600*24*7))
	}
	if seconds/(3600*24*30) < 256 {
		return fmt.Sprintf("%dM", seconds/(3600*24*30))
	}
	if seconds/(3600*24*365) < 256 {
		return fmt.Sprintf("%dy", seconds/(3600*24*365))
	}
	return ""
}
