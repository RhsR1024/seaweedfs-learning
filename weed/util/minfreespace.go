package util

import (
	"errors"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"strconv"
	"strings"
)

// MinFreeSpaceType 是最小空闲空间的类型枚举
type MinFreeSpaceType int

const (
	// AsPercent 将 MinFreeSpaceType 设置为百分比值(0-100)
	// 例如：10 表示磁盘空闲空间需要至少为总容量的 10%
	AsPercent MinFreeSpaceType = iota
	// AsBytes 将 MinFreeSpaceType 设置为绝对字节数值
	// 例如：10G 表示磁盘空闲空间需要至少有 10GB
	AsBytes
)

// MinFreeSpace 定义了最小空闲空间的限制
// 支持两种表示方式：百分比或绝对字节数
// 用于判断磁盘空间是否充足，避免磁盘写满导致系统故障
type MinFreeSpace struct {
	Type    MinFreeSpaceType // 类型：百分比或字节数
	Bytes   uint64           // 绝对字节数值（当 Type 为 AsBytes 时使用）
	Percent float32          // 百分比值（当 Type 为 AsPercent 时使用）
	Raw     string           // 原始字符串表示，用于显示和日志
}

// IsLow 判断当前空闲空间是否低于设定的最小值
// 参数:
//   - freeBytes: 当前空闲字节数
//   - freePercent: 当前空闲空间百分比
// 返回值:
//   - yes: 是否低于最小值（true 表示空间不足）
//   - desc: 描述性信息，用于日志输出
func (s MinFreeSpace) IsLow(freeBytes uint64, freePercent float32) (yes bool, desc string) {
	switch s.Type {
	case AsPercent:
		// 按百分比判断：当前空闲百分比 < 设定的最小百分比
		yes = freePercent < s.Percent
		op := IfElse(yes, "<", ">=")
		return yes, fmt.Sprintf("disk free %.2f%% %s required %.2f%%", freePercent, op, s.Percent)
	case AsBytes:
		// 按绝对值判断：当前空闲字节数 < 设定的最小字节数
		yes = freeBytes < s.Bytes
		op := IfElse(yes, "<", ">=")
		return yes, fmt.Sprintf("disk free %s %s required %s",
			BytesToHumanReadable(freeBytes), op, BytesToHumanReadable(s.Bytes))
	}

	return false, ""
}

// String 返回 MinFreeSpace 的字符串表示
// 用于配置显示和日志输出
func (s MinFreeSpace) String() string {
	switch s.Type {
	case AsPercent:
		return fmt.Sprintf("%.2f%%", s.Percent)
	default:
		return s.Raw
	}
}

// MustParseMinFreeSpace 解析逗号分隔的最小空闲空间设置参数
// 如果 minFreeSpace 已设置，则它的优先级高于 minFreeSpacePercent
// 参数:
//   - minFreeSpace: 绝对值或百分比设置，如 "10G,20G" 或 "10,20"
//   - minFreeSpacePercent: 百分比设置（作为后备选项）
// 返回值:
//   - spaces: 解析后的 MinFreeSpace 切片，支持为不同磁盘设置不同的限制
// 注意: 解析失败会导致程序退出（Fatal）
func MustParseMinFreeSpace(minFreeSpace string, minFreeSpacePercent string) (spaces []MinFreeSpace) {
	// EmptyTo 函数：如果第一个参数为空，则使用第二个参数
	ss := strings.Split(EmptyTo(minFreeSpace, minFreeSpacePercent), ",")
	for _, freeString := range ss {
		if vv, e := ParseMinFreeSpace(freeString); e == nil {
			spaces = append(spaces, *vv)
		} else {
			glog.Fatalf("The value specified in -minFreeSpace not a valid value %s", freeString)
		}
	}

	return spaces
}

// ErrMinFreeSpaceBadValue 表示最小空闲空间参数值无效
var ErrMinFreeSpaceBadValue = errors.New("minFreeSpace is invalid")

// ParseMinFreeSpace 解析最小空闲空间表达式
// 支持两种格式:
//   - 百分比: 纯数字，如 "1", "10", "99.5"（范围 0-100）
//   - 绝对值: 带单位的大小，如 "10G", "500M", "1T"（必须大于 100 字节）
// 参数:
//   - s: 待解析的字符串
// 返回值:
//   - MinFreeSpace 指针，解析成功时返回
//   - error: 解析错误或值无效时返回
// 注意: 小于等于 100 的绝对值会被视为无效（避免与百分比混淆）
func ParseMinFreeSpace(s string) (*MinFreeSpace, error) {
	// 首先尝试解析为浮点数（百分比）
	if percent, e := strconv.ParseFloat(s, 32); e == nil {
		if percent < 0 || percent > 100 {
			return nil, ErrMinFreeSpaceBadValue
		}
		return &MinFreeSpace{Type: AsPercent, Percent: float32(percent), Raw: s}, nil
	}

	// 尝试解析为带单位的大小（绝对值）
	if directSize, e := ParseBytes(s); e == nil {
		// 避免与百分比混淆，绝对值必须大于 100
		if directSize <= 100 {
			return nil, ErrMinFreeSpaceBadValue
		}
		return &MinFreeSpace{Type: AsBytes, Bytes: directSize, Raw: s}, nil
	}

	return nil, ErrMinFreeSpaceBadValue
}
