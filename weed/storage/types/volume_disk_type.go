package types

import (
	"strings"
)

// DiskType 表示磁盘类型
// 用于区分不同类型的存储介质，以便进行性能优化和资源分配
// SeaweedFS 可以根据磁盘类型来选择合适的 Volume 进行数据存储
type DiskType string

// 磁盘类型常量定义
const (
	HardDriveType DiskType = ""    // 默认类型，表示机械硬盘(HDD)，空字符串用于向后兼容
	HddType                = "hdd" // 明确指定为机械硬盘(HDD - Hard Disk Drive)
	SsdType                = "ssd" // 固态硬盘(SSD - Solid State Drive)
)

// ToDiskType 将字符串转换为 DiskType 类型
// 参数:
//   - vt: 磁盘类型的字符串表示，不区分大小写
// 返回值:
//   - diskType: 对应的 DiskType 枚举值
// 转换规则:
//   - 空字符串或 "hdd" -> HardDriveType (机械硬盘)
//   - "ssd" -> SsdType (固态硬盘)
//   - 其他值 -> 直接转换为自定义 DiskType (支持扩展)
func ToDiskType(vt string) (diskType DiskType) {
	vt = strings.ToLower(vt) // 转换为小写以支持不区分大小写
	diskType = HardDriveType
	switch vt {
	case "", HddType:
		// 空字符串或 "hdd" 都表示机械硬盘
		diskType = HardDriveType
	case "ssd":
		// 固态硬盘
		diskType = SsdType
	default:
		// 支持自定义磁盘类型，例如 "nvme", "cloud" 等
		diskType = DiskType(vt)
	}
	return
}

// String 返回 DiskType 的字符串表示
// 如果是默认类型(HardDriveType)，返回空字符串
// 否则返回实际的磁盘类型字符串
// 该方法用于序列化和日志输出
func (diskType DiskType) String() string {
	if diskType == "" {
		return ""
	}
	return string(diskType)
}

// ReadableString 返回 DiskType 的可读字符串表示
// 与 String() 方法的区别是:
//   - 如果是默认类型(HardDriveType/空字符串)，返回 "hdd" 而不是空字符串
//   - 否则返回实际的磁盘类型字符串
// 该方法用于用户界面显示和 API 响应，确保始终有明确的类型标识
func (diskType DiskType) ReadableString() string {
	if diskType == "" {
		return HddType // 默认类型显示为 "hdd"
	}
	return string(diskType)
}
