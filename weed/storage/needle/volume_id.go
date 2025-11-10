package needle

import (
	"strconv"
)

// VolumeId 定义卷ID类型
//
// 在 SeaweedFS 中，每个存储卷(Volume)都有一个唯一的数字ID用于标识
// 使用 uint32 类型意味着支持最多 2^32 (约42亿) 个不同的卷
//
// 设计考虑：
// 1. uint32 提供足够大的ID空间，同时保持较小的内存占用
// 2. 使用类型别名提供更好的类型安全性和代码可读性
// 3. 便于在不同组件间传递和识别卷ID
type VolumeId uint32

// NewVolumeId 从字符串解析创建一个 VolumeId
//
// 参数：
//   vid: 卷ID的字符串表示，例如 "123"
//
// 返回：
//   VolumeId: 解析后的卷ID
//   error: 如果字符串不是有效的数字则返回错误
//
// 使用场景：
// - 从HTTP请求参数中解析卷ID
// - 从配置文件或命令行参数中读取卷ID
// - 从存储的元数据中恢复卷ID
//
// 注意：虽然使用 ParseUint 的 64 位版本，但最终转换为 uint32
// 这意味着如果输入的数字超过 uint32 的最大值(4294967295)，
// 会发生截断而不是返回错误
func NewVolumeId(vid string) (VolumeId, error) {
	volumeId, err := strconv.ParseUint(vid, 10, 64)
	return VolumeId(volumeId), err
}

// String 将 VolumeId 转换为字符串表示
//
// 实现了 fmt.Stringer 接口，使得 VolumeId 可以方便地用于：
// - 日志输出
// - HTTP 响应
// - 调试信息
// - 序列化操作
//
// 返回：
//   卷ID的十进制字符串表示，例如 "123"
func (vid VolumeId) String() string {
	return strconv.FormatUint(uint64(vid), 10)
}

// Next 返回下一个连续的 VolumeId
//
// 用于生成新的卷ID，通常在以下场景使用：
// - Master 服务器分配新的卷时
// - 顺序创建多个卷时
// - 测试场景中需要连续的卷ID
//
// 返回：
//   当前卷ID + 1 的新 VolumeId
//
// 注意：
// 1. 显式转换为 uint32 确保溢出行为一致
// 2. 如果当前ID是最大值(4294967295)，调用Next()会溢出回到0
// 3. 调用者需要自行处理ID用尽的情况
func (vid VolumeId) Next() VolumeId {
	return VolumeId(uint32(vid) + 1)
}
