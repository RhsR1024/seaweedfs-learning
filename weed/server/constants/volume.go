// Package constants 定义了 SeaweedFS 服务器相关的常量配置
// 这些常量用于控制服务器行为、心跳间隔、超时等核心参数
package constants

const (
	// VolumePulseSeconds 定义 Volume Server 向 Master 发送心跳的时间间隔（秒）
	// 心跳机制用于：
	//   1. 向 Master 报告 Volume 健康状态
	//   2. 同步 Volume 容量和使用情况
	//   3. 检测 Volume Server 是否在线
	// 默认值 5 秒表示每 5 秒发送一次心跳
	// Master 通常会在 3 倍心跳间隔（15 秒）后认为 Volume Server 离线
	VolumePulseSeconds = 5
)
