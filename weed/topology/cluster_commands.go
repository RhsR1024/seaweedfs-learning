// Package topology 实现了 SeaweedFS 的拓扑管理功能
// 本文件定义了通过 Raft 共识协议在集群中同步的命令
package topology

import (
	"encoding/json"
	"fmt"
	hashicorpRaft "github.com/hashicorp/raft"
	"github.com/seaweedfs/raft"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// MaxVolumeIdCommand 表示更新最大 Volume ID 的 Raft 命令
// 这个命令通过 Raft 共识协议在所有 Master 节点间同步，确保集群的一致性
//
// 工作原理：
//   1. Leader 节点接收到分配新 Volume ID 的请求
//   2. Leader 创建 MaxVolumeIdCommand 并通过 Raft 日志提交
//   3. 所有 Follower 节点接收并执行该命令
//   4. 所有节点的 maxVolumeId 保持一致
//
// 为什么需要 Raft 同步：
//   - Volume ID 是全局唯一的资源
//   - 多个 Master 节点需要协调避免分配重复的 ID
//   - Raft 保证即使 Leader 切换，也不会分配重复 ID
type MaxVolumeIdCommand struct {
	// MaxVolumeId 新的最大 Volume ID
	// 这个值必须大于当前的 maxVolumeId，否则会被忽略
	MaxVolumeId needle.VolumeId `json:"maxVolumeId"`
}

// NewMaxVolumeIdCommand 创建一个新的 MaxVolumeIdCommand
// 通常在以下场景调用：
//   1. Master 启动时，从 Volume Server 心跳中发现更大的 Volume ID
//   2. Master 分配新 Volume 时，递增 maxVolumeId
//   3. Master 从快照恢复时，重建 maxVolumeId
//
// 参数:
//   - value: 新的最大 Volume ID
// 返回:
//   - *MaxVolumeIdCommand: 命令对象，可以提交给 Raft
func NewMaxVolumeIdCommand(value needle.VolumeId) *MaxVolumeIdCommand {
	return &MaxVolumeIdCommand{
		MaxVolumeId: value,
	}
}

// CommandName 返回命令名称
// 用于 Raft 日志记录和调试
func (c *MaxVolumeIdCommand) CommandName() string {
	return "MaxVolumeId"
}

// Apply 在 Raft 状态机上应用此命令
// 这是 Raft 框架的回调函数，当日志被提交后会调用此方法
//
// 执行流程：
//   1. 从 Raft Server 上下文获取 Topology 对象
//   2. 记录当前的 maxVolumeId（用于日志）
//   3. 调用 UpAdjustMaxVolumeId 更新 maxVolumeId
//      - 如果新值更大，则更新
//      - 如果新值更小或相等，则忽略（保护性措施）
//   4. 记录更新日志
//
// 参数:
//   - server: Raft Server 对象
// 返回:
//   - interface{}: 命令执行结果（本命令返回 nil）
//   - error: 执行错误（本命令总是返回 nil）
//
// 注意：
//   - 这是旧版 Raft 框架的接口，标记为 deprecatedCommandApply
//   - 新版本可能使用不同的接口，但保留了向后兼容性
func (c *MaxVolumeIdCommand) Apply(server raft.Server) (interface{}, error) {
	// 获取 Topology 对象（Raft Server 的应用层上下文）
	topo := server.Context().(*Topology)

	// 记录更新前的值，用于日志
	before := topo.GetMaxVolumeId()

	// 更新 maxVolumeId
	// UpAdjustMaxVolumeId 只在新值更大时才更新，确保 ID 单调递增
	topo.UpAdjustMaxVolumeId(c.MaxVolumeId)

	// 记录 maxVolumeId 的变化
	// V(1) 表示详细日志级别 1，通过 -v=1 参数启用
	glog.V(1).Infoln("max volume id", before, "==>", topo.GetMaxVolumeId())

	return nil, nil
}

// Persist 将命令持久化到 Raft 快照
// 当 Raft 创建快照时会调用此方法，将状态写入快照文件
//
// 执行流程：
//   1. 将命令序列化为 JSON
//   2. 写入快照 sink
//   3. 如果写入失败，取消快照
//   4. 关闭 sink
//
// 参数:
//   - sink: Raft 快照写入器
// 返回:
//   - error: 持久化错误
func (s *MaxVolumeIdCommand) Persist(sink hashicorpRaft.SnapshotSink) error {
	// 序列化命令为 JSON 格式
	b, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	// 写入快照
	_, err = sink.Write(b)
	if err != nil {
		// 写入失败，取消快照
		// 这会告诉 Raft 框架快照创建失败，不要使用这个快照
		sink.Cancel()
		return fmt.Errorf("sink.Write(): %w", err)
	}

	// 关闭快照写入器
	// Close 会完成快照的最终提交
	return sink.Close()
}

// Release 释放命令占用的资源
// 这是 Raft 框架的回调函数，在命令不再需要时调用
// 本命令不需要额外清理，所以为空实现
func (s *MaxVolumeIdCommand) Release() {
	// 无需释放资源
}
