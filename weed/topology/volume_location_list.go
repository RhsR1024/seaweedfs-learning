// Package topology 实现了 SeaweedFS 的拓扑管理
// 本文件定义 VolumeLocationList，用于管理单个卷的副本位置列表
package topology

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// VolumeLocationList 管理单个 Volume 的所有副本位置
// 核心概念：
//   - 一个 Volume ID 可能有多个副本（根据 ReplicaPlacement）
//   - 每个副本存储在不同的 DataNode 上
//   - 这个列表按照创建顺序维护所有副本的位置
//
// 列表顺序的重要性：
//   - list[0] 是主副本（Master Volume），优先读取
//   - list[1:] 是备份副本（Replica Volumes），用于容灾
//
// 示例：
//   - 副本策略 "001"（同机架不同服务器 1 个副本）:
//     list = [server1, server2]  // 2 个副本
//   - 副本策略 "010"（不同机架 1 个副本）:
//     list = [server1_rack1, server2_rack2]  // 2 个副本
type VolumeLocationList struct {
	// list 存储所有副本所在的 DataNode 列表
	// 按照 [主副本, 备份副本1, 备份副本2, ...] 的顺序排列
	list []*DataNode
}

// NewVolumeLocationList 创建一个空的卷位置列表
func NewVolumeLocationList() *VolumeLocationList {
	return &VolumeLocationList{}
}

// String 返回卷位置列表的字符串表示
// 用于日志输出和调试
func (dnll *VolumeLocationList) String() string {
	return fmt.Sprintf("%v", dnll.list)
}

// Copy 创建卷位置列表的深拷贝
// 使用场景：
//   - 在返回给客户端前复制一份，避免并发修改
//   - 在修改列表前先复制，实现写时复制（Copy-on-Write）
func (dnll *VolumeLocationList) Copy() *VolumeLocationList {
	// 创建新的 slice，复制所有元素
	list := make([]*DataNode, len(dnll.list))
	copy(list, dnll.list)
	return &VolumeLocationList{
		list: list,
	}
}

// Head 返回主副本所在的 DataNode
// 主副本（Master Volume）是列表中的第一个节点
//
// 主副本的特殊性：
//   - 优先用于读取操作
//   - 写操作先写主副本，再同步到备份副本
//   - 如果主副本不可用，会自动切换到备份副本
//
// 返回:
//   - *DataNode: 主副本所在的节点
//   - nil: 列表为空（卷没有任何副本）
func (dnll *VolumeLocationList) Head() *DataNode {
	// 列表为空检查
	if dnll.Length() == 0 {
		return nil
	}
	// 返回第一个节点（主副本）
	return dnll.list[0]
}

// Rest 返回所有备份副本所在的 DataNode 列表
// 备份副本用于：
//   - 数据冗余和容灾
//   - 负载均衡（多个客户端可以从不同副本读取）
//   - 主副本不可用时的故障转移
//
// 返回:
//   - []*DataNode: 备份副本节点列表（可能为空）
func (dnll *VolumeLocationList) Rest() []*DataNode {
	// 返回从第二个元素开始的所有节点
	// 如果只有一个副本，返回空切片
	return dnll.list[1:]
}

// Length 返回副本数量
// 副本数量应该等于 ReplicaPlacement.GetCopyCount()
//
// 示例：
//   - "000"（无副本）: Length = 1（只有主副本）
//   - "001"（1 个副本）: Length = 2（主副本 + 1 个备份副本）
//   - "010"（1 个副本）: Length = 2
//
// 返回:
//   - int: 副本数量
func (dnll *VolumeLocationList) Length() int {
	// nil 检查：避免空指针异常
	if dnll == nil {
		return 0
	}
	return len(dnll.list)
}

// Set 添加或更新 DataNode 到副本列表
// 使用场景：
//   - Volume Server 注册卷时调用
//   - Volume Server 重启后重新注册卷时更新节点信息
//
// 行为：
//   - 如果节点已存在（根据 IP:Port 匹配），更新节点引用
//   - 如果节点不存在，追加到列表末尾
//
// 注意：
//   - 节点标识由 (IP, Port) 唯一确定
//   - 更新节点引用可能包含新的健康状态、容量信息等
func (dnll *VolumeLocationList) Set(loc *DataNode) {
	// 遍历现有列表，查找匹配的节点
	for i := 0; i < len(dnll.list); i++ {
		// 根据 IP 和 Port 匹配节点
		if loc.Ip == dnll.list[i].Ip && loc.Port == dnll.list[i].Port {
			// 找到匹配节点，更新引用
			dnll.list[i] = loc
			return
		}
	}
	// 节点不存在，追加到列表末尾
	dnll.list = append(dnll.list, loc)
}

// Remove 从副本列表中移除指定的 DataNode
// 使用场景：
//   - Volume Server 下线或删除卷
//   - 卷迁移到其他节点
//   - 节点健康检查失败，临时移除
//
// 参数:
//   - loc: 要移除的 DataNode
// 返回:
//   - bool: true 表示成功移除，false 表示节点不存在
func (dnll *VolumeLocationList) Remove(loc *DataNode) bool {
	// 遍历列表查找匹配的节点
	for i, dnl := range dnll.list {
		// 根据 IP 和 Port 匹配节点
		if loc.Ip == dnl.Ip && loc.Port == dnl.Port {
			// 找到匹配节点，从列表中删除
			// 使用切片技巧：list[:i] + list[i+1:]
			dnll.list = append(dnll.list[:i], dnll.list[i+1:]...)
			return true
		}
	}
	// 节点不存在，返回 false
	return false
}

// Refresh 移除长时间未心跳的 DataNode
// 健康检查机制：
//   - Volume Server 定期向 Master 发送心跳（默认 5 秒）
//   - LastSeen 记录节点最后一次心跳时间
//   - 超过阈值的节点被认为已下线，从列表中移除
//
// 参数:
//   - freshThreshHold: 新鲜度阈值（Unix 时间戳）
//     节点的 LastSeen < freshThreshHold 时被认为过期
//
// 示例：
//   freshThreshHold = time.Now().Unix() - 60  // 60 秒未心跳
func (dnll *VolumeLocationList) Refresh(freshThreshHold int64) {
	// 【第一步：检查是否有过期节点】
	// 快速路径：如果所有节点都新鲜，直接返回
	var changed bool
	for _, dnl := range dnll.list {
		if dnl.LastSeen < freshThreshHold {
			// 发现过期节点，需要清理
			changed = true
			break
		}
	}

	// 【第二步：过滤过期节点】
	// 只有发现过期节点时才执行清理操作
	if changed {
		var l []*DataNode
		for _, dnl := range dnll.list {
			// 只保留新鲜的节点
			if dnl.LastSeen >= freshThreshHold {
				l = append(l, dnl)
			}
		}
		// 替换原列表
		dnll.list = l
	}
}

// Stats 返回卷的逻辑大小和文件数量
// 使用场景：
//   - 统计集群存储使用情况
//   - 显示卷的实际数据量（扣除已删除的数据）
//
// 查询策略：
//   - 只查询"过期"的节点（LastSeen < freshThreshHold）
//   - 这个设计看起来有问题，通常应该查询最新的节点
//   - 实际代码中可能是为了避免查询正在同步的节点
//
// 参数:
//   - vid: Volume ID
//   - freshThreshHold: 新鲜度阈值（只查询过期节点）
// 返回:
//   - size: 卷的逻辑大小（实际数据 = 总大小 - 删除的数据）
//   - fileCount: 文件数量（总文件数 - 删除的文件数）
func (dnll *VolumeLocationList) Stats(vid needle.VolumeId, freshThreshHold int64) (size uint64, fileCount int) {
	// 遍历所有节点，查找符合条件的节点
	for _, dnl := range dnll.list {
		// 只查询"过期"的节点（注意：这个逻辑可能有问题）
		if dnl.LastSeen < freshThreshHold {
			// 从节点获取卷的详细信息
			vinfo, err := dnl.GetVolumesById(vid)
			if err == nil {
				// 计算逻辑大小：总大小 - 删除的字节数
				// 计算文件数量：总文件数 - 删除的文件数
				return (vinfo.Size - vinfo.DeletedByteCount), vinfo.FileCount - vinfo.DeleteCount
			}
		}
	}
	// 没有找到符合条件的节点，返回 0
	return 0, 0
}
