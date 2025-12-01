// Package topology 实现了 SeaweedFS 的拓扑管理功能
// 本文件定义了 Collection（集合），用于逻辑分组和隔离不同类型的文件
package topology

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// Collection 表示一个文件集合
// Collection 是 SeaweedFS 中的逻辑分组单位，用于隔离不同类型的文件
//
// 核心概念：
//   - 不同的 Collection 有独立的 Volume 集合
//   - 可以为每个 Collection 配置不同的副本策略、TTL、磁盘类型
//   - Collection 名称会作为 Volume 文件名的一部分：<collection>_<volumeId>.dat
//
// 使用场景：
//   - 按业务线隔离：user_photos、product_images、log_files
//   - 按数据特性隔离：hot_data（SSD）、cold_data（HDD）
//   - 按生命周期隔离：permanent（无 TTL）、temporary（有 TTL）
//
// 示例：
//   - Collection "photos"：副本策略 "001"，SSD，无 TTL
//   - Collection "backups"：副本策略 "200"，HDD，TTL 30 天
type Collection struct {
	// Name 集合名称，必须唯一
	// 空字符串表示默认集合（所有未指定 Collection 的文件）
	Name string

	// volumeSizeLimit 单个卷的大小限制（字节）
	// 默认 30GB，可通过配置调整
	// 当卷达到此大小时，会标记为只读并创建新卷
	volumeSizeLimit uint64

	// replicationAsMin 是否将副本策略作为最小要求
	// true: 副本策略是最小值，实际副本数可以更多
	// false: 严格按照副本策略分配
	replicationAsMin bool

	// storageType2VolumeLayout 存储类型到 VolumeLayout 的映射
	// Key 格式：<replication><ttl><diskType>
	// 例如：
	//   - "001" -> VolumeLayout for 副本策略 001，无 TTL，HDD
	//   - "0013d" -> VolumeLayout for 副本策略 001，TTL 3 天，HDD
	//   - "001ssd" -> VolumeLayout for 副本策略 001，无 TTL，SSD
	// 这样可以在同一个 Collection 内，为不同存储需求的文件使用不同的卷
	storageType2VolumeLayout *util.ConcurrentReadMap
}

// NewCollection 创建一个新的 Collection
//
// 参数:
//   - name: 集合名称，空字符串表示默认集合
//   - volumeSizeLimit: 单个卷的大小限制（字节）
//   - replicationAsMin: 是否将副本策略作为最小要求
// 返回:
//   - *Collection: 新创建的 Collection 对象
func NewCollection(name string, volumeSizeLimit uint64, replicationAsMin bool) *Collection {
	c := &Collection{
		Name:             name,
		volumeSizeLimit:  volumeSizeLimit,
		replicationAsMin: replicationAsMin,
	}
	// 初始化存储类型到 VolumeLayout 的映射
	// 使用并发安全的 Map，支持多线程读写
	c.storageType2VolumeLayout = util.NewConcurrentReadMap()
	return c
}

// String 返回 Collection 的字符串表示
// 用于日志记录和调试
func (c *Collection) String() string {
	return fmt.Sprintf("Name:%s, volumeSizeLimit:%d, storageType2VolumeLayout:%v", c.Name, c.volumeSizeLimit, c.storageType2VolumeLayout)
}

// GetOrCreateVolumeLayout 获取或创建指定存储类型的 VolumeLayout
// 如果该存储类型的 VolumeLayout 不存在，则创建一个新的
//
// 核心逻辑：
//   1. 根据副本策略、TTL、磁盘类型生成 key
//   2. 在 Map 中查找该 key 对应的 VolumeLayout
//   3. 如果不存在，创建一个新的 VolumeLayout
//
// Key 生成规则：
//   - 基础 key：副本策略字符串（如 "001"）
//   - 如果有 TTL：追加 TTL 字符串（如 "3d"），key = "0013d"
//   - 如果不是 HDD：追加磁盘类型（如 "ssd"），key = "001ssd" 或 "0013dssd"
//   - HDD 是默认值，不追加到 key 中
//
// 参数:
//   - rp: 副本放置策略
//   - ttl: 生存时间（可为 nil）
//   - diskType: 磁盘类型（HDD/SSD/NVMe）
// 返回:
//   - *VolumeLayout: 对应的 VolumeLayout 对象
func (c *Collection) GetOrCreateVolumeLayout(rp *super_block.ReplicaPlacement, ttl *needle.TTL, diskType types.DiskType) *VolumeLayout {
	// 【步骤 1：生成存储类型的 key】
	// 基础 key：副本策略字符串
	keyString := rp.String()

	// 追加 TTL（如果存在）
	if ttl != nil {
		keyString += ttl.String()
	}

	// 追加磁盘类型（如果不是默认的 HDD）
	if diskType != types.HardDriveType {
		keyString += string(diskType)
	}

	// 【步骤 2：获取或创建 VolumeLayout】
	// Get 方法是线程安全的，第二个参数是创建函数
	// 如果 key 不存在，会调用创建函数并将结果存入 Map
	vl := c.storageType2VolumeLayout.Get(keyString, func() interface{} {
		// 创建新的 VolumeLayout
		return NewVolumeLayout(rp, ttl, diskType, c.volumeSizeLimit, c.replicationAsMin)
	})

	// 类型断言：将 interface{} 转换为 *VolumeLayout
	return vl.(*VolumeLayout)
}

// GetVolumeLayout 获取指定存储类型的 VolumeLayout
// 与 GetOrCreateVolumeLayout 的区别：如果不存在，不会创建，而是返回 false
//
// 使用场景：
//   - 查询操作：查找文件时，只需要查找已存在的 VolumeLayout
//   - 检查操作：检查某个存储类型是否有卷
//
// 参数:
//   - rp: 副本放置策略
//   - ttl: 生存时间（可为 nil）
//   - diskType: 磁盘类型
// 返回:
//   - *VolumeLayout: VolumeLayout 对象（如果存在）
//   - bool: 是否找到
func (c *Collection) GetVolumeLayout(rp *super_block.ReplicaPlacement, ttl *needle.TTL, diskType types.DiskType) (*VolumeLayout, bool) {
	// 【步骤 1：生成存储类型的 key】
	// Key 生成规则与 GetOrCreateVolumeLayout 完全相同
	keyString := rp.String()
	if ttl != nil {
		keyString += ttl.String()
	}
	if diskType != types.HardDriveType {
		keyString += string(diskType)
	}

	// 【步骤 2：查找 VolumeLayout】
	// Find 方法只查找，不创建
	vl, ok := c.storageType2VolumeLayout.Find(keyString)

	// 类型断言并返回
	// 如果 ok=false，vl 为 nil，类型断言仍然安全（返回 nil）
	return vl.(*VolumeLayout), ok
}

// GetAllVolumeLayouts 获取该 Collection 的所有 VolumeLayout
// 用于遍历操作，如统计、报告、垃圾回收等
//
// 返回:
//   - []*VolumeLayout: 所有 VolumeLayout 的切片
func (c *Collection) GetAllVolumeLayouts() []*VolumeLayout {
	var vls []*VolumeLayout

	// 遍历 Map 中的所有 VolumeLayout
	for _, vl := range c.storageType2VolumeLayout.Items() {
		// 过滤 nil 值（理论上不应该有，但安全起见）
		if vl != nil {
			vls = append(vls, vl.(*VolumeLayout))
		}
	}

	return vls
}

// DeleteVolumeLayout 删除指定存储类型的 VolumeLayout
// 通常在以下场景使用：
//   - VolumeLayout 中的所有卷都已删除
//   - 清理空的 VolumeLayout 以释放内存
//
// 参数:
//   - rp: 副本放置策略
//   - ttl: 生存时间（可为 nil）
//   - diskType: 磁盘类型
func (c *Collection) DeleteVolumeLayout(rp *super_block.ReplicaPlacement, ttl *needle.TTL, diskType types.DiskType) {
	// 【步骤 1：生成存储类型的 key】
	// Key 生成规则与 GetVolumeLayout 完全相同
	keyString := rp.String()
	if ttl != nil {
		keyString += ttl.String()
	}
	if diskType != types.HardDriveType {
		keyString += string(diskType)
	}

	// 【步骤 2：从 Map 中删除】
	c.storageType2VolumeLayout.Delete(keyString)
}

// Lookup 根据 Volume ID 查找存储该卷的所有数据节点
// 遍历该 Collection 的所有 VolumeLayout，找到包含该卷的节点列表
//
// 工作流程：
//   1. 遍历所有 VolumeLayout（不同副本策略、TTL、磁盘类型）
//   2. 在每个 VolumeLayout 中查找该 Volume ID
//   3. 一旦找到，立即返回节点列表（一个卷只会在一个 VolumeLayout 中）
//
// 参数:
//   - vid: Volume ID
// 返回:
//   - []*DataNode: 存储该卷的所有数据节点（包含所有副本）
//     如果找不到，返回 nil
func (c *Collection) Lookup(vid needle.VolumeId) []*DataNode {
	// 遍历所有 VolumeLayout
	for _, vl := range c.storageType2VolumeLayout.Items() {
		if vl != nil {
			// 在该 VolumeLayout 中查找
			if list := vl.(*VolumeLayout).Lookup(vid); list != nil {
				// 找到了，直接返回
				return list
			}
		}
	}

	// 遍历完所有 VolumeLayout 都没找到
	return nil
}

// ListVolumeServers 列出该 Collection 的所有卷服务器节点
// 遍历所有 VolumeLayout，收集所有的数据节点
//
// 注意：
//   - 返回的列表可能有重复节点（同一个节点可能在多个 VolumeLayout 中）
//   - 调用方需要自行去重（如果需要）
//
// 返回:
//   - []*DataNode: 所有数据节点的列表
func (c *Collection) ListVolumeServers() (nodes []*DataNode) {
	// 遍历所有 VolumeLayout
	for _, vl := range c.storageType2VolumeLayout.Items() {
		if vl != nil {
			// 获取该 VolumeLayout 的所有卷服务器
			if list := vl.(*VolumeLayout).ListVolumeServers(); list != nil {
				// 追加到结果列表
				nodes = append(nodes, list...)
			}
		}
	}
	return
}
