// Package needle_map 提供 Needle 索引映射的接口和实现
// 定义了 NeedleValueMap 接口，所有索引映射实现都需要遵循此接口
package needle_map

import (
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// =====================================================
// NeedleValueMap 接口说明
// =====================================================
// NeedleValueMap 是 SeaweedFS Needle 索引映射的核心接口
// 用于在内存中维护 NeedleId 到磁盘位置 (Offset, Size) 的映射关系
//
// 接口设计目标：
//   - 统一不同索引实现的访问方式
//   - 支持高效的 CRUD 操作
//   - 支持有序遍历（用于索引重建、vacuum 等）
//
// 接口实现：
//   - CompactMap: 内存紧凑型实现，使用分段 + 二分查找
//   - MemDb: 基于内存 LevelDB 的实现
//   - 旧版 CompactMap: weed/storage/needle_map/old/ 目录下
//
// 使用场景：
//   - Volume Server 启动时加载索引到内存
//   - 文件读取时快速定位 Needle 位置
//   - 文件写入/删除时更新索引
//   - Vacuum 压缩时遍历所有有效 Needle
// =====================================================

// NeedleValueMap 定义了 Needle 索引映射必须实现的接口
// 所有索引映射实现（内存、LevelDB 等）都需要实现此接口
//
// 接口方法：
//   - Set: 插入或更新索引条目
//   - Delete: 删除索引条目（软删除）
//   - Get: 查询索引条目
//   - AscendingVisit: 按 Key 升序遍历所有条目
//
// 线程安全：
//   - 接口本身不保证线程安全
//   - 具体实现需要自行处理并发访问
//   - CompactMap 使用 RWMutex 实现读写锁
type NeedleValueMap interface {
	// Set 设置或更新指定 NeedleId 的索引信息
	// 参数:
	//   - key: Needle 的唯一标识符
	//   - offset: Needle 在 Volume 文件中的偏移量
	//   - size: Needle 数据部分的大小
	// 返回值:
	//   - oldOffset: 如果是更新操作，返回旧的偏移量；否则返回零值
	//   - oldSize: 如果是更新操作，返回旧的大小；否则返回 0
	//
	// 使用场景:
	//   - 新文件写入后更新索引
	//   - 文件覆盖写入时更新位置信息
	//   - 从索引文件加载数据
	Set(key NeedleId, offset Offset, size Size) (oldOffset Offset, oldSize Size)

	// Delete 删除指定 NeedleId 的索引条目
	// 采用软删除策略：将 Size 设为负数，而不是真正移除
	// 参数:
	//   - key: 要删除的 Needle 的唯一标识符
	// 返回值:
	//   - Size: 被删除条目的原始大小；如果条目不存在返回 0
	//
	// 软删除原因:
	//   - 保留删除记录用于后续 Vacuum 压缩
	//   - 避免频繁的内存移动操作
	//   - 支持删除操作的幂等性
	Delete(key NeedleId) Size

	// Get 获取指定 NeedleId 的索引信息
	// 参数:
	//   - key: 要查询的 Needle 的唯一标识符
	// 返回值:
	//   - *NeedleValue: 找到时返回索引信息指针；未找到返回 nil
	//   - bool: 是否找到指定的 Needle
	//
	// 注意事项:
	//   - 即使找到条目，如果 Size < 0 表示已删除
	//   - 调用者需要检查 Size.IsDeleted() 判断是否有效
	Get(key NeedleId) (*NeedleValue, bool)

	// AscendingVisit 按 NeedleId 升序遍历所有索引条目
	// 参数:
	//   - visit: 访问函数，对每个条目调用一次
	//     返回 error 时终止遍历
	// 返回值:
	//   - error: visit 函数返回的错误；遍历完成返回 nil
	//
	// 使用场景:
	//   - 保存索引到 .idx 文件
	//   - Vacuum 压缩时识别有效 Needle
	//   - 索引完整性检查
	//   - 统计分析
	//
	// 注意事项:
	//   - 遍历期间会持有读锁，避免长时间操作
	//   - 包含已删除的条目（Size < 0）
	AscendingVisit(visit func(NeedleValue) error) error
}
