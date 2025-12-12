// Package needle_map 提供 Needle 索引映射的接口和实现
// 本文件实现了基于内存 LevelDB 的索引映射 MemDb
package needle_map

// =====================================================
// MemDb 设计概述
// =====================================================
// MemDb 是基于内存 LevelDB 的 Needle 索引实现
// 使用 goleveldb 库的内存存储后端
//
// 设计特点：
//   - 基于 LSM-Tree：LevelDB 的底层数据结构
//   - 有序存储：支持高效的范围查询和升序/降序遍历
//   - 内存存储：使用 storage.NewMemStorage() 不持久化
//   - 简单可靠：利用成熟的 LevelDB 实现
//
// 使用场景：
//   - 临时索引存储
//   - 索引重建过程中的中间存储
//   - 需要有序遍历但不需要持久化的场景
//
// 与 CompactMap 的区别：
//   - MemDb 使用 LevelDB 实现，CompactMap 使用自定义数据结构
//   - MemDb 支持降序遍历，CompactMap 只支持升序
//   - MemDb 有更高的内存开销，但实现更简单
//   - CompactMap 内存效率更高，但功能较少
//
// 存储格式：
//   - Key:   NeedleId (8 字节)
//   - Value: Offset + Size (8 字节)
// =====================================================

import (
	"fmt"
	"io"
	"os"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// MemDb 是基于内存 LevelDB 的 Needle 索引映射
// 使用 goleveldb 库提供的内存存储后端
//
// 特点：
//   - 所有数据存储在内存中，不持久化到磁盘
//   - 支持升序和降序遍历
//   - 关闭后数据丢失
//
// 使用示例：
//
//	db := NewMemDb()
//	defer db.Close()
//	db.Set(needleId, offset, size)
//	nv, found := db.Get(needleId)
//
// This map uses in memory level db
type MemDb struct {
	db *leveldb.DB // 内存 LevelDB 实例
}

// NewMemDb 创建新的内存 LevelDB 索引
// 返回值：
//   - *MemDb: 初始化的内存数据库；失败时返回 nil
//
// 使用说明：
//   - 创建后需要调用 Close() 释放资源
//   - 数据仅存在于内存中，关闭后丢失
func NewMemDb() *MemDb {
	opts := &opt.Options{}

	var err error
	t := &MemDb{}
	// 使用内存存储后端创建 LevelDB
	if t.db, err = leveldb.Open(storage.NewMemStorage(), opts); err != nil {
		glog.V(0).Infof("MemDb fails to open: %v", err)
		return nil
	}

	return t
}

// Set 设置或更新指定 NeedleId 的索引信息
// 参数：
//   - key:    Needle 的唯一标识符
//   - offset: Needle 在 Volume 文件中的偏移量
//   - size:   Needle 数据部分的大小
//
// 返回值：
//   - error: 写入失败时返回错误
//
// 存储格式：
//   - LevelDB Key:   NeedleId 的 8 字节大端序编码
//   - LevelDB Value: Offset (4 字节) + Size (4 字节) 的大端序编码
func (cm *MemDb) Set(key NeedleId, offset Offset, size Size) error {

	// 序列化为 16 字节数组
	bytes := ToBytes(key, offset, size)

	// 写入 LevelDB
	// Key: bytes[0:8] (NeedleId)
	// Value: bytes[8:16] (Offset + Size)
	if err := cm.db.Put(bytes[0:NeedleIdSize], bytes[NeedleIdSize:NeedleIdSize+OffsetSize+SizeSize], nil); err != nil {
		return fmt.Errorf("failed to write temp leveldb: %w", err)
	}
	return nil
}

// Delete 删除指定 NeedleId 的索引条目
// 参数：
//   - key: 要删除的 Needle 的唯一标识符
//
// 返回值：
//   - error: 删除失败时返回错误
//
// 注意：
//   - 这是真正的删除，不是软删除
//   - 与 CompactMap 的软删除策略不同
func (cm *MemDb) Delete(key NeedleId) error {
	// 序列化 NeedleId
	bytes := make([]byte, NeedleIdSize)
	NeedleIdToBytes(bytes, key)
	// 从 LevelDB 删除
	return cm.db.Delete(bytes, nil)

}

// Get 获取指定 NeedleId 的索引信息
// 参数：
//   - key: 要查询的 Needle 的唯一标识符
//
// 返回值：
//   - *NeedleValue: 找到时返回索引信息；未找到返回 nil
//   - bool: 是否找到
func (cm *MemDb) Get(key NeedleId) (*NeedleValue, bool) {
	// 序列化 NeedleId 作为查询 key
	bytes := make([]byte, NeedleIdSize)
	NeedleIdToBytes(bytes[0:NeedleIdSize], key)

	// 从 LevelDB 读取
	data, err := cm.db.Get(bytes, nil)
	if err != nil || len(data) != OffsetSize+SizeSize {
		return nil, false
	}

	// 反序列化 Offset 和 Size
	offset := BytesToOffset(data[0:OffsetSize])
	size := BytesToSize(data[OffsetSize : OffsetSize+SizeSize])
	return &NeedleValue{Key: key, Offset: offset, Size: size}, true
}

// doVisit 是遍历的内部辅助函数
// 从迭代器当前位置读取数据并调用 visit 函数
// 参数：
//   - iter:  LevelDB 迭代器
//   - visit: 访问函数
//
// 返回值：
//   - error: visit 函数返回的错误
//
// Visit visits all entries or stop if any error when visiting
func doVisit(iter iterator.Iterator, visit func(NeedleValue) error) (ret error) {
	// 反序列化 Key
	key := BytesToNeedleId(iter.Key())

	// 反序列化 Value
	data := iter.Value()
	offset := BytesToOffset(data[0:OffsetSize])
	size := BytesToSize(data[OffsetSize : OffsetSize+SizeSize])

	// 调用访问函数
	needle := NeedleValue{Key: key, Offset: offset, Size: size}
	ret = visit(needle)
	if ret != nil {
		return
	}
	return nil
}

// AscendingVisit 按 NeedleId 升序遍历所有条目
// 参数：
//   - visit: 访问函数，对每个条目调用一次
//
// 返回值：
//   - error: visit 函数返回的错误或迭代器错误
//
// 遍历顺序：
//   - 从最小 NeedleId 到最大 NeedleId
//   - LevelDB 保证按 key 字节序排序
func (cm *MemDb) AscendingVisit(visit func(NeedleValue) error) (ret error) {
	// 创建迭代器
	iter := cm.db.NewIterator(nil, nil)

	// 移动到第一个条目
	if iter.First() {
		if ret = doVisit(iter, visit); ret != nil {
			return
		}
	}

	// 按升序遍历剩余条目
	for iter.Next() {
		if ret = doVisit(iter, visit); ret != nil {
			return
		}
	}

	// 释放迭代器资源
	iter.Release()
	ret = iter.Error()

	return
}

// DescendingVisit 按 NeedleId 降序遍历所有条目
// 这是 MemDb 独有的功能，CompactMap 不支持
// 参数：
//   - visit: 访问函数，对每个条目调用一次
//
// 返回值：
//   - error: visit 函数返回的错误或迭代器错误
//
// 遍历顺序：
//   - 从最大 NeedleId 到最小 NeedleId
func (cm *MemDb) DescendingVisit(visit func(NeedleValue) error) (ret error) {
	// 创建迭代器
	iter := cm.db.NewIterator(nil, nil)

	// 移动到最后一个条目
	if iter.Last() {
		if ret = doVisit(iter, visit); ret != nil {
			return
		}
	}

	// 按降序遍历剩余条目
	for iter.Prev() {
		if ret = doVisit(iter, visit); ret != nil {
			return
		}
	}

	// 释放迭代器资源
	iter.Release()
	ret = iter.Error()

	return
}

// SaveToIdx 将内存索引保存到 .idx 文件
// 参数：
//   - idxName: 索引文件路径
//
// 返回值：
//   - error: 保存失败时返回错误
//
// 文件格式：
//   - 连续的 16 字节记录
//   - 每条记录：NeedleId (8) + Offset (4) + Size (4)
//
// 过滤规则：
//   - 跳过 Offset 为零的条目
//   - 跳过已删除的条目 (Size < 0)
func (cm *MemDb) SaveToIdx(idxName string) (ret error) {
	// 创建或覆盖索引文件
	idxFile, err := os.OpenFile(idxName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return
	}
	defer func() {
		idxFile.Sync()  // 确保数据写入磁盘
		idxFile.Close()
	}()

	// 按升序遍历并写入文件
	return cm.AscendingVisit(func(value NeedleValue) error {
		// 跳过无效条目
		if value.Offset.IsZero() || value.Size.IsDeleted() {
			return nil
		}
		// 写入 16 字节记录
		_, err := idxFile.Write(value.ToBytes())
		return err
	})

}

// LoadFromIdx 从 .idx 文件加载索引到内存
// 参数：
//   - idxName: 索引文件路径
//
// 返回值：
//   - error: 加载失败时返回错误
func (cm *MemDb) LoadFromIdx(idxName string) (ret error) {
	// 打开索引文件
	idxFile, err := os.OpenFile(idxName, os.O_RDONLY, 0644)
	if err != nil {
		return
	}
	defer idxFile.Close()

	// 调用通用加载方法
	return cm.LoadFromReaderAt(idxFile)

}

// LoadFromReaderAt 从 io.ReaderAt 加载索引
// 这是一个通用方法，可以从任何支持随机读取的源加载
// 参数：
//   - readerAt: 支持随机读取的数据源
//
// 返回值：
//   - error: 加载失败时返回错误
func (cm *MemDb) LoadFromReaderAt(readerAt io.ReaderAt) (ret error) {

	// 使用默认过滤选项
	return cm.LoadFilterFromReaderAt(readerAt, true, true)
}

// LoadFilterFromReaderAt 从 io.ReaderAt 加载索引，支持过滤选项
// 参数：
//   - readerAt:           支持随机读取的数据源
//   - isFilterOffsetZero: 是否过滤 Offset 为零的条目
//   - isFilterDeleted:    是否过滤已删除的条目 (Size < 0)
//
// 返回值：
//   - error: 加载失败时返回错误
//
// 处理逻辑：
//   - 遍历索引文件中的每条记录
//   - 根据过滤条件决定 Set 或 Delete
func (cm *MemDb) LoadFilterFromReaderAt(readerAt io.ReaderAt, isFilterOffsetZero bool, isFilterDeleted bool) (ret error) {
	// 使用 idx.WalkIndexFile 遍历索引文件
	return idx.WalkIndexFile(readerAt, 0, func(key NeedleId, offset Offset, size Size) error {
		// 检查是否需要删除
		if (isFilterOffsetZero && offset.IsZero()) || (isFilterDeleted && size.IsDeleted()) {
			return cm.Delete(key)
		}
		// 正常条目则设置
		return cm.Set(key, offset, size)
	})

}

// Close 关闭内存数据库并释放资源
// 关闭后所有数据丢失
func (cm *MemDb) Close() {
	cm.db.Close()
}
