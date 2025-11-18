// Package needle_map 提供 Needle 索引映射的值类型定义
// 用于在内存或持久化存储中保存 Needle 的位置和大小信息
package needle_map

import (
	"github.com/google/btree"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// NeedleValue 表示 Needle 在 Volume 文件中的索引值
// 这是 SeaweedFS 索引系统的核心数据结构,用于快速定位文件数据
//
// 结构说明:
//   - Key: Needle 的唯一标识符 (8 字节)
//   - Offset: Needle 在 Volume 文件中的字节偏移量,按 8 字节对齐
//   - Size: Needle 数据部分的大小
//
// 设计特点:
//   - 紧凑的内存布局: 总共 16 字节 (8+4+4)
//   - 8 字节对齐: Offset 按 8 字节边界对齐,提升磁盘 I/O 性能
//   - 地址范围: 由于 8 字节对齐,32 位 Offset 可寻址 4GB * 8 = 32GB 空间
//   - 实现 btree.Item 接口: 支持在 B-Tree 中高效存储和查询
//
// 使用场景:
//   - NeedleMap: 作为内存索引的值类型
//   - LevelDB: 序列化后存储在持久化索引中
//   - .idx 文件: 直接写入索引文件
type NeedleValue struct {
	Key    NeedleId // Needle 唯一标识符,对应文件的 fid (8 字节)
	Offset Offset   `comment:"Volume offset"` // Volume 文件中的偏移量(按 8 字节对齐),范围 4G*8=32G (4 字节)
	Size   Size     `comment:"Size of the data portion"` // Needle 数据部分的大小(不包含头部) (4 字节)
}

// Less 实现 btree.Item 接口的比较方法
// 用于在 B-Tree 中维护 NeedleValue 的有序性
//
// 参数:
//   - than: 要比较的另一个 btree.Item (必须是 NeedleValue 类型)
//
// 返回值:
//   - true: 当前 NeedleValue 的 Key 小于比较对象的 Key
//   - false: 当前 NeedleValue 的 Key 大于等于比较对象的 Key
//
// 设计说明:
//   - 只比较 Key 字段,不比较 Offset 和 Size
//   - 使得 NeedleValue 可以在 B-Tree 中按 Key 排序
//   - 支持 O(log n) 的查找、插入和删除操作
//
// 注意:
//   - 类型断言失败会导致 panic,调用者需确保传入正确类型
func (this NeedleValue) Less(than btree.Item) bool {
	that := than.(NeedleValue)
	return this.Key < that.Key
}

// ToBytes 将 NeedleValue 序列化为字节数组
// 用于持久化存储到索引文件(.idx)或 LevelDB
//
// 返回值:
//   - []byte: 16 字节的二进制数据
//     * 0-7 字节: NeedleId (大端序)
//     * 8-11 字节: Offset (大端序)
//     * 12-15 字节: Size (大端序)
//
// 使用场景:
//   - 写入 .idx 索引文件
//   - 存储到 LevelDB 的 value
//   - 网络传输索引数据
//
// 示例:
//   nv := NeedleValue{Key: 12345, Offset: 128, Size: 4096}
//   bytes := nv.ToBytes()  // 返回 16 字节数组
func (nv NeedleValue) ToBytes() []byte {
	return ToBytes(nv.Key, nv.Offset, nv.Size)
}

// ToBytes 将 NeedleId、Offset 和 Size 序列化为字节数组
// 这是底层的序列化函数,被 NeedleValue.ToBytes() 调用
//
// 参数:
//   - key: Needle 的唯一标识符 (8 字节)
//   - offset: Needle 在 Volume 中的偏移量 (4 字节)
//   - size: Needle 数据部分的大小 (4 字节)
//
// 返回值:
//   - []byte: 16 字节的二进制数据,布局如下:
//     * [0:8]   - NeedleId (大端序)
//     * [8:12]  - Offset (大端序)
//     * [12:16] - Size (大端序)
//
// 字节布局:
//   NeedleIdSize = 8, OffsetSize = 4, SizeSize = 4
//   总大小 = 8 + 4 + 4 = 16 字节
//
// 编码方式:
//   - 使用大端序(Big Endian)保证跨平台兼容性
//   - 固定长度编码,无需额外的长度字段
//
// 使用场景:
//   - .idx 文件格式: 连续写入多个 NeedleValue
//   - LevelDB 存储: key=NeedleId, value=Offset+Size
//   - 内存索引加载: 从文件读取并反序列化
func ToBytes(key NeedleId, offset Offset, size Size) []byte {
	bytes := make([]byte, NeedleIdSize+OffsetSize+SizeSize)
	NeedleIdToBytes(bytes[0:NeedleIdSize], key)
	OffsetToBytes(bytes[NeedleIdSize:NeedleIdSize+OffsetSize], offset)
	util.Uint32toBytes(bytes[NeedleIdSize+OffsetSize:NeedleIdSize+OffsetSize+SizeSize], uint32(size))
	return bytes
}
