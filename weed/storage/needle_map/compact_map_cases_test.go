// Package needle_map 实现了 SeaweedFS 的 Needle 索引映射
// 本文件包含针对 5 字节 Offset 模式的特殊测试用例
//
// 构建标签说明：
//   //go:build 5BytesOffset
//   只有在编译时指定 -tags "5BytesOffset" 才会包含此文件
//   go test -tags "5BytesOffset" ./weed/storage/needle_map/

//go:build 5BytesOffset
// +build 5BytesOffset

package needle_map

// =====================================================
// 5 字节 Offset 模式测试说明
// =====================================================
// SeaweedFS 支持两种 Offset 存储模式：
//
// 1. 标准模式（4 字节 Offset）：
//    - Offset 使用 4 字节存储
//    - 支持最大 4GB * 8 = 32GB 的 Volume 文件
//    - 默认编译模式
//
// 2. 5 字节 Offset 模式：
//    - Offset 使用 5 字节存储
//    - 支持最大 1PB 的 Volume 文件
//    - 需要编译时指定 -tags "5BytesOffset"
//
// 此文件测试 5 字节 Offset 模式下的索引加载
// 使用专门的测试数据文件 187.idx
//
// 测试数据特点：
//   - 包含大于 32GB 的 Offset 值
//   - 验证 5 字节 Offset 的正确解析
// =====================================================

import (
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/stretchr/testify/assert"
)

// Test5bytesIndexLoading 测试 5 字节 Offset 模式下的索引加载
// 验证大 Offset 值（超过 32GB）能够正确存储和检索
//
// 测试场景：
//   1. 加载包含大 Offset 的索引文件
//   2. 查询特定 NeedleId
//   3. 验证返回的 Offset 值正确
//
// 测试数据说明：
//   - 文件：../../../test/data/187.idx
//   - 包含 Offset > 32GB 的记录
//   - NeedleId 0x671b905 的 Offset = 12884911892 * 8 字节
//     = 103,079,295,136 字节 ≈ 96GB
//
// 5 字节 Offset 计算：
//   - 存储值：12884911892（约 120.4 亿）
//   - 实际偏移：12884911892 * 8 = 103,079,295,136 字节
//   - SeaweedFS 使用 8 字节对齐，所以存储值需要乘以 8
//
// 运行方式：
//   go test -tags "5BytesOffset" -v -run Test5bytesIndexLoading ./weed/storage/needle_map/
func Test5bytesIndexLoading(t *testing.T) {

	// 打开测试索引文件
	// 该文件包含 5 字节 Offset 格式的索引数据
	indexFile, ie := os.OpenFile("../../../test/data/187.idx", os.O_RDWR|os.O_RDONLY, 0644)
	if ie != nil {
		log.Fatalln(ie)
	}
	defer indexFile.Close()

	// 加载索引文件到 CompactMap
	m, rowCount := loadNewNeedleMap(indexFile)

	// 打印加载的总条目数
	println("total entries:", rowCount)

	// 要查询的 NeedleId
	// 0x671b905 = 108116229（十进制）
	key := types.NeedleId(0x671b905) // 108116229

	// 从 CompactMap 中查询
	needle, found := m.Get(types.NeedleId(0x671b905))

	// 打印查询结果
	// 格式：found key:xxx offset:xxx size:xxx
	fmt.Printf("%v key:%v offset:%v size:%v\n", found, key, needle.Offset, needle.Size)

	// 验证 Offset 值
	// 预期值：12884911892 * 8 = 103,079,295,136 字节
	// 这个值远超 4 字节 Offset 的最大值（约 32GB）
	// 证明 5 字节 Offset 模式正常工作
	assert.Equal(t, int64(12884911892)*8, needle.Offset.ToActualOffset(), "offset")

}
