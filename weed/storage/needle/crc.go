// Package needle 实现 SeaweedFS 的 Needle 数据结构和操作
// 本文件实现 CRC32 校验功能，用于确保 Needle 数据的完整性
//
// CRC 算法:
//   使用 CRC-32C (Castagnoli) 多项式
//   - 比标准 CRC-32 (IEEE) 更快
//   - 更好的错误检测能力
//   - 硬件加速支持（Intel SSE4.2）
//
// 使用场景:
//  1. Needle 写入：计算数据的 CRC，存储在 Needle Footer
//  2. Needle 读取：验证读取数据的 CRC，检测数据损坏
//  3. 副本同步：确保副本数据与主副本一致
//
// 兼容性说明:
//   - 旧版本使用 crc.Value() 函数（已废弃）
//   - 新版本（>= 3.09）直接使用 uint32(crc)
//   - 读取时兼容两种格式
package needle

import (
	"fmt"
	"io"

	"hash/crc32"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

// table CRC-32C (Castagnoli) 查找表
// 预计算的查找表，加速 CRC 计算
var table = crc32.MakeTable(crc32.Castagnoli)

// CRC CRC32 校验和类型
// 封装 uint32，提供链式操作和兼容性支持
type CRC uint32

// NewCRC 计算字节切片的 CRC32 校验和
//
// 参数:
//   - b: 要计算 CRC 的字节切片
//
// 返回值:
//   - CRC: 计算得到的 CRC32 值
//
// 示例:
//   data := []byte("hello world")
//   checksum := NewCRC(data)
//   fmt.Printf("CRC: 0x%x\n", uint32(checksum))
func NewCRC(b []byte) CRC {
	return CRC(0).Update(b)
}

// Update 增量更新 CRC 值（用于分块计算）
//
// 参数:
//   - b: 新的数据块
//
// 返回值:
//   - CRC: 更新后的 CRC 值
//
// 用途:
//   这是一个增量计算函数，允许分块计算大数据的 CRC
//   避免一次性将整个文件加载到内存
//
// 示例:
//   crc := CRC(0)
//   crc = crc.Update([]byte("hello "))
//   crc = crc.Update([]byte("world"))
//   // 等价于 NewCRC([]byte("hello world"))
func (c CRC) Update(b []byte) CRC {
	return CRC(crc32.Update(uint32(c), table, b))
}

// Value 旧版 CRC 计算方法（已废弃，仅用于兼容性）
//
// 返回值:
//   - uint32: 变换后的 CRC 值
//
// 历史:
//   - 旧版本（< 3.09）使用此函数进行 CRC 计算
//   - commit 056c480eb 引入，在 version 3.09 切换为直接使用 uint32(crc)
//
// 兼容性:
//   读取 Needle 时，需要同时检查 crc 和 crc.Value() 两种格式
//   以支持旧版本写入的数据
//
// 注意:
//   新代码应使用 uint32(crc) 而不是 crc.Value()
//
// 变换公式:
//   result = (crc >> 15 | crc << 17) + 0xa282ead8
func (c CRC) Value() uint32 {
	return uint32(c>>15|c<<17) + 0xa282ead8
}

// Etag 生成 Needle 的 ETag（HTTP ETag 头）
//
// 返回值:
//   - string: 16 进制格式的 ETag 字符串
//
// ETag 说明:
//   ETag（Entity Tag）是 HTTP 协议中用于缓存验证的标识符
//   客户端可以使用 ETag 进行条件请求（If-None-Match）
//
// 格式:
//   8 位 16 进制字符串（例如: "a1b2c3d4"）
//
// 示例:
//   n := &Needle{Checksum: 0x12345678}
//   etag := n.Etag() // "78563412" (小端字节序)
//
// 用途:
//   - HTTP 缓存控制（304 Not Modified 响应）
//   - CDN 缓存键
//   - 客户端缓存验证
func (n *Needle) Etag() string {
	bits := make([]byte, 4)
	util.Uint32toBytes(bits, uint32(n.Checksum))
	return fmt.Sprintf("%x", bits)
}

// NewCRCwriter 创建一个带 CRC 计算的 Writer
//
// 参数:
//   - w: 底层 Writer（实际写入数据的目标）
//
// 返回值:
//   - *CRCwriter: CRC Writer 包装器
//
// 用途:
//   在写入数据的同时自动计算 CRC 校验和
//   避免额外的遍历和内存拷贝
//
// 示例:
//   var buf bytes.Buffer
//   crcWriter := NewCRCwriter(&buf)
//   crcWriter.Write([]byte("hello world"))
//   checksum := crcWriter.Sum()
//   fmt.Printf("Data: %s, CRC: 0x%x\n", buf.String(), checksum)
func NewCRCwriter(w io.Writer) *CRCwriter {

	return &CRCwriter{
		crc: CRC(0),
		w:   w,
	}

}

// CRCwriter 实现 io.Writer 接口，同时计算 CRC32 校验和
// 这是一个透明的 Writer 包装器，写入数据的同时自动更新 CRC
//
// 字段:
//   - crc: 当前的 CRC 值（增量更新）
//   - w: 底层 Writer（实际数据写入目标）
//
// 使用场景:
//   - Needle 写入：边写入边计算 CRC
//   - 数据上传：流式计算上传数据的 CRC
//   - 副本同步：确保同步数据的完整性
type CRCwriter struct {
	crc CRC       // 当前累积的 CRC 值
	w   io.Writer // 底层 Writer
}

// Write 实现 io.Writer 接口，写入数据并更新 CRC
//
// 参数:
//   - p: 要写入的字节切片
//
// 返回值:
//   - n: 实际写入的字节数
//   - err: 写入错误
//
// 工作原理:
//  1. 将数据写入底层 Writer
//  2. 同时更新 CRC 值
//  3. 返回写入结果
//
// 注意:
//   如果底层 Writer 写入失败，CRC 仍会更新
//   调用者需要根据返回的错误判断是否写入成功
func (c *CRCwriter) Write(p []byte) (n int, err error) {
	n, err = c.w.Write(p)    // 写入底层 Writer
	c.crc = c.crc.Update(p)  // 同时更新 CRC
	return
}

// Sum 获取最终的 CRC32 校验和
//
// 返回值:
//   - uint32: CRC32 校验和（直接使用，不是 crc.Value()）
//
// 用途:
//   在完成所有数据写入后，调用此方法获取最终的 CRC 值
//   通常存储在 Needle Footer 中
//
// 示例:
//   crcWriter := NewCRCwriter(file)
//   crcWriter.Write(data1)
//   crcWriter.Write(data2)
//   checksum := crcWriter.Sum() // 获取最终 CRC
func (c *CRCwriter) Sum() uint32 { return uint32(c.crc) } // 返回最终 CRC 值
