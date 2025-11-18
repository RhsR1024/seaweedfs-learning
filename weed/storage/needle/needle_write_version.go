// Package needle 实现 Needle 的版本化写入逻辑
// 根据不同的 Volume 版本选择相应的序列化格式
package needle

import (
	"bytes"
	"fmt"

	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// writeNeedleByVersion 根据 Volume 版本选择相应的 Needle 序列化方法
// 这是一个路由函数，将写入请求分发到特定版本的实现
//
// 参数:
//   - version: Volume 版本号（Version1/Version2/Version3）
//   - n: 要序列化的 Needle 对象
//   - offset: 写入偏移量（用于某些版本计算校验和）
//   - bytesBuffer: 输出缓冲区
//
// 返回值:
//   - size: Needle 的逻辑大小（数据部分，不含 padding）
//   - actualSize: 实际序列化后的总字节数（含 header/checksum/padding）
//   - err: 错误信息
//
// 版本差异:
//   - Version1: 基础格式（header + data + checksum + padding）
//   - Version2: 添加了 Last-Modified 时间戳支持
//   - Version3: 添加了 AppendAtNs 追加时间戳，支持更精细的时间控制
//
// 工作流程:
//  1. 根据 version 参数选择对应的写入函数
//  2. 调用版本特定的序列化逻辑
//  3. 将序列化后的数据写入 bytesBuffer
//
// 注意:
//   - 不支持的版本号会返回错误
//   - 各版本格式必须向后兼容读取
//   - bytesBuffer 会被重置并填充序列化数据
func writeNeedleByVersion(version Version, n *Needle, offset uint64, bytesBuffer *bytes.Buffer) (size Size, actualSize int64, err error) {
	// 根据版本路由到对应的序列化函数
	switch version {
	case Version1:
		// Version1: 最初版本，基础格式
		size, actualSize, err = writeNeedleV1(n, offset, bytesBuffer)
	case Version2:
		// Version2: 添加 Last-Modified 时间戳
		size, actualSize, err = writeNeedleV2(n, offset, bytesBuffer)
	case Version3:
		// Version3: 添加 AppendAtNs 追加时间戳
		size, actualSize, err = writeNeedleV3(n, offset, bytesBuffer)
	default:
		// 不支持的版本号
		err = fmt.Errorf("unsupported version: %d", version)
	}
	return
}
