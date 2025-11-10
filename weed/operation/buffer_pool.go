// Package operation 提供 SeaweedFS 的核心操作功能
// 本文件实现了字节缓冲池的封装，用于高效管理内存缓冲区
package operation

import (
	"github.com/valyala/bytebufferpool" // 高性能字节缓冲池库
	"sync/atomic"                       // 原子操作，用于并发安全的计数器
)

// bufferCounter 全局缓冲区计数器
// 用于追踪当前正在使用的缓冲区数量，帮助监控内存使用情况
// 使用 atomic 操作保证并发安全
var bufferCounter int64

// GetBuffer 从缓冲池中获取一个字节缓冲区
// 返回值:
//   - *bytebufferpool.ByteBuffer: 字节缓冲区对象，可以复用以减少内存分配
//
// 工作原理:
//  1. 从全局缓冲池中获取一个缓冲区（可能是新建的，也可能是复用的）
//  2. 使用 defer 在函数返回前原子性地增加计数器
//  3. 计数器用于调试和监控（注释掉的 println 可用于调试）
//
// 使用场景:
//   - 处理文件上传/下载时的数据缓冲
//   - 构建 HTTP 请求/响应内容
//   - 任何需要临时字节缓冲的场景
//
// 注意: 使用完毕后必须调用 PutBuffer 归还缓冲区，避免内存泄漏
func GetBuffer() *bytebufferpool.ByteBuffer {
	defer func() {
		// 原子性增加缓冲区计数器
		// 使用 atomic 而不是简单的 ++ 操作，确保在并发场景下的正确性
		atomic.AddInt64(&bufferCounter, 1)
		// println("+", bufferCounter) // 调试时可启用，查看缓冲区分配情况
	}()
	// 从池中获取缓冲区，如果池为空则创建新缓冲区
	return bytebufferpool.Get()
}

// PutBuffer 将字节缓冲区归还到缓冲池中
// 参数:
//   - buf: 要归还的字节缓冲区
//
// 工作原理:
//  1. 将缓冲区归还到全局缓冲池中，供后续复用
//  2. 使用 defer 在函数返回前原子性地减少计数器
//  3. 缓冲池会自动清理缓冲区内容，准备下次复用
//
// 使用场景:
//   - 配合 GetBuffer 使用，形成"获取-使用-归还"的模式
//   - 通常在 defer 语句中调用，确保缓冲区一定会被归还
//
// 示例:
//   buf := GetBuffer()
//   defer PutBuffer(buf)
//   // ... 使用 buf 进行操作 ...
//
// 注意: 归还后不应再使用该缓冲区，因为它可能被其他 goroutine 复用
func PutBuffer(buf *bytebufferpool.ByteBuffer) {
	defer func() {
		// 原子性减少缓冲区计数器
		// 使用 atomic 确保并发安全
		atomic.AddInt64(&bufferCounter, -1)
		// println("-", bufferCounter) // 调试时可启用，查看缓冲区释放情况
	}()
	// 将缓冲区归还到池中，缓冲区会被重置并等待下次复用
	bytebufferpool.Put(buf)
}
