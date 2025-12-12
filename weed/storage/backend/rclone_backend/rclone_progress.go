//go:build rclone
// +build rclone

// Package rclone_backend 实现基于 Rclone 的远程存储后端
//
// Rclone 是一个强大的云存储同步工具，支持 70+ 种云存储服务（如 S3、Google Drive、Azure、Dropbox 等）
// 本包将 Rclone 集成到 SeaweedFS 中，提供统一的远程存储抽象层
//
// 核心功能：
//   - 文件传输进度跟踪（ProgressReader）
//   - 上传/下载进度回调
//   - 支持所有 Rclone 支持的云存储服务
//
// 使用方式：
//   在编译时添加 -tags "rclone" 标签启用此功能
//   配置示例：
//     backend = "rclone"
//     remote_name = "myremote"
//     key_template = "seaweedfs/{{ . }}"
package rclone_backend

import "github.com/rclone/rclone/fs/accounting"

// ProgressReader 实现带进度跟踪的读取器
//
// 通过包装 Rclone 的 accounting 模块，在文件传输过程中实时上报进度信息
//
// 字段说明：
//   - acc: Rclone 的计数器，用于统计实际读取的字节数
//   - tr: Rclone 的传输对象，包含文件大小、已传输字节等元数据
//   - fn: 进度回调函数，接收已传输字节数和百分比
//
// 典型用法：
//   pr := &ProgressReader{
//       acc: transfer.Account(ctx, file),
//       tr:  transfer,
//       fn:  func(bytes int64, pct float32) error {
//           fmt.Printf("进度: %d 字节 (%.2f%%)\n", bytes, pct)
//           return nil
//       },
//   }
type ProgressReader struct {
	acc *accounting.Account // Rclone 计数器，跟踪读取的字节数
	tr  *accounting.Transfer // Rclone 传输对象，包含文件大小等元数据
	fn  func(progressed int64, percentage float32) error // 进度回调函数
}

// Read 实现 io.Reader 接口，读取数据并上报进度
//
// 参数:
//   - p: 目标缓冲区，读取的数据将写入此缓冲区
// 返回:
//   - n: 实际读取的字节数
//   - err: 读取错误（如果有）
//
// 实现细节：
//   1. 通过 acc.Read(p) 读取数据并自动统计字节数
//   2. 从 tr.Snapshot() 获取当前传输状态（已传输字节、总大小）
//   3. 计算百分比并调用回调函数上报进度
//
// 百分比计算公式：
//   percentage = (已传输字节数 / 文件总大小) × 100
//
// 注意事项：
//   - 如果回调函数返回错误，Read 会立即停止并返回该错误
//   - 适用于上传和下载两种场景
func (pr *ProgressReader) Read(p []byte) (n int, err error) {
	// 从底层读取器读取数据，acc 会自动记录读取的字节数
	n, err = pr.acc.Read(p)
	if err != nil {
		return
	}

	// 获取当前传输快照（包含已传输字节数、文件总大小等）
	snap := pr.tr.Snapshot()

	// 调用进度回调函数
	// snap.Bytes: 已传输的字节数
	// snap.Size: 文件总大小
	// 百分比 = (已传输 / 总大小) × 100
	err = pr.fn(snap.Bytes, 100*float32(snap.Bytes)/float32(snap.Size))
	return
}
