// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package os_overloads Windows 常量定义
package os_overloads

// FILE_FLAG_DELETE_ON_CLOSE Windows 文件标志常量
//
// 用于 CreateFile API 的 dwFlagsAndAttributes 参数。
// 当设置此标志时，文件会在最后一个句柄关闭时自动删除。
//
// 值：0x04000000
//
// 文档：https://docs.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-createfilew
//
// 使用场景：
//   - 临时文件：不需要手动删除，关闭句柄后自动清理
//   - 原子替换：创建新文件，成功后重命名并删除旧文件
//   - 测试文件：测试结束后自动清理
//
// 注意事项：
//   - 必须所有句柄都关闭才会删除（如果多个进程打开同一文件）
//   - 删除操作可能失败（如文件被锁定、权限不足）
//   - 删除前文件仍然占用磁盘空间
//   - 可以与 FILE_ATTRIBUTE_TEMPORARY 结合使用，优化性能
//
// 示例：
//   h, err := syscall.CreateFile(
//       pathp,
//       access,
//       sharemode,
//       sa,
//       createmode,
//       FILE_FLAG_DELETE_ON_CLOSE, // 关闭时自动删除
//       0,
//   )
//   // 使用文件...
//   syscall.CloseHandle(h) // 文件将被自动删除
const (
	FILE_FLAG_DELETE_ON_CLOSE = 0x04000000
)
