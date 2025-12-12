// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package os_overloads 提供增强的 Windows 文件操作函数
//
// 本包基于 Go 标准库的 os 包源代码，扩展了 Windows 平台的文件操作能力。
// 主要增强功能：
//   - 支持长路径（绕过 Windows 260 字符限制）
//   - 支持临时文件和关闭时删除（FILE_FLAG_DELETE_ON_CLOSE）
//   - 支持 FILE_FLAG_RANDOM_ACCESS 优化
//
// 与标准库 os.OpenFile 的区别：
//   - 自动处理长路径（添加 \\?\ 前缀）
//   - 额外参数 setToTempAndDelete 控制临时文件行为
//   - 更灵活的文件创建选项
package os_overloads

import (
	"os"
	"syscall"

	"golang.org/x/sys/windows"
)

// isAbs 检查路径是否为绝对路径
//
// Windows 绝对路径的两种形式：
//   1. 带盘符：C:\path\file.txt
//   2. UNC 路径：\\server\share\file.txt
//
// 参数：
//   - path: 要检查的路径字符串
//
// 返回：
//   - bool: true 表示绝对路径，false 表示相对路径
//
// 判断逻辑：
//   - 先提取卷标（盘符或 UNC 前缀）
//   - 检查卷标后的字符是否为路径分隔符
func isAbs(path string) (b bool) {
	// 提取卷标部分（如 "C:" 或 "\\server\share"）
	v := volumeName(path)
	if v == "" {
		return false // 没有卷标，不是绝对路径
	}

	// 去掉卷标后的路径部分
	path = path[len(v):]
	if path == "" {
		return false // 只有卷标，没有路径，不是绝对路径
	}

	// 检查卷标后第一个字符是否为路径分隔符（\ 或 /）
	return os.IsPathSeparator(path[0])
}

// volumeName 提取路径中的卷标（盘符或 UNC 前缀）
//
// Windows 卷标的两种形式：
//   1. 盘符：C:, D:, Z: 等
//   2. UNC 路径：\\server\share
//
// 参数：
//   - path: 文件路径字符串
//
// 返回：
//   - string: 卷标字符串，如果不包含卷标则返回空字符串
//
// 示例：
//   volumeName("C:\\path\\file.txt") -> "C:"
//   volumeName("\\\\server\\share\\file.txt") -> "\\\\server\\share"
//   volumeName("relative\\path") -> ""
func volumeName(path string) (v string) {
	// 路径长度必须至少为 2 才可能包含卷标
	if len(path) < 2 {
		return ""
	}

	// 情况 1: 检查是否为盘符形式（如 C:）
	c := path[0]
	if path[1] == ':' &&
		('0' <= c && c <= '9' || 'a' <= c && c <= 'z' ||
			'A' <= c && c <= 'Z') {
		return path[:2] // 返回盘符（如 "C:"）
	}

	// 情况 2: 检查是否为 UNC 路径（如 \\server\share）
	// UNC 路径格式：\\server\share\...
	// - 前两个字符必须是路径分隔符
	// - 第三个字符不能是路径分隔符或点号
	if l := len(path); l >= 5 && os.IsPathSeparator(path[0]) && os.IsPathSeparator(path[1]) &&
		!os.IsPathSeparator(path[2]) && path[2] != '.' {

		// 查找服务器名后的路径分隔符
		for n := 3; n < l-1; n++ {
			if os.IsPathSeparator(path[n]) {
				n++
				// 检查共享名是否有效
				if !os.IsPathSeparator(path[n]) {
					if path[n] == '.' {
						break // 共享名不能以点号开头
					}
					// 查找共享名后的路径分隔符
					for ; n < l; n++ {
						if os.IsPathSeparator(path[n]) {
							break
						}
					}
					return path[:n] // 返回完整的 UNC 前缀
				}
				break
			}
		}
	}

	return "" // 不是有效的卷标格式
}

// fixLongPath 将路径转换为扩展长度形式（绕过 260 字符限制）
//
// Windows 默认的文件路径限制为 260 个字符（MAX_PATH）。
// 通过添加 \\?\ 前缀，可以支持最长 32,767 字符的路径。
//
// 参数：
//   - path: 原始文件路径
//
// 返回：
//   - string: 转换后的路径（可能添加了 \\?\ 前缀）
//
// 转换规则：
//   - 路径 < 248 字符：不转换（保持兼容性）
//   - UNC 路径：不转换（避免复杂的规则）
//   - 相对路径：不转换（扩展形式只支持绝对路径）
//   - 包含 .. 元素：不转换（扩展形式禁用 . 和 .. 解析）
//   - 其他绝对路径：转换为 \\?\C:\path\... 形式
//
// 注意：
//   - 扩展形式禁用 . 和 .. 的解析
//   - 扩展形式将 / 视为普通字符而非路径分隔符
//   - 248 而非 260 的限制是为了预留 8.3 文件名空间（260 - 12 = 248）
//
// 示例：
//   fixLongPath("C:\\short\\path.txt") -> "C:\\short\\path.txt" (不变)
//   fixLongPath("C:\\very\\long\\...\\path.txt") -> "\\\\?\\C:\\very\\long\\...\\path.txt"
//
// 参考：https://msdn.microsoft.com/en-us/library/windows/desktop/aa365247(v=vs.85).aspx#maxpath
func fixLongPath(path string) string {
	// 情况 1: 路径短于 248 字符，不需要转换
	// 248 = MAX_PATH (260) - 12（预留给 8.3 文件名）
	if len(path) < 248 {
		return path
	}

	// 情况 2: UNC 路径（\\server\share），不转换
	// 原因：UNC 转扩展形式的规则较复杂，暂不支持
	if len(path) >= 2 && path[:2] == `\\` {
		return path
	}

	// 情况 3: 相对路径，不转换
	// 扩展形式只支持绝对路径
	if !isAbs(path) {
		return path
	}

	// 步骤 1: 准备扩展形式前缀和缓冲区
	const prefix = `\\?\` // 扩展长度路径前缀

	// 分配缓冲区（预留前缀、原路径和可能的尾部反斜杠）
	pathbuf := make([]byte, len(prefix)+len(path)+len(`\`))
	copy(pathbuf, prefix) // 拷贝前缀 \\?\

	// 步骤 2: 规范化路径（去除 . 元素和多余的分隔符）
	n := len(path)
	r, w := 0, len(prefix) // r: 读取位置, w: 写入位置

	for r < n {
		switch {
		case os.IsPathSeparator(path[r]):
			// 跳过连续的路径分隔符（如 C:\\\\path -> C:\\path）
			r++

		case path[r] == '.' && (r+1 == n || os.IsPathSeparator(path[r+1])):
			// 跳过 . 元素（如 C:\.\path -> C:\path）
			r++

		case r+1 < n && path[r] == '.' && path[r+1] == '.' && (r+2 == n || os.IsPathSeparator(path[r+2])):
			// 发现 .. 元素，不支持转换，返回原路径
			// 原因：扩展形式禁用 .. 解析，需要自己实现复杂的规范化逻辑
			return path

		default:
			// 拷贝路径组件
			pathbuf[w] = '\\' // 添加路径分隔符
			w++
			// 拷贝直到下一个分隔符
			for ; r < n && !os.IsPathSeparator(path[r]); r++ {
				pathbuf[w] = path[r]
				w++
			}
		}
	}

	// 步骤 3: 特殊处理盘符根目录
	// 盘符根目录需要尾部反斜杠（如 \\?\C:\）
	if w == len(`\\?\c:`) {
		pathbuf[w] = '\\'
		w++
	}

	return string(pathbuf[:w])
}

// syscallMode 将 Go 的可移植文件模式转换为 syscall 专用的模式位
//
// 参数：
//   - i: Go 的 os.FileMode
//
// 返回：
//   - uint32: syscall 模式位（Unix 风格）
//
// 转换映射：
//   - os.FileMode.Perm() -> 低 9 位权限（rwxrwxrwx）
//   - os.ModeSetuid -> syscall.S_ISUID
//   - os.ModeSetgid -> syscall.S_ISGID
//   - os.ModeSticky -> syscall.S_ISVTX
//
// 注意：
//   - Windows 不完全支持 Unix 风格权限，会被映射到 ACL
//   - ModeTemporary 是 Plan 9 专用，不转换
func syscallMode(i os.FileMode) (o uint32) {
	// 基础权限位（rwxrwxrwx）
	o |= uint32(i.Perm())

	// Setuid 位（执行时设置用户 ID）
	if i&os.ModeSetuid != 0 {
		o |= syscall.S_ISUID
	}

	// Setgid 位（执行时设置组 ID）
	if i&os.ModeSetgid != 0 {
		o |= syscall.S_ISGID
	}

	// Sticky 位（限制删除权限）
	if i&os.ModeSticky != 0 {
		o |= syscall.S_ISVTX
	}

	// 注意：不映射 ModeTemporary（Plan 9 专用标志）

	return
}

// OpenFile 打开文件（扩展版，支持临时文件和关闭时删除）
//
// 这是 os.OpenFile 的增强版本，提供额外的 Windows 特定功能。
//
// 参数：
//   - name: 文件路径（支持长路径）
//   - flag: 打开标志（os.O_RDONLY, os.O_WRONLY, os.O_CREATE 等）
//   - perm: 文件权限（Unix 风格，如 0644）
//   - setToTempAndDelete: 是否设置为临时文件并在关闭时删除
//
// 返回：
//   - *os.File: 打开的文件对象
//   - error: 错误信息
//
// 特殊功能：
//   - 自动处理长路径（> 248 字符）
//   - setToTempAndDelete = true 时：
//     * 设置 FILE_ATTRIBUTE_TEMPORARY（提示系统尽量缓存在内存）
//     * 设置 FILE_FLAG_DELETE_ON_CLOSE（关闭时自动删除）
//     * 适用于临时文件场景
//
// 使用示例：
//   // 普通文件
//   f, err := OpenFile("data.txt", os.O_RDWR|os.O_CREATE, 0644, false)
//
//   // 临时文件（关闭时自动删除）
//   tmp, err := OpenFile("temp.dat", os.O_RDWR|os.O_CREATE, 0644, true)
//   defer tmp.Close() // 关闭时自动删除文件
func OpenFile(name string, flag int, perm os.FileMode, setToTempAndDelete bool) (file *os.File, err error) {
	// 步骤 1: 打开文件（底层 Windows API）
	// fixLongPath: 处理长路径
	// O_CLOEXEC: 子进程不继承文件描述符
	// syscallMode: 转换文件权限
	r, e := Open(fixLongPath(name), flag|windows.O_CLOEXEC, syscallMode(perm), setToTempAndDelete)
	if e != nil {
		return nil, e
	}

	// 步骤 2: 将 syscall.Handle 包装为 os.File
	return os.NewFile(uintptr(r), name), nil
}
