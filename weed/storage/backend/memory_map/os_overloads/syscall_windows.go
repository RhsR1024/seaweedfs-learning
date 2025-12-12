// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package os_overloads Windows 系统调用封装
//
// 本文件提供了 Windows 平台特定的底层文件操作系统调用。
// 是 file_windows.go 的底层实现部分。
package os_overloads

import (
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

// ========== Windows API 声明 ==========

// CreateFile 是 Windows CreateFileW API 的声明（由 go:sys 指令生成）
//
// 注意：这行注释是 go:sys 指令，Go 工具链会根据它自动生成系统调用代码。
// 实际的函数实现在编译时由 golang.org/x/sys/windows 包生成。
//
// API 文档：https://docs.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-createfilew
//
//sys	CreateFile(name *uint16, access uint32, mode uint32, sa *SecurityAttributes, createmode uint32, attrs uint32, templatefile int32) (handle Handle, err error) [failretval==InvalidHandle] = CreateFileW

// ========== 辅助函数 ==========

// makeInheritSa 创建可继承的安全属性
//
// 返回：
//   - *syscall.SecurityAttributes: 配置为可继承的安全属性结构体
//
// 用途：
//   - 当子进程需要继承父进程的文件句柄时使用
//   - InheritHandle = 1 表示子进程可以继承该句柄
//
// 注意：
//   - 如果不需要继承（如设置了 O_CLOEXEC），则传递 nil
func makeInheritSa() *syscall.SecurityAttributes {
	var sa syscall.SecurityAttributes
	sa.Length = uint32(unsafe.Sizeof(sa)) // 结构体大小（Windows API 要求）
	sa.InheritHandle = 1                  // 允许子进程继承
	return &sa
}

// Open 打开或创建文件（底层实现）
//
// 这是 OpenFile 的底层实现，直接调用 Windows CreateFile API。
//
// 参数：
//   - path: 文件路径（UTF-16 编码）
//   - mode: 打开模式（O_RDONLY, O_WRONLY, O_RDWR, O_CREATE, O_TRUNC 等）
//   - perm: 文件权限（Unix 风格，Windows 上大部分被忽略）
//   - setToTempAndDelete: 是否设置临时文件标志和关闭时删除
//
// 返回：
//   - syscall.Handle: 文件句柄
//   - error: 错误信息
//
// 参数映射（mode -> Windows API）：
//   - O_RDONLY -> GENERIC_READ
//   - O_WRONLY -> GENERIC_WRITE
//   - O_RDWR -> GENERIC_READ | GENERIC_WRITE
//   - O_CREATE -> OPEN_ALWAYS 或 CREATE_NEW
//   - O_TRUNC -> CREATE_ALWAYS 或 TRUNCATE_EXISTING
//   - O_EXCL -> CREATE_NEW
//   - O_APPEND -> FILE_APPEND_DATA
//
// 文件属性：
//   - setToTempAndDelete = false: FILE_ATTRIBUTE_NORMAL
//   - setToTempAndDelete = true: FILE_ATTRIBUTE_TEMPORARY | FILE_FLAG_DELETE_ON_CLOSE
func Open(path string, mode int, perm uint32, setToTempAndDelete bool) (fd syscall.Handle, err error) {
	// 步骤 1: 验证路径非空
	if len(path) == 0 {
		return syscall.InvalidHandle, windows.ERROR_FILE_NOT_FOUND
	}

	// 步骤 2: 转换路径为 UTF-16（Windows API 要求）
	pathp, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return syscall.InvalidHandle, err
	}

	// 步骤 3: 确定访问权限（access）
	var access uint32
	switch mode & (windows.O_RDONLY | windows.O_WRONLY | windows.O_RDWR) {
	case windows.O_RDONLY:
		access = windows.GENERIC_READ // 只读

	case windows.O_WRONLY:
		access = windows.GENERIC_WRITE // 只写

	case windows.O_RDWR:
		access = windows.GENERIC_READ | windows.GENERIC_WRITE // 读写
	}

	// 创建文件时需要写权限
	if mode&windows.O_CREAT != 0 {
		access |= windows.GENERIC_WRITE
	}

	// 追加模式的特殊处理
	if mode&windows.O_APPEND != 0 {
		access &^= windows.GENERIC_WRITE        // 移除 GENERIC_WRITE
		access |= windows.FILE_APPEND_DATA      // 添加 FILE_APPEND_DATA（只能追加）
	}

	// 步骤 4: 设置共享模式（sharemode）
	// 允许其他进程同时读写（SeaweedFS 的 Volume 可能被多个进程访问）
	sharemode := uint32(windows.FILE_SHARE_READ | windows.FILE_SHARE_WRITE)

	// 步骤 5: 确定安全属性（sa）
	var sa *syscall.SecurityAttributes
	// 如果没有设置 O_CLOEXEC，则允许子进程继承
	if mode&windows.O_CLOEXEC == 0 {
		sa = makeInheritSa()
	}

	// 步骤 6: 确定创建模式（createmode）
	// 根据 O_CREAT, O_EXCL, O_TRUNC 标志组合决定
	var createmode uint32
	switch {
	case mode&(windows.O_CREAT|windows.O_EXCL) == (windows.O_CREAT | windows.O_EXCL):
		// O_CREAT | O_EXCL: 文件必须不存在，创建新文件
		createmode = windows.CREATE_NEW

	case mode&(windows.O_CREAT|windows.O_TRUNC) == (windows.O_CREAT | windows.O_TRUNC):
		// O_CREAT | O_TRUNC: 总是创建新文件或截断已存在的文件
		createmode = windows.CREATE_ALWAYS

	case mode&windows.O_CREAT == windows.O_CREAT:
		// O_CREAT: 如果文件不存在则创建，否则打开
		createmode = windows.OPEN_ALWAYS

	case mode&windows.O_TRUNC == windows.O_TRUNC:
		// O_TRUNC: 截断已存在的文件
		createmode = windows.TRUNCATE_EXISTING

	default:
		// 默认：打开已存在的文件
		createmode = windows.OPEN_EXISTING
	}

	// 步骤 7: 调用 Windows CreateFile API
	var h syscall.Handle
	var e error

	if setToTempAndDelete {
		// 临时文件模式：
		// - FILE_ATTRIBUTE_TEMPORARY: 提示系统尽量缓存在内存，减少磁盘 I/O
		// - FILE_FLAG_DELETE_ON_CLOSE: 关闭句柄时自动删除文件
		h, e = syscall.CreateFile(
			pathp,
			access,
			sharemode,
			sa,
			createmode,
			(windows.FILE_ATTRIBUTE_TEMPORARY | FILE_FLAG_DELETE_ON_CLOSE), // 临时 + 自动删除
			0, // 不使用模板文件
		)
	} else {
		// 普通文件模式：
		// - FILE_ATTRIBUTE_NORMAL: 标准文件属性
		h, e = syscall.CreateFile(
			pathp,
			access,
			sharemode,
			sa,
			createmode,
			windows.FILE_ATTRIBUTE_NORMAL, // 普通文件
			0, // 不使用模板文件
		)
	}

	return h, e
}
