// Package weed_server 实现 WebDAV 文件系统的包装器
// 本文件提供子文件夹访问功能，允许将 WebDAV 文件系统限制在特定子目录
//
// 核心功能:
//   - wrappedFs: WebDAV 文件系统包装器，提供子文件夹访问
//   - wrappedFile: 文件包装器，透明处理路径转换
//   - wrappedFileInfo: 文件信息包装器，剥离子文件夹前缀
//
// 使用场景:
//   - 多租户隔离：为每个用户提供独立的 WebDAV 访问空间
//   - 权限控制：限制用户只能访问特定子目录
//   - 虚拟路径：将物理路径 /data/user123 映射为 WebDAV 根目录
//   - 简化客户端：客户端看到的路径更简洁（不包含子文件夹前缀）
//
// 工作原理:
//   1. 客户端请求：/myfile.txt
//   2. wrappedFs 转换：/subfolder/myfile.txt
//   3. 底层文件系统处理：读取实际文件
//   4. wrappedFileInfo 转换：文件名显示为 myfile.txt（剥离前缀）
//
// 路径转换示例:
//   - subFolder = "/users/alice"
//   - 客户端路径：/documents/report.pdf
//   - 实际路径：/users/alice/documents/report.pdf
//   - 返回名称：documents/report.pdf（不包含 /users/alice）
//
// 透明性保证:
//   - 客户端无感知：完全不知道子文件夹的存在
//   - 路径一致性：所有操作（读、写、删除、重命名）都使用相对路径
//   - 安全隔离：无法访问子文件夹外的内容
//
// 注意事项:
//   - subFolder 应以 / 开头和结尾（如 "/users/alice/"）
//   - 所有路径操作都会自动添加 subFolder 前缀
//   - FileInfo.Name() 会自动剥离 subFolder 前缀
package weed_server

import (
	"context"
	"golang.org/x/net/webdav"
	"io/fs"
	"os"
	"strings"
)

// wrappedFs WebDAV 文件系统包装器，限制访问到特定子文件夹
// 所有文件操作都会自动添加 subFolder 前缀
//
// 字段说明:
//   - subFolder: 子文件夹路径（如 "/users/alice/"）
//   - FileSystem: 底层 WebDAV 文件系统
//
// 示例:
//   baseFs := NewWebDavFileSystem(...)
//   wrappedFs := NewWrappedFs(baseFs, "/users/alice/")
//   // 客户端访问 /file.txt，实际访问 /users/alice/file.txt
type wrappedFs struct {
	subFolder string            // 子文件夹路径前缀
	webdav.FileSystem            // 底层文件系统（嵌入）
}

// NewWrappedFs 创建一个 WebDAV 文件系统包装器，提供子文件夹访问
// 返回的文件系统与原文件系统功能相同，但所有路径都相对于 subFolder
//
// 参数:
//   - fs: 底层 WebDAV 文件系统
//   - subFolder: 子文件夹路径（如 "/users/alice/"）
//
// 返回:
//   - webdav.FileSystem: 包装后的文件系统
//
// 功能特性:
//   - 路径转换：自动在所有路径前添加 subFolder
//   - 名称剥离：FileInfo.Name() 自动移除 subFolder 前缀
//   - 完全透明：客户端完全不知道子文件夹的存在
//
// 使用示例（多租户隔离）:
//   // 为用户 alice 创建独立的 WebDAV 空间
//   baseFs := NewWebDavFileSystem(filerAddress, "webdav", "000")
//   aliceFs := NewWrappedFs(baseFs, "/users/alice/")
//   // alice 看到的根目录实际是 /users/alice/
//
//   // 为用户 bob 创建独立的 WebDAV 空间
//   bobFs := NewWrappedFs(baseFs, "/users/bob/")
//   // bob 看到的根目录实际是 /users/bob/
//
// 使用示例（权限控制）:
//   // 限制用户只能访问 /public/ 目录
//   publicFs := NewWrappedFs(baseFs, "/public/")
//   // 用户无法访问 /public/ 之外的文件
func NewWrappedFs(fs webdav.FileSystem, subFolder string) webdav.FileSystem {
	return wrappedFs{
		subFolder:  subFolder,
		FileSystem: fs,
	}
}

// Mkdir 创建目录，自动添加 subFolder 前缀
//
// 参数:
//   - ctx: 上下文
//   - name: 客户端提供的相对路径（如 "/documents"）
//   - perm: 目录权限
//
// 返回:
//   - error: 创建错误
//
// 路径转换:
//   客户端请求：Mkdir("/documents")
//   实际调用：底层fs.Mkdir("/users/alice/documents")
func (w wrappedFs) Mkdir(ctx context.Context, name string, perm os.FileMode) error {
	// 【添加子文件夹前缀】
	name = w.subFolder + name
	// 【调用底层文件系统】
	return w.FileSystem.Mkdir(ctx, name, perm)
}

// OpenFile 打开文件，自动添加 subFolder 前缀，并包装返回的文件对象
//
// 参数:
//   - ctx: 上下文
//   - name: 客户端提供的相对路径（如 "/report.pdf"）
//   - flag: 打开标志（os.O_RDONLY、os.O_WRONLY 等）
//   - perm: 文件权限（创建文件时使用）
//
// 返回:
//   - webdav.File: 包装后的文件对象
//   - error: 打开错误
//
// 路径转换:
//   客户端请求：OpenFile("/report.pdf")
//   实际调用：底层fs.OpenFile("/users/alice/report.pdf")
func (w wrappedFs) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (webdav.File, error) {
	// 【添加子文件夹前缀】
	name = w.subFolder + name
	// 【调用底层文件系统打开文件】
	file, err := w.FileSystem.OpenFile(ctx, name, flag, perm)
	// 【包装文件对象】
	// 包装后的文件对象会在 Readdir 和 Stat 中剥离路径前缀
	file = wrappedFile{
		File:      file,
		subFolder: &w.subFolder,
	}

	return file, err
}

// RemoveAll 删除文件或目录（递归），自动添加 subFolder 前缀
//
// 参数:
//   - ctx: 上下文
//   - name: 客户端提供的相对路径（如 "/old_folder"）
//
// 返回:
//   - error: 删除错误
//
// 路径转换:
//   客户端请求：RemoveAll("/old_folder")
//   实际调用：底层fs.RemoveAll("/users/alice/old_folder")
func (w wrappedFs) RemoveAll(ctx context.Context, name string) error {
	// 【添加子文件夹前缀】
	name = w.subFolder + name
	// 【调用底层文件系统】
	return w.FileSystem.RemoveAll(ctx, name)
}

// Rename 重命名文件或目录，自动为新旧路径添加 subFolder 前缀
//
// 参数:
//   - ctx: 上下文
//   - oldName: 客户端提供的旧路径（如 "/old.txt"）
//   - newName: 客户端提供的新路径（如 "/new.txt"）
//
// 返回:
//   - error: 重命名错误
//
// 路径转换:
//   客户端请求：Rename("/old.txt", "/new.txt")
//   实际调用：底层fs.Rename("/users/alice/old.txt", "/users/alice/new.txt")
func (w wrappedFs) Rename(ctx context.Context, oldName, newName string) error {
	// 【为新旧路径添加前缀】
	oldName = w.subFolder + oldName
	newName = w.subFolder + newName
	// 【调用底层文件系统】
	return w.FileSystem.Rename(ctx, oldName, newName)
}

// Stat 获取文件信息，自动添加 subFolder 前缀，并包装返回的 FileInfo
//
// 参数:
//   - ctx: 上下文
//   - name: 客户端提供的相对路径（如 "/file.txt"）
//
// 返回:
//   - os.FileInfo: 包装后的文件信息（Name() 会剥离前缀）
//   - error: 获取错误
//
// 路径转换:
//   客户端请求：Stat("/file.txt")
//   实际调用：底层fs.Stat("/users/alice/file.txt")
//   返回名称：file.txt（剥离 /users/alice/ 前缀）
func (w wrappedFs) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	// 【添加子文件夹前缀】
	name = w.subFolder + name
	// 【调用底层文件系统】
	info, err := w.FileSystem.Stat(ctx, name)
	// 【包装文件信息】
	// 包装后的 FileInfo 会在 Name() 中剥离路径前缀
	info = wrappedFileInfo{
		subFolder: &w.subFolder,
		FileInfo:  info,
	}
	return info, err
}

// wrappedFile 文件包装器，在 Readdir 和 Stat 中剥离路径前缀
//
// 字段说明:
//   - File: 底层 WebDAV 文件对象
//   - subFolder: 子文件夹路径指针（用于剥离前缀）
type wrappedFile struct {
	webdav.File              // 嵌入底层文件对象
	subFolder *string        // 子文件夹路径前缀
}

// Readdir 读取目录内容，自动剥离所有文件名的 subFolder 前缀
//
// 参数:
//   - count: 读取的条目数（-1 表示全部）
//
// 返回:
//   - []fs.FileInfo: 文件信息列表（Name() 已剥离前缀）
//   - error: 读取错误
//
// 名称转换:
//   底层返回：/users/alice/documents/report.pdf
//   客户端看到：documents/report.pdf
func (w wrappedFile) Readdir(count int) ([]fs.FileInfo, error) {
	// 【调用底层文件的 Readdir】
	infos, err := w.File.Readdir(count)

	// 【包装所有文件信息】
	// 确保每个文件名都剥离了 subFolder 前缀
	for i, info := range infos {
		infos[i] = wrappedFileInfo{
			subFolder: w.subFolder,
			FileInfo:  info,
		}
	}

	return infos, err
}

// Stat 获取文件自身的信息，自动剥离文件名的 subFolder 前缀
//
// 返回:
//   - fs.FileInfo: 包装后的文件信息（Name() 已剥离前缀）
//   - error: 获取错误
func (w wrappedFile) Stat() (fs.FileInfo, error) {
	// 【调用底层文件的 Stat】
	info, err := w.File.Stat()

	// 【包装文件信息】
	info = wrappedFileInfo{
		subFolder: w.subFolder,
		FileInfo:  info,
	}

	return info, err
}

// wrappedFileInfo 文件信息包装器，在 Name() 中剥离路径前缀
//
// 字段说明:
//   - subFolder: 子文件夹路径指针
//   - FileInfo: 底层文件信息对象
type wrappedFileInfo struct {
	subFolder *string      // 子文件夹路径前缀
	fs.FileInfo              // 嵌入底层文件信息
}

// Name 返回文件名，自动剥离 subFolder 前缀
//
// 返回:
//   - string: 剥离前缀后的文件名
//
// 名称转换:
//   底层返回：/users/alice/documents/report.pdf
//   客户端看到：documents/report.pdf
func (w wrappedFileInfo) Name() string {
	// 【获取底层文件名】
	name := w.FileInfo.Name()

	// 【剥离子文件夹前缀】
	// strings.TrimPrefix 移除名称中的 subFolder 部分
	return strings.TrimPrefix(name, *w.subFolder)
}

// ETag 返回文件的 ETag（用于缓存验证）
// 实现 webdav.ETager 接口
//
// 参数:
//   - ctx: 上下文
//
// 返回:
//   - string: ETag 值（如 "abc123def456"）
//   - error: 获取错误或 webdav.ErrNotImplemented
//
// ETag 用途:
//   - HTTP 缓存验证（If-None-Match 请求头）
//   - 并发控制（防止同时编辑）
//   - 内容变更检测
func (w wrappedFileInfo) ETag(ctx context.Context) (string, error) {
	// 【尝试从底层 FileInfo 获取 ETag】
	// 类型断言为 webdav.ETager 接口
	etag, _ := w.FileInfo.(webdav.ETager).ETag(ctx)

	if len(etag) == 0 {
		// 【ETag 不可用】
		return etag, webdav.ErrNotImplemented
	}

	// 【返回 ETag】
	return etag, nil
}
