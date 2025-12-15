// Package util 提供 SeaweedFS 通用工具函数
// 本文件实现了 FullPath 类型，用于统一处理文件系统路径操作
//
// FullPath 是 SeaweedFS Filer 层的核心类型之一，用于：
//   - 表示文件或目录的完整路径（从根目录 "/" 开始）
//   - 提供路径解析、拼接、分割等操作
//   - 生成 inode 号用于 FUSE 挂载
//   - 支持路径层级判断（父子关系）
package util

import (
	"path/filepath"
	"strings"
)

// FullPath 表示一个完整的文件系统路径
// 基于 string 类型，但提供了丰富的路径操作方法
//
// 路径格式规范：
//   - 必须以 "/" 开头（根目录）
//   - 目录路径不以 "/" 结尾（除了根目录本身）
//   - 使用 "/" 作为路径分隔符（跨平台统一）
//
// 示例：
//   - "/" - 根目录
//   - "/documents" - 一级目录
//   - "/documents/file.txt" - 文件路径
//   - "/a/b/c/d" - 多级目录路径
type FullPath string

// NewFullPath 根据目录路径和名称创建新的 FullPath
// 这是创建 FullPath 的推荐方式，会自动处理路径拼接
//
// 参数：
//   - dir: 父目录路径，如 "/documents"
//   - name: 文件或目录名称，如 "file.txt"
//
// 返回：
//   - 拼接后的完整路径，如 "/documents/file.txt"
//
// 特殊处理：
//   - 自动移除 name 末尾的 "/"（目录路径规范化）
//   - 使用 Child 方法处理路径拼接，避免重复的 "/"
//
// 示例：
//
//	NewFullPath("/a", "b")      -> "/a/b"
//	NewFullPath("/a/", "b")     -> "/a/b"
//	NewFullPath("/a", "b/")     -> "/a/b"
//	NewFullPath("/a", "/b")     -> "/a/b"
func NewFullPath(dir, name string) FullPath {
	// 移除名称末尾的 "/"，确保路径规范化
	// 例如："folder/" -> "folder"
	name = strings.TrimSuffix(name, "/")
	// 调用 Child 方法进行路径拼接
	return FullPath(dir).Child(name)
}

// DirAndName 将路径分割为目录部分和名称部分
// 类似于 filepath.Split，但做了额外的处理：
//   - 目录部分不包含末尾的 "/"（除非是根目录）
//   - 名称部分会进行 UTF-8 有效性检查
//
// 返回值：
//   - dir: 父目录路径
//   - name: 文件或目录名称
//
// 示例：
//
//	"/a/b/c".DirAndName()    -> ("/a/b", "c")
//	"/a/b".DirAndName()      -> ("/a", "b")
//	"/a".DirAndName()        -> ("/", "a")
//	"/".DirAndName()         -> ("/", "")
//	"".DirAndName()          -> ("/", "")
//
// UTF-8 处理：
//   - 如果名称包含无效的 UTF-8 字节，会用 "?" 替换
//   - 这保证了返回的名称始终是有效的 UTF-8 字符串
func (fp FullPath) DirAndName() (string, string) {
	// 使用标准库分割路径
	// filepath.Split("/a/b/c") -> ("/a/b/", "c")
	dir, name := filepath.Split(string(fp))

	// 将无效的 UTF-8 字节替换为 "?"
	// 防止文件系统中的非法字符导致问题
	name = strings.ToValidUTF8(name, "?")

	// 特殊情况：根目录
	// filepath.Split("/") 返回 ("/", "")
	if dir == "/" {
		return dir, name
	}

	// 特殊情况：空路径或无目录部分
	// 返回根目录作为默认目录
	if len(dir) < 1 {
		return "/", ""
	}

	// 移除目录末尾的 "/"
	// filepath.Split 返回的目录带末尾 "/"，如 "/a/b/"
	// 我们需要 "/a/b" 格式
	return dir[:len(dir)-1], name
}

// Name 返回路径中的文件或目录名称部分
// 只返回最后一个路径组件，不包含父目录
//
// 示例：
//
//	"/a/b/c".Name()      -> "c"
//	"/a/file.txt".Name() -> "file.txt"
//	"/a".Name()          -> "a"
//	"/".Name()           -> ""
//
// UTF-8 处理：
//   - 无效的 UTF-8 字节会被替换为 "?"
func (fp FullPath) Name() string {
	// 使用标准库提取名称部分
	_, name := filepath.Split(string(fp))

	// 确保返回有效的 UTF-8 字符串
	name = strings.ToValidUTF8(name, "?")
	return name
}

// IsLongerFileName 检查文件名长度是否超过指定限制
// 用于验证文件系统的文件名长度约束
//
// 参数：
//   - maxFilenameLength: 允许的最大文件名长度（字节数）
//     如果为 0，表示不限制长度
//
// 返回：
//   - true: 文件名超过长度限制
//   - false: 文件名在限制范围内，或无限制
//
// 注意：
//   - 使用字节长度而非字符数（UTF-8 字符可能占多个字节）
//   - 不同文件系统有不同的限制，如 ext4 是 255 字节
//
// 示例：
//
//	"/path/to/file.txt".IsLongerFileName(255)  -> false（8 字节 < 255）
//	"/path/to/very_long_name...".IsLongerFileName(10) -> true
//	"/path/to/file.txt".IsLongerFileName(0)    -> false（0 表示无限制）
func (fp FullPath) IsLongerFileName(maxFilenameLength uint32) bool {
	// maxFilenameLength 为 0 表示不限制长度
	if maxFilenameLength == 0 {
		return false
	}
	// 将文件名转换为字节切片，计算实际字节长度
	// 这对于包含中文等多字节字符的文件名很重要
	return uint32(len([]byte(fp.Name()))) > maxFilenameLength
}

// Child 在当前路径下添加子路径，返回新的完整路径
// 这是路径拼接的核心方法，会自动处理各种边界情况
//
// 参数：
//   - name: 要添加的子路径名称，可以带或不带前导 "/"
//
// 返回：
//   - 拼接后的新 FullPath
//
// 特殊处理：
//   - 自动移除 name 的前导 "/"，避免变成绝对路径
//   - 自动处理父路径末尾的 "/"，避免出现 "//"
//
// 示例：
//
//	FullPath("/a").Child("b")      -> "/a/b"
//	FullPath("/a/").Child("b")     -> "/a/b"
//	FullPath("/a").Child("/b")     -> "/a/b"（移除前导 /）
//	FullPath("/a/").Child("/b")    -> "/a/b"
//	FullPath("/").Child("a")       -> "/a"
//	FullPath("").Child("a")        -> "/a"
func (fp FullPath) Child(name string) FullPath {
	dir := string(fp)

	// 移除子路径的前导 "/"
	// 这确保了 "/a".Child("/b") 得到 "/a/b" 而不是 "//b"
	noPrefix := name
	if strings.HasPrefix(name, "/") {
		noPrefix = name[1:]
	}

	// 检查父路径是否已经以 "/" 结尾
	// 如果是，直接拼接；否则添加 "/" 分隔符
	if strings.HasSuffix(dir, "/") {
		// 父路径已有末尾 "/"，如 "/a/" + "b" -> "/a/b"
		return FullPath(dir + noPrefix)
	}
	// 父路径无末尾 "/"，需要添加分隔符，如 "/a" + "/" + "b" -> "/a/b"
	return FullPath(dir + "/" + noPrefix)
}

// AsInode 根据路径生成一个内存中的 inode 号
// 用于 FUSE 挂载时为文件/目录分配唯一标识符
//
// 参数：
//   - unixTime: Unix 时间戳（通常是文件的修改时间）
//     用于增加 inode 的唯一性
//
// 返回：
//   - uint64 类型的 inode 号
//
// 算法说明：
//
//  1. 首先对路径字符串进行哈希，得到基础 inode 值
//  2. 然后混入时间戳（乘以素数 37）增加唯一性
//     使用素数 37 是为了减少哈希碰撞的概率
//
// 注意：
//   - 这个 inode 号仅在内存中使用，不持久化存储
//   - 相同路径和时间戳会生成相同的 inode
//   - 主要用于 FUSE 文件系统接口
//
// 示例：
//
//	"/a/b/c".AsInode(1234567890) -> 某个 uint64 值
func (fp FullPath) AsInode(unixTime int64) uint64 {
	// 对路径进行哈希，生成基础 inode 值
	inode := uint64(HashStringToLong(string(fp)))

	// 混入时间戳，使用素数 37 作为乘数
	// 这样相同路径但不同时间的文件会有不同的 inode
	inode = inode + uint64(unixTime)*37
	return inode
}

// Split 将路径分割为各个组件（目录和文件名）
// 返回从根目录开始的路径组件列表，不包含根目录本身
//
// 返回：
//   - []string: 路径组件切片
//   - 空路径或根目录返回空切片
//
// 示例：
//
//	"/a/b/c".Split()     -> ["a", "b", "c"]
//	"/a".Split()         -> ["a"]
//	"/a/b/".Split()      -> ["a", "b", ""]（注意末尾空字符串）
//	"/".Split()          -> []
//	"".Split()           -> []
//
// 注意：
//   - 跳过开头的 "/"，所以 "/a/b" 分割为 ["a", "b"] 而不是 ["", "a", "b"]
//   - 如果路径以 "/" 结尾，会产生一个空字符串元素
func (fp FullPath) Split() []string {
	// 空路径或根目录，返回空切片
	if fp == "" || fp == "/" {
		return []string{}
	}
	// 跳过开头的 "/" (即 string(fp)[1:])，然后按 "/" 分割
	// 例如："/a/b/c"[1:] = "a/b/c" -> ["a", "b", "c"]
	return strings.Split(string(fp)[1:], "/")
}

// Join 将多个路径组件拼接成单个路径字符串
// 这是一个工具函数，封装了 filepath.Join 并统一使用正斜杠
//
// 参数：
//   - names: 可变参数，一个或多个路径组件
//
// 返回：
//   - 拼接后的路径字符串，使用 "/" 作为分隔符
//
// 特点：
//   - 使用 filepath.Join 处理路径清理（如 ".." 和 "."）
//   - 使用 filepath.ToSlash 确保跨平台一致性（Windows 上 \ -> /）
//
// 示例：
//
//	Join("a", "b", "c")      -> "a/b/c"
//	Join("/a", "b")          -> "/a/b"
//	Join("a", "../b")        -> "b"（处理 ..）
//	Join("a//b", "c")        -> "a/b/c"（清理多余斜杠）
func Join(names ...string) string {
	// filepath.Join 会清理路径（处理 .. 和 .，移除多余斜杠）
	// filepath.ToSlash 将反斜杠转换为正斜杠（Windows 兼容）
	return filepath.ToSlash(filepath.Join(names...))
}

// JoinPath 将多个路径组件拼接成 FullPath
// 是 Join 函数的 FullPath 返回值版本
//
// 参数：
//   - names: 可变参数，一个或多个路径组件
//
// 返回：
//   - FullPath 类型的拼接结果
//
// 示例：
//
//	JoinPath("/", "a", "b")  -> FullPath("/a/b")
//	JoinPath("a", "b", "c")  -> FullPath("a/b/c")
func JoinPath(names ...string) FullPath {
	return FullPath(Join(names...))
}

// IsUnder 检查当前路径是否在另一个路径之下（子路径关系）
// 用于判断目录层级关系，常用于权限检查和路径过滤
//
// 参数：
//   - other: 要比较的父路径
//
// 返回：
//   - true: 当前路径是 other 的子路径
//   - false: 当前路径不是 other 的子路径
//
// 判断逻辑：
//   - 根目录 "/" 是所有路径的父路径（除了自己）
//   - 通过检查前缀来判断父子关系
//   - 必须是严格的子路径（other + "/" 是 fp 的前缀）
//
// 示例：
//
//	"/a/b/c".IsUnder("/a")      -> true
//	"/a/b/c".IsUnder("/a/b")    -> true
//	"/a/b/c".IsUnder("/a/b/c")  -> false（不是子路径，是同一路径）
//	"/a/b/c".IsUnder("/")       -> true（所有路径都在根目录下）
//	"/abc".IsUnder("/a")        -> false（/abc 不是 /a 的子路径）
//	"/a".IsUnder("/ab")         -> false
//
// 注意：
//   - 使用 other+"/" 前缀匹配，避免 "/abc".IsUnder("/a") 误判为 true
func (fp FullPath) IsUnder(other FullPath) bool {
	// 根目录是所有路径的祖先
	if other == "/" {
		return true
	}
	// 检查 fp 是否以 "other/" 开头
	// 例如："/a/b" 以 "/a/" 开头，所以 "/a/b".IsUnder("/a") 为 true
	// 但 "/abc" 不以 "/a/" 开头，所以 "/abc".IsUnder("/a") 为 false
	return strings.HasPrefix(string(fp), string(other)+"/")
}

// StringSplit 按指定分隔符分割字符串
// 这是一个通用工具函数，是 strings.Split 的安全封装
//
// 参数：
//   - separatedValues: 要分割的字符串
//   - sep: 分隔符
//
// 返回：
//   - []string: 分割后的字符串切片
//   - 如果输入为空字符串，返回 nil（而非 [""]）
//
// 与 strings.Split 的区别：
//   - strings.Split("", ",") 返回 [""]（包含一个空字符串的切片）
//   - StringSplit("", ",") 返回 nil（空切片）
//
// 示例：
//
//	StringSplit("a,b,c", ",")   -> ["a", "b", "c"]
//	StringSplit("a", ",")       -> ["a"]
//	StringSplit("", ",")        -> nil
//	StringSplit("a:b:c", ":")   -> ["a", "b", "c"]
//
// 使用场景：
//   - 解析配置中的逗号分隔列表（如存储目录列表）
//   - 处理用户输入的多值参数
func StringSplit(separatedValues string, sep string) []string {
	// 空字符串直接返回 nil，避免返回包含空字符串的切片
	if separatedValues == "" {
		return nil
	}
	return strings.Split(separatedValues, sep)
}
