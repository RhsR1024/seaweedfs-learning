// Package filer_ui 实现了 Filer Web UI 的模板渲染功能
// 提供文件浏览界面的 HTML 模板和辅助函数
package filer_ui

import (
	_ "embed"  // 使用 embed 指令嵌入 HTML 文件到二进制中
	"github.com/dustin/go-humanize"
	"html/template"
	"net/url"
	"strings"
)

// printpath 生成 URL 安全的路径字符串
// 这个函数用于在 HTML 模板中生成文件/目录的链接
//
// 处理逻辑：
//   1. 拼接所有路径部分
//   2. URL 编码特殊字符（空格、中文等）
//   3. 保留路径分隔符 "/"（不编码）
//
// 参数:
//   - parts: 路径的各个部分，会被拼接成完整路径
//
// 返回:
//   - URL 安全的路径字符串
//
// 示例:
//   printpath("/", "my file.txt")        => "/my%20file.txt"  （空格被编码）
//   printpath("/dir/", "中文文件.txt")   => "/dir/%E4%B8%AD%E6%96%87%E6%96%87%E4%BB%B6.txt"  （中文被编码）
//   printpath("/a", "/b")                => "/a/b"  （斜杠不被编码）
func printpath(parts ...string) string {
	// 拼接所有路径部分为单个字符串
	concat := strings.Join(parts, "")

	// 使用 URL 编码处理特殊字符
	// PathEscape 会编码所有非 ASCII 字符和特殊符号
	escaped := url.PathEscape(concat)

	// 将被编码的斜杠（%2F）恢复为斜杠（/）
	// 这样路径中的 "/" 不会被编码，保持 URL 可读性
	return strings.ReplaceAll(escaped, "%2F", "/")
}

// funcMap 定义了 HTML 模板中可用的辅助函数
// 这些函数可以在 filer.html 模板中通过 {{ funcName args }} 调用
var funcMap = template.FuncMap{
	// humanizeBytes: 将字节数转换为人类可读格式
	// 例如：1024 -> "1.0 KB", 1048576 -> "1.0 MB"
	"humanizeBytes": humanize.Bytes,

	// printpath: 生成 URL 安全的路径
	// 用于生成文件/目录的可点击链接
	"printpath":     printpath,
}

//go:embed filer.html
// filerHtml 是嵌入的 HTML 模板内容
// 使用 go:embed 指令将 filer.html 文件内容编译到二进制中
// 这样部署时不需要单独携带 HTML 文件
var filerHtml string

// StatusTpl 是编译后的 Filer Web UI 模板
// 用于渲染文件浏览界面
// 模板名称为 "status"，包含了 funcMap 中定义的辅助函数
//
// 使用方式：
//   err := StatusTpl.Execute(w, data)
//
// 如果模板解析失败，template.Must 会触发 panic
var StatusTpl = template.Must(template.New("status").Funcs(funcMap).Parse(filerHtml))
