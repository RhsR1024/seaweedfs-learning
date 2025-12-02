// Package filer_ui 实现了 Filer 服务器的 Web UI 相关功能
// 包含面包屑导航、模板渲染等前端展示组件
package filer_ui

import (
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

// Breadcrumb 表示面包屑导航中的一个节点
// 用于在 Web UI 中显示当前路径的层级导航
// 例如：对于路径 /a/b/c，会生成三个节点：/ -> a -> b -> c
type Breadcrumb struct {
	Name string // 显示名称（目录名或 "/"）
	Link string // 链接地址（完整路径）
}

// ToBreadcrumb 将完整文件路径转换为面包屑导航数组
// 这个函数用于在 Filer Web UI 中生成路径导航条
//
// 转换逻辑：
//   - 根目录 "/" 显示为单个 "/" 节点
//   - 子路径按 "/" 分割，每个部分生成一个导航节点
//   - 每个节点的 Link 是从根到该节点的完整路径
//
// 参数:
//   - fullpath: 完整文件路径，如 "/abc/def" 或 "/"
//
// 返回:
//   - crumbs: 面包屑导航数组
//
// 示例:
//   ToBreadcrumb("/")           => [{Name: "/", Link: "/"}]
//   ToBreadcrumb("/abc")        => [{Name: "/", Link: "/"}, {Name: "abc", Link: "/abc/"}]
//   ToBreadcrumb("/abc/def")    => [{Name: "/", Link: "/"}, {Name: "abc", Link: "/abc/"}, {Name: "def", Link: "/abc/def/"}]
func ToBreadcrumb(fullpath string) (crumbs []Breadcrumb) {
	// 按 "/" 分割路径为多个部分
	parts := strings.Split(fullpath, "/")

	// 特殊处理根目录 "/"
	// 分割后会得到 ["", ""]，需要转换为 [""]
	if fullpath == "/" {
		parts = []string{""}
	}

	// 遍历路径的每个部分，生成对应的面包屑节点
	for i := 0; i < len(parts); i++ {
		name := parts[i]

		// 空字符串表示根目录，显示为 "/"
		if name == "" {
			name = "/"
		}

		// 构建面包屑节点
		crumb := Breadcrumb{
			Name: name,
			// Link 是从根到当前节点的完整路径
			// util.Join 会用 "/" 连接所有部分
			Link: "/" + util.Join(parts[0:i+1]...),
		}

		// 确保所有链接以 "/" 结尾（目录路径约定）
		if !strings.HasSuffix(crumb.Link, "/") {
			crumb.Link += "/"
		}

		crumbs = append(crumbs, crumb)
	}

	return
}
