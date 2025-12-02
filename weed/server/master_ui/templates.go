// Package master_ui 实现了 Master Server 的 Web UI 模板渲染功能
// 提供集群状态、拓扑结构、Volume 分布等信息的 Web 展示界面
package master_ui

import (
	_ "embed"  // 使用 embed 指令嵌入 HTML 文件到二进制中
	"html/template"
	"strings"
)

//go:embed master.html
// masterHtml 是嵌入的标准 Master UI HTML 模板内容
// 显示传统的 Master 集群信息（基于 Goraft 的旧版 Raft 实现）
var masterHtml string

//go:embed masterNewRaft.html
// masterNewRaftHtml 是嵌入的新版 Raft Master UI HTML 模板内容
// 显示基于 hashicorp/raft 的新版 Raft 实现的集群信息
// 新版 Raft 提供更好的性能和稳定性
var masterNewRaftHtml string

// templateFunctions 定义了 HTML 模板中可用的辅助函数
// 这些函数可以在模板中通过 {{ funcName args }} 调用
var templateFunctions = template.FuncMap{
	// url: 确保 URL 包含协议前缀（http:// 或 https://）
	// 用于在 UI 中显示可点击的链接
	//
	// 转换逻辑：
	//   - 如果 URL 已有 http:// 或 https:// 前缀，直接返回
	//   - 否则自动添加 http:// 前缀
	//
	// 示例:
	//   url("localhost:9333")           => "http://localhost:9333"
	//   url("http://localhost:9333")    => "http://localhost:9333"（不变）
	//   url("https://master.example")   => "https://master.example"（不变）
	"url": func(input string) string {
		// 检查是否已经包含协议前缀
		if !strings.HasPrefix(input, "http://") && !strings.HasPrefix(input, "https://") {
			// 没有协议前缀，添加 http://
			return "http://" + input
		}

		// 已有协议前缀，直接返回
		return input
	},
}

// StatusTpl 是编译后的传统 Master Web UI 模板
// 用于渲染基于 Goraft 的 Master 集群状态页面
// 模板名称为 "status"，包含了 templateFunctions 中定义的辅助函数
//
// 使用场景：
//   - 显示 Master Leader 和 Peer 信息
//   - 显示 Volume 分配情况
//   - 显示拓扑结构（DataCenter -> Rack -> Node -> Volume）
//   - 显示集群容量统计
//
// 使用方式：
//   err := StatusTpl.Execute(w, data)
//
// 如果模板解析失败，template.Must 会触发 panic
var StatusTpl = template.Must(template.New("status").Funcs(templateFunctions).Parse(masterHtml))

// StatusNewRaftTpl 是编译后的新版 Raft Master Web UI 模板
// 用于渲染基于 hashicorp/raft 的 Master 集群状态页面
// 模板名称为 "status"，不包含额外的辅助函数（使用内置函数即可）
//
// 使用场景：
//   - 显示新版 Raft 的 Leader 和 Follower 信息
//   - 显示 Raft 日志索引和任期（Term）
//   - 显示集群成员状态
//   - 显示 Volume 分配和拓扑信息
//
// 新版 Raft 相比旧版的改进：
//   - 更好的性能和稳定性
//   - 更完善的日志压缩
//   - 更健壮的故障恢复
//   - 更清晰的状态机设计
//
// 使用方式：
//   err := StatusNewRaftTpl.Execute(w, data)
//
// 如果模板解析失败，template.Must 会触发 panic
var StatusNewRaftTpl = template.Must(template.New("status").Parse(masterNewRaftHtml))
