// Package volume_server_ui 提供 Volume Server 的 Web UI 模板和渲染功能
// 包含用于显示卷状态、磁盘使用情况等信息的 HTML 模板和辅助函数
package volume_server_ui

import (
	_ "embed" // 使用 go:embed 指令嵌入静态文件
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"html/template"
	"strconv"
	"strings"
)

// percentFrom 计算百分比
// 用于 Web UI 中显示磁盘使用率、卷容量占用率等信息
//
// 参数:
//   - total: 总数（如磁盘总容量）
//   - part_of: 部分数量（如已使用容量）
//
// 返回值:
//   - string: 百分比字符串，保留两位小数（如 "75.32"）
//
// 使用示例:
//   percentFrom(1000, 750) -> "75.00"
//   percentFrom(10240, 5120) -> "50.00"
//
// 注意:
//   - 返回的字符串不包含 "%" 符号，需要在模板中自行添加
//   - 如果 total 为 0，会产生 NaN 或 Inf，调用前需确保 total > 0
func percentFrom(total uint64, part_of uint64) string {
	// 转换为 float64 进行精确计算
	// 避免整数除法导致的精度丢失
	return fmt.Sprintf("%.2f", (float64(part_of)/float64(total))*100)
}

// join 将 int64 数组转换为逗号分隔的字符串
// 用于在 Web UI 中展示数组数据（如卷 ID 列表、统计数据等）
//
// 参数:
//   - data: int64 数组（如 []int64{1, 2, 3}）
//
// 返回值:
//   - string: 逗号分隔的字符串（如 "1,2,3"）
//
// 使用示例:
//   join([]int64{1, 2, 3}) -> "1,2,3"
//   join([]int64{100, 200, 300}) -> "100,200,300"
//   join([]int64{}) -> ""
//
// 应用场景:
//   - 显示卷 ID 列表
//   - 展示统计数据数组
//   - JavaScript 图表数据格式化
func join(data []int64) string {
	var ret []string
	// 遍历数组，将每个 int64 转换为字符串
	for _, d := range data {
		ret = append(ret, strconv.Itoa(int(d)))
	}
	// 使用逗号连接所有字符串
	return strings.Join(ret, ",")
}

// funcMap 模板函数映射表
// 定义了可在 HTML 模板中调用的自定义函数
//
// 包含的函数：
//   - join: 将 int64 数组转换为逗号分隔字符串
//   - bytesToHumanReadable: 将字节数转换为人类可读格式（如 "1.5 GB"）
//   - percentFrom: 计算百分比（如磁盘使用率）
//   - isNotEmpty: 检查字符串是否非空
//
// 使用示例（在 HTML 模板中）：
//   {{ join .VolumeIds }}                      -> "1,2,3"
//   {{ bytesToHumanReadable .DiskSize }}       -> "10.5 GB"
//   {{ percentFrom .TotalSize .UsedSize }}%    -> "75.32%"
//   {{ if isNotEmpty .Message }}显示消息{{ end }}
//
// 功能说明：
//   - 这些函数让模板可以直接格式化数据，无需在后端预处理
//   - 提高了模板的灵活性和可读性
var funcMap = template.FuncMap{
	"join":                 join,                          // int64 数组转字符串
	"bytesToHumanReadable": util.BytesToHumanReadable,     // 字节数转人类可读格式
	"percentFrom":          percentFrom,                   // 计算百分比
	"isNotEmpty":           util.IsNotEmpty,               // 检查字符串非空
}

//go:embed volume.html
// volumeHtml 嵌入的 HTML 模板字符串
// 使用 Go 1.16+ 的 embed 特性，在编译时将 volume.html 文件内容嵌入到二进制中
//
// 优势:
//   - 单一二进制文件，无需外部依赖
//   - 避免运行时读取文件的 I/O 开销
//   - 简化部署（不需要额外复制模板文件）
//
// 文件位置: weed/server/volume_server_ui/volume.html
var volumeHtml string

// StatusTpl Volume Server 状态页面模板
// 编译后的 HTML 模板，用于渲染 Volume Server 的 Web UI
//
// 功能:
//   - 显示卷列表及其状态（可写、只读、纠删码等）
//   - 展示磁盘使用情况和容量统计
//   - 显示副本配置、数据中心信息等
//
// 使用方式:
//   StatusTpl.Execute(w, data)  // data 包含卷信息、磁盘统计等
//
// 模板初始化过程:
//   1. 创建名为 "status" 的新模板
//   2. 注册自定义函数（funcMap）
//   3. 解析嵌入的 HTML 字符串（volumeHtml）
//   4. template.Must 确保解析成功，失败则 panic
//
// 注意:
//   - 使用 template.Must 包装，解析失败会导致程序启动失败（这是期望的行为）
//   - 模板在包初始化时就已编译，运行时无需重新解析
var StatusTpl = template.Must(template.New("status").Funcs(funcMap).Parse(volumeHtml))
