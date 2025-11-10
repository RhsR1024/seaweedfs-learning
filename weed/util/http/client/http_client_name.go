// Package client 定义 HTTP 客户端名称类型
package client

import "strings"

// ClientName HTTP 客户端名称枚举类型
// 用于标识不同的客户端配置,每个客户端可以有独立的安全设置
// 配置文件中使用 https.<clientName>.*  格式进行配置
type ClientName int

//go:generate stringer -type=ClientName -output=http_client_name_string.go

// 客户端名称常量定义
// 可以扩展添加更多客户端类型,如 VolumeServerClient, FilerClient 等
const (
	Client ClientName = iota  // 默认客户端,值为 0
)

// LowerCaseString 返回客户端名称的小写字符串形式
// 用于生成配置文件中的键名,例如: "client"
//
// 返回:
//   string: 小写的客户端名称字符串
//
// 示例:
//   name := Client
//   key := name.LowerCaseString()  // 返回 "client"
//   // 用于配置: https.client.enabled
func (name *ClientName) LowerCaseString() string {
	return strings.ToLower(name.String())
}
