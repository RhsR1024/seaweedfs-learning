// Package client 提供 HTTP 客户端配置选项
package client

import (
	"net"
	"time"
)

// HttpClientOpt HTTP 客户端配置选项函数类型
// 用于在创建客户端时进行自定义配置
// 遵循函数式选项模式(Functional Options Pattern)
type HttpClientOpt = func(clientCfg *HTTPClient)

// AddDialContext 添加拨号上下文配置
// 为 HTTP 客户端配置连接拨号器,设置连接超时和保活参数
// 这是一个常用的配置选项,可以传递给 NewHttpClient 函数
//
// 配置内容:
//   - Timeout: 10秒 - 建立连接的超时时间
//   - KeepAlive: 10秒 - TCP Keep-Alive 探测间隔
//
// 参数:
//   httpClient: 要配置的 HTTP 客户端
//
// 使用示例:
//   client, err := NewHttpClient(ClientName, AddDialContext)
func AddDialContext(httpClient *HTTPClient) {
	// 创建拨号器,配置超时和保活参数
	dialContext := (&net.Dialer{
		Timeout:   10 * time.Second,  // 连接超时时间
		KeepAlive: 10 * time.Second,  // TCP Keep-Alive 间隔
	}).DialContext

	// 将拨号上下文应用到传输层
	httpClient.Transport.DialContext = dialContext
	httpClient.Client.Transport = httpClient.Transport
}
