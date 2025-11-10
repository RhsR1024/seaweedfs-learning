// Package http 提供全局 HTTP 客户端的初始化和管理
// 全局客户端在整个应用程序中共享,避免重复创建连接池
package http

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	util_http_client "github.com/seaweedfs/seaweedfs/weed/util/http/client"
)

var (
	// globalHttpClient 全局 HTTP 客户端实例
	// 在应用启动时初始化,全局共享使用
	globalHttpClient *util_http_client.HTTPClient
)

// NewGlobalHttpClient 创建新的全局 HTTP 客户端
// 使用默认的 Client 名称,并应用提供的配置选项
//
// 参数:
//   opt: 可变数量的配置选项函数(如 AddDialContext)
//
// 返回:
//   *util_http_client.HTTPClient: 配置好的 HTTP 客户端
//   error: 初始化错误
//
// 示例:
//   client, err := NewGlobalHttpClient(util_http_client.AddDialContext)
func NewGlobalHttpClient(opt ...util_http_client.HttpClientOpt) (*util_http_client.HTTPClient, error) {
	return util_http_client.NewHttpClient(util_http_client.Client, opt...)
}

// GetGlobalHttpClient 获取全局 HTTP 客户端实例
// 必须在调用 InitGlobalHttpClient() 之后使用
// 如果未初始化,返回 nil
//
// 返回:
//   *util_http_client.HTTPClient: 全局 HTTP 客户端实例
func GetGlobalHttpClient() *util_http_client.HTTPClient {
	return globalHttpClient
}

// InitGlobalHttpClient 初始化全局 HTTP 客户端
// 在应用启动时调用一次,初始化全局共享的 HTTP 客户端
// 如果初始化失败,程序会致命退出
//
// 使用场景:
//   - 在 main 函数或 init 函数中调用
//   - 确保在使用 GetGlobalHttpClient() 之前调用
func InitGlobalHttpClient() {
	var err error

	// 创建全局 HTTP 客户端
	globalHttpClient, err = NewGlobalHttpClient()
	if err != nil {
		// 初始化失败是致命错误,因为很多功能依赖全局客户端
		glog.Fatalf("error init global http client: %v", err)
	}
}
