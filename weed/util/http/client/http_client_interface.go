// Package client 定义 HTTP 客户端接口
package client

import (
	"io"
	"net/http"
	"net/url"
)

// HTTPClientInterface HTTP 客户端接口定义
// 定义了 HTTP 客户端必须实现的标准方法
// 这个接口使得 HTTPClient 可以被 mock,便于单元测试
// 也允许不同的 HTTP 客户端实现(如标准库、第三方库)使用统一接口
type HTTPClientInterface interface {
	// Do 执行 HTTP 请求
	// 参数:
	//   req: HTTP 请求对象
	// 返回:
	//   *http.Response: HTTP 响应
	//   error: 错误信息
	Do(req *http.Request) (*http.Response, error)

	// Get 发送 GET 请求
	// 参数:
	//   url: 请求的 URL
	// 返回:
	//   resp: HTTP 响应
	//   err: 错误信息
	Get(url string) (resp *http.Response, err error)

	// Post 发送 POST 请求
	// 参数:
	//   url: 请求的 URL
	//   contentType: 内容类型(如 "application/json")
	//   body: 请求体数据
	// 返回:
	//   resp: HTTP 响应
	//   err: 错误信息
	Post(url, contentType string, body io.Reader) (resp *http.Response, err error)

	// PostForm 发送表单 POST 请求
	// 参数:
	//   url: 请求的 URL
	//   data: 表单数据(键值对)
	// 返回:
	//   resp: HTTP 响应
	//   err: 错误信息
	PostForm(url string, data url.Values) (resp *http.Response, err error)

	// Head 发送 HEAD 请求
	// HEAD 请求只获取响应头,不获取响应体
	// 参数:
	//   url: 请求的 URL
	// 返回:
	//   resp: HTTP 响应
	//   err: 错误信息
	Head(url string) (resp *http.Response, err error)

	// CloseIdleConnections 关闭所有空闲连接
	// 用于释放资源,在客户端不再使用时调用
	CloseIdleConnections()
}
