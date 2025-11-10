// Package client 提供 HTTP 客户端的封装和 TLS 安全配置
// 支持 HTTP/HTTPS 自动切换、客户端证书认证和 CA 证书配置
package client

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	util "github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/spf13/viper"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
)

var (
	// loadSecurityConfigOnce 确保安全配置只加载一次
	loadSecurityConfigOnce sync.Once
)

// HTTPClient SeaweedFS 的 HTTP 客户端封装
// 提供 HTTP/HTTPS 协议自动切换、TLS 配置和连接池管理
type HTTPClient struct {
	Client            *http.Client     // 底层 HTTP 客户端
	Transport         *http.Transport  // HTTP 传输层配置(连接池、超时等)
	expectHttpsScheme bool             // 是否期望使用 HTTPS 协议
}

// Do 执行 HTTP 请求
// 自动设置请求的协议方案(HTTP 或 HTTPS)
//
// 参数:
//   req: HTTP 请求对象
//
// 返回:
//   *http.Response: HTTP 响应
//   error: 错误信息
func (httpClient *HTTPClient) Do(req *http.Request) (*http.Response, error) {
	req.URL.Scheme = httpClient.GetHttpScheme()  // 根据配置设置正确的协议
	return httpClient.Client.Do(req)
}

// Get 发送 GET 请求
// 自动规范化 URL 的协议方案
//
// 参数:
//   url: 请求的 URL
//
// 返回:
//   resp: HTTP 响应
//   err: 错误信息
func (httpClient *HTTPClient) Get(url string) (resp *http.Response, err error) {
	url, err = httpClient.NormalizeHttpScheme(url)  // 规范化 URL 协议
	if err != nil {
		return nil, err
	}
	return httpClient.Client.Get(url)
}

// Post 发送 POST 请求
// 自动规范化 URL 的协议方案
//
// 参数:
//   url: 请求的 URL
//   contentType: 内容类型(如 application/json)
//   body: 请求体数据
//
// 返回:
//   resp: HTTP 响应
//   err: 错误信息
func (httpClient *HTTPClient) Post(url, contentType string, body io.Reader) (resp *http.Response, err error) {
	url, err = httpClient.NormalizeHttpScheme(url)
	if err != nil {
		return nil, err
	}
	return httpClient.Client.Post(url, contentType, body)
}

// PostForm 发送表单 POST 请求
// 自动规范化 URL 的协议方案
//
// 参数:
//   url: 请求的 URL
//   data: 表单数据
//
// 返回:
//   resp: HTTP 响应
//   err: 错误信息
func (httpClient *HTTPClient) PostForm(url string, data url.Values) (resp *http.Response, err error) {
	url, err = httpClient.NormalizeHttpScheme(url)
	if err != nil {
		return nil, err
	}
	return httpClient.Client.PostForm(url, data)
}

// Head 发送 HEAD 请求
// 自动规范化 URL 的协议方案
// HEAD 请求只获取响应头,不获取响应体,常用于检查资源是否存在
//
// 参数:
//   url: 请求的 URL
//
// 返回:
//   resp: HTTP 响应
//   err: 错误信息
func (httpClient *HTTPClient) Head(url string) (resp *http.Response, err error) {
	url, err = httpClient.NormalizeHttpScheme(url)
	if err != nil {
		return nil, err
	}
	return httpClient.Client.Head(url)
}

// CloseIdleConnections 关闭所有空闲连接
// 用于释放资源,在客户端不再使用时调用
func (httpClient *HTTPClient) CloseIdleConnections() {
	httpClient.Client.CloseIdleConnections()
}

// GetClientTransport 获取客户端的传输层配置
// 可用于查看或修改连接池、超时等配置
//
// 返回:
//   *http.Transport: HTTP 传输层对象
func (httpClient *HTTPClient) GetClientTransport() *http.Transport {
	return httpClient.Transport
}

// GetHttpScheme 获取当前客户端使用的协议方案
// 根据配置返回 "http" 或 "https"
//
// 返回:
//   string: "http" 或 "https"
func (httpClient *HTTPClient) GetHttpScheme() string {
	if httpClient.expectHttpsScheme {
		return "https"
	}
	return "http"
}

// NormalizeHttpScheme 规范化 URL 的协议方案
// 确保 URL 使用正确的协议(HTTP 或 HTTPS)
// 如果 URL 没有协议前缀,会自动添加;如果协议不匹配,会自动转换
//
// 参数:
//   rawURL: 原始 URL 字符串
//
// 返回:
//   string: 规范化后的 URL
//   error: 解析错误
func (httpClient *HTTPClient) NormalizeHttpScheme(rawURL string) (string, error) {
	expectedScheme := httpClient.GetHttpScheme()

	// 如果 URL 没有协议前缀,直接添加期望的协议
	if !(strings.HasPrefix(rawURL, "http://") || strings.HasPrefix(rawURL, "https://")) {
		return expectedScheme + "://" + rawURL, nil
	}

	// 解析 URL
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}

	// 如果协议不匹配,替换为期望的协议
	if expectedScheme != parsedURL.Scheme {
		parsedURL.Scheme = expectedScheme
	}
	return parsedURL.String(), nil
}

// NewHttpClient 创建新的 HTTP 客户端
// 根据客户端名称从配置文件加载安全设置(HTTPS、证书等)
// 支持通过选项函数进行自定义配置
//
// 参数:
//   clientName: 客户端名称,用于从配置中查找对应的安全设置
//   opts: 可选的配置选项函数,用于自定义客户端行为
//
// 返回:
//   *HTTPClient: 配置好的 HTTP 客户端
//   error: 初始化错误
func NewHttpClient(clientName ClientName, opts ...HttpClientOpt) (*HTTPClient, error) {
	httpClient := HTTPClient{}
	// 检查是否启用 HTTPS
	httpClient.expectHttpsScheme = checkIsHttpsClientEnabled(clientName)
	var tlsConfig *tls.Config = nil

	// 如果启用了 HTTPS,配置 TLS
	if httpClient.expectHttpsScheme {
		// 加载客户端证书对(cert + key)
		clientCertPair, err := getClientCertPair(clientName)
		if err != nil {
			return nil, err
		}

		// 加载客户端 CA 证书
		clientCaCert, clientCaCertName, err := getClientCaCert(clientName)
		if err != nil {
			return nil, err
		}

		// 如果配置了证书,创建 TLS 配置
		if clientCertPair != nil || len(clientCaCert) != 0 {
			// 创建 CA 证书池
			caCertPool, err := createHTTPClientCertPool(clientCaCert, clientCaCertName)
			if err != nil {
				return nil, err
			}

			// 配置 TLS
			tlsConfig = &tls.Config{
				Certificates:       []tls.Certificate{},  // 客户端证书列表
				RootCAs:            caCertPool,           // 受信任的 CA 证书池
				InsecureSkipVerify: false,                // 不跳过证书验证(安全)
			}

			// 如果有客户端证书,添加到配置
			if clientCertPair != nil {
				tlsConfig.Certificates = append(tlsConfig.Certificates, *clientCertPair)
			}
		}
	}

	// 创建 HTTP 传输层
	// 配置连接池大小和 TLS
	httpClient.Transport = &http.Transport{
		MaxIdleConns:        1024,       // 最大空闲连接数
		MaxIdleConnsPerHost: 1024,       // 每个主机的最大空闲连接数
		TLSClientConfig:     tlsConfig,  // TLS 配置
	}

	// 创建 HTTP 客户端
	httpClient.Client = &http.Client{
		Transport: httpClient.Transport,
	}

	// 应用自定义配置选项
	for _, opt := range opts {
		opt(&httpClient)
	}
	return &httpClient, nil
}

// getStringOptionFromSecurityConfiguration 从安全配置中读取字符串选项
// 配置格式: https.<clientName>.<optionName>
//
// 参数:
//   clientName: 客户端名称
//   stringOptionName: 选项名称(如 cert, key, ca)
//
// 返回:
//   string: 配置值
func getStringOptionFromSecurityConfiguration(clientName ClientName, stringOptionName string) string {
	util.LoadSecurityConfiguration()
	return viper.GetString(fmt.Sprintf("https.%s.%s", clientName.LowerCaseString(), stringOptionName))
}

// getBoolOptionFromSecurityConfiguration 从安全配置中读取布尔选项
// 配置格式: https.<clientName>.<optionName>
//
// 参数:
//   clientName: 客户端名称
//   boolOptionName: 选项名称(如 enabled)
//
// 返回:
//   bool: 配置值
func getBoolOptionFromSecurityConfiguration(clientName ClientName, boolOptionName string) bool {
	util.LoadSecurityConfiguration()
	return viper.GetBool(fmt.Sprintf("https.%s.%s", clientName.LowerCaseString(), boolOptionName))
}

// checkIsHttpsClientEnabled 检查指定客户端是否启用 HTTPS
// 从配置读取 https.<clientName>.enabled
//
// 参数:
//   clientName: 客户端名称
//
// 返回:
//   bool: true 表示启用 HTTPS
func checkIsHttpsClientEnabled(clientName ClientName) bool {
	return getBoolOptionFromSecurityConfiguration(clientName, "enabled")
}

// getFileContentFromSecurityConfiguration 从安全配置中读取文件内容
// 先从配置获取文件路径,然后读取文件内容
//
// 参数:
//   clientName: 客户端名称
//   fileType: 文件类型(如 cert, key, ca)
//
// 返回:
//   []byte: 文件内容
//   string: 文件名
//   error: 读取错误
func getFileContentFromSecurityConfiguration(clientName ClientName, fileType string) ([]byte, string, error) {
	if fileName := getStringOptionFromSecurityConfiguration(clientName, fileType); fileName != "" {
		fileContent, err := os.ReadFile(fileName)
		if err != nil {
			return nil, fileName, err
		}
		return fileContent, fileName, err
	}
	return nil, "", nil
}

// getClientCertPair 获取客户端证书对(证书 + 私钥)
// 从配置中读取证书和私钥文件路径,加载为 TLS 证书对
//
// 参数:
//   clientName: 客户端名称
//
// 返回:
//   *tls.Certificate: TLS 证书对
//   error: 加载错误
func getClientCertPair(clientName ClientName) (*tls.Certificate, error) {
	certFileName := getStringOptionFromSecurityConfiguration(clientName, "cert")
	keyFileName := getStringOptionFromSecurityConfiguration(clientName, "key")

	// 如果都没配置,返回 nil(不使用客户端证书)
	if certFileName == "" && keyFileName == "" {
		return nil, nil
	}

	// 如果都配置了,加载证书对
	if certFileName != "" && keyFileName != "" {
		clientCert, err := tls.LoadX509KeyPair(certFileName, keyFileName)
		if err != nil {
			return nil, fmt.Errorf("error loading client certificate and key: %s", err)
		}
		return &clientCert, nil
	}

	// 如果只配置了其中一个,返回错误
	return nil, fmt.Errorf("error loading key pair: key `%s` and certificate `%s`", keyFileName, certFileName)
}

// getClientCaCert 获取客户端 CA 证书
// 从配置中读取 CA 证书文件
//
// 参数:
//   clientName: 客户端名称
//
// 返回:
//   []byte: CA 证书内容
//   string: CA 证书文件名
//   error: 读取错误
func getClientCaCert(clientName ClientName) ([]byte, string, error) {
	return getFileContentFromSecurityConfiguration(clientName, "ca")
}

// createHTTPClientCertPool 创建 HTTP 客户端的 CA 证书池
// 将 PEM 格式的证书添加到证书池中,用于验证服务器证书
//
// 参数:
//   certContent: PEM 格式的证书内容
//   fileName: 证书文件名(用于错误提示)
//
// 返回:
//   *x509.CertPool: CA 证书池
//   error: 证书解析错误
func createHTTPClientCertPool(certContent []byte, fileName string) (*x509.CertPool, error) {
	certPool := x509.NewCertPool()
	// 如果证书内容为空,返回空的证书池
	if len(certContent) == 0 {
		return certPool, nil
	}

	// 将 PEM 格式的证书添加到证书池
	ok := certPool.AppendCertsFromPEM(certContent)
	if !ok {
		return nil, fmt.Errorf("error processing certificate in %s", fileName)
	}
	return certPool, nil
}
