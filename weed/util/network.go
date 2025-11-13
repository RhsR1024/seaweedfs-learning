package util

import (
	"net"
	"strconv"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// DetectedHostAddress 自动检测本机的主机地址
// 按优先级尝试获取可用的网络地址:
// 1. 首先尝试获取 IPv4 地址
// 2. 如果没有 IPv4,尝试获取 IPv6 地址
// 3. 如果都没有,返回 "localhost"
//
// 此函数会过滤掉回环地址(loopback)和未激活的网络接口
//
// 返回: 检测到的主机地址字符串
func DetectedHostAddress() string {
	// 获取所有网络接口
	netInterfaces, err := net.Interfaces()
	if err != nil {
		glog.V(0).Infof("failed to detect net interfaces: %v", err)
		return ""
	}

	// 优先尝试获取 IPv4 地址
	if v4Address := selectIpV4(netInterfaces, true); v4Address != "" {
		return v4Address
	}

	// 如果没有 IPv4,尝试获取 IPv6 地址
	if v6Address := selectIpV4(netInterfaces, false); v6Address != "" {
		return v6Address
	}

	// 都没有则返回 localhost
	return "localhost"
}

// selectIpV4 从网络接口列表中选择合适的 IP 地址
// 遍历所有网络接口,查找符合条件的 IP 地址
//
// 参数:
//   netInterfaces: 网络接口列表
//   isIpV4: true 表示选择 IPv4 地址,false 表示选择 IPv6 地址
//
// 返回: 找到的 IP 地址字符串,未找到则返回空字符串
//
// 过滤规则:
//   - 跳过未激活的网络接口(FlagUp)
//   - 跳过回环地址(loopback)
//   - 对于 IPv6,跳过链路本地地址(link-local,fe80::/10)
func selectIpV4(netInterfaces []net.Interface, isIpV4 bool) string {
	for _, netInterface := range netInterfaces {
		// 检查网络接口是否激活
		// FlagUp 标志表示接口已启用
		if (netInterface.Flags & net.FlagUp) == 0 {
			continue
		}

		// 获取接口的所有地址
		addrs, err := netInterface.Addrs()
		if err != nil {
			glog.V(0).Infof("get interface addresses: %v", err)
		}

		// 遍历接口地址
		for _, a := range addrs {
			if ipNet, ok := a.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
				if isIpV4 {
					// 选择 IPv4 地址
					// To4() 如果是 IPv4 返回 4 字节表示,否则返回 nil
					if ipNet.IP.To4() != nil {
						return ipNet.IP.String()
					}
				} else {
					// 选择 IPv6 地址
					// To4() == nil 且 To16() != nil 表示是纯 IPv6 地址
					if ipNet.IP.To4() == nil && ipNet.IP.To16() != nil {
						// 过滤掉链路本地 IPv6 地址(fe80::/10)
						// 这类地址需要区域标识符,不适合服务器绑定
						// Filter out link-local IPv6 addresses (fe80::/10)
						// They require zone identifiers and are not suitable for server binding
						if !ipNet.IP.IsLinkLocalUnicast() {
							return ipNet.IP.String()
						}
					}
				}
			}
		}
	}
	return ""
}

// JoinHostPort 将主机名和端口号组合成地址字符串
// 正确处理 IPv6 地址的格式,确保生成有效的网络地址
//
// 参数:
//   host: 主机名或 IP 地址
//   port: 端口号
//
// 返回: 格式化的地址字符串
//
// 示例:
//   JoinHostPort("192.168.1.1", 8080) => "192.168.1.1:8080"
//   JoinHostPort("[::1]", 8080) => "[::1]:8080"
//   JoinHostPort("localhost", 9333) => "localhost:9333"
//
// 注意:
//   - 对于已经包含方括号的 IPv6 地址(如 "[::1]"),直接拼接端口
//   - 对于其他情况,使用标准库 net.JoinHostPort 处理
func JoinHostPort(host string, port int) string {
	portStr := strconv.Itoa(port)

	// 检查是否已经是带方括号的 IPv6 地址格式
	// 这种情况下直接拼接冒号和端口号
	if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		return host + ":" + portStr
	}

	// 使用标准库处理其他情况
	// net.JoinHostPort 会自动为裸 IPv6 地址添加方括号
	return net.JoinHostPort(host, portStr)
}
