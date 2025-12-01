// Package pb 提供 SeaweedFS 的 Protocol Buffers 相关工具
//
// 功能说明：
// 本文件实现了服务器地址的管理和解析功能，是 SeaweedFS 节点间通信的基础
//
// 核心类型：
//   - ServerAddress: 单个服务器地址，格式：host:port 或 host:port.grpcPort
//   - ServerAddresses: 服务器地址列表，逗号分隔
//   - ServerSrvAddress: DNS SRV 记录地址
//
// 地址格式：
//   - 标准格式：192.168.1.100:8080（HTTP 端口）
//   - 带 gRPC 端口：192.168.1.100:8080.18080（HTTP 端口 + gRPC 端口）
//   - 默认规则：如果 gRPC 端口 = HTTP 端口 + 10000，则省略 gRPC 端口
//
// 应用场景：
//   1. Master Server 管理 Volume Server 地址
//   2. Filer 连接到 Master 集群
//   3. 客户端查找服务器位置
//   4. 服务发现（支持 DNS SRV）
package pb

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"net"
	"strconv"
	"strings"
)

// ServerAddress 表示单个服务器地址
// 格式：host:port 或 host:port.grpcPort
// 例如：
//   - "192.168.1.100:8080" - 仅 HTTP 端口
//   - "192.168.1.100:8080.18080" - HTTP 端口 + gRPC 端口
type ServerAddress string

// ServerAddresses 表示服务器地址列表（逗号分隔）
// 格式：address1,address2,address3
// 例如：
//   - "192.168.1.100:8080,192.168.1.101:8080"
//   - "dnssrv+_grpc._tcp.master.consul" - DNS SRV 记录
type ServerAddresses string

// ServerSrvAddress 表示 DNS SRV 记录地址
// 用于动态服务发现，支持 Consul、Kubernetes 等
// 例如：
//   - "_grpc._tcp.master.consul"
//   - "_grpc._tcp.headless.default.svc.cluster.local"
type ServerSrvAddress string

// NewServerAddress 创建服务器地址
//
// 功能说明：
// 根据主机名、HTTP 端口和 gRPC 端口构造 ServerAddress
// 如果 gRPC 端口符合默认规则（HTTP 端口 + 10000），则省略 gRPC 端口
//
// 参数:
//   - host: 主机名或 IP 地址
//   - port: HTTP 端口号
//   - grpcPort: gRPC 端口号
//
// 返回:
//   - ServerAddress: 构造的服务器地址
//
// 示例：
//   NewServerAddress("192.168.1.100", 8080, 0)       -> "192.168.1.100:8080"
//   NewServerAddress("192.168.1.100", 8080, 18080)   -> "192.168.1.100:8080"（默认规则）
//   NewServerAddress("192.168.1.100", 8080, 19999)   -> "192.168.1.100:8080.19999"（自定义端口）
func NewServerAddress(host string, port int, grpcPort int) ServerAddress {
	// 如果 gRPC 端口未指定（0）或符合默认规则（HTTP 端口 + 10000）
	// 则仅返回 host:port 格式
	if grpcPort == 0 || grpcPort == port+10000 {
		return ServerAddress(util.JoinHostPort(host, port))
	}
	// 否则返回 host:port.grpcPort 格式
	return ServerAddress(util.JoinHostPort(host, port) + "." + strconv.Itoa(grpcPort))
}

// NewServerAddressWithGrpcPort 从已有地址和 gRPC 端口创建服务器地址
//
// 功能说明：
// 在已有的 HTTP 地址基础上，附加 gRPC 端口信息
//
// 参数:
//   - address: 已有的服务器地址（host:port 格式）
//   - grpcPort: gRPC 端口号
//
// 返回:
//   - ServerAddress: 构造的服务器地址
//
// 示例：
//   NewServerAddressWithGrpcPort("192.168.1.100:8080", 0)      -> "192.168.1.100:8080"
//   NewServerAddressWithGrpcPort("192.168.1.100:8080", 18080)  -> "192.168.1.100:8080"
//   NewServerAddressWithGrpcPort("192.168.1.100:8080", 19999)  -> "192.168.1.100:8080.19999"
func NewServerAddressWithGrpcPort(address string, grpcPort int) ServerAddress {
	// 如果 gRPC 端口未指定，直接返回原地址
	if grpcPort == 0 {
		return ServerAddress(address)
	}

	// 解析地址中的端口号
	_, port, _ := hostAndPort(address)

	// 如果 gRPC 端口符合默认规则，直接返回原地址
	if uint64(grpcPort) == port+10000 {
		return ServerAddress(address)
	}

	// 否则附加 gRPC 端口
	return ServerAddress(address + "." + strconv.Itoa(grpcPort))
}

// NewServerAddressFromDataNode 从 DataNodeInfo 创建服务器地址
//
// 功能说明：
// 从 Master gRPC 消息中的 DataNodeInfo 提取地址信息
//
// 参数:
//   - dn: DataNodeInfo protobuf 消息
//
// 返回:
//   - ServerAddress: 构造的服务器地址
//
// 应用场景：
// Master 返回 Volume Server 列表时，将 DataNodeInfo 转换为 ServerAddress
func NewServerAddressFromDataNode(dn *master_pb.DataNodeInfo) ServerAddress {
	return NewServerAddressWithGrpcPort(dn.Id, int(dn.GrpcPort))
}

// NewServerAddressFromLocation 从 Location 创建服务器地址
//
// 功能说明：
// 从 Master gRPC 消息中的 Location 提取地址信息
//
// 参数:
//   - dn: Location protobuf 消息
//
// 返回:
//   - ServerAddress: 构造的服务器地址
//
// 应用场景：
// Master 广播 Volume 位置变更时，将 Location 转换为 ServerAddress
func NewServerAddressFromLocation(dn *master_pb.Location) ServerAddress {
	return NewServerAddressWithGrpcPort(dn.Url, int(dn.GrpcPort))
}

// String 实现 Stringer 接口
//
// 功能说明：
// 返回 HTTP 地址格式（不包含 gRPC 端口）
//
// 返回:
//   - string: HTTP 地址字符串
func (sa ServerAddress) String() string {
	return sa.ToHttpAddress()
}

// ToHttpAddress 提取 HTTP 地址
//
// 功能说明：
// 从 ServerAddress 中提取 HTTP 部分（host:port）
// 如果地址包含 gRPC 端口（host:port.grpcPort），则仅返回 host:port
//
// 返回:
//   - string: HTTP 地址，格式：host:port
//
// 示例：
//   "192.168.1.100:8080"           -> "192.168.1.100:8080"
//   "192.168.1.100:8080.19999"     -> "192.168.1.100:8080"
//
// 应用场景：
// 1. 通过 HTTP API 访问 Volume Server
// 2. 构造文件下载 URL
// 3. Master 管理界面显示
func (sa ServerAddress) ToHttpAddress() string {
	// 查找最后一个冒号的位置（分隔 host 和 ports）
	portsSepIndex := strings.LastIndex(string(sa), ":")
	if portsSepIndex < 0 {
		// 没有端口信息，直接返回
		return string(sa)
	}

	// 检查冒号后是否有内容
	if portsSepIndex+1 >= len(sa) {
		return string(sa)
	}

	// 提取端口部分（可能是 "port" 或 "port.grpcPort"）
	ports := string(sa[portsSepIndex+1:])

	// 查找点号（分隔 HTTP 端口和 gRPC 端口）
	sepIndex := strings.LastIndex(string(ports), ".")
	if sepIndex >= 0 {
		// 存在 gRPC 端口，提取 HTTP 端口
		host := string(sa[0:portsSepIndex])
		return net.JoinHostPort(host, ports[0:sepIndex])
	}

	// 没有 gRPC 端口，直接返回
	return string(sa)
}

// ToGrpcAddress 提取 gRPC 地址
//
// 功能说明：
// 从 ServerAddress 中提取 gRPC 部分（host:grpcPort）
// 如果地址不包含显式 gRPC 端口，则使用默认规则（HTTP 端口 + 10000）
//
// 返回:
//   - string: gRPC 地址，格式：host:grpcPort
//
// 示例：
//   "192.168.1.100:8080"           -> "192.168.1.100:18080"（默认规则）
//   "192.168.1.100:8080.19999"     -> "192.168.1.100:19999"（显式指定）
//
// 应用场景：
// 1. 建立 gRPC 连接到 Master/Volume/Filer
// 2. 心跳通信
// 3. 集群内部调用
func (sa ServerAddress) ToGrpcAddress() string {
	// 查找最后一个冒号的位置
	portsSepIndex := strings.LastIndex(string(sa), ":")
	if portsSepIndex < 0 {
		return string(sa)
	}

	// 检查冒号后是否有内容
	if portsSepIndex+1 >= len(sa) {
		return string(sa)
	}

	// 提取端口部分
	ports := string(sa[portsSepIndex+1:])

	// 查找点号（分隔 HTTP 端口和 gRPC 端口）
	sepIndex := strings.LastIndex(ports, ".")
	if sepIndex >= 0 {
		// 存在显式 gRPC 端口，直接返回
		host := string(sa[0:portsSepIndex])
		return net.JoinHostPort(host, ports[sepIndex+1:])
	}

	// 没有显式 gRPC 端口，使用默认规则（HTTP 端口 + 10000）
	return ServerToGrpcAddress(string(sa))
}

// LookUp 查询 DNS SRV 记录
//
// 功能说明：
// 通过 DNS SRV 记录查询服务器地址列表
// 支持动态服务发现（Consul、Kubernetes 等）
//
// 返回:
//   - addresses: 查询到的服务器地址列表
//   - err: 查询错误（可能部分成功）
//
// 注意事项：
// 即使 err != nil，addresses 也可能包含部分成功的查询结果
// 调用者应该检查 addresses 是否为空，而不是仅检查 err
//
// 应用场景：
// 1. Filer 连接到 Master 集群（dnssrv+_grpc._tcp.master.consul）
// 2. Kubernetes 中的 Headless Service
// 3. Consul 服务发现
//
// 示例：
//   addr := ServerSrvAddress("_grpc._tcp.master.consul")
//   addresses, err := addr.LookUp()
//   // addresses 可能包含：["10.0.0.1:9333", "10.0.0.2:9333"]
func (r ServerSrvAddress) LookUp() (addresses []ServerAddress, err error) {
	// 查询 DNS SRV 记录
	// 参数：service="", proto="", name=SRV记录名称
	_, records, lookupErr := net.LookupSRV("", "", string(r))
	if lookupErr != nil {
		err = fmt.Errorf("lookup SRV address %s: %v", r, lookupErr)
	}

	// 解析 SRV 记录，提取目标地址和端口
	for _, srv := range records {
		address := fmt.Sprintf("%s:%d", srv.Target, srv.Port)
		addresses = append(addresses, ServerAddress(address))
	}

	return
}

// ToServiceDiscovery 创建服务发现对象
//
// 功能说明：
// 解析服务器地址列表，支持两种格式：
//   1. 逗号分隔的地址列表：10.0.0.1:9999,10.0.0.2:9999
//   2. DNS SRV 记录：dnssrv+_grpc._tcp.master.consul
//
// 返回:
//   - sd: 服务发现对象
//
// 示例：
//   // 静态地址列表
//   addrs := ServerAddresses("192.168.1.100:9333,192.168.1.101:9333")
//   sd := addrs.ToServiceDiscovery()
//
//   // DNS SRV 记录
//   addrs := ServerAddresses("dnssrv+_grpc._tcp.master.consul")
//   sd := addrs.ToServiceDiscovery()
//
// 应用场景：
// 1. Filer 启动时连接到 Master 集群
// 2. Volume Server 注册到 Master
// 3. 客户端连接到分布式集群
func (sa ServerAddresses) ToServiceDiscovery() (sd *ServerDiscovery) {
	sd = &ServerDiscovery{}

	// DNS SRV 记录前缀
	prefix := "dnssrv+"

	if strings.HasPrefix(string(sa), prefix) {
		// 【情况 1：DNS SRV 记录】
		// 去除前缀，保存 SRV 记录地址
		trimmed := strings.TrimPrefix(string(sa), prefix)
		srv := ServerSrvAddress(trimmed)
		sd.srvRecord = &srv
	} else {
		// 【情况 2：静态地址列表】
		// 解析逗号分隔的地址
		sd.list = sa.ToAddresses()
	}

	return
}

// ToAddresses 解析地址列表
//
// 功能说明：
// 将逗号分隔的地址字符串解析为 ServerAddress 切片
//
// 返回:
//   - addresses: 服务器地址切片
//
// 示例：
//   addrs := ServerAddresses("192.168.1.100:8080,192.168.1.101:8080")
//   list := addrs.ToAddresses()
//   // list = [ServerAddress("192.168.1.100:8080"), ServerAddress("192.168.1.101:8080")]
func (sa ServerAddresses) ToAddresses() (addresses []ServerAddress) {
	// 按逗号分割
	parts := strings.Split(string(sa), ",")
	for _, address := range parts {
		// 跳过空地址
		if address != "" {
			addresses = append(addresses, ServerAddress(address))
		}
	}
	return
}

// ToAddressMap 解析地址映射
//
// 功能说明：
// 将逗号分隔的地址字符串解析为 map[string]ServerAddress
// map 的 key 和 value 都是地址字符串
//
// 返回:
//   - addresses: 服务器地址映射
//
// 应用场景：
// 1. 快速查找地址是否存在（O(1) 时间复杂度）
// 2. 地址去重
// 3. 地址集合操作（并集、交集）
func (sa ServerAddresses) ToAddressMap() (addresses map[string]ServerAddress) {
	addresses = make(map[string]ServerAddress)
	for _, address := range sa.ToAddresses() {
		addresses[string(address)] = address
	}
	return
}

// ToAddressStrings 解析地址字符串列表
//
// 功能说明：
// 将逗号分隔的地址字符串解析为字符串切片
//
// 返回:
//   - addresses: 地址字符串切片
//
// 应用场景：
// 1. 配置文件解析
// 2. 命令行参数处理
// 3. 日志记录
func (sa ServerAddresses) ToAddressStrings() (addresses []string) {
	parts := strings.Split(string(sa), ",")
	for _, address := range parts {
		addresses = append(addresses, address)
	}
	return
}

// ToAddressStrings 将 ServerAddress 切片转换为字符串切片
//
// 功能说明：
// 类型转换工具函数
//
// 参数:
//   - addresses: ServerAddress 切片
//
// 返回:
//   - []string: 地址字符串切片
func ToAddressStrings(addresses []ServerAddress) []string {
	var strings []string
	for _, addr := range addresses {
		strings = append(strings, string(addr))
	}
	return strings
}

// ToAddressStringsFromMap 将地址映射转换为字符串切片
//
// 功能说明：
// 从 map[string]ServerAddress 提取所有地址值
//
// 参数:
//   - addresses: 地址映射
//
// 返回:
//   - []string: 地址字符串切片
//
// 注意：
// map 遍历顺序是随机的，返回的切片顺序不确定
func ToAddressStringsFromMap(addresses map[string]ServerAddress) []string {
	var strings []string
	for _, addr := range addresses {
		strings = append(strings, string(addr))
	}
	return strings
}

// FromAddressStrings 将字符串切片转换为 ServerAddress 切片
//
// 功能说明：
// 类型转换工具函数
//
// 参数:
//   - strings: 地址字符串切片
//
// 返回:
//   - []ServerAddress: ServerAddress 切片
func FromAddressStrings(strings []string) []ServerAddress {
	var addresses []ServerAddress
	for _, addr := range strings {
		addresses = append(addresses, ServerAddress(addr))
	}
	return addresses
}

// ParseUrl 解析 HTTP URL
//
// 功能说明：
// 从完整的 HTTP URL 中解析出服务器地址和路径
//
// 参数:
//   - input: HTTP URL，必须以 "http://" 开头
//
// 返回:
//   - address: 服务器地址（host:port）
//   - path: URL 路径部分（包含前导 /）
//   - err: 解析错误
//
// 示例：
//   ParseUrl("http://192.168.1.100:8080/status")
//   // 返回：ServerAddress("192.168.1.100:8080"), "/status", nil
//
//   ParseUrl("http://192.168.1.100:8080")
//   // 返回：ServerAddress("192.168.1.100:8080"), "", nil
//
// 应用场景：
// 1. 解析用户提供的 Volume Server URL
// 2. 从完整 URL 提取地址信息
// 3. 构造 HTTP 客户端请求
func ParseUrl(input string) (address ServerAddress, path string, err error) {
	// 检查 URL 前缀
	if !strings.HasPrefix(input, "http://") {
		return "", "", fmt.Errorf("url %s needs prefix 'http://'", input)
	}

	// 去除 "http://" 前缀
	input = input[7:]

	// 查找路径分隔符
	pathSeparatorIndex := strings.Index(input, "/")
	hostAndPorts := input

	if pathSeparatorIndex > 0 {
		// 存在路径部分
		path = input[pathSeparatorIndex:]
		hostAndPorts = input[0:pathSeparatorIndex]
	}

	// 检查是否包含端口号
	commaSeparatorIndex := strings.Index(input, ":")
	if commaSeparatorIndex < 0 {
		err = fmt.Errorf("port should be specified in %s", input)
		return
	}

	address = ServerAddress(hostAndPorts)
	return
}
