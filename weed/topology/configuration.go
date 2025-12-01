// Package topology 实现了 SeaweedFS 的拓扑管理功能
// 本文件定义了拓扑配置的 XML 结构，用于从配置文件加载数据中心/机架/节点的层级关系
package topology

import (
	"encoding/xml"
)

// loc 表示一个位置（数据中心 + 机架）
// 用于内部缓存和查找
type loc struct {
	// dcName 数据中心名称
	dcName string
	// rackName 机架名称
	rackName string
}

// rack 表示一个机架的配置
// 从 XML 配置文件中解析
type rack struct {
	// Name 机架名称，从 XML 属性 "name" 读取
	Name string `xml:"name,attr"`
	// Ips 该机架内的服务器 IP 列表，从 XML 元素 "Ip" 读取
	Ips []string `xml:"Ip"`
}

// dataCenter 表示一个数据中心的配置
// 从 XML 配置文件中解析
type dataCenter struct {
	// Name 数据中心名称，从 XML 属性 "name" 读取
	Name string `xml:"name,attr"`
	// Racks 该数据中心内的机架列表，从 XML 元素 "Rack" 读取
	Racks []rack `xml:"Rack"`
}

// topology 表示整个拓扑结构的配置
// 从 XML 配置文件中解析
type topology struct {
	// DataCenters 所有数据中心的列表，从 XML 元素 "DataCenter" 读取
	DataCenters []dataCenter `xml:"DataCenter"`
}

// Configuration 表示 SeaweedFS 的拓扑配置
// 从 XML 配置文件中加载，用于预定义数据中心、机架和节点的层级关系
//
// XML 配置文件示例：
//   <Configuration>
//     <Topology>
//       <DataCenter name="dc1">
//         <Rack name="rack1">
//           <Ip>192.168.1.1</Ip>
//           <Ip>192.168.1.2</Ip>
//         </Rack>
//         <Rack name="rack2">
//           <Ip>192.168.1.3</Ip>
//         </Rack>
//       </DataCenter>
//       <DataCenter name="dc2">
//         <Rack name="rack1">
//           <Ip>192.168.2.1</Ip>
//         </Rack>
//       </DataCenter>
//     </Topology>
//   </Configuration>
//
// 用途：
//   1. 预定义物理拓扑结构
//   2. 根据 IP 自动分配数据中心和机架
//   3. 优化副本放置策略
//
// 注意：
//   - 目前的实现（Locate 方法）并未真正使用 IP 映射
//   - 实际使用中，通常通过命令行参数指定数据中心和机架
//   - 这个配置结构保留了扩展性，可以在未来实现基于 IP 的自动定位
type Configuration struct {
	// XMLName XML 根元素名称，必须是 "Configuration"
	XMLName xml.Name `xml:"Configuration"`
	// Topo 拓扑结构配置
	Topo topology `xml:"Topology"`
}

// String 返回配置的 XML 字符串表示
// 用于调试和日志记录
//
// 返回:
//   - string: 格式化的 XML 字符串，如果序列化失败则返回空字符串
func (c *Configuration) String() string {
	// 序列化为格式化的 XML（带缩进）
	// 第二个参数 "  " 是前缀（每行开头），第三个参数 "  " 是缩进
	if b, e := xml.MarshalIndent(c, "  ", "  "); e == nil {
		return string(b)
	}
	// 序列化失败，返回空字符串
	return ""
}

// Locate 根据 IP 和名称定位数据中心和机架
// 当前实现是简化版本，主要处理默认值
//
// 工作流程（当前实现）：
//   1. 如果未指定数据中心名称，使用 "DefaultDataCenter"
//   2. 如果未指定机架名称，使用 "DefaultRack"
//   3. 直接返回（未实际查找 IP 对应的位置）
//
// 扩展方向：
//   - 可以根据 IP 在配置中查找对应的数据中心和机架
//   - 可以使用 IP 段（CIDR）进行匹配
//   - 可以实现基于网络拓扑的智能定位
//
// 参数:
//   - ip: 节点 IP 地址（当前未使用）
//   - dcName: 指定的数据中心名称（优先使用）
//   - rackName: 指定的机架名称（优先使用）
// 返回:
//   - dc: 数据中心名称
//   - rack: 机架名称
func (c *Configuration) Locate(ip string, dcName string, rackName string) (dc string, rack string) {
	// 如果未指定数据中心，使用默认值
	if dcName == "" {
		dcName = "DefaultDataCenter"
	}

	// 如果未指定机架，使用默认值
	if rackName == "" {
		rackName = "DefaultRack"
	}

	// 返回数据中心和机架名称
	// 注意：当前实现未使用 ip 参数和配置中的 IP 映射
	// 这为未来的增强留下了空间
	return dcName, rackName
}
