// Package pb 提供 SeaweedFS 的 Protocol Buffers 定义和服务发现功能
// 本文件实现了基于 DNS SRV 记录的服务发现机制
package pb

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"reflect"
)

// ServerDiscovery 服务发现器
// 提供动态服务实例发现和刷新功能，支持通过 DNS SRV 记录自动更新服务列表
//
// 核心功能：
//   - 维护服务实例地址列表
//   - 通过 DNS SRV 记录动态刷新实例列表
//   - 提供多种格式的实例列表导出（数组、字符串数组、Map）
//
// 使用场景：
//   - Master Server 发现：Client 通过 DNS SRV 记录发现所有 Master 实例
//   - Filer Server 发现：Volume Server 通过 DNS SRV 发现 Filer 集群
//   - 动态扩缩容：新增/移除服务器时，DNS SRV 更新自动生效
//
// 示例：
//   sd := &ServerDiscovery{
//       srvRecord: &ServerSrvAddress{service: "_seaweedfs-master._tcp"},
//   }
//   sd.RefreshBySrvIfAvailable()  // 执行 DNS SRV 查询
//   instances := sd.GetInstances()  // 获取所有 Master 实例地址
type ServerDiscovery struct {
	// list 当前已知的服务实例地址列表
	// 通过 RefreshBySrvIfAvailable() 从 DNS SRV 记录更新
	list []ServerAddress

	// srvRecord DNS SRV 记录配置
	// 如果为 nil，表示不使用 DNS SRV 发现，仅使用静态地址列表
	// 如果非 nil，会定期通过 DNS 查询刷新 list
	srvRecord *ServerSrvAddress
}

// NewServiceDiscoveryFromMap 从静态地址 Map 创建服务发现器
// 用于已知固定服务器地址的场景，不使用 DNS SRV 动态发现
//
// 参数:
//   - m: 服务器地址 Map，key 通常是服务器标识，value 是服务器地址
//
// 返回:
//   - sd: 初始化的服务发现器，srvRecord 为 nil（不使用 DNS SRV）
//
// 使用场景：
//   - 配置文件中明确指定了所有服务器地址
//   - 测试环境中使用固定地址
//   - 不支持 DNS SRV 的网络环境
//
// 示例：
//   masters := map[string]ServerAddress{
//       "master1": ServerAddress("192.168.1.10:9333"),
//       "master2": ServerAddress("192.168.1.11:9333"),
//   }
//   sd := NewServiceDiscoveryFromMap(masters)
func NewServiceDiscoveryFromMap(m map[string]ServerAddress) (sd *ServerDiscovery) {
	// 创建空的服务发现器
	sd = &ServerDiscovery{}

	// 将 Map 中的所有地址添加到 list 中
	// 注意：这里忽略了 Map 的 key，只使用 value（地址）
	for _, s := range m {
		sd.list = append(sd.list, s)
	}

	// srvRecord 保持为 nil，表示使用静态地址列表
	return sd
}

// RefreshBySrvIfAvailable 通过 DNS SRV 记录刷新服务实例列表
// 如果配置了 srvRecord，执行 DNS 查询并更新 list；否则保持 list 不变
//
// DNS SRV 记录格式：
//   _service._proto.name TTL class SRV priority weight port target
//   示例：_seaweedfs-master._tcp.example.com 300 IN SRV 0 5 9333 master1.example.com
//
// 工作流程：
//   1. 检查是否配置了 srvRecord
//   2. 执行 DNS SRV 查询获取新的服务器列表
//   3. 比较新旧列表，如有变化则更新
//
// 注意事项：
//   - DNS 查询失败时保持原有 list 不变，仅记录日志
//   - 查询结果为空时也保持原有 list，防止服务中断
//   - 使用 reflect.DeepEqual 精确比较列表变化
//
// 使用场景：
//   - 定期调用（如每分钟）保持服务列表最新
//   - 在连接失败时主动刷新寻找可用服务器
//   - 服务器扩缩容时自动发现新实例
func (sd *ServerDiscovery) RefreshBySrvIfAvailable() {
	// 检查是否配置了 DNS SRV 记录
	// 如果为 nil，说明使用静态地址列表，无需刷新
	if sd.srvRecord == nil {
		return
	}

	// 执行 DNS SRV 查询
	// LookUp() 会解析 SRV 记录并返回所有服务器地址
	newList, err := sd.srvRecord.LookUp()
	if err != nil {
		// DNS 查询失败，记录日志但保持原有列表
		// 这样即使 DNS 临时故障也不会影响服务
		glog.V(0).Infof("failed to lookup SRV for %s: %v", *sd.srvRecord, err)
	}

	// 验证查询结果
	if newList == nil || len(newList) == 0 {
		// 查询结果为空，可能原因：
		// 1. DNS 记录不存在或已删除
		// 2. 所有服务器都已下线
		// 3. DNS 服务器返回错误
		// 保持原有列表以维持服务可用性
		glog.V(0).Infof("looked up SRV for %s, but found no well-formed names", *sd.srvRecord)
		return
	}

	// 比较新旧列表是否相同
	// 使用 DeepEqual 确保地址顺序和内容完全一致
	if !reflect.DeepEqual(sd.list, newList) {
		// 发现变化，更新服务器列表
		// 这会影响后续的 GetInstances() 调用
		sd.list = newList
	}
}

// GetInstances 返回当前已知服务实例地址列表的副本
// 返回的是副本而非原始切片，避免调用方意外修改内部状态
//
// 使用建议：
//   - 在调用此方法前先调用 RefreshBySrvIfAvailable() 获取最新列表
//   - 返回的是快照，后续 Refresh 不会影响已获取的列表
//
// 返回:
//   - addresses: ServerAddress 类型的切片副本
//
// 示例：
//   sd.RefreshBySrvIfAvailable()  // 先刷新
//   instances := sd.GetInstances()  // 再获取
//   for _, addr := range instances {
//       client := connectTo(addr)
//   }
func (sd *ServerDiscovery) GetInstances() (addresses []ServerAddress) {
	// 遍历内部列表，逐个复制到新切片
	// 这样调用方修改返回值不会影响 ServerDiscovery 内部状态
	for _, a := range sd.list {
		addresses = append(addresses, a)
	}
	return addresses
}

// GetInstancesAsStrings 返回服务实例地址的字符串形式列表
// 与 GetInstances() 类似，但返回类型为 []string，方便日志输出和配置文件生成
//
// 返回:
//   - addresses: 字符串类型的地址列表
//
// 使用场景：
//   - 日志输出：glog.Infof("masters: %v", sd.GetInstancesAsStrings())
//   - 配置生成：将地址列表写入配置文件
//   - UI 展示：在 Web 界面显示服务器列表
func (sd *ServerDiscovery) GetInstancesAsStrings() (addresses []string) {
	// 将 ServerAddress 类型转换为 string
	for _, i := range sd.list {
		addresses = append(addresses, string(i))
	}
	return addresses
}

// GetInstancesAsMap 返回服务实例地址的 Map 形式
// key 和 value 都是地址，用于快速检查某个地址是否在列表中
//
// 返回:
//   - addresses: Map[string]ServerAddress，key 为地址字符串，value 为 ServerAddress
//
// 使用场景：
//   - 快速查找：if _, exists := addrMap[targetAddr]; exists { ... }
//   - 去重操作：合并多个服务发现器的结果
//   - 差异对比：比较新旧两个地址集合的差异
//
// 示例：
//   addrMap := sd.GetInstancesAsMap()
//   if _, found := addrMap["192.168.1.10:9333"]; found {
//       fmt.Println("Master found")
//   }
func (sd *ServerDiscovery) GetInstancesAsMap() (addresses map[string]ServerAddress) {
	// 初始化 Map
	addresses = make(map[string]ServerAddress)

	// 将每个地址同时作为 key 和 value 存入 Map
	// 这样可以用 Map 的 O(1) 查找特性快速判断地址是否存在
	for _, i := range sd.list {
		addresses[string(i)] = i
	}
	return addresses
}
