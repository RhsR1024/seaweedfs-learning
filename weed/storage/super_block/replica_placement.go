package super_block

import (
	"fmt"
)

// ReplicaPlacement 副本放置策略结构
// 定义了数据副本在集群中的分布规则,用于保证数据的高可用性和容错性
//
// 副本放置策略采用三位数字表示法(例如 "001", "010", "100"):
// - 第一位:不同数据中心(DataCenter)的副本数
// - 第二位:不同机架(Rack)的副本数
// - 第三位:相同机架内不同节点(Node)的副本数
//
// 示例:
// - "000": 无副本,只有一份数据
// - "001": 同机架内 1 个副本(共 2 份数据)
// - "010": 不同机架 1 个副本(共 2 份数据)
// - "100": 不同数据中心 1 个副本(共 2 份数据)
// - "200": 不同数据中心 2 个副本(共 3 份数据)
type ReplicaPlacement struct {
	// SameRackCount 相同机架内的副本数
	// 这些副本位于同一个机架的不同节点上
	// JSON 标签为 "node",表示节点级别的冗余
	SameRackCount int `json:"node,omitempty"`

	// DiffRackCount 不同机架的副本数
	// 这些副本位于同一数据中心的不同机架上
	// 提供机架级别的容错能力
	DiffRackCount int `json:"rack,omitempty"`

	// DiffDataCenterCount 不同数据中心的副本数
	// 这些副本分布在不同的数据中心
	// 提供数据中心级别的容错能力,是最高级别的冗余
	DiffDataCenterCount int `json:"dc,omitempty"`
}

// NewReplicaPlacementFromString 从字符串解析副本放置策略
// 输入字符串格式为 1-3 位数字,例如 "1", "01", "001", "100", "200"
//
// 参数 t: 副本策略字符串
//   - 空字符串 "": 默认为 "000" (无副本)
//   - 单字符 "1": 补齐为 "001" (同机架 1 副本)
//   - 双字符 "10": 补齐为 "010" (不同机架 1 副本)
//   - 三字符 "100": 完整格式 (不同数据中心 1 副本)
//
// 返回:
//   - *ReplicaPlacement: 解析后的副本策略对象
//   - error: 如果格式非法或值超出范围,返回错误
func NewReplicaPlacementFromString(t string) (*ReplicaPlacement, error) {
	rp := &ReplicaPlacement{}

	// 将输入字符串补齐为 3 位
	switch len(t) {
	case 0:
		t = "000" // 空字符串默认无副本
	case 1:
		t = "00" + t // 单字符,补齐为同机架副本
	case 2:
		t = "0" + t // 双字符,补齐为不同机架副本
	}

	// 解析每一位数字
	for i, c := range t {
		count := int(c - '0') // 将字符转换为数字
		if count < 0 {
			return rp, fmt.Errorf("unknown replication type: %s", t)
		}
		// 根据位置设置对应的副本数
		switch i {
		case 0:
			rp.DiffDataCenterCount = count // 第一位:不同数据中心副本数
		case 1:
			rp.DiffRackCount = count // 第二位:不同机架副本数
		case 2:
			rp.SameRackCount = count // 第三位:同机架副本数
		}
	}

	// 验证副本策略的合法性
	// 使用加权值检查:数据中心*100 + 机架*10 + 节点
	// 最大值不能超过 255(一个字节能表示的最大值)
	value := rp.DiffDataCenterCount*100 + rp.DiffRackCount*10 + rp.SameRackCount
	if value > 255 {
		return rp, fmt.Errorf("unexpected replication type: %s", t)
	}
	return rp, nil
}

// NewReplicaPlacementFromByte 从字节值创建副本放置策略
// 将字节值转换为三位数字字符串,然后调用 NewReplicaPlacementFromString 解析
//
// 参数 b: 字节值,例如 1 会被解析为 "001"
// 返回: 副本策略对象和可能的错误
func NewReplicaPlacementFromByte(b byte) (*ReplicaPlacement, error) {
	// 使用 %03d 格式化为 3 位数字字符串,不足 3 位前面补 0
	return NewReplicaPlacementFromString(fmt.Sprintf("%03d", b))
}

// HasReplication 判断是否配置了副本
// 如果三个计数器都为 0,则表示没有配置副本(只有一份数据)
// 返回: true 表示有副本配置,false 表示无副本
func (rp *ReplicaPlacement) HasReplication() bool {
	return rp.DiffDataCenterCount != 0 || rp.DiffRackCount != 0 || rp.SameRackCount != 0
}

// Equals 比较两个副本放置策略是否相同
// 比较三个维度的副本数是否完全一致
//
// 参数 b: 要比较的另一个副本策略对象
// 返回: true 表示相同,false 表示不同或任一为 nil
func (a *ReplicaPlacement) Equals(b *ReplicaPlacement) bool {
	if a == nil || b == nil {
		return false
	}
	return (a.SameRackCount == b.SameRackCount &&
		a.DiffRackCount == b.DiffRackCount &&
		a.DiffDataCenterCount == b.DiffDataCenterCount)
}

// Byte 将副本放置策略转换为字节值
// 使用加权公式计算:数据中心*100 + 机架*10 + 节点
// 例如 "210" 会被转换为 210
//
// 返回: 表示副本策略的字节值,如果对象为 nil 则返回 0
func (rp *ReplicaPlacement) Byte() byte {
	if rp == nil {
		return 0
	}
	ret := rp.DiffDataCenterCount*100 + rp.DiffRackCount*10 + rp.SameRackCount
	return byte(ret)
}

// String 将副本放置策略转换为三位数字字符串
// 例如 DiffDataCenterCount=1, DiffRackCount=0, SameRackCount=2 会返回 "102"
//
// 返回: 三位数字字符串表示,每位范围 0-9
func (rp *ReplicaPlacement) String() string {
	b := make([]byte, 3)
	b[0] = byte(rp.DiffDataCenterCount + '0') // 第一位:数据中心
	b[1] = byte(rp.DiffRackCount + '0')       // 第二位:机架
	b[2] = byte(rp.SameRackCount + '0')       // 第三位:节点
	return string(b)
}

// GetCopyCount 获取总的数据副本数(包括原始数据)
// 计算公式:数据中心副本数 + 机架副本数 + 节点副本数 + 1(原始数据)
//
// 示例:
// - "000": 返回 1(只有原始数据,无副本)
// - "001": 返回 2(原始数据 + 1 个同机架副本)
// - "100": 返回 2(原始数据 + 1 个不同数据中心副本)
// - "200": 返回 3(原始数据 + 2 个不同数据中心副本)
//
// 返回: 数据副本的总数量
func (rp *ReplicaPlacement) GetCopyCount() int {
	return rp.DiffDataCenterCount + rp.DiffRackCount + rp.SameRackCount + 1
}
