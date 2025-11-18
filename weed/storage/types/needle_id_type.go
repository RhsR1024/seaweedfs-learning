// Package types 定义 SeaweedFS 存储层的核心类型
package types

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"strconv"
)

// NeedleId 是 Needle 的唯一标识符
// 对应文件 ID (fid) 中的 key 部分
//
// 组成说明:
//   - 64 位无符号整数
//   - 由 Master 节点分配和管理
//   - 在单个 Volume 内唯一
//
// fid 格式: volumeId,needleId,cookie
//   例如: 3,01637037d6,cookie
//   其中 01637037d6 就是 NeedleId 的 16 进制表示
//
// 特点:
//   - 单调递增: 便于索引和范围查询
//   - 全局唯一: Volume ID + Needle ID 组合全局唯一
//   - 紧凑存储: 8 字节可表示海量文件
type NeedleId uint64

// 常量定义
const (
	NeedleIdSize  = 8 // NeedleId 的字节数 (uint64)
	NeedleIdEmpty = 0 // 空 NeedleId,表示无效或未分配
)

// NeedleIdToBytes 将 NeedleId 序列化为字节数组
// 使用大端序编码
//
// 参数:
//   - bytes: 目标字节数组,至少 8 字节
//   - needleId: 要序列化的 NeedleId
//
// 使用场景:
//   - 写入索引文件 (.idx)
//   - 存储到 LevelDB
//   - 网络传输
func NeedleIdToBytes(bytes []byte, needleId NeedleId) {
	util.Uint64toBytes(bytes, uint64(needleId))
}

// NeedleIdToUint64 将 NeedleId 转换为 uint64
// 用于向 Master 发送最大 Needle ID
//
// 参数:
//   - needleId: 要转换的 NeedleId
//
// 返回值:
//   - uint64: 转换后的值
//
// 使用场景:
//   - Volume Server 向 Master 报告 Volume 状态
//   - 统计和监控
func NeedleIdToUint64(needleId NeedleId) uint64 {
	return uint64(needleId)
}

// Uint64ToNeedleId 将 uint64 转换为 NeedleId
//
// 参数:
//   - needleId: uint64 类型的 Needle ID 值
//
// 返回值:
//   - NeedleId: 转换后的 NeedleId 类型
//
// 使用场景:
//   - 从 Master 接收新分配的 Needle ID
//   - 解析配置或请求参数
func Uint64ToNeedleId(needleId uint64) NeedleId {
	return NeedleId(needleId)
}

// BytesToNeedleId 从字节数组反序列化 NeedleId
// 使用大端序解码
//
// 参数:
//   - bytes: 源字节数组,至少 8 字节
//
// 返回值:
//   - NeedleId: 解析出的 NeedleId
//
// 使用场景:
//   - 从索引文件加载
//   - 从 LevelDB 读取
//   - 解析网络数据
func BytesToNeedleId(bytes []byte) NeedleId {
	return NeedleId(util.BytesToUint64(bytes))
}

// String 将 NeedleId 转换为 16 进制字符串
// 用于日志输出和 URL 构造
//
// 返回值:
//   - string: 16 进制格式的字符串 (如 "1637037d6")
//
// 示例:
//   nid := NeedleId(23456789462)
//   fmt.Println(nid.String())  // 输出: "1637037d6"
func (k NeedleId) String() string {
	return strconv.FormatUint(uint64(k), 16)
}

// FileId 根据 NeedleId 和 VolumeId 生成完整的文件 ID
// 生成的 fid 格式: volumeId,needleId,00000000 (cookie 占位符)
//
// 参数:
//   - volumeId: Volume ID
//
// 返回值:
//   - string: 完整的文件 ID 字符串
//
// 注意:
//   - cookie 部分使用 00000000 占位,实际使用时需要替换为真实 cookie
//   - 这个方法主要用于内部测试和调试
//
// 示例:
//   nid := NeedleId(23456789462)
//   fid := nid.FileId(3)  // 返回 "3,1637037d6,00000000"
func (k NeedleId) FileId(volumeId uint32) string {
	return fmt.Sprintf("%d,%s00000000", volumeId, k.String())
}

// ParseNeedleId 从 16 进制字符串解析 NeedleId
// 用于解析 URL 或 API 请求中的 Needle ID
//
// 参数:
//   - idString: 16 进制格式的 ID 字符串 (如 "1637037d6")
//
// 返回值:
//   - NeedleId: 解析出的 NeedleId
//   - error: 解析失败时返回错误
//
// 示例:
//   nid, err := ParseNeedleId("1637037d6")
//   if err != nil {
//       // 处理解析错误
//   }
func ParseNeedleId(idString string) (NeedleId, error) {
	key, err := strconv.ParseUint(idString, 16, 64)
	if err != nil {
		return 0, fmt.Errorf("needle id %s format error: %v", idString, err)
	}
	return NeedleId(key), nil
}
