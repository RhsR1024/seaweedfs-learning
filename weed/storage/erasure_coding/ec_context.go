// Package erasure_coding 实现了 SeaweedFS 的纠删码(Erasure Coding)功能
// 纠删码是一种数据冗余技术，将数据分成多个分片(shard)并添加校验分片
// 即使部分分片丢失，也能通过剩余分片恢复原始数据
//
// 核心概念:
//   - 数据分片(Data Shards): 存储原始数据的分片
//   - 校验分片(Parity Shards): 存储冗余校验信息的分片
//   - Reed-Solomon 编码: 使用的纠删码算法
//
// 默认配置: 10 数据分片 + 4 校验分片 (10+4)
//   - 可以容忍最多 4 个分片丢失
//   - 存储开销: 140% (14个分片存储10个分片的数据)
//   - 相比3副本(300%开销)节省存储空间
package erasure_coding

import (
	"fmt"

	"github.com/klauspost/reedsolomon"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// ECContext 封装了纠删码编码/解码操作的参数上下文
// 用于在编码和解码过程中传递配置信息
type ECContext struct {
	DataShards   int             // 数据分片数量（存储原始数据的分片数）
	ParityShards int             // 校验分片数量（存储冗余校验信息的分片数）
	Collection   string          // 集合名称（用于逻辑分组）
	VolumeId     needle.VolumeId // Volume ID
}

// Total 返回分片总数（数据分片 + 校验分片）
// 例如：10 数据分片 + 4 校验分片 = 14 总分片数
func (ctx *ECContext) Total() int {
	return ctx.DataShards + ctx.ParityShards
}

// NewDefaultECContext 创建一个使用默认配置的 EC 上下文
// 默认配置: 10 数据分片 + 4 校验分片 (10+4)
//
// 参数:
//   - collection: 集合名称
//   - volumeId: Volume ID
// 返回值:
//   - *ECContext: 使用默认配置的 EC 上下文
func NewDefaultECContext(collection string, volumeId needle.VolumeId) *ECContext {
	return &ECContext{
		DataShards:   DataShardsCount,   // 默认 10 个数据分片
		ParityShards: ParityShardsCount, // 默认 4 个校验分片
		Collection:   collection,
		VolumeId:     volumeId,
	}
}

// CreateEncoder 创建一个 Reed-Solomon 编码器
// Reed-Solomon 是一种纠删码算法，用于生成校验分片
//
// 返回值:
//   - reedsolomon.Encoder: Reed-Solomon 编码器实例
//   - error: 创建失败时返回错误
// 使用示例:
//   enc, err := ctx.CreateEncoder()
//   if err != nil {
//       return err
//   }
//   // 使用编码器对数据进行编码
//   err = enc.Encode(dataShards)
func (ctx *ECContext) CreateEncoder() (reedsolomon.Encoder, error) {
	return reedsolomon.New(ctx.DataShards, ctx.ParityShards)
}

// ToExt 返回指定分片索引的文件扩展名
// 参数:
//   - shardIndex: 分片索引（0 到 总分片数-1）
// 返回值:
//   - string: 分片文件扩展名
// 示例:
//   - shardIndex=0  -> ".ec00"  (第1个数据分片)
//   - shardIndex=9  -> ".ec09"  (第10个数据分片)
//   - shardIndex=10 -> ".ec10"  (第1个校验分片)
//   - shardIndex=13 -> ".ec13"  (第4个校验分片)
func (ctx *ECContext) ToExt(shardIndex int) string {
	return fmt.Sprintf(".ec%02d", shardIndex)
}

// String 返回 EC 配置的人类可读表示
// 返回值:
//   - string: 格式为 "数据分片数+校验分片数 (total: 总数)"
// 示例:
//   - "10+4 (total: 14)"  - 默认配置
//   - "6+3 (total: 9)"    - 自定义配置
func (ctx *ECContext) String() string {
	return fmt.Sprintf("%d+%d (total: %d)", ctx.DataShards, ctx.ParityShards, ctx.Total())
}
