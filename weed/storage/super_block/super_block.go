package super_block

import (
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	// SuperBlockSize 超级块的固定大小(字节)
	// 每个 volume 文件的开头都有 8 字节的超级块
	SuperBlockSize = 8
)

/*
 * SuperBlock 超级块结构
 *
 * 超级块是每个 volume 文件开头的元数据区域,当前固定为 8 字节
 * 它存储了 volume 的关键配置信息,用于初始化和验证 volume
 *
 * 字节布局(基础 8 字节):
 * - Byte 0: 版本号(Version),目前支持 1, 2, 3
 * - Byte 1: 副本放置策略(ReplicaPlacement),如 000, 001, 010, 100 等
 * - Byte 2-3: 生存时间(TTL),定义数据的过期策略
 * - Byte 4-5: 压缩修订版本(CompactionRevision),记录 volume 被压缩的次数
 * - Byte 6-7: 扩展数据大小(ExtraSize),指示额外数据的字节数
 * - 如果有扩展数据: 后续字节存储 protobuf 序列化的 Extra 信息
 */
type SuperBlock struct {
	// Version needle 格式版本
	// Version 1: 基础版本
	// Version 2: 支持扩展数据
	// Version 3: 最新版本,增加了更多特性
	Version needle.Version

	// ReplicaPlacement 副本放置策略
	// 定义数据副本在集群中的分布方式(数据中心、机架、节点级别)
	ReplicaPlacement *ReplicaPlacement

	// Ttl 数据生存时间
	// 定义数据的过期时间,过期后可以被自动清理
	// nil 表示数据永不过期
	Ttl *needle.TTL

	// CompactionRevision 压缩修订版本号
	// 每次对 volume 执行压缩操作时递增
	// 用于跟踪 volume 的历史变更和同步状态
	CompactionRevision uint16

	// Extra 扩展元数据
	// 使用 protobuf 存储额外的配置信息
	// 仅在 Version >= 2 时有效
	Extra *master_pb.SuperBlockExtra

	// ExtraSize 扩展数据的大小(字节数)
	// 指示 Extra 字段序列化后的字节长度
	// 最大值为 65534 (256*256-2)
	ExtraSize uint16
}

// BlockSize 返回超级块的实际大小(字节)
// 基础大小为 8 字节,如果有扩展数据则加上扩展数据的大小
//
// 返回值:
//   - Version 2 或 3: SuperBlockSize(8) + ExtraSize
//   - 其他版本: SuperBlockSize(8)
func (s *SuperBlock) BlockSize() int {
	switch s.Version {
	case needle.Version2, needle.Version3:
		// 新版本支持扩展数据,需要加上扩展数据的大小
		return SuperBlockSize + int(s.ExtraSize)
	}
	// 旧版本固定为 8 字节
	return SuperBlockSize
}

// Bytes 将超级块序列化为字节数组
// 生成的字节数组会被写入到 volume 文件的开头
//
// 序列化格式:
//   - 前 8 字节:固定的基础超级块数据
//   - 后续字节(如果有):protobuf 序列化的扩展数据
//
// 返回: 序列化后的字节数组
func (s *SuperBlock) Bytes() []byte {
	// 分配基础 8 字节的缓冲区
	header := make([]byte, SuperBlockSize)

	// Byte 0: 版本号
	header[0] = byte(s.Version)

	// Byte 1: 副本放置策略
	header[1] = s.ReplicaPlacement.Byte()

	// Byte 2-3: TTL (生存时间)
	s.Ttl.ToBytes(header[2:4])

	// Byte 4-5: 压缩修订版本号
	util.Uint16toBytes(header[4:6], s.CompactionRevision)

	// 如果有扩展数据,进行序列化并追加
	if s.Extra != nil {
		// 使用 protobuf 序列化扩展数据
		extraData, err := proto.Marshal(s.Extra)
		if err != nil {
			glog.Fatalf("cannot marshal super block extra %+v: %v", s.Extra, err)
		}
		extraSize := len(extraData)

		// 验证扩展数据大小不超过限制
		// 预留 2 字节用于未来扩展
		if extraSize > 256*256-2 {
			// reserve a couple of bits for future extension
			glog.Fatalf("super block extra size is %d bigger than %d", extraSize, 256*256-2)
		}

		// Byte 6-7: 扩展数据大小
		s.ExtraSize = uint16(extraSize)
		util.Uint16toBytes(header[6:8], s.ExtraSize)

		// 将扩展数据追加到 header 后面
		header = append(header, extraData...)
	}

	return header
}

// Initialized 检查超级块是否已正确初始化
// 一个有效的超级块必须同时设置了副本策略和 TTL
//
// 返回:
//   - true: 超级块已初始化,可以正常使用
//   - false: 超级块未初始化或数据不完整
func (s *SuperBlock) Initialized() bool {
	return s.ReplicaPlacement != nil && s.Ttl != nil
}
