package erasure_coding

import (
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// ShardId 分片 ID 类型，使用 uint8 表示分片编号（0-255）
type ShardId uint8

// EcVolumeShard 表示一个 EC Volume 的单个分片
// EC Volume 将数据分成多个分片存储在不同的服务器上
// 每个分片是一个独立的文件，扩展名为 .ecXX（如 .ec00, .ec01 等）
type EcVolumeShard struct {
	VolumeId    needle.VolumeId // 所属 Volume 的 ID
	ShardId     ShardId          // 分片 ID（0-13，默认配置下）
	Collection  string           // 集合名称
	dir         string           // 分片文件所在目录
	ecdFile     *os.File         // 分片文件句柄
	ecdFileSize int64            // 分片文件大小
	DiskType    types.DiskType   // 磁盘类型
}

// NewEcVolumeShard 创建并加载一个 EC Volume 分片
// 参数:
//   - diskType: 磁盘类型
//   - dirname: 分片文件目录
//   - collection: 集合名称
//   - id: Volume ID
//   - shardId: 分片 ID
// 返回值:
//   - v: EC Volume 分片实例
//   - error: 错误信息（如果文件不存在返回 os.ErrNotExist）
func NewEcVolumeShard(diskType types.DiskType, dirname string, collection string, id needle.VolumeId, shardId ShardId) (v *EcVolumeShard, e error) {

	v = &EcVolumeShard{dir: dirname, Collection: collection, VolumeId: id, ShardId: shardId, DiskType: diskType}

	baseFileName := v.FileName()

	// 打开分片文件（.ecXX）
	if v.ecdFile, e = os.OpenFile(baseFileName+ToExt(int(shardId)), os.O_RDONLY, 0644); e != nil {
		if e == os.ErrNotExist || strings.Contains(e.Error(), "no such file or directory") {
			return nil, os.ErrNotExist
		}
		return nil, fmt.Errorf("cannot read ec volume shard %s%s: %v", baseFileName, ToExt(int(shardId)), e)
	}

	// 获取分片文件大小
	ecdFi, statErr := v.ecdFile.Stat()
	if statErr != nil {
		_ = v.ecdFile.Close()
		return nil, fmt.Errorf("can not stat ec volume shard %s%s: %v", baseFileName, ToExt(int(shardId)), statErr)
	}
	v.ecdFileSize = ecdFi.Size()

	// 挂载分片（更新监控指标）
	v.Mount()

	return
}

// Mount 挂载分片，更新监控指标
// 增加 EC 分片计数器
func (shard *EcVolumeShard) Mount() {
	stats.VolumeServerVolumeGauge.WithLabelValues(shard.Collection, "ec_shards").Inc()
}

// Unmount 卸载分片，更新监控指标
// 减少 EC 分片计数器
func (shard *EcVolumeShard) Unmount() {
	stats.VolumeServerVolumeGauge.WithLabelValues(shard.Collection, "ec_shards").Dec()
}

// Size 返回分片文件的大小
func (shard *EcVolumeShard) Size() int64 {
	return shard.ecdFileSize
}

// String 返回分片的字符串表示
// 格���：ec shard volumeId:shardId, dir:目录, Collection:集合
func (shard *EcVolumeShard) String() string {
	return fmt.Sprintf("ec shard %v:%v, dir:%s, Collection:%s", shard.VolumeId, shard.ShardId, shard.dir, shard.Collection)
}

// FileName 返回分片文件的基础路径（不含 .ecXX 扩展名）
func (shard *EcVolumeShard) FileName() (fileName string) {
	return EcShardFileName(shard.Collection, shard.dir, int(shard.VolumeId))
}

// EcShardFileName 构建 EC 分片文件的基础路径
// 参数:
//   - collection: 集合名称
//   - dir: 目录路径
//   - id: Volume ID
// 返回值:
//   - fileName: 文件基础路径
// 示例:
//   - EcShardFileName("", "/data", 5) -> "/data/5"
//   - EcShardFileName("pics", "/data", 5) -> "/data/pics_5"
func EcShardFileName(collection string, dir string, id int) (fileName string) {
	idString := strconv.Itoa(id)
	if collection == "" {
		fileName = path.Join(dir, idString)
	} else {
		fileName = path.Join(dir, collection+"_"+idString)
	}
	return
}

// EcShardBaseFileName 返回 EC 分片的基础文件名（不含目录）
// 参数:
//   - collection: 集合名称
//   - id: Volume ID
// 返回值:
//   - baseFileName: 基础文件名
// 示例:
//   - EcShardBaseFileName("", 5) -> "5"
//   - EcShardBaseFileName("pics", 5) -> "pics_5"
func EcShardBaseFileName(collection string, id int) (baseFileName string) {
	baseFileName = strconv.Itoa(id)
	if collection != "" {
		baseFileName = collection + "_" + baseFileName
	}
	return
}

// Close 关闭分片文件
func (shard *EcVolumeShard) Close() {
	if shard.ecdFile != nil {
		_ = shard.ecdFile.Close()
		shard.ecdFile = nil
	}
}

// Destroy 销毁分片，删除分片文件
// 先卸载（更新监控指标），然后删除文件
func (shard *EcVolumeShard) Destroy() {
	shard.Unmount()
	os.Remove(shard.FileName() + ToExt(int(shard.ShardId)))
}

// ReadAt 从指定偏移量读取数据到缓冲区
// 参数:
//   - buf: 读取缓冲区
//   - offset: 读取偏移量
// 返回值:
//   - int: 读取的字节数
//   - error: 错误信息（如果到达文件末尾但读取了完整缓冲区，则不返回 EOF 错误）
func (shard *EcVolumeShard) ReadAt(buf []byte, offset int64) (int, error) {

	n, err := shard.ecdFile.ReadAt(buf, offset)
	// 如果读取了完整的缓冲区但遇到 EOF，则清除 EOF 错误
	// 这是��为我们成功读取了请求的所有数据
	if err == io.EOF && n == len(buf) {
		err = nil
	}
	return n, err

}
