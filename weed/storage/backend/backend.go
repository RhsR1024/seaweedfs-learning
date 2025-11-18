// Package backend 提供 SeaweedFS 的后端存储抽象层
//
// 该包定义了统一的后端存储接口，支持多种存储后端实现：
// - 本地磁盘存储 (disk_file)
// - Amazon S3
// - 阿里云 OSS
// - 腾讯云 COS
// - Google Cloud Storage
// - Azure Blob Storage
//
// 通过抽象层，SeaweedFS 可以无缝切换不同的存储后端，
// 实现数据的冷热分层存储和跨云存储。
package backend

import (
	"github.com/seaweedfs/seaweedfs/weed/util"
	"io"
	"os"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

// BackendStorageFile 后端存储文件接口
//
// 该接口定义了存储文件的基本操作，所有后端存储实现都需要实现此接口。
// 接口设计参考了标准库的 io 包，提供类似文件系统的操作语义。
//
// 实现示例：
// - DiskFile: 本地磁盘文件
// - S3BackendStorageFile: S3 对象存储文件
// - OSSBackendStorageFile: 阿里云 OSS 文件
type BackendStorageFile interface {
	// io.ReaderAt 在指定偏移量读取数据
	// 支持并发读取，线程安全
	io.ReaderAt

	// io.WriterAt 在指定偏移量写入数据
	// 支持随机写入和追加写入
	io.WriterAt

	// Truncate 截断文件到指定大小
	// off: 目标文件大小
	// 如果 off < 当前大小，截断文件；如果 off > 当前大小，扩展文件
	Truncate(off int64) error

	// io.Closer 关闭文件，释放资源
	// 对于云存储后端，可能会触发最终的数据上传
	io.Closer

	// GetStat 获取文件统计信息
	// 返回：
	//   datSize: 文件数据大小（字节）
	//   modTime: 最后修改时间
	//   err: 错误信息
	GetStat() (datSize int64, modTime time.Time, err error)

	// Name 返回文件名称或存储键
	// 对于本地文件返回文件路径，对于云存储返回对象键
	Name() string

	// Sync 将文件数据同步到持久化存储
	// 对于本地文件调用 fsync，对于云存储可能触发数据上传
	Sync() error
}

// BackendStorage 后端存储接口
//
// 该接口定义了存储后端的核心操作，包括文件的创建、上传、下载和删除。
// 每个存储后端（如 S3、OSS）都需要实现此接口。
//
// 使用场景：
// 1. Volume 数据分层存储（热数据 -> 冷数据）
// 2. Volume 数据迁移到云存储
// 3. Volume 数据备份和恢复
type BackendStorage interface {
	// ToProperties 将存储配置转换为属性映射
	// 返回包含存储后端配置信息的 map，用于序列化和传输
	ToProperties() map[string]string

	// NewStorageFile 创建新的存储文件对象
	// 参数：
	//   key: 存储键/对象名称（如 "volume_1.dat"）
	//   tierInfo: Volume 层级信息，包含存储类型和配置
	// 返回：BackendStorageFile 接口实例
	//
	// 注意：此方法只创建对象，不会立即创建物理文件
	NewStorageFile(key string, tierInfo *volume_server_pb.VolumeInfo) BackendStorageFile

	// CopyFile 将本地文件复制到后端存储
	// 参数：
	//   f: 源文件句柄
	//   fn: 进度回调函数，参数为已传输字节数和完成百分比
	// 返回：
	//   key: 存储键/对象名称
	//   size: 文件大小（字节）
	//   err: 错误信息
	//
	// 使用示例：
	//   backend.CopyFile(file, func(progressed int64, percentage float32) error {
	//       log.Printf("上传进度: %d bytes (%.2f%%)", progressed, percentage)
	//       return nil
	//   })
	CopyFile(f *os.File, fn func(progressed int64, percentage float32) error) (key string, size int64, err error)

	// DownloadFile 从后端存储下载文件到本地
	// 参数：
	//   fileName: 本地文件路径
	//   key: 远程存储键/对象名称
	//   fn: 进度回调函数，参数为已下载字节数和完成百分比
	// 返回：
	//   size: 文件大小（字节）
	//   err: 错误信息
	DownloadFile(fileName string, key string, fn func(progressed int64, percentage float32) error) (size int64, err error)

	// DeleteFile 从后端存储删除文件
	// 参数：
	//   key: 存储键/对象名称
	// 返回：错误信息
	DeleteFile(key string) (err error)
}

// StringProperties 字符串属性访问接口
//
// 用于从配置源（如 Viper 配置、属性 map）中读取字符串属性。
// 这是一个轻量级的配置抽象接口。
type StringProperties interface {
	// GetString 获取指定键的字符串值
	// 如果键不存在，返回空字符串
	GetString(key string) string
}

// StorageType 存储类型别名
//
// 常见的存储类型：
// - "s3": Amazon S3 或 S3 兼容存储
// - "oss": 阿里云 OSS
// - "cos": 腾讯云 COS
// - "gcs": Google Cloud Storage
// - "azure": Azure Blob Storage
type StorageType string

// BackendStorageFactory 后端存储工厂接口
//
// 每种存储类型都需要实现一个工厂，用于创建存储实例。
// 工厂模式使得可以动态注册和创建不同类型的存储后端。
type BackendStorageFactory interface {
	// StorageType 返回存储类型标识
	StorageType() StorageType

	// BuildStorage 根据配置构建存储实例
	// 参数：
	//   configuration: 配置源接口
	//   configPrefix: 配置键前缀（如 "storage.backend.s3.default."）
	//   id: 存储实例 ID（如 "default", "backup"）
	// 返回：
	//   BackendStorage: 存储实例
	//   error: 构建错误
	BuildStorage(configuration StringProperties, configPrefix string, id string) (BackendStorage, error)
}

var (
	// BackendStorageFactories 全局存储工厂注册表
	// key: StorageType (如 "s3", "oss")
	// value: BackendStorageFactory 实现
	//
	// 各存储后端通过 init() 函数注册自己的工厂：
	//   func init() {
	//       backend.BackendStorageFactories["s3"] = &S3StorageFactory{}
	//   }
	BackendStorageFactories = make(map[StorageType]BackendStorageFactory)

	// BackendStorages 全局存储实例缓存
	// key: "type.id" 格式（如 "s3.default", "oss.backup"）
	// value: BackendStorage 实例
	//
	// 特殊情况：
	// - "type.default" 会同时注册到 "type" 键
	// - 例如 "s3.default" 同时注册为 "s3" 和 "s3.default"
	BackendStorages = make(map[string]BackendStorage)
)

// LoadConfiguration 从配置文件加载后端存储配置（Master 节点使用）
//
// 该函数在 Master 节点启动时调用，从 Viper 配置中读取所有后端存储配置，
// 并创建相应的存储实例。
//
// 配置文件格式示例（TOML）：
//   [storage.backend.s3.default]
//   enabled = true
//   aws_access_key_id = "YOUR_ACCESS_KEY"
//   aws_secret_access_key = "YOUR_SECRET_KEY"
//   region = "us-east-1"
//   bucket = "my-bucket"
//
//   [storage.backend.oss.backup]
//   enabled = true
//   endpoint = "oss-cn-hangzhou.aliyuncs.com"
//   access_key_id = "YOUR_ACCESS_KEY"
//   access_key_secret = "YOUR_SECRET"
//   bucket = "my-oss-bucket"
//
// 参数：
//   config: Viper 配置代理对象
//
// 加载流程：
//   1. 遍历 storage.backend 下的所有类型（s3, oss, cos 等）
//   2. 对于每个类型，查找对应的工厂
//   3. 遍历该类型下的所有实例 ID（default, backup 等）
//   4. 检查 enabled 配置，跳过未启用的实例
//   5. 使用工厂创建存储实例
//   6. 将实例注册到 BackendStorages 全局映射
//   7. 如果 ID 为 "default"，同时注册简化名称（如 "s3"）
func LoadConfiguration(config *util.ViperProxy) {

	StorageBackendPrefix := "storage.backend"

	// 遍历所有后端存储类型（s3, oss, cos 等）
	for backendTypeName := range config.GetStringMap(StorageBackendPrefix) {
		backendStorageFactory, found := BackendStorageFactories[StorageType(backendTypeName)]
		if !found {
			glog.Fatalf("backend storage type %s not found", backendTypeName)
		}
		// 遍历该类型下的所有存储实例 ID
		for backendStorageId := range config.GetStringMap(StorageBackendPrefix + "." + backendTypeName) {
			// 检查是否启用该存储实例
			if !config.GetBool(StorageBackendPrefix + "." + backendTypeName + "." + backendStorageId + ".enabled") {
				continue
			}
			// 避免重复注册
			if _, found := BackendStorages[backendTypeName+"."+backendStorageId]; found {
				continue
			}
			// 使用工厂构建存储实例
			backendStorage, buildErr := backendStorageFactory.BuildStorage(config,
				StorageBackendPrefix+"."+backendTypeName+"."+backendStorageId+".", backendStorageId)
			if buildErr != nil {
				glog.Fatalf("fail to create backend storage %s.%s", backendTypeName, backendStorageId)
			}
			// 注册存储实例到全局映射
			BackendStorages[backendTypeName+"."+backendStorageId] = backendStorage
			// 如果是 default 实例，同时注册简化名称
			// 例如 "s3.default" 同时注册为 "s3"
			if backendStorageId == "default" {
				BackendStorages[backendTypeName] = backendStorage
			}
		}
	}

}

// LoadFromPbStorageBackends 从 Protobuf 消息加载后端存储配置（Volume Server 使用）
//
// 该函数在 Volume Server 启动时调用，从 Master 节点接收后端存储配置。
// Master 通过 gRPC 将配置推送给 Volume Server，避免在每个节点重复配置。
//
// 与 LoadConfiguration 的区别：
// - LoadConfiguration: Master 从配置文件加载
// - LoadFromPbStorageBackends: Volume Server 从 Master 接收
//
// 参数：
//   storageBackends: Master 推送的存储后端配置列表
//
// 每个 StorageBackend 包含：
//   - Type: 存储类型（如 "s3", "oss"）
//   - Id: 实例 ID（如 "default", "backup"）
//   - Properties: 配置属性映射（endpoint, bucket, credentials 等）
//
// 处理流程：
//   1. 遍历所有推送的存储后端配置
//   2. 查找对应的存储工厂
//   3. 如果已存在相同实例，跳过（避免重复创建）
//   4. 使用工厂和属性创建存储实例
//   5. 注册到全局映射
func LoadFromPbStorageBackends(storageBackends []*master_pb.StorageBackend) {

	for _, storageBackend := range storageBackends {
		backendStorageFactory, found := BackendStorageFactories[StorageType(storageBackend.Type)]
		if !found {
			glog.Warningf("storage type %s not found", storageBackend.Type)
			continue
		}
		// 避免重复注册
		if _, found := BackendStorages[storageBackend.Type+"."+storageBackend.Id]; found {
			continue
		}
		// 使用属性映射构建存储实例（不需要配置前缀）
		backendStorage, buildErr := backendStorageFactory.BuildStorage(newProperties(storageBackend.Properties), "", storageBackend.Id)
		if buildErr != nil {
			glog.Fatalf("fail to create backend storage %s.%s", storageBackend.Type, storageBackend.Id)
		}
		// 注册存储实例
		BackendStorages[storageBackend.Type+"."+storageBackend.Id] = backendStorage
		// 如果是 default 实例，同时注册简化名称
		if storageBackend.Id == "default" {
			BackendStorages[storageBackend.Type] = backendStorage
		}
	}
}

// Properties 属性映射实现
//
// 实现 StringProperties 接口，用于从 map 中读取配置属性。
// 这是一个简单的包装器，用于适配不同的配置源。
type Properties struct {
	m map[string]string // 属性键值对映射
}

// newProperties 创建新的属性对象
//
// 参数：
//   m: 属性键值对映射
// 返回：Properties 实例指针
func newProperties(m map[string]string) *Properties {
	return &Properties{m: m}
}

// GetString 获取指定键的字符串值
//
// 实现 StringProperties 接口。
// 如果键不存在，返回空字符串（而不是报错）。
//
// 参数：
//   key: 属性键
// 返回：属性值，如果不存在返回空字符串
func (p *Properties) GetString(key string) string {
	if v, found := p.m[key]; found {
		return v
	}
	return ""
}

// ToPbStorageBackends 将全局存储实例转换为 Protobuf 消息列表
//
// 该函数将 BackendStorages 全局映射中的所有存储实例转换为
// Protobuf 消息格式，用于在 Master 和 Volume Server 之间传输配置。
//
// 返回：StorageBackend 消息列表，包含所有已配置的存储后端
//
// 消息内容：
//   - Type: 存储类型（如 "s3", "oss"）
//   - Id: 实例 ID（如 "default", "backup"）
//   - Properties: 存储配置属性映射
//
// 使用场景：
//   - Master 节点推送配置到 Volume Server
//   - 集群状态同步和配置备份
//
// 注意：
//   - 跳过无法解析的存储名称（如简化名称 "s3"）
//   - 只返回完整名称的存储实例（如 "s3.default"）
func ToPbStorageBackends() (backends []*master_pb.StorageBackend) {
	for sName, s := range BackendStorages {
		sType, sId := BackendNameToTypeId(sName)
		// 跳过无法解析的名称（如简化名称）
		if sType == "" {
			continue
		}
		backends = append(backends, &master_pb.StorageBackend{
			Type:       sType,
			Id:         sId,
			Properties: s.ToProperties(),
		})
	}
	return
}

// BackendNameToTypeId 解析后端存储名称为类型和 ID
//
// 后端存储名称有两种格式：
// 1. 完整格式："type.id"（如 "s3.default", "oss.backup"）
// 2. 简化格式："type"（如 "s3", "oss"）
//
// 参数：
//   backendName: 后端存储名称
//
// 返回：
//   backendType: 存储类型（如 "s3", "oss"）
//   backendId: 实例 ID（如 "default", "backup"）
//
// 解析规则：
//   - "s3.default" -> ("s3", "default")
//   - "s3" -> ("s3", "default")  // 简化格式默认为 "default"
//   - "s3.backup.extra" -> ("", "")  // 无效格式，返回空字符串
//
// 示例：
//   type1, id1 := BackendNameToTypeId("s3.default")
//   // type1 = "s3", id1 = "default"
//
//   type2, id2 := BackendNameToTypeId("oss")
//   // type2 = "oss", id2 = "default"
func BackendNameToTypeId(backendName string) (backendType, backendId string) {
	parts := strings.Split(backendName, ".")
	// 简化格式：只有类型名称
	if len(parts) == 1 {
		return backendName, "default"
	}
	// 无效格式：超过两个部分
	if len(parts) != 2 {
		return
	}

	// 完整格式：type.id
	backendType, backendId = parts[0], parts[1]
	return
}
