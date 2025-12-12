// Package s3_backend 实现基于 AWS S3 的远程存储后端
//
// AWS S3（Simple Storage Service）是一个高可用、高耐久的对象存储服务
// 本包将 S3 集成到 SeaweedFS 中，作为 Volume 数据的远程存储层
//
// 核心概念：
//   1. Bucket：S3 的存储桶，用于组织和管理对象
//   2. Key：对象在 Bucket 中的唯一标识符（路径）
//   3. Storage Class：存储类别（STANDARD、STANDARD_IA、GLACIER 等）
//   4. Region：AWS 数据中心区域（如 us-east-1、ap-southeast-1）
//
// 支持的存储类别：
//   - STANDARD：标准存储，高性能、高可用
//   - STANDARD_IA：低频访问存储，成本较低
//   - INTELLIGENT_TIERING：智能分层存储
//   - GLACIER：归档存储，成本最低但访问延迟高
//
// 兼容性：
//   支持所有兼容 S3 API 的对象存储服务（如 MinIO、Ceph、阿里云 OSS 等）
//
// 配置示例：
//   [storage.backend.s3]
//   aws_access_key_id = "AKIAIOSFODNN7EXAMPLE"
//   aws_secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
//   region = "us-east-1"
//   bucket = "my-seaweedfs-bucket"
//   endpoint = ""                          # 可选，自定义 endpoint（如 MinIO）
//   storage_class = "STANDARD_IA"          # 可选，默认 STANDARD_IA
//   force_path_style = true                # 可选，使用路径风格 URL（兼容 MinIO）
package s3_backend

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/google/uuid"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
)

// init 函数在包加载时自动执行，注册 S3 后端工厂
func init() {
	backend.BackendStorageFactories["s3"] = &S3BackendFactory{}
}

// S3BackendFactory 实现 BackendStorageFactory 接口
// 负责创建 S3 后端存储实例
type S3BackendFactory struct {
}

// StorageType 返回存储类型标识符 "s3"
func (factory *S3BackendFactory) StorageType() backend.StorageType {
	return backend.StorageType("s3")
}

// BuildStorage 根据配置创建 S3 后端存储实例
//
// 参数:
//   - configuration: 配置属性集合
//   - configPrefix: 配置前缀（如 "storage.backend.s3."）
//   - id: 后端存储的唯一标识符
// 返回:
//   - backend.BackendStorage: 创建的存储实例
//   - error: 创建失败时的错误
func (factory *S3BackendFactory) BuildStorage(configuration backend.StringProperties, configPrefix string, id string) (backend.BackendStorage, error) {
	return newS3BackendStorage(configuration, configPrefix, id)
}

// S3BackendStorage 表示一个 S3 远程存储后端
//
// 字段说明：
//   - id: 后端存储的唯一标识符
//   - aws_access_key_id: AWS 访问密钥 ID（用于身份验证）
//   - aws_secret_access_key: AWS 访问密钥 Secret（用于身份验证）
//   - region: AWS 区域（如 us-east-1、ap-southeast-1）
//   - bucket: S3 存储桶名称
//   - endpoint: 自定义 endpoint（可选，用于兼容 S3 的服务如 MinIO）
//   - storageClass: 存储类别（STANDARD、STANDARD_IA、GLACIER 等）
//   - forcePathStyle: 是否使用路径风格 URL（兼容 MinIO 等服务）
//   - conn: S3 API 客户端连接
//
// 设计要点：
//   - 支持所有兼容 S3 API 的对象存储服务
//   - 默认使用 STANDARD_IA 存储类别（低频访问，成本较低）
//   - 通过 forcePathStyle 兼容 MinIO 等非 AWS 服务
//
// URL 风格说明：
//   - 虚拟主机风格：http://bucket.s3.amazonaws.com/key
//   - 路径风格：http://s3.amazonaws.com/bucket/key
//   MinIO 等服务通常需要路径风格
type S3BackendStorage struct {
	id                    string        // 后端存储唯一标识符
	aws_access_key_id     string        // AWS 访问密钥 ID
	aws_secret_access_key string        // AWS 访问密钥 Secret
	region                string        // AWS 区域
	bucket                string        // S3 存储桶名称
	endpoint              string        // 自定义 endpoint（可选）
	storageClass          string        // 存储类别
	forcePathStyle        bool          // 是否使用路径风格 URL
	conn                  s3iface.S3API // S3 API 客户端连接
}

// newS3BackendStorage 创建并初始化 S3 后端存储实例
//
// 参数:
//   - configuration: 配置属性集合
//   - configPrefix: 配置前缀（如 "storage.backend.s3."）
//   - id: 后端存储的唯一标识符
// 返回:
//   - s: 创建的存储实例
//   - err: 错误信息（如果有）
//
// 初始化流程：
//   1. 从配置中读取所有 S3 相关参数
//   2. 设置默认存储类别为 STANDARD_IA（如果未配置）
//   3. 创建 S3 会话连接
//   4. 记录日志并返回实例
func newS3BackendStorage(configuration backend.StringProperties, configPrefix string, id string) (s *S3BackendStorage, err error) {
	s = &S3BackendStorage{}
	s.id = id

	// 读取 AWS 凭证
	s.aws_access_key_id = configuration.GetString(configPrefix + "aws_access_key_id")
	s.aws_secret_access_key = configuration.GetString(configPrefix + "aws_secret_access_key")

	// 读取区域和存储桶配置
	s.region = configuration.GetString(configPrefix + "region")
	s.bucket = configuration.GetString(configPrefix + "bucket")

	// 读取可选配置
	s.endpoint = configuration.GetString(configPrefix + "endpoint")
	s.storageClass = configuration.GetString(configPrefix + "storage_class")
	s.forcePathStyle = util.ParseBool(configuration.GetString(configPrefix+"force_path_style"), true)

	// 设置默认存储类别为 STANDARD_IA（低频访问，成本较低）
	if s.storageClass == "" {
		s.storageClass = "STANDARD_IA"
	}

	// 创建 S3 会话连接
	s.conn, err = createSession(s.aws_access_key_id, s.aws_secret_access_key, s.region, s.endpoint, s.forcePathStyle)

	glog.V(0).Infof("created backend storage s3.%s for region %s bucket %s", s.id, s.region, s.bucket)
	return
}

// ToProperties 将存储配置序列化为 map 格式
//
// 返回:
//   包含所有 S3 配置参数的映射
//
// 用途：
//   用于配置持久化、调试输出、状态查询等场景
func (s *S3BackendStorage) ToProperties() map[string]string {
	m := make(map[string]string)
	m["aws_access_key_id"] = s.aws_access_key_id
	m["aws_secret_access_key"] = s.aws_secret_access_key
	m["region"] = s.region
	m["bucket"] = s.bucket
	m["endpoint"] = s.endpoint
	m["storage_class"] = s.storageClass
	m["force_path_style"] = util.BoolToString(s.forcePathStyle)
	return m
}

// NewStorageFile 创建远程存储文件的访问接口
//
// 参数:
//   - key: 文件在 S3 中的 key（路径）
//   - tierInfo: Volume 的分层存储信息（包含文件大小、修改时间等元数据）
// 返回:
//   backend.BackendStorageFile 接口实例，支持 ReadAt 等操作
//
// 实现细节：
//   移除 key 开头的 "/" 符号（如果有）
//   确保 key 格式符合 S3 规范
func (s *S3BackendStorage) NewStorageFile(key string, tierInfo *volume_server_pb.VolumeInfo) backend.BackendStorageFile {
	// 移除 key 开头的 "/"，S3 的 key 不应该以 "/" 开头
	if strings.HasPrefix(key, "/") {
		key = key[1:]
	}

	f := &S3BackendStorageFile{
		backendStorage: s,
		key:            key,
		tierInfo:       tierInfo,
	}

	return f
}

// CopyFile 将本地文件上传到 S3
//
// 参数:
//   - f: 本地文件句柄（通常是 .dat 文件）
//   - fn: 进度回调函数，接收已传输字节数和百分比
// 返回:
//   - key: S3 中的文件 key（UUID 格式）
//   - size: 上传的文件大小（字节）
//   - err: 错误信息（如果有）
//
// 实现流程：
//   1. 生成随机 UUID 作为文件 key
//   2. 通过 S3 SDK 上传文件，支持失败重试
//   3. 返回 key 和文件大小
func (s *S3BackendStorage) CopyFile(f *os.File, fn func(progressed int64, percentage float32) error) (key string, size int64, err error) {
	// 生成随机 UUID 作为文件 key
	randomUuid, _ := uuid.NewRandom()
	key = randomUuid.String()

	glog.V(1).Infof("copying dat file of %s to remote s3.%s as %s", f.Name(), s.id, key)

	// 使用重试机制上传文件到 S3
	util.Retry("upload to S3", func() error {
		size, err = uploadToS3(s.conn, f.Name(), s.bucket, key, s.storageClass, fn)
		return err
	})

	return
}

// DownloadFile 从 S3 下载文件到本地
//
// 参数:
//   - fileName: 本地文件路径（下载目标）
//   - key: S3 中的文件 key
//   - fn: 进度回调函数
// 返回:
//   - size: 下载的文件大小（字节）
//   - err: 错误信息（如果有）
func (s *S3BackendStorage) DownloadFile(fileName string, key string, fn func(progressed int64, percentage float32) error) (size int64, err error) {

	glog.V(1).Infof("download dat file of %s from remote s3.%s as %s", fileName, s.id, key)

	// 从 S3 下载文件
	size, err = downloadFromS3(s.conn, fileName, s.bucket, key, fn)

	return
}

// DeleteFile 从 S3 删除文件
//
// 参数:
//   - key: S3 中的文件 key
// 返回:
//   - err: 错误信息（如果有）
func (s *S3BackendStorage) DeleteFile(key string) (err error) {

	glog.V(1).Infof("delete dat file %s from remote", key)

	// 从 S3 删除文件
	err = deleteFromS3(s.conn, s.bucket, key)

	return
}

// S3BackendStorageFile 表示 S3 中的一个文件
//
// 实现 backend.BackendStorageFile 接口，提供类似本地文件的访问方式
//
// 字段说明：
//   - backendStorage: 关联的 S3 后端存储实例
//   - key: 文件在 S3 中的 key（路径）
//   - tierInfo: Volume 的分层存储信息（包含文件大小、修改时间等元数据）
//
// 设计要点：
//   - 支持随机读取（ReadAt），用于读取 Volume 的特定 Needle
//   - 元数据从 tierInfo 中获取，避免额外的 S3 HeadObject 请求
//   - 只读访问，不支持写入操作（WriteAt、Truncate）
type S3BackendStorageFile struct {
	backendStorage *S3BackendStorage        // 关联的 S3 后端存储实例
	key            string                    // 文件在 S3 中的 key
	tierInfo       *volume_server_pb.VolumeInfo // Volume 的分层存储元数据
}

// ReadAt 从指定偏移量读取数据（实现 io.ReaderAt 接口）
//
// 参数:
//   - p: 目标缓冲区
//   - off: 读取偏移量（字节）
// 返回:
//   - n: 实际读取的字节数
//   - err: 错误信息（如果有）
//
// 实现细节：
//   1. 检查偏移量是否超出文件大小（提前返回 EOF）
//   2. 构造 HTTP Range 头（bytes=off-end）
//   3. 调用 S3 GetObject API 读取指定范围的数据
//   4. 循环读取直到填满缓冲区或到达 EOF
//
// 性能考虑：
//   - 每次 ReadAt 都会发起一次 S3 GetObject 请求（使用 Range 头）
//   - 适合随机读取场景（如读取 Volume 中的特定 Needle）
//   - 不适合顺序读取大文件（应使用 DownloadFile）
//
// 边界情况：
//   - 如果 off >= 文件大小，直接返回 EOF
//   - 读取完成后将 io.EOF 转换为 nil（符合 io.ReaderAt 规范）
func (s3backendStorageFile S3BackendStorageFile) ReadAt(p []byte, off int64) (n int, err error) {
	// 获取文件大小，检查偏移量是否越界
	datSize, _, _ := s3backendStorageFile.GetStat()

	if datSize > 0 && off >= datSize {
		return 0, io.EOF
	}

	// 构造 HTTP Range 头
	// 例如：off=100, len(p)=50 => "bytes=100-149"
	bytesRange := fmt.Sprintf("bytes=%d-%d", off, off+int64(len(p))-1)

	// 调用 S3 GetObject API，使用 Range 请求只读取指定范围
	getObjectOutput, getObjectErr := s3backendStorageFile.backendStorage.conn.GetObject(&s3.GetObjectInput{
		Bucket: &s3backendStorageFile.backendStorage.bucket,
		Key:    &s3backendStorageFile.key,
		Range:  &bytesRange,
	})

	if getObjectErr != nil {
		return 0, fmt.Errorf("bucket %s GetObject %s: %v", s3backendStorageFile.backendStorage.bucket, s3backendStorageFile.key, getObjectErr)
	}
	defer getObjectOutput.Body.Close()

	// glog.V(3).Infof("read %s %s", s3backendStorageFile.key, bytesRange)
	// glog.V(3).Infof("content range: %s, contentLength: %d", *getObjectOutput.ContentRange, *getObjectOutput.ContentLength)

	// 循环读取数据，直到填满缓冲区或到达 EOF
	var readCount int
	for {
		p = p[readCount:] // 移动缓冲区指针
		readCount, err = getObjectOutput.Body.Read(p)
		n += readCount

		if err != nil {
			break
		}
	}

	// 将 io.EOF 转换为 nil（符合 io.ReaderAt 规范）
	if err == io.EOF {
		err = nil
	}

	return
}

// WriteAt 不支持写入操作（S3 存储只读）
func (s3backendStorageFile S3BackendStorageFile) WriteAt(p []byte, off int64) (n int, err error) {
	panic("not implemented")
}

// Truncate 不支持截断操作（S3 存储只读）
func (s3backendStorageFile S3BackendStorageFile) Truncate(off int64) error {
	panic("not implemented")
}

// Close 关闭文件（空操作，因为 ReadAt 每次都会创建新连接）
func (s3backendStorageFile S3BackendStorageFile) Close() error {
	return nil
}

// GetStat 获取文件元数据（大小、修改时间）
//
// 返回:
//   - datSize: 文件大小（字节）
//   - modTime: 修改时间
//   - err: 错误信息（如果有）
//
// 实现细节：
//   从 tierInfo 中获取元数据，避免额外的 S3 HeadObject 请求
//   tierInfo 在创建 Volume 时从 Master 获取
func (s3backendStorageFile S3BackendStorageFile) GetStat() (datSize int64, modTime time.Time, err error) {

	files := s3backendStorageFile.tierInfo.GetFiles()

	// tierInfo 中必须包含文件信息
	if len(files) == 0 {
		err = fmt.Errorf("remote file info not found")
		return
	}

	// 获取文件大小和修改时间
	datSize = int64(files[0].FileSize)
	modTime = time.Unix(int64(files[0].ModifiedTime), 0)

	return
}

// Name 返回文件名（即 key）
func (s3backendStorageFile S3BackendStorageFile) Name() string {
	return s3backendStorageFile.key
}

// Sync 同步文件（空操作，S3 存储不需要同步）
func (s3backendStorageFile S3BackendStorageFile) Sync() error {
	return nil
}
