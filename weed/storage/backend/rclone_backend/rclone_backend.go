//go:build rclone
// +build rclone

// Package rclone_backend 实现基于 Rclone 的远程存储后端
//
// Rclone 是一个强大的云存储同步工具，支持 70+ 种云存储服务（如 S3、Google Drive、Azure、Dropbox 等）
// 本包将 Rclone 集成到 SeaweedFS 中，作为 Volume 数据的远程存储层
//
// 核心概念：
//   1. Remote Name：Rclone 配置中的远程存储名称（在 rclone.conf 中定义）
//   2. Key Template：文件 key 的路径模板，支持 Go template 语法
//   3. Backend Storage：远程存储抽象，负责上传/下载/删除操作
//   4. Storage File：远程文件抽象，实现 io.ReaderAt 接口用于随机读取
//
// 编译要求：
//   需要在编译时添加 -tags "rclone" 标签
//   示例：go build -tags "rclone" ./weed
//
// 配置示例：
//   [storage.backend.rclone]
//   remote_name = "myremote"              # Rclone 远程存储名称
//   key_template = "seaweedfs/{{ . }}"    # 可选，文件 key 的路径模板
//
// 支持的云存储服务（部分）：
//   - AWS S3、Google Cloud Storage、Azure Blob
//   - Dropbox、Google Drive、OneDrive
//   - SFTP、WebDAV、FTP
//   - 等 70+ 种服务
package rclone_backend

import (
	"bytes"
	"context"
	"fmt"
	"github.com/rclone/rclone/fs/config/configfile"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"io"
	"os"
	"text/template"
	"time"

	"github.com/google/uuid"

	_ "github.com/rclone/rclone/backend/all" // 导入所有 Rclone 支持的后端
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fs/object"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
)

// init 函数在包加载时自动执行，注册 Rclone 后端工厂
func init() {
	// 注册到 SeaweedFS 的后端存储工厂映射中
	backend.BackendStorageFactories["rclone"] = &RcloneBackendFactory{}

	// 安装 Rclone 配置文件读取器，加载 rclone.conf
	configfile.Install()
}

// RcloneBackendFactory 实现 BackendStorageFactory 接口
// 负责创建 Rclone 后端存储实例
type RcloneBackendFactory struct {
}

// StorageType 返回存储类型标识符 "rclone"
// 用于在配置文件中标识此后端类型
func (factory *RcloneBackendFactory) StorageType() backend.StorageType {
	return "rclone"
}

// BuildStorage 根据配置创建 Rclone 后端存储实例
//
// 参数:
//   - configuration: 配置属性集合（包含 remote_name、key_template 等）
//   - configPrefix: 配置前缀（如 "storage.backend.rclone."）
//   - id: 后端存储的唯一标识符
// 返回:
//   - backend.BackendStorage: 创建的存储实例
//   - error: 创建失败时的错误
func (factory *RcloneBackendFactory) BuildStorage(configuration backend.StringProperties, configPrefix string, id string) (backend.BackendStorage, error) {
	return newRcloneBackendStorage(configuration, configPrefix, id)
}

// RcloneBackendStorage 表示一个 Rclone 远程存储后端
//
// 字段说明：
//   - id: 后端存储的唯一标识符，用于日志和调试
//   - remoteName: Rclone 配置中的远程存储名称（如 "myremote"）
//   - keyTemplate: 文件 key 的路径模板（可选），用于自定义远程文件路径
//   - keyTemplateText: 模板的原始文本，用于序列化配置
//   - fs: Rclone 文件系统接口，封装了所有云存储操作
//
// 设计要点：
//   - 通过 keyTemplate 可以灵活控制远程文件的存储路径
//   - fs 接口统一了 70+ 种云存储服务的访问方式
//   - 所有操作都支持失败重试（通过 util.Retry）
//
// 示例：
//   如果 keyTemplate = "seaweedfs/{{ . }}"，key = "abc123"
//   则实际存储路径为 "seaweedfs/abc123"
type RcloneBackendStorage struct {
	id              string             // 后端存储唯一标识符
	remoteName      string             // Rclone 远程存储名称（对应 rclone.conf 中的配置）
	keyTemplate     *template.Template // 文件 key 的路径模板（可选）
	keyTemplateText string             // 模板原始文本（用于序列化）
	fs              fs.Fs              // Rclone 文件系统接口，封装云存储操作
}

// newRcloneBackendStorage 创建并初始化 Rclone 后端存储实例
//
// 参数:
//   - configuration: 配置属性集合
//   - configPrefix: 配置前缀（如 "storage.backend.rclone."）
//   - id: 后端存储的唯一标识符
// 返回:
//   - s: 创建的存储实例
//   - err: 错误信息（如果有）
//
// 初始化流程：
//   1. 从配置中读取 remote_name 和 key_template
//   2. 解析 key_template 为 Go template 对象
//   3. 启动 Rclone 计数统计系统
//   4. 连接到指定的 Rclone 远程存储
//
// 配置示例：
//   remote_name = "myremote"              # 必需，Rclone 配置名称
//   key_template = "seaweedfs/{{ . }}"    # 可选，文件路径模板
func newRcloneBackendStorage(configuration backend.StringProperties, configPrefix string, id string) (s *RcloneBackendStorage, err error) {
	s = &RcloneBackendStorage{}
	s.id = id

	// 读取 Rclone 远程存储名称（必需配置）
	s.remoteName = configuration.GetString(configPrefix + "remote_name")

	// 读取文件 key 的路径模板（可选配置）
	s.keyTemplateText = configuration.GetString(configPrefix + "key_template")

	// 解析路径模板为 Go template 对象
	s.keyTemplate, err = template.New("keyTemplate").Parse(s.keyTemplateText)
	if err != nil {
		return
	}

	// 创建 context 用于 Rclone 操作
	ctx := context.TODO()

	// 启动 Rclone 的流量统计和进度跟踪系统
	accounting.Start(ctx)

	// 构造 Rclone 文件系统路径，格式：<remoteName>:
	// 例如：remoteName = "myremote" => fsPath = "myremote:"
	fsPath := fmt.Sprintf("%s:", s.remoteName)

	// 初始化 Rclone 文件系统接口
	// 这会连接到指定的云存储服务
	s.fs, err = fs.NewFs(ctx, fsPath)
	if err != nil {
		glog.Errorf("failed to instantiate Rclone filesystem: %s", err)
		return
	}

	glog.V(0).Infof("created backend storage rclone.%s for remote name %s", s.id, s.remoteName)
	return
}

// ToProperties 将存储配置序列化为 map 格式
//
// 返回:
//   包含 remote_name 和 key_template（如果有）的配置映射
//
// 用途：
//   用于配置持久化、调试输出、状态查询等场景
func (s *RcloneBackendStorage) ToProperties() map[string]string {
	m := make(map[string]string)
	m["remote_name"] = s.remoteName

	// 只有配置了 key_template 时才添加到输出中
	if len(s.keyTemplateText) > 0 {
		m["key_template"] = s.keyTemplateText
	}
	return m
}

// formatKey 根据模板格式化文件 key
//
// 参数:
//   - key: 原始 key（通常是 UUID）
//   - storage: Rclone 存储实例
// 返回:
//   - fKey: 格式化后的 key
//   - err: 模板执行错误（如果有）
//
// 实现逻辑：
//   - 如果没有配置模板，直接返回原始 key
//   - 如果配置了模板，使用 Go template 引擎格式化 key
//
// 示例：
//   key = "abc-123"
//   keyTemplate = "seaweedfs/{{ . }}" => fKey = "seaweedfs/abc-123"
//   keyTemplate = "data/{{ . }}.dat"  => fKey = "data/abc-123.dat"
func formatKey(key string, storage RcloneBackendStorage) (fKey string, err error) {
	var b bytes.Buffer

	// 如果没有配置模板，直接使用原始 key
	if len(storage.keyTemplateText) == 0 {
		fKey = key
	} else {
		// 使用 Go template 引擎执行模板，将 key 作为模板数据
		err = storage.keyTemplate.Execute(&b, key)
		if err == nil {
			fKey = b.String()
		}
	}
	return
}

// NewStorageFile 创建远程存储文件的访问接口
//
// 参数:
//   - key: 文件在远程存储中的 key（已格式化的路径）
//   - tierInfo: Volume 的分层存储信息（包含文件大小、修改时间等元数据）
// 返回:
//   backend.BackendStorageFile 接口实例，支持 ReadAt 等操作
//
// 用途：
//   为远程存储中的文件提供类似本地文件的访问接口
//   支持随机读取（ReadAt），用于读取 Volume 的特定 Needle
func (s *RcloneBackendStorage) NewStorageFile(key string, tierInfo *volume_server_pb.VolumeInfo) backend.BackendStorageFile {
	f := &RcloneBackendStorageFile{
		backendStorage: s,
		key:            key,
		tierInfo:       tierInfo,
	}

	return f
}

// CopyFile 将本地文件上传到远程存储
//
// 参数:
//   - f: 本地文件句柄（通常是 .dat 文件）
//   - fn: 进度回调函数，接收已传输字节数和百分比
// 返回:
//   - key: 远程存储中的文件 key（UUID 格式）
//   - size: 上传的文件大小（字节）
//   - err: 错误信息（如果有）
//
// 实现流程：
//   1. 生成随机 UUID 作为文件 key
//   2. 使用 keyTemplate 格式化 key（如果配置了模板）
//   3. 通过 Rclone 上传文件，支持失败重试
//   4. 返回 key 和文件大小
//
// 重试机制：
//   使用 util.Retry 包装上传操作，失败时会自动重试
//   重试策略由 util.Retry 函数控制
func (s *RcloneBackendStorage) CopyFile(f *os.File, fn func(progressed int64, percentage float32) error) (key string, size int64, err error) {
	// 生成随机 UUID 作为文件 key
	randomUuid, err := uuid.NewRandom()
	if err != nil {
		return key, 0, err
	}
	key = randomUuid.String()

	// 根据 keyTemplate 格式化 key
	// 例如：key = "abc-123" => formatKey => "seaweedfs/abc-123"
	key, err = formatKey(key, *s)
	if err != nil {
		return key, 0, err
	}

	glog.V(1).Infof("copy dat file of %s to remote rclone.%s as %s", f.Name(), s.id, key)

	// 使用重试机制上传文件
	// 如果上传失败，会自动重试多次
	util.Retry("upload via Rclone", func() error {
		size, err = uploadViaRclone(s.fs, f.Name(), key, fn)
		return err
	})

	return
}

// uploadViaRclone 通过 Rclone 上传文件到远程存储
//
// 参数:
//   - rfs: Rclone 文件系统接口
//   - filename: 本地文件路径
//   - key: 远程存储中的文件 key
//   - fn: 进度回调函数
// 返回:
//   - fileSize: 上传的文件大小（字节）
//   - err: 错误信息（如果有）
//
// 实现流程：
//   1. 打开本地文件并获取文件元数据（大小、修改时间）
//   2. 创建 Rclone 传输对象和计数器，用于跟踪传输进度
//   3. 创建 ProgressReader 包装文件读取器，实现进度回调
//   4. 调用 rfs.Put 上传文件
//   5. 返回文件大小
//
// 进度跟踪：
//   通过 ProgressReader 包装文件读取器，每次读取数据时都会调用回调函数
//   回调函数接收已传输字节数和百分比
func uploadViaRclone(rfs fs.Fs, filename string, key string, fn func(progressed int64, percentage float32) error) (fileSize int64, err error) {
	ctx := context.TODO()

	// 打开本地文件
	file, err := os.Open(filename)
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			return
		}
	}(file)

	if err != nil {
		return 0, err
	}

	// 获取文件元数据（大小、修改时间）
	stat, err := file.Stat()
	if err != nil {
		return 0, err
	}

	// 创建 Rclone 对象信息（包含 key、修改时间、文件大小等）
	info := object.NewStaticObjectInfo(key, stat.ModTime(), stat.Size(), true, nil, rfs)

	// 创建传输对象，用于跟踪传输进度
	tr := accounting.NewStats(ctx).NewTransfer(info, rfs)
	defer tr.Done(ctx, err)

	// 创建计数器，包装文件读取器以统计读取的字节数
	acc := tr.Account(ctx, file)

	// 创建进度读取器，在读取数据时调用回调函数
	pr := ProgressReader{acc: acc, tr: tr, fn: fn}

	// 上传文件到远程存储
	obj, err := rfs.Put(ctx, &pr, info)
	if err != nil {
		return 0, err
	}

	return obj.Size(), err
}

// DownloadFile 从远程存储下载文件到本地
//
// 参数:
//   - filename: 本地文件路径（下载目标）
//   - key: 远程存储中的文件 key
//   - fn: 进度回调函数
// 返回:
//   - size: 下载的文件大小（字节）
//   - err: 错误信息（如果有）
//
// 重试机制：
//   使用 util.Retry 包装下载操作，失败时会自动重试
func (s *RcloneBackendStorage) DownloadFile(filename string, key string, fn func(progressed int64, percentage float32) error) (size int64, err error) {
	glog.V(1).Infof("download dat file of %s from remote rclone.%s as %s", filename, s.id, key)

	// 使用重试机制下载文件
	util.Retry("download via Rclone", func() error {
		size, err = downloadViaRclone(s.fs, filename, key, fn)
		return err
	})

	return
}

// downloadViaRclone 通过 Rclone 从远程存储下载文件
//
// 参数:
//   - fs: Rclone 文件系统接口
//   - filename: 本地文件路径（下载目标）
//   - key: 远程存储中的文件 key
//   - fn: 进度回调函数
// 返回:
//   - fileSize: 下载的文件大小（字节）
//   - err: 错误信息（如果有）
//
// 实现流程：
//   1. 从 Rclone 文件系统获取远程对象
//   2. 打开远程对象的读取流
//   3. 创建本地文件
//   4. 创建传输对象和进度读取器
//   5. 通过 io.Copy 将数据从远程复制到本地，并跟踪进度
//   6. 返回写入的字节数
func downloadViaRclone(fs fs.Fs, filename string, key string, fn func(progressed int64, percentage float32) error) (fileSize int64, err error) {
	ctx := context.TODO()

	// 从 Rclone 文件系统获取远程对象
	obj, err := fs.NewObject(ctx, key)
	if err != nil {
		return 0, err
	}

	// 打开远程对象的读取流
	rc, err := obj.Open(ctx)
	defer func(rc io.ReadCloser) {
		err := rc.Close()
		if err != nil {
			return
		}
	}(rc)

	if err != nil {
		return 0, err
	}

	// 创建本地文件
	file, err := os.Create(filename)
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			return
		}
	}(file)

	// 创建传输对象，用于跟踪下载进度
	tr := accounting.NewStats(ctx).NewTransfer(obj, fs)
	defer tr.Done(ctx, err)

	// 创建计数器，包装远程读取流以统计读取的字节数
	acc := tr.Account(ctx, rc)

	// 创建进度读取器，在读取数据时调用回调函数
	pr := ProgressReader{acc: acc, tr: tr, fn: fn}

	// 将数据从远程复制到本地文件
	written, err := io.Copy(file, &pr)
	if err != nil {
		return 0, err
	}

	return written, nil
}

// DeleteFile 从远程存储删除文件
//
// 参数:
//   - key: 远程存储中的文件 key
// 返回:
//   - err: 错误信息（如果有）
//
// 重试机制：
//   使用 util.Retry 包装删除操作，失败时会自动重试
func (s *RcloneBackendStorage) DeleteFile(key string) (err error) {
	glog.V(1).Infof("delete dat file %s from remote", key)

	// 使用重试机制删除文件
	util.Retry("delete via Rclone", func() error {
		err = deleteViaRclone(s.fs, key)
		return err
	})

	return
}

// deleteViaRclone 通过 Rclone 删除远程文件
//
// 参数:
//   - fs: Rclone 文件系统接口
//   - key: 远程存储中的文件 key
// 返回:
//   - err: 错误信息（如果有）
//
// 实现流程：
//   1. 从 Rclone 文件系统获取远程对象
//   2. 调用 Remove 方法删除对象
func deleteViaRclone(fs fs.Fs, key string) (err error) {
	ctx := context.TODO()

	// 从 Rclone 文件系统获取远程对象
	obj, err := fs.NewObject(ctx, key)
	if err != nil {
		return err
	}

	// 删除远程对象
	return obj.Remove(ctx)
}

// RcloneBackendStorageFile 表示远程存储中的一个文件
//
// 实现 backend.BackendStorageFile 接口，提供类似本地文件的访问方式
//
// 字段说明：
//   - backendStorage: 关联的 Rclone 后端存储实例
//   - key: 文件在远程存储中的 key（路径）
//   - tierInfo: Volume 的分层存储信息（包含文件大小、修改时间等元数据）
//
// 设计要点：
//   - 支持随机读取（ReadAt），用于读取 Volume 的特定 Needle
//   - 元数据（大小、修改时间）从 tierInfo 中获取，避免额外的网络请求
//   - 只读访问，不支持写入操作（WriteAt、Truncate）
type RcloneBackendStorageFile struct {
	backendStorage *RcloneBackendStorage         // 关联的 Rclone 后端存储实例
	key            string                         // 文件在远程存储中的 key
	tierInfo       *volume_server_pb.VolumeInfo  // Volume 的分层存储元数据
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
//   1. 从 Rclone 文件系统获取远程对象
//   2. 使用 RangeOption 指定读取范围（off ~ off+len(p)-1）
//   3. 打开远程对象的读取流
//   4. 使用 io.ReadFull 读取完整的数据到缓冲区
//
// 性能考虑：
//   - 每次 ReadAt 都会创建新的 HTTP 连接（通过 Range 请求）
//   - 适合随机读取场景（如读取 Volume 中的特定 Needle）
//   - 不适合顺序读取大文件（应使用 DownloadFile）
func (rcloneBackendStorageFile RcloneBackendStorageFile) ReadAt(p []byte, off int64) (n int, err error) {
	ctx := context.TODO()

	// 从 Rclone 文件系统获取远程对象
	obj, err := rcloneBackendStorageFile.backendStorage.fs.NewObject(ctx, rcloneBackendStorageFile.key)
	if err != nil {
		return 0, err
	}

	// 构造 HTTP Range 请求的范围
	// 例如：off=100, len(p)=50 => Range: bytes=100-149
	opt := fs.RangeOption{Start: off, End: off + int64(len(p)) - 1}

	// 打开远程对象的读取流，只读取指定范围的数据
	rc, err := obj.Open(ctx, &opt)
	defer func(rc io.ReadCloser) {
		err := rc.Close()
		if err != nil {
			return
		}
	}(rc)

	if err != nil {
		return 0, err
	}

	// 读取完整的数据到缓冲区
	// io.ReadFull 会确保读取 len(p) 字节（除非到达 EOF）
	return io.ReadFull(rc, p)
}

// WriteAt 不支持写入操作（远程存储只读）
func (rcloneBackendStorageFile RcloneBackendStorageFile) WriteAt(p []byte, off int64) (n int, err error) {
	panic("not implemented")
}

// Truncate 不支持截断操作（远程存储只读）
func (rcloneBackendStorageFile RcloneBackendStorageFile) Truncate(off int64) error {
	panic("not implemented")
}

// Close 关闭文件（空操作，因为 ReadAt 每次都会创建新连接）
func (rcloneBackendStorageFile RcloneBackendStorageFile) Close() error {
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
//   从 tierInfo 中获取元数据，避免额外的网络请求
//   tierInfo 在创建 Volume 时从 Master 获取
func (rcloneBackendStorageFile RcloneBackendStorageFile) GetStat() (datSize int64, modTime time.Time, err error) {
	files := rcloneBackendStorageFile.tierInfo.GetFiles()

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
func (rcloneBackendStorageFile RcloneBackendStorageFile) Name() string {
	return rcloneBackendStorageFile.key
}

// Sync 同步文件（空操作，远程存储不需要同步）
func (rcloneBackendStorageFile RcloneBackendStorageFile) Sync() error {
	return nil
}
