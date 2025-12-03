// Package weed_server 实现 WebDAV 服务器功能
// 本文件提供完整的 WebDAV 协议支持，允许通过 WebDAV 客户端访问 SeaweedFS Filer
//
// 核心功能:
//   - WebDavServer: WebDAV 服务器，处理 WebDAV 请求
//   - WebDavFileSystem: 实现 webdav.FileSystem 接口，桥接到 SeaweedFS Filer
//   - WebDavFile: 文件操作对象，支持读写、Seek、目录遍历
//   - FileInfo: 文件信息对象，实现 os.FileInfo 和 webdav.ETager 接口
//
// WebDAV 协议支持:
//   - 标准方法：GET、PUT、DELETE、MKCOL、PROPFIND、PROPPATCH
//   - 文件操作：读取、写入、删除、重命名、复制、移动
//   - 目录操作：创建、列举、删除
//   - 属性查询：文件大小、修改时间、权限、ETag
//   - 锁定机制：防止并发修改冲突
//
// 使用场景:
//   - Windows 资源管理器：映射网络驱动器访问 SeaweedFS
//   - macOS Finder：连接到服务器，浏览文件
//   - Linux davfs2：挂载 WebDAV 为本地文件系统
//   - 第三方应用：任何支持 WebDAV 的应用（如办公软件）
//
// 架构设计:
//   1. WebDAV 客户端 → WebDavServer → webdav.Handler
//   2. webdav.Handler → WebDavFileSystem（实现文件系统接口）
//   3. WebDavFileSystem → Filer gRPC Client → SeaweedFS Filer
//   4. 文件数据：通过 chunk 分片存储在 Volume Server
//
// 缓存机制:
//   - ReaderCache：缓存文件读取器，减少重复打开文件
//   - TieredChunkCache：两层缓存（内存 + 磁盘），加速数据读取
//   - 缓存唯一 ID：基于 Filer 地址和版本号，避免冲突
//
// 分块上传:
//   - 使用 BufferedWriteCloser 缓冲写入
//   - 达到阈值（MaxMB）后自动上传到 Volume Server
//   - 支持增量追加写入
//
// 权限控制:
//   - Uid/Gid：创建文件和目录时设置所有者
//   - FileMode：支持标准 Unix 权限位
//   - Collection/Replication：控制数据存储策略
//
// 注意事项:
//   - WebDAV 性能低于原生 API（多次往返、协议开销）
//   - 适合小文件和低频访问场景
//   - 大文件上传建议直接使用 HTTP API
//   - 锁定机制在内存中（重启丢失）
package weed_server

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util/version"

	"github.com/seaweedfs/seaweedfs/weed/util/buffered_writer"
	"golang.org/x/net/webdav"
	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/chunk_cache"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/security"
)

// WebDavOption WebDAV 服务器配置选项
//
// 字段说明:
//   - Filer: Filer 服务器地址（如 "localhost:8888"）
//   - FilerRootPath: Filer 根路径，WebDAV 访问的起始目录（如 "/webdav"）
//   - DomainName: 域名（可选）
//   - BucketsPath: S3 bucket 路径（可选）
//   - GrpcDialOption: gRPC 连接选项（TLS、认证等）
//   - Collection: 文件集合名称，用于逻辑分组
//   - Replication: 副本策略（如 "000"、"001"、"010"）
//   - DiskType: 磁盘类型（hdd、ssd、nvme）
//   - Uid: 创建文件/目录的用户 ID
//   - Gid: 创建文件/目录的组 ID
//   - Cipher: 是否启用加密
//   - CacheDir: 缓存目录路径
//   - CacheSizeMB: 磁盘缓存大小（MB）
//   - MaxMB: 单个文件分片大小上限（MB）
//
// 示例配置:
//   option := &WebDavOption{
//       Filer: "localhost:8888",
//       FilerRootPath: "/webdav",
//       Collection: "documents",
//       Replication: "001",
//       DiskType: "hdd",
//       Uid: 1000,
//       Gid: 1000,
//       CacheDir: "/tmp/seaweedfs-cache",
//       CacheSizeMB: 1000,
//       MaxMB: 4,
//   }
type WebDavOption struct {
	Filer          pb.ServerAddress  // Filer 服务器地址
	FilerRootPath  string            // Filer 根路径（WebDAV 起始目录）
	DomainName     string            // 域名（可选）
	BucketsPath    string            // S3 bucket 路径（可选）
	GrpcDialOption grpc.DialOption   // gRPC 连接选项
	Collection     string            // 文件集合名称
	Replication    string            // 副本策略（如 "001"）
	DiskType       string            // 磁盘类型（hdd/ssd/nvme）
	Uid            uint32            // 用户 ID
	Gid            uint32            // 组 ID
	Cipher         bool              // 是否启用加密
	CacheDir       string            // 缓存目录
	CacheSizeMB    int64             // 磁盘缓存大小（MB）
	MaxMB          int               // 单个文件分片大小上限（MB）
}

// WebDavServer WebDAV 服务器主对象
// 处理所有 WebDAV 协议请求
//
// 字段说明:
//   - option: WebDAV 配置选项
//   - secret: 签名密钥（用于认证）
//   - filer: Filer 对象（可选，用于本地 Filer）
//   - grpcDialOption: gRPC 连接选项
//   - Handler: webdav.Handler（标准 WebDAV 处理器）
type WebDavServer struct {
	option         *WebDavOption        // 配置选项
	secret         security.SigningKey  // 签名密钥
	filer          *filer.Filer         // Filer 对象（可选）
	grpcDialOption grpc.DialOption      // gRPC 连接选项
	Handler        *webdav.Handler      // WebDAV 处理器
}

// max 返回两个 int64 中的较大值
func max(x, y int64) int64 {
	if x <= y {
		return y
	}
	return x
}

// NewWebDavServer 创建 WebDAV 服务器实例
//
// 参数:
//   - option: WebDAV 配置选项
//
// 返回:
//   - ws: WebDavServer 实例
//   - err: 创建错误
//
// 功能:
//   1. 创建 WebDavFileSystem（连接到 Filer）
//   2. 根据 FilerRootPath 配置创建包装文件系统
//   3. 创建 webdav.Handler（标准 WebDAV 处理器）
//   4. 配置内存锁系统（NewMemLS）
//
// 路径处理:
//   - FilerRootPath = "/": 访问 Filer 根目录
//   - FilerRootPath = "/webdav": 访问 Filer 的 /webdav 子目录
//   - 使用 WrappedFs 实现路径映射（对客户端透明）
//
// 使用示例:
//   option := &WebDavOption{
//       Filer: "localhost:8888",
//       FilerRootPath: "/webdav",
//       Collection: "documents",
//       Replication: "001",
//   }
//   server, err := NewWebDavServer(option)
//   if err != nil {
//       log.Fatal(err)
//   }
//   http.Handle("/", server.Handler)
func NewWebDavServer(option *WebDavOption) (ws *WebDavServer, err error) {

	// 【创建 WebDAV 文件系统】
	// 连接到 Filer，实现 webdav.FileSystem 接口
	fs, _ := NewWebDavFileSystem(option)

	// 【修正根路径配置】
	// 避免访问 "/" 时返回 "//"
	if option.FilerRootPath == "/" {
		option.FilerRootPath = ""
	}

	// 【配置子文件夹访问】
	// FilerRootPath 不为空表示访问 Filer 的子文件夹
	// 使用 WrappedFs 实现路径映射，对客户端透明
	if option.FilerRootPath != "" {
		fs = NewWrappedFs(fs, path.Clean(option.FilerRootPath))
	}

	// 【创建 WebDavServer】
	ws = &WebDavServer{
		option:         option,
		grpcDialOption: security.LoadClientTLS(util.GetViper(), "grpc.filer"),
		Handler: &webdav.Handler{
			FileSystem: fs,                 // 文件系统实现
			LockSystem: webdav.NewMemLS(),  // 内存锁系统
		},
	}

	return ws, nil
}

// 本文件的 WebDAV 文件系统实现改编自：
// https://github.com/mattn/davfs/blob/master/plugin/mysql/mysql.go

// WebDavFileSystem WebDAV 文件系统实现
// 实现 webdav.FileSystem 接口，桥接到 SeaweedFS Filer
//
// 字段说明:
//   - option: WebDAV 配置选项
//   - secret: 签名密钥（认证）
//   - grpcDialOption: gRPC 连接选项
//   - chunkCache: 分片缓存（内存 + 磁盘两层）
//   - readerCache: 读取器缓存（减少重复打开文件）
//   - signature: 签名标识符（随机生成，用于识别客户端）
//
// 接口实现:
//   - Mkdir: 创建目录
//   - OpenFile: 打开或创建文件
//   - RemoveAll: 删除文件或目录
//   - Rename: 重命名或移动
//   - Stat: 获取文件信息
//
// 缓存机制:
//   - TieredChunkCache: 两层缓存（内存 256MB + 磁盘可配置）
//   - ReaderCache: 缓存 32 个文件读取器
//   - 缓存目录：<CacheDir>/<UniqueId>/
type WebDavFileSystem struct {
	option         *WebDavOption                   // 配置选项
	secret         security.SigningKey             // 签名密钥
	grpcDialOption grpc.DialOption                 // gRPC 连接选项
	chunkCache     *chunk_cache.TieredChunkCache   // 分片缓存
	readerCache    *filer.ReaderCache              // 读取器缓存
	signature      int32                           // 签名标识符
}

// FileInfo 文件信息对象
// 实现 os.FileInfo 和 webdav.ETager 接口
//
// 字段说明:
//   - name: 文件名（不包含路径）
//   - size: 文件大小（字节）
//   - mode: 文件权限模式
//   - modifiedTime: 最后修改时间
//   - etag: ETag 值（用于缓存验证）
//   - isDirectory: 是否为目录
//   - err: 错误信息（如果获取 FileInfo 时出错）
type FileInfo struct {
	name         string      // 文件名
	size         int64       // 文件大小
	mode         os.FileMode // 权限模式
	modifiedTime time.Time   // 修改时间
	etag         string      // ETag
	isDirectory  bool        // 是否为目录
	err          error       // 错误信息
}

// 以下方法实现 os.FileInfo 接口

func (fi *FileInfo) Name() string       { return fi.name }         // 返回文件名
func (fi *FileInfo) Size() int64        { return fi.size }         // 返回文件大小
func (fi *FileInfo) Mode() os.FileMode  { return fi.mode }         // 返回权限模式
func (fi *FileInfo) ModTime() time.Time { return fi.modifiedTime } // 返回修改时间
func (fi *FileInfo) IsDir() bool        { return fi.isDirectory }  // 是否为目录
func (fi *FileInfo) Sys() interface{}   { return nil }             // 系统相关信息（未使用）

// ETag 返回文件的 ETag 值（实现 webdav.ETager 接口）
// ETag 用于 HTTP 缓存验证和并发控制
//
// 参数:
//   - ctx: 上下文
//
// 返回:
//   - string: ETag 值
//   - error: 错误信息
func (fi *FileInfo) ETag(ctx context.Context) (string, error) {
	if fi.err != nil {
		return "", fi.err
	}
	return fi.etag, nil
}

// WebDavFile WebDAV 文件对象
// 实现 webdav.File 接口，支持读写、Seek、目录遍历
//
// 字段说明:
//   - fs: 所属的 WebDavFileSystem
//   - name: 文件完整路径
//   - isDirectory: 是否为目录
//   - off: 当前读取偏移量（用于 Seek）
//   - entry: Filer 的 Entry 对象（文件元数据）
//   - visibleIntervals: 可见的数据区间（用于稀疏文件）
//   - reader: 文件读取器
//   - bufWriter: 缓冲写入器（用于上传）
//   - ctx: 上下文
//
// 文件操作:
//   - Read: 读取文件数据
//   - Write: 写入文件数据
//   - Seek: 移动读取位置
//   - Readdir: 读取目录内容
//   - Stat: 获取文件信息
//   - Close: 关闭文件（刷新写入缓冲）
type WebDavFile struct {
	fs               *WebDavFileSystem                            // 所属文件系统
	name             string                                       // 文件路径
	isDirectory      bool                                         // 是否为目录
	off              int64                                        // 当前偏移量
	entry            *filer_pb.Entry                              // Filer Entry
	visibleIntervals *filer.IntervalList[*filer.VisibleInterval] // 可见区间
	reader           io.ReaderAt                                  // 读取器
	bufWriter        *buffered_writer.BufferedWriteCloser        // 缓冲写入器
	ctx              context.Context                              // 上下文
}

// NewWebDavFileSystem 创建 WebDAV 文件系统实例
//
// 参数:
//   - option: WebDAV 配置选项
//
// 返回:
//   - webdav.FileSystem: 文件系统实例
//   - error: 创建错误
//
// 功能:
//   1. 创建缓存目录（基于 Filer 地址和版本号生成唯一 ID）
//   2. 初始化两层分片缓存（内存 256MB + 磁盘）
//   3. 创建读取器缓存（缓存 32 个文件读取器）
//   4. 生成随机签名标识符
//
// 缓存设计:
//   - 内存缓存：256MB，存储热点数据
//   - 磁盘缓存：可配置大小（CacheSizeMB），存储温数据
//   - 缓存粒度：1MB 分片
//   - 缓存目录：<CacheDir>/<UniqueId>/
//
// 使用示例:
//   option := &WebDavOption{
//       Filer: "localhost:8888",
//       CacheDir: "/tmp/webdav-cache",
//       CacheSizeMB: 1000,
//   }
//   fs, err := NewWebDavFileSystem(option)
func NewWebDavFileSystem(option *WebDavOption) (webdav.FileSystem, error) {

	// 【生成缓存唯一 ID】
	// 基于 "webdav" + Filer 地址 + SeaweedFS 版本号
	// 确保不同 Filer 或版本不会共享缓存
	cacheUniqueId := util.Md5String([]byte("webdav" + string(option.Filer) + version.Version()))[0:8]
	cacheDir := path.Join(option.CacheDir, cacheUniqueId)

	// 【创建缓存目录】
	os.MkdirAll(cacheDir, os.FileMode(0755))

	// 【创建两层分片缓存】
	// 参数：内存缓存 256MB、磁盘缓存目录、磁盘缓存大小、分片大小 1MB
	chunkCache := chunk_cache.NewTieredChunkCache(256, cacheDir, option.CacheSizeMB, 1024*1024)

	// 【创建 WebDavFileSystem】
	t := &WebDavFileSystem{
		option:     option,
		chunkCache: chunkCache,
		signature:  util.RandomInt32(),  // 随机签名标识符
	}

	// 【创建读取器缓存】
	// 缓存 32 个文件读取器，减少重复打开文件的开销
	t.readerCache = filer.NewReaderCache(32, chunkCache, filer.LookupFn(t))

	return t, nil
}

// 编译时检查：确保 WebDavFileSystem 实现了 filer_pb.FilerClient 接口
var _ = filer_pb.FilerClient(&WebDavFileSystem{})

// WithFilerClient 执行需要 Filer gRPC 客户端的操作
// 实现 filer_pb.FilerClient 接口
//
// 参数:
//   - streamingMode: 是否使用流式传输模式
//   - fn: 使用 Filer 客户端的回调函数
//
// 返回:
//   - error: 执行错误
//
// 功能:
//   - 建立到 Filer 的 gRPC 连接
//   - 创建 SeaweedFilerClient
//   - 执行回调函数
//   - 自动关闭连接
func (fs *WebDavFileSystem) WithFilerClient(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) error {

	return pb.WithGrpcClient(streamingMode, fs.signature, func(grpcConnection *grpc.ClientConn) error {
		client := filer_pb.NewSeaweedFilerClient(grpcConnection)
		return fn(client)
	}, fs.option.Filer.ToGrpcAddress(), false, fs.option.GrpcDialOption)

}

// AdjustedUrl 返回调整后的 Volume Server URL
// 实现 filer_pb.FilerClient 接口
//
// 参数:
//   - location: Volume 位置信息
//
// 返回:
//   - string: Volume Server URL
func (fs *WebDavFileSystem) AdjustedUrl(location *filer_pb.Location) string {
	return location.Url
}

// GetDataCenter 返回数据中心名称
// 实现 filer_pb.FilerClient 接口
//
// 返回:
//   - string: 数据中心名称（WebDAV 不使用，返回空字符串）
func (fs *WebDavFileSystem) GetDataCenter() string {
	return ""
}

// clearName 清理和规范化路径名称
// 确保路径以 / 开头，保留尾部 / 的语义（表示目录）
//
// 参数:
//   - name: 原始路径名
//
// 返回:
//   - string: 清理后的路径
//   - error: 路径无效时返回 os.ErrInvalid
//
// 规范化规则:
//   - 使用 path.Clean 清理路径（去除 .、..、多余的 /）
//   - 保留尾部 /（表示这是目录路径）
//   - 确保以 / 开头（绝对路径）
//
// 示例:
//   - "/a/b/" → "/a/b/"（保留尾部 /）
//   - "/a//b" → "/a/b"（去除多余 /）
//   - "a/b" → error（必须是绝对路径）
func clearName(name string) (string, error) {
	// 【记录是否以 / 结尾】
	slashed := strings.HasSuffix(name, "/")

	// 【清理路径】
	// path.Clean 会去除尾部 /，需要后续恢复
	name = path.Clean(name)

	// 【恢复尾部 /】
	// 如果原路径以 / 结尾（目录），添加回去
	if !strings.HasSuffix(name, "/") && slashed {
		name += "/"
	}

	// 【验证是绝对路径】
	// WebDAV 要求所有路径都是绝对路径
	if !strings.HasPrefix(name, "/") {
		return "", os.ErrInvalid
	}

	return name, nil
}

// Mkdir 创建目录
// 实现 webdav.FileSystem 接口
//
// 参数:
//   - ctx: 上下文
//   - fullDirPath: 目录完整路径（如 "/documents/2023"）
//   - perm: 目录权限
//
// 返回:
//   - error: 创建错误（目录已存在、权限不足等）
//
// 工作流程:
//   1. 确保路径以 / 结尾（目录标记）
//   2. 清理和验证路径
//   3. 检查目录是否已存在
//   4. 调用 Filer gRPC 创建目录
//   5. 设置目录属性（权限、所有者、时间戳）
func (fs *WebDavFileSystem) Mkdir(ctx context.Context, fullDirPath string, perm os.FileMode) error {

	glog.V(2).Infof("WebDavFileSystem.Mkdir %v", fullDirPath)

	// 【确保目录路径以 / 结尾】
	if !strings.HasSuffix(fullDirPath, "/") {
		fullDirPath += "/"
	}

	// 【清理路径】
	var err error
	if fullDirPath, err = clearName(fullDirPath); err != nil {
		return err
	}

	// 【检查目录是否已存在】
	_, err = fs.stat(ctx, fullDirPath)
	if err == nil {
		return os.ErrExist
	}

	// 【调用 Filer 创建目录】
	return fs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		dir, name := util.FullPath(fullDirPath).DirAndName()
		request := &filer_pb.CreateEntryRequest{
			Directory: dir,
			Entry: &filer_pb.Entry{
				Name:        name,
				IsDirectory: true,
				Attributes: &filer_pb.FuseAttributes{
					Mtime:    time.Now().Unix(),
					Crtime:   time.Now().Unix(),
					FileMode: uint32(perm | os.ModeDir),
					Uid:      fs.option.Uid,
					Gid:      fs.option.Gid,
				},
			},
			Signatures: []int32{fs.signature},
		}

		glog.V(1).Infof("mkdir: %v", request)
		if err := filer_pb.CreateEntry(context.Background(), client, request); err != nil {
			return fmt.Errorf("mkdir %s/%s: %v", dir, name, err)
		}

		return nil
	})
}

// OpenFile 打开或创建文件
// 实现 webdav.FileSystem 接口
//
// 参数:
//   - ctx: 上下文
//   - fullFilePath: 文件完整路径（如 "/documents/report.pdf"）
//   - flag: 打开标志（os.O_RDONLY、os.O_WRONLY、os.O_CREATE 等）
//   - perm: 文件权限（创建文件时使用）
//
// 返回:
//   - webdav.File: WebDavFile 对象，支持读写、Seek 等操作
//   - error: 打开错误
//
// 打开标志:
//   - os.O_RDONLY: 只读
//   - os.O_WRONLY: 只写
//   - os.O_RDWR: 读写
//   - os.O_CREATE: 文件不存在时创建
//   - os.O_EXCL: 与 O_CREATE 配合使用，文件存在时返回错误
//   - os.O_TRUNC: 打开时清空文件内容
//
// 工作模式:
//   - 创建模式（O_CREATE）：创建新文件，设置属性
//   - 读取模式：从 Filer 查询文件，返回 WebDavFile
//   - 写入模式：打开文件并创建 BufferedWriteCloser
func (fs *WebDavFileSystem) OpenFile(ctx context.Context, fullFilePath string, flag int, perm os.FileMode) (webdav.File, error) {
	glog.V(2).Infof("WebDavFileSystem.OpenFile %v %x", fullFilePath, flag)

	// 【清理路径】
	var err error
	if fullFilePath, err = clearName(fullFilePath); err != nil {
		return nil, err
	}

	// 【创建模式】
	if flag&os.O_CREATE != 0 {
		// 文件路径不应以 / 结尾
		if strings.HasSuffix(fullFilePath, "/") {
			return nil, os.ErrInvalid
		}
		_, err = fs.stat(ctx, fullFilePath)
		if err == nil {
			if flag&os.O_EXCL != 0 {
				return nil, os.ErrExist
			}
			fs.removeAll(ctx, fullFilePath)
		}

		dir, name := util.FullPath(fullFilePath).DirAndName()
		err = fs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
			if err := filer_pb.CreateEntry(context.Background(), client, &filer_pb.CreateEntryRequest{
				Directory: dir,
				Entry: &filer_pb.Entry{
					Name:        name,
					IsDirectory: perm&os.ModeDir > 0,
					Attributes: &filer_pb.FuseAttributes{
						Mtime:    0,
						Crtime:   time.Now().Unix(),
						FileMode: uint32(perm),
						Uid:      fs.option.Uid,
						Gid:      fs.option.Gid,
						TtlSec:   0,
					},
				},
				Signatures: []int32{fs.signature},
			}); err != nil {
				return fmt.Errorf("create %s: %v", fullFilePath, err)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		return &WebDavFile{
			fs:          fs,
			name:        fullFilePath,
			isDirectory: false,
			bufWriter:   buffered_writer.NewBufferedWriteCloser(fs.option.MaxMB * 1024 * 1024),
			ctx:         ctx,
		}, nil
	}

	// 【读取模式】
	// 查询文件信息
	fi, err := fs.stat(ctx, fullFilePath)
	if err != nil {
		if err == os.ErrNotExist {
			return nil, err
		}
		return &WebDavFile{fs: fs, ctx: ctx}, nil
	}

	// 如果是目录且路径不以 / 结尾，添加 /
	if !strings.HasSuffix(fullFilePath, "/") && fi.IsDir() {
		fullFilePath += "/"
	}

	// 【返回 WebDavFile 对象】
	return &WebDavFile{
		fs:          fs,
		name:        fullFilePath,
		isDirectory: false,
		bufWriter:   buffered_writer.NewBufferedWriteCloser(fs.option.MaxMB * 1024 * 1024),
		ctx:         ctx,
	}, nil

}

// removeAll 内部删除函数，递归删除文件或目录
//
// 参数:
//   - ctx: 上下文
//   - fullFilePath: 文件或目录的完整路径
//
// 返回:
//   - error: 删除错误
func (fs *WebDavFileSystem) removeAll(ctx context.Context, fullFilePath string) error {
	// 【清理路径】
	var err error
	if fullFilePath, err = clearName(fullFilePath); err != nil {
		return err
	}

	// 【分离目录和文件名】
	dir, name := util.FullPath(fullFilePath).DirAndName()

	// 【调用 Filer 删除】
	// 参数说明：
	//   - true: 递归删除（删除目录及其内容）
	//   - false: 不删除分块
	//   - false: 不是从其他集群删除
	//   - false: 不强制删除
	//   - []int32{fs.signature}: 签名列表
	return filer_pb.Remove(context.Background(), fs, dir, name, true, false, false, false, []int32{fs.signature})

}

// RemoveAll 删除文件或目录（递归）
// 实现 webdav.FileSystem 接口
//
// 参数:
//   - ctx: 上下文
//   - name: 文件或目录路径
//
// 返回:
//   - error: 删除错误
//
// 功能:
//   - 删除文件：直接删除
//   - 删除目录：递归删除目录及其所有内容
func (fs *WebDavFileSystem) RemoveAll(ctx context.Context, name string) error {

	glog.V(2).Infof("WebDavFileSystem.RemoveAll %v", name)

	return fs.removeAll(ctx, name)
}

// Rename 重命名或移动文件/目录
// 实现 webdav.FileSystem 接口
//
// 参数:
//   - ctx: 上下文
//   - oldName: 旧路径
//   - newName: 新路径
//
// 返回:
//   - error: 重命名错误
//
// 功能:
//   - 重命名：oldName 和 newName 在同一目录
//   - 移动：oldName 和 newName 在不同目录
//   - 支持文件和目录
//
// 注意:
//   - 如果 newName 已存在，返回 os.ErrExist
//   - 如果 oldName 不存在，返回 os.ErrExist
func (fs *WebDavFileSystem) Rename(ctx context.Context, oldName, newName string) error {

	glog.V(2).Infof("WebDavFileSystem.Rename %v to %v", oldName, newName)

	// 【清理路径】
	var err error
	if oldName, err = clearName(oldName); err != nil {
		return err
	}
	if newName, err = clearName(newName); err != nil {
		return err
	}

	// 【检查源文件是否存在】
	of, err := fs.stat(ctx, oldName)
	if err != nil {
		return os.ErrExist
	}

	// 【处理目录路径】
	// 目录重命名时，去除尾部 /
	if of.IsDir() {
		if strings.HasSuffix(oldName, "/") {
			oldName = strings.TrimRight(oldName, "/")
		}
		if strings.HasSuffix(newName, "/") {
			newName = strings.TrimRight(newName, "/")
		}
	}

	// 【检查目标路径是否已存在】
	_, err = fs.stat(ctx, newName)
	if err == nil {
		return os.ErrExist
	}

	// 【分离路径】
	oldDir, oldBaseName := util.FullPath(oldName).DirAndName()
	newDir, newBaseName := util.FullPath(newName).DirAndName()

	// 【调用 Filer 原子重命名】
	return fs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.AtomicRenameEntryRequest{
			OldDirectory: oldDir,
			OldName:      oldBaseName,
			NewDirectory: newDir,
			NewName:      newBaseName,
		}

		_, err := client.AtomicRenameEntry(ctx, request)
		if err != nil {
			return fmt.Errorf("renaming %s/%s => %s/%s: %v", oldDir, oldBaseName, newDir, newBaseName, err)
		}

		return nil

	})
}

// stat 内部函数，获取文件信息
//
// 参数:
//   - ctx: 上下文
//   - fullFilePath: 文件完整路径
//
// 返回:
//   - os.FileInfo: 文件信息对象
//   - error: 获取错误
//
// 功能:
//   - 从 Filer 查询 Entry
//   - 构造 FileInfo 对象
//   - 特殊处理根目录 "/"
func (fs *WebDavFileSystem) stat(ctx context.Context, fullFilePath string) (os.FileInfo, error) {
	// 【清理路径】
	var err error
	if fullFilePath, err = clearName(fullFilePath); err != nil {
		return nil, err
	}

	fullpath := util.FullPath(fullFilePath)

	// 【从 Filer 获取 Entry】
	var fi FileInfo
	entry, err := filer_pb.GetEntry(context.Background(), fs, fullpath)
	if err != nil {
		if err == filer_pb.ErrNotFound {
			return nil, os.ErrNotExist
		}
		fi.err = err
		return &fi, nil
	}
	if entry == nil {
		return nil, os.ErrNotExist
	}

	// 【构造 FileInfo】
	fi.size = int64(filer.FileSize(entry))
	fi.name = string(fullpath)
	fi.mode = os.FileMode(entry.Attributes.FileMode)
	fi.modifiedTime = time.Unix(entry.Attributes.Mtime, 0)
	fi.etag = filer.ETag(entry)
	fi.isDirectory = entry.IsDirectory

	// 【特殊处理根目录】
	if fi.name == "/" {
		fi.modifiedTime = time.Now()
		fi.isDirectory = true
	}

	return &fi, nil
}

// Stat 获取文件信息
// 实现 webdav.FileSystem 接口
//
// 参数:
//   - ctx: 上下文
//   - name: 文件路径
//
// 返回:
//   - os.FileInfo: 文件信息对象
//   - error: 获取错误
func (fs *WebDavFileSystem) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	glog.V(2).Infof("WebDavFileSystem.Stat %v", name)

	return fs.stat(ctx, name)
}

// saveDataAsChunk 将数据保存为一个 chunk
// 用于文件上传时的分块存储
//
// 参数:
//   - reader: 数据读取器
//   - name: 文件名
//   - offset: 文件内偏移量
//   - tsNs: 时间戳（纳秒）
//
// 返回:
//   - chunk: 上传成功的 FileChunk 信息
//   - err: 上传错误
//
// 工作流程:
//   1. 创建 Uploader
//   2. 向 Master 请求分配 Volume
//   3. 上传数据到 Volume Server
//   4. 返回 FileChunk 元数据
func (f *WebDavFile) saveDataAsChunk(reader io.Reader, name string, offset int64, tsNs int64) (chunk *filer_pb.FileChunk, err error) {
	uploader, uploaderErr := operation.NewUploader()
	if uploaderErr != nil {
		glog.V(0).Infof("upload data %v: %v", f.name, uploaderErr)
		return nil, fmt.Errorf("upload data: %w", uploaderErr)
	}

	fileId, uploadResult, flushErr, _ := uploader.UploadWithRetry(
		f.fs,
		&filer_pb.AssignVolumeRequest{
			Count:       1,
			Replication: f.fs.option.Replication,
			Collection:  f.fs.option.Collection,
			DiskType:    f.fs.option.DiskType,
			Path:        name,
		},
		&operation.UploadOption{
			Filename:          f.name,
			Cipher:            f.fs.option.Cipher,
			IsInputCompressed: false,
			MimeType:          "",
			PairMap:           nil,
		},
		func(host, fileId string) string {
			return fmt.Sprintf("http://%s/%s", host, fileId)
		},
		reader,
	)

	if flushErr != nil {
		glog.V(0).Infof("upload data %v: %v", f.name, flushErr)
		return nil, fmt.Errorf("upload data: %w", flushErr)
	}
	if uploadResult.Error != "" {
		glog.V(0).Infof("upload failure %v: %v", f.name, flushErr)
		return nil, fmt.Errorf("upload result: %v", uploadResult.Error)
	}
	// 【返回 FileChunk】
	return uploadResult.ToPbFileChunk(fileId, offset, tsNs), nil
}

// Write 写入数据到文件
// 实现 io.Writer 接口
//
// 参数:
//   - buf: 要写入的数据
//
// 返回:
//   - int: 实际写入的字节数
//   - error: 写入错误
//
// 工作流程:
//   1. 获取文件的 Entry（如果还没有）
//   2. 配置 FlushFunc（数据上传到 Volume Server）
//   3. 配置 CloseFunc（更新 Filer 元数据）
//   4. 写入数据到缓冲区
//   5. 达到阈值时自动上传
//
// 缓冲机制:
//   - 使用 BufferedWriteCloser 缓冲写入
//   - 达到 MaxMB 阈值时触发 FlushFunc
//   - FlushFunc 将数据上传为一个 chunk
//   - CloseFunc 在关闭时更新 Entry
func (f *WebDavFile) Write(buf []byte) (int, error) {

	glog.V(2).Infof("WebDavFileSystem.Write %v", f.name)

	fullPath := util.FullPath(f.name)
	dir, _ := fullPath.DirAndName()

	// 【获取文件 Entry】
	var getErr error
	ctx := context.Background()
	if f.entry == nil {
		f.entry, getErr = filer_pb.GetEntry(context.Background(), f.fs, fullPath)
	}

	if f.entry == nil {
		return 0, getErr
	}
	if getErr != nil {
		return 0, getErr
	}

	// 【配置 FlushFunc】
	// 当缓冲区满时，自动上传数据
	if f.bufWriter.FlushFunc == nil {
		// FlushFunc：将缓冲数据上传到 Volume Server
		f.bufWriter.FlushFunc = func(data []byte, offset int64) (flushErr error) {

			// 【上传数据为 chunk】
			var chunk *filer_pb.FileChunk
			chunk, flushErr = f.saveDataAsChunk(util.NewBytesReader(data), f.name, offset, time.Now().UnixNano())

			if flushErr != nil {
				// 【上传失败处理】
				// 如果是新文件（Mtime=0），删除失败的文件
				if f.entry.Attributes.Mtime == 0 {
					if err := f.fs.removeAll(ctx, f.name); err != nil {
						glog.Errorf("bufWriter.Flush remove file error: %+v", f.name)
					}
				}
				return fmt.Errorf("%s upload result: %v", f.name, flushErr)
			}

			// 【添加 chunk 到 Entry】
			f.entry.Content = nil
			f.entry.Chunks = append(f.entry.GetChunks(), chunk)

			return flushErr
		}

		// CloseFunc：关闭时更新 Filer 元数据
		f.bufWriter.CloseFunc = func() error {

			// 【处理 chunk manifest】
			// 如果 chunk 数量过多，创建 manifest chunk
			manifestedChunks, manifestErr := filer.MaybeManifestize(f.saveDataAsChunk, f.entry.GetChunks())
			if manifestErr != nil {
				// 不是致命错误，可以继续
				glog.V(0).Infof("file %s close MaybeManifestize: %v", f.name, manifestErr)
			} else {
				f.entry.Chunks = manifestedChunks
			}

			// 【更新 Filer Entry】
			flushErr := f.fs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
				f.entry.Attributes.Mtime = time.Now().Unix()

				request := &filer_pb.UpdateEntryRequest{
					Directory:  dir,
					Entry:      f.entry,
					Signatures: []int32{f.fs.signature},
				}

				if _, err := client.UpdateEntry(ctx, request); err != nil {
					return fmt.Errorf("update %s: %v", f.name, err)
				}

				return nil
			})
			return flushErr
		}
	}

	// 【写入缓冲区】
	written, err := f.bufWriter.Write(buf)

	// 【更新文件大小和偏移量】
	if err == nil {
		f.entry.Attributes.FileSize = uint64(max(f.off+int64(written), int64(f.entry.Attributes.FileSize)))
		glog.V(3).Infof("WebDavFileSystem.Write %v: written [%d,%d)", f.name, f.off, f.off+int64(len(buf)))
		f.off += int64(written)
	}

	return written, err
}

// Close 关闭文件
// 实现 webdav.File 接口
//
// 返回:
//   - error: 关闭错误
//
// 功能:
//   - 刷新缓冲区（触发 FlushFunc）
//   - 更新 Filer 元数据（触发 CloseFunc）
//   - 清理内部状态
func (f *WebDavFile) Close() error {

	glog.V(2).Infof("WebDavFileSystem.Close %v", f.name)

	// 【关闭缓冲写入器】
	if f.bufWriter == nil {
		return nil
	}
	err := f.bufWriter.Close()

	// 【清理内部状态】
	if f.entry != nil {
		f.entry = nil
		f.visibleIntervals = nil
	}

	return err
}

// Read 从文件读取数据
// 实现 io.Reader 接口
//
// 参数:
//   - p: 读取数据的缓冲区
//
// 返回:
//   - readSize: 实际读取的字节数
//   - err: 读取错误
//
// 工作流程:
//   1. 获取文件 Entry（如果还没有）
//   2. 计算可见区间（处理覆盖的 chunk）
//   3. 创建 ChunkReader（从 Volume Server 读取）
//   4. 从当前偏移量读取数据
//   5. 更新偏移量
//
// 缓存机制:
//   - 使用 readerCache 缓存 ChunkReader
//   - 减少重复读取的开销
func (f *WebDavFile) Read(p []byte) (readSize int, err error) {

	glog.V(2).Infof("WebDavFileSystem.Read %v", f.name)

	// 【获取文件 Entry】
	if f.entry == nil {
		f.entry, err = filer_pb.GetEntry(context.Background(), f.fs, util.FullPath(f.name))
	}
	if f.entry == nil {
		return 0, err
	}
	if err != nil {
		return 0, err
	}

	// 【检查文件大小】
	fileSize := int64(filer.FileSize(f.entry))
	if fileSize == 0 {
		return 0, io.EOF
	}

	// 【计算可见区间】
	// 处理覆盖的 chunk，确定实际可见的数据
	if f.visibleIntervals == nil {
		f.visibleIntervals, _ = filer.NonOverlappingVisibleIntervals(f.ctx, filer.LookupFn(f.fs), f.entry.GetChunks(), 0, fileSize)
		f.reader = nil
	}

	// 【创建 ChunkReader】
	if f.reader == nil {
		chunkViews := filer.ViewFromVisibleIntervals(f.visibleIntervals, 0, fileSize)
		f.reader = filer.NewChunkReaderAtFromClient(f.ctx, f.fs.readerCache, chunkViews, fileSize)
	}

	// 【从当前偏移量读取数据】
	readSize, err = f.reader.ReadAt(p, f.off)

	glog.V(3).Infof("WebDavFileSystem.Read %v: [%d,%d)", f.name, f.off, f.off+int64(readSize))

	// 【更新偏移量】
	f.off += int64(readSize)

	if err != nil && err != io.EOF {
		glog.Errorf("file read %s: %v", f.name, err)
	}

	return

}

// Readdir 读取目录内容
// 实现 webdav.File 接口
//
// 参数:
//   - count: 读取的条目数（-1 表示全部，0 表示全部）
//
// 返回:
//   - ret: 文件信息列表
//   - err: 读取错误
//
// 功能:
//   - 从 Filer 读取目录所有条目
//   - 支持分页（通过 count 和内部偏移量）
//   - 自动为目录名添加 / 后缀
func (f *WebDavFile) Readdir(count int) (ret []os.FileInfo, err error) {

	glog.V(2).Infof("WebDavFileSystem.Readdir %v count %d", f.name, count)

	// 【获取目录路径】
	dir, _ := util.FullPath(f.name).DirAndName()

	// 【读取目录所有条目】
	err = filer_pb.ReadDirAllEntries(context.Background(), f.fs, util.FullPath(dir), "", func(entry *filer_pb.Entry, isLast bool) error {
		// 构造 FileInfo
		fi := FileInfo{
			size:         int64(filer.FileSize(entry)),
			name:         entry.Name,
			mode:         os.FileMode(entry.Attributes.FileMode),
			modifiedTime: time.Unix(entry.Attributes.Mtime, 0),
			isDirectory:  entry.IsDirectory,
		}

		// 为目录名添加 / 后缀
		if !strings.HasSuffix(fi.name, "/") && fi.IsDir() {
			fi.name += "/"
		}
		glog.V(4).Infof("entry: %v", fi.name)
		ret = append(ret, &fi)
		return nil
	})
	if err != nil {
		return nil, err
	}

	// 【处理分页】
	old := f.off
	if old >= int64(len(ret)) {
		// 已经读到末尾
		if count > 0 {
			return nil, io.EOF
		}
		return nil, nil
	}
	if count > 0 {
		// 读取指定数量
		f.off += int64(count)
		if f.off > int64(len(ret)) {
			f.off = int64(len(ret))
		}
	} else {
		// 读取全部
		f.off = int64(len(ret))
		old = 0
	}

	return ret[old:f.off], nil
}

// Seek 移动文件读取位置
// 实现 io.Seeker 接口
//
// 参数:
//   - offset: 偏移量
//   - whence: 起始位置（io.SeekStart、io.SeekCurrent、io.SeekEnd）
//
// 返回:
//   - int64: 新的偏移量
//   - error: Seek 错误
//
// 起始位置:
//   - io.SeekStart: 从文件开头
//   - io.SeekCurrent: 从当前位置
//   - io.SeekEnd: 从文件末尾
func (f *WebDavFile) Seek(offset int64, whence int) (int64, error) {

	glog.V(2).Infof("WebDavFile.Seek %v %v %v", f.name, offset, whence)

	ctx := context.Background()

	var err error
	switch whence {
	case io.SeekStart:
		// 从文件开头
		f.off = 0
	case io.SeekEnd:
		// 从文件末尾
		if fi, err := f.fs.stat(ctx, f.name); err != nil {
			return 0, err
		} else {
			f.off = fi.Size()
		}
	}
	f.off += offset
	return f.off, err
}

func (f *WebDavFile) Stat() (os.FileInfo, error) {

	glog.V(2).Infof("WebDavFile.Stat %v", f.name)

	ctx := context.Background()

	return f.fs.stat(ctx, f.name)
}
