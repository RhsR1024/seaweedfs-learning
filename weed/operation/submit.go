package operation

import (
	"context"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"io"
	"math/rand/v2"
	"mime"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/security"
)

// FilePart 文件部分结构
// 表示要上传的一个文件或文件的一部分
// 包含了上传所需的所有信息:数据流、文件名、大小、元数据等
type FilePart struct {
	// Reader 文件内容读取器
	// 提供文件数据的读取接口,支持任何实现了 io.Reader 的类型
	Reader io.Reader

	// FileName 文件名
	// 上传时使用的文件名,会作为元数据保存
	FileName string

	// FileSize 文件大小(字节)
	// 用于决定是否需要分块上传
	FileSize int64

	// MimeType MIME 类型
	// 文件的内容类型,如 "image/jpeg", "text/plain" 等
	MimeType string

	// ModTime 文件修改时间(Unix 时间戳,秒)
	// 保存文件的最后修改时间,用于版本控制和缓存
	ModTime int64 //in seconds

	// Pref 存储偏好设置
	// 定义文件的存储策略(副本、集合、数据中心等)
	Pref StoragePreference

	// Server 目标服务器地址
	// 从 assign 结果中获取,指定文件上传的目标 volume 服务器
	Server string //this comes from assign result

	// Fid 文件 ID
	// 从 assign 结果中获取,但可以自定义
	// 用于唯一标识这个文件
	Fid string //this comes from assign result, but customizable

	// Fsync 是否同步刷盘
	// true 表示写入后立即调用 fsync,确保数据持久化
	Fsync bool
}

// SubmitResult 文件提交结果
// 包含文件上传后的完整信息和状态
type SubmitResult struct {
	// FileName 文件名
	FileName string `json:"fileName,omitempty"`

	// FileUrl 文件访问 URL
	// 完整的公开访问地址,格式: http://server/fid
	FileUrl string `json:"url,omitempty"`

	// Fid 文件 ID
	// SeaweedFS 分配的唯一文件标识符
	Fid string `json:"fid,omitempty"`

	// Size 文件大小(字节)
	// 实际上传的文件大小
	Size uint32 `json:"size,omitempty"`

	// Error 错误信息
	// 如果上传失败,这里包含错误描述
	Error string `json:"error,omitempty"`
}

// StoragePreference 存储偏好设置
// 定义文件的存储策略和约束条件
type StoragePreference struct {
	// Replication 副本策略
	// 格式如 "001", "010", "100" 等,定义副本分布规则
	Replication string

	// Collection 集合名称
	// 用于逻辑分组,同一集合的文件存储在一起
	Collection string

	// DataCenter 数据中心
	// 指定文件存储的数据中心位置
	DataCenter string

	// Ttl 生存时间
	// 文件的过期时间设置,如 "3m" (3分钟), "2h" (2小时), "5d" (5天)
	Ttl string

	// DiskType 磁盘类型
	// 如 "hdd", "ssd" 等,用于性能优化
	DiskType string

	// MaxMB 最大分块大小(MB)
	// 超过此大小的文件会被分块上传
	// 0 表示不分块
	MaxMB int
}

// GetMasterFn 获取 Master 节点地址的函数类型
// 用于动态获取当前可用的 Master 服务器地址
// 返回: Master 服务器的地址
type GetMasterFn func(ctx context.Context) pb.ServerAddress

// SubmitFiles 批量提交上传文件
// 这是文件上传的高层封装函数,处理多个文件的上传流程:
// 1. 从 Master 请求分配文件 ID 和存储位置
// 2. 依次上传每个文件到指定的 Volume 服务器
// 3. 返回每个文件的上传结果
//
// 参数:
//   masterFn: 获取 Master 地址的函数
//   grpcDialOption: gRPC 连接选项
//   files: 要上传的文件列表
//   pref: 存储偏好设置
//   usePublicUrl: 是否使用公开 URL(用于外部访问)
//
// 返回:
//   []SubmitResult: 每个文件的上传结果
//   error: 如果分配失败则返回错误,单个文件上传失败会记录在结果中
func SubmitFiles(masterFn GetMasterFn, grpcDialOption grpc.DialOption, files []*FilePart, pref StoragePreference, usePublicUrl bool) ([]SubmitResult, error) {
	// 初始化结果数组,预先设置文件名
	results := make([]SubmitResult, len(files))
	for index, file := range files {
		results[index].FileName = file.FileName
	}

	// 构建 volume 分配请求
	// 一次性为所有文件申请存储空间
	ar := &VolumeAssignRequest{
		Count:       uint64(len(files)),     // 请求的文件数量
		Replication: pref.Replication,       // 副本策略
		Collection:  pref.Collection,        // 集合名称
		DataCenter:  pref.DataCenter,        // 数据中心
		Ttl:         pref.Ttl,               // 生存时间
		DiskType:    pref.DiskType,          // 磁盘类型
	}

	// 向 Master 请求分配存储位置
	ret, err := Assign(context.Background(), masterFn, grpcDialOption, ar)
	if err != nil {
		// 分配失败,标记所有文件的错误
		for index := range files {
			results[index].Error = err.Error()
		}
		return results, err
	}

	// 依次上传每个文件
	for index, file := range files {
		// 设置文件 ID
		// 第一个文件使用分配的 Fid
		// 后续文件添加 "_序号" 后缀
		file.Fid = ret.Fid
		if index > 0 {
			file.Fid = file.Fid + "_" + strconv.Itoa(index)
		}

		// 设置目标服务器地址
		file.Server = ret.Url
		if usePublicUrl {
			file.Server = ret.PublicUrl
		}

		// 设置存储偏好
		file.Pref = pref

		// 执行上传
		results[index].Size, err = file.Upload(pref.MaxMB, masterFn, usePublicUrl, ret.Auth, grpcDialOption)
		if err != nil {
			results[index].Error = err.Error()
		}

		// 记录结果
		results[index].Fid = file.Fid
		results[index].FileUrl = ret.PublicUrl + "/" + file.Fid
	}
	return results, nil
}

// NewFileParts 从文件路径列表创建 FilePart 数组
// 批量打开文件并读取元数据,准备上传
//
// 参数:
//   fullPathFilenames: 文件的完整路径列表
//
// 返回:
//   []*FilePart: FilePart 对象数组
//   error: 如果任何文件打开失败则返回错误
func NewFileParts(fullPathFilenames []string) (ret []*FilePart, err error) {
	ret = make([]*FilePart, len(fullPathFilenames))
	for index, file := range fullPathFilenames {
		if ret[index], err = newFilePart(file); err != nil {
			return
		}
	}
	return
}
// newFilePart 从文件路径创建单个 FilePart 对象
// 打开文件,读取文件信息(大小、修改时间、MIME 类型等)
//
// 参数:
//   fullPathFilename: 文件的完整路径
//
// 返回:
//   *FilePart: FilePart 对象
//   error: 文件打开或读取失败时返回错误
func newFilePart(fullPathFilename string) (ret *FilePart, err error) {
	ret = &FilePart{}

	// 打开文件
	fh, openErr := os.Open(fullPathFilename)
	if openErr != nil {
		glog.V(0).Info("Failed to open file: ", fullPathFilename)
		return ret, openErr
	}
	ret.Reader = fh

	// 获取文件信息
	fi, fiErr := fh.Stat()
	if fiErr != nil {
		glog.V(0).Info("Failed to stat file:", fullPathFilename)
		return ret, fiErr
	}

	// 设置文件元数据
	ret.ModTime = fi.ModTime().UTC().Unix() // 修改时间(UTC)
	ret.FileSize = fi.Size()                 // 文件大小
	ext := strings.ToLower(path.Ext(fullPathFilename)) // 文件扩展名
	ret.FileName = fi.Name()                 // 文件名(不含路径)

	// 根据扩展名推断 MIME 类型
	if ext != "" {
		ret.MimeType = mime.TypeByExtension(ext)
	}

	return ret, nil
}

// Upload 上传文件到 Volume 服务器
// 根据文件大小决定是否分块上传:
// - 文件小于 maxMB: 直接上传整个文件
// - 文件大于 maxMB: 分块上传,并创建 chunk manifest
//
// 参数:
//   maxMB: 分块大小阈值(MB),0 表示不分块
//   masterFn: 获取 Master 地址的函数
//   usePublicUrl: 是否使用公开 URL
//   jwt: JWT 认证令牌
//   grpcDialOption: gRPC 连接选项
//
// 返回:
//   retSize: 上传的总字节数
//   err: 上传错误
func (fi *FilePart) Upload(maxMB int, masterFn GetMasterFn, usePublicUrl bool, jwt security.EncodedJwt, grpcDialOption grpc.DialOption) (retSize uint32, err error) {
	// 构建文件 URL
	fileUrl := "http://" + fi.Server + "/" + fi.Fid
	if fi.ModTime != 0 {
		// 添加修改时间参数,用于版本控制
		fileUrl += "?ts=" + strconv.Itoa(int(fi.ModTime))
	}
	if fi.Fsync {
		// 添加 fsync 参数,强制同步到磁盘
		fileUrl += "?fsync=true"
	}

	// 确保在函数退出时关闭文件
	if closer, ok := fi.Reader.(io.Closer); ok {
		defer closer.Close()
	}

	baseName := path.Base(fi.FileName)

	// 判断是否需要分块上传
	if maxMB > 0 && fi.FileSize > int64(maxMB*1024*1024) {
		// 文件大小超过阈值,执行分块上传
		chunkSize := int64(maxMB * 1024 * 1024)
		chunks := fi.FileSize/chunkSize + 1 // 计算需要的分块数

		// 创建 chunk manifest,记录所有分块信息
		cm := ChunkManifest{
			Name:   baseName,
			Size:   fi.FileSize,
			Mime:   fi.MimeType,
			Chunks: make([]*ChunkInfo, 0, chunks),
		}

		var ret *AssignResult
		var id string

		// 如果指定了数据中心,一次性为所有 chunk 分配 ID
		if fi.Pref.DataCenter != "" {
			ar := &VolumeAssignRequest{
				Count:       uint64(chunks),
				Replication: fi.Pref.Replication,
				Collection:  fi.Pref.Collection,
				Ttl:         fi.Pref.Ttl,
				DiskType:    fi.Pref.DiskType,
			}
			ret, err = Assign(context.Background(), masterFn, grpcDialOption, ar)
			if err != nil {
				return
			}
		}

		// 逐个上传每个 chunk
		for i := int64(0); i < chunks; i++ {
			// 如果未指定数据中心,为每个 chunk 单独分配 ID
			if fi.Pref.DataCenter == "" {
				ar := &VolumeAssignRequest{
					Count:       1,
					Replication: fi.Pref.Replication,
					Collection:  fi.Pref.Collection,
					Ttl:         fi.Pref.Ttl,
					DiskType:    fi.Pref.DiskType,
				}
				ret, err = Assign(context.Background(), masterFn, grpcDialOption, ar)
				if err != nil {
					// 分配失败,删除已上传的 chunk
					// delete all uploaded chunks
					cm.DeleteChunks(masterFn, usePublicUrl, grpcDialOption)
					return
				}
				id = ret.Fid
			} else {
				// 使用批量分配的 ID,为每个 chunk 添加序号后缀
				id = ret.Fid
				if i > 0 {
					id += "_" + strconv.FormatInt(i, 10)
				}
			}

			// 生成 chunk 上传 URL
			fileUrl := genFileUrl(ret, id, usePublicUrl)

			// 上传单个 chunk
			count, e := uploadOneChunk(
				baseName+"-"+strconv.FormatInt(i+1, 10), // chunk 文件名
				io.LimitReader(fi.Reader, chunkSize),    // 限制读取大小
				masterFn, fileUrl,
				ret.Auth)
			if e != nil {
				// 上传失败,删除所有已上传的 chunk
				// delete all uploaded chunks
				cm.DeleteChunks(masterFn, usePublicUrl, grpcDialOption)
				return 0, e
			}

			// 记录 chunk 信息
			cm.Chunks = append(cm.Chunks,
				&ChunkInfo{
					Offset: i * chunkSize, // chunk 在原文件中的偏移量
					Size:   int64(count),  // chunk 大小
					Fid:    id,            // chunk 的文件 ID
				},
			)
			retSize += count
		}

		// 上传 chunk manifest
		err = uploadChunkedFileManifest(fileUrl, &cm, jwt)
		if err != nil {
			// 上传 manifest 失败,删除所有 chunk
			// delete all uploaded chunks
			cm.DeleteChunks(masterFn, usePublicUrl, grpcDialOption)
		}
	} else {
		// 文件较小,直接上传整个文件
		uploadOption := &UploadOption{
			UploadUrl:         fileUrl,
			Filename:          baseName,
			Cipher:            false,
			IsInputCompressed: false,
			MimeType:          fi.MimeType,
			PairMap:           nil,
			Jwt:               jwt,
		}

		uploader, e := NewUploader()
		if e != nil {
			return 0, e
		}

		ret, e, _ := uploader.Upload(context.Background(), fi.Reader, uploadOption)
		if e != nil {
			return 0, e
		}
		return ret.Size, e
	}
	return
}

// genFileUrl 生成文件上传 URL
// 随机选择一个副本服务器用于上传,实现负载均衡
//
// 参数:
//   ret: 分配结果,包含主服务器和副本服务器地址
//   id: 文件 ID
//   usePublicUrl: 是否使用公开 URL
//
// 返回: 文件上传 URL
func genFileUrl(ret *AssignResult, id string, usePublicUrl bool) string {
	// 默认使用主服务器地址
	fileUrl := "http://" + ret.Url + "/" + id
	if usePublicUrl {
		fileUrl = "http://" + ret.PublicUrl + "/" + id
	}

	// 随机选择一个副本服务器
	// 有一定概率使用副本服务器,实现负载均衡
	for _, replica := range ret.Replicas {
		if rand.IntN(len(ret.Replicas)+1) == 0 {
			fileUrl = "http://" + replica.Url + "/" + id
			if usePublicUrl {
				fileUrl = "http://" + replica.PublicUrl + "/" + id
			}
		}
	}
	return fileUrl
}

// uploadOneChunk 上传单个文件块
// 将一个 chunk 的数据上传到指定的 Volume 服务器
//
// 参数:
//   filename: chunk 文件名
//   reader: chunk 数据读取器
//   masterFn: 获取 Master 地址的函数
//   fileUrl: 上传目标 URL
//   jwt: JWT 认证令牌
//
// 返回:
//   size: 上传的字节数
//   e: 错误信息
func uploadOneChunk(filename string, reader io.Reader, masterFn GetMasterFn,
	fileUrl string, jwt security.EncodedJwt,
) (size uint32, e error) {
	glog.V(4).Info("Uploading part ", filename, " to ", fileUrl, "...")
	uploadOption := &UploadOption{
		UploadUrl:         fileUrl,
		Filename:          filename,
		Cipher:            false,
		IsInputCompressed: false,
		MimeType:          "",
		PairMap:           nil,
		Jwt:               jwt,
	}

	uploader, uploaderError := NewUploader()
	if uploaderError != nil {
		return 0, uploaderError
	}

	uploadResult, uploadError, _ := uploader.Upload(context.Background(), reader, uploadOption)
	if uploadError != nil {
		return 0, uploadError
	}
	return uploadResult.Size, nil
}

// uploadChunkedFileManifest 上传分块文件的 manifest
// manifest 是一个 JSON 文件,记录了所有 chunk 的元数据
// 包括每个 chunk 的 ID、偏移量、大小等信息
// 用于后续读取时重组完整文件
//
// 参数:
//   fileUrl: 原文件的 URL
//   manifest: chunk manifest 对象
//   jwt: JWT 认证令牌
//
// 返回: 错误信息
func uploadChunkedFileManifest(fileUrl string, manifest *ChunkManifest, jwt security.EncodedJwt) error {
	// 序列化 manifest 为 JSON
	buf, e := manifest.Marshal()
	if e != nil {
		return e
	}
	glog.V(4).Info("Uploading chunks manifest ", manifest.Name, " to ", fileUrl, "...")

	// 添加 cm=true 参数,标识这是一个 chunk manifest
	u, _ := url.Parse(fileUrl)
	q := u.Query()
	q.Set("cm", "true")
	u.RawQuery = q.Encode()

	uploadOption := &UploadOption{
		UploadUrl:         u.String(),
		Filename:          manifest.Name,
		Cipher:            false,
		IsInputCompressed: false,
		MimeType:          "application/json",
		PairMap:           nil,
		Jwt:               jwt,
	}

	uploader, e := NewUploader()
	if e != nil {
		return e
	}

	_, e = uploader.UploadData(context.Background(), buf, uploadOption)
	return e
}
