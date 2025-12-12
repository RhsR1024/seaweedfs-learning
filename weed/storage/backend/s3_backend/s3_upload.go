// Package s3_backend 实现基于 AWS S3 的远程存储后端
// 本文件实现 S3 上传功能，支持分片上传和进度跟踪
package s3_backend

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"os"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// uploadToS3 将本地文件上传到 S3
//
// 参数:
//   - sess: S3 API 客户端
//   - filename: 本地文件路径
//   - destBucket: 目标 S3 存储桶
//   - destKey: 目标 S3 对象 key
//   - storageClass: 存储类别（STANDARD、STANDARD_IA、GLACIER 等）
//   - fn: 进度回调函数，接收已上传字节数和百分比
// 返回:
//   - fileSize: 上传的文件大小（字节）
//   - err: 错误信息（如果有）
//
// 实现细节：
//   1. 打开本地文件并获取文件大小
//   2. 根据文件大小动态调整分片大小（64MB 起步，最大支持 1000 个分片）
//   3. 创建 S3 上传器，配置分片大小和并发数
//   4. 使用带进度跟踪的读取器上传文件
//   5. 返回文件大小和上传结果
//
// 分片策略：
//   - 起始分片大小：64MB
//   - 如果文件大小 > 64GB（64MB × 1000），则增大分片大小
//   - 每次增大 4 倍，确保分片数不超过 1000（S3 限制最多 10000 个分片）
//   - 并发上传 5 个分片
//
// 性能考虑：
//   - 大文件使用分片上传，提高上传速度和可靠性
//   - 分片上传失败时可以只重传失败的分片
//   - 并发上传 5 个分片，充分利用网络带宽
func uploadToS3(sess s3iface.S3API, filename string, destBucket string, destKey string, storageClass string, fn func(progressed int64, percentage float32) error) (fileSize int64, err error) {

	// 打开本地文件
	f, err := os.Open(filename)
	if err != nil {
		return 0, fmt.Errorf("failed to open file %q, %v", filename, err)
	}
	defer f.Close()

	// 获取文件大小
	info, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to stat file %q, %v", filename, err)
	}

	fileSize = info.Size()

	// 动态计算分片大小
	// S3 最小分片大小为 5MB，默认使用 64MB
	partSize := int64(64 * 1024 * 1024) // 64MB

	// 如果文件大小超过 64GB（64MB × 1000），则增大分片大小
	// 确保分片数不超过 1000（避免达到 S3 的 10000 分片上限）
	for partSize*1000 < fileSize {
		partSize *= 4 // 每次增大 4 倍
	}

	// 创建 S3 上传器，配置分片大小和并发数
	uploader := s3manager.NewUploaderWithClient(sess, func(u *s3manager.Uploader) {
		u.PartSize = partSize  // 分片大小
		u.Concurrency = 5       // 并发上传 5 个分片
	})

	// 创建带进度跟踪的文件读取器
	fileReader := &s3UploadProgressedReader{
		fp:      f,
		size:    fileSize,
		signMap: map[int64]struct{}{}, // 用于去重签名请求
		fn:      fn,
	}

	// 上传文件到 S3
	var result *s3manager.UploadOutput
	result, err = uploader.Upload(&s3manager.UploadInput{
		Bucket:       aws.String(destBucket),
		Key:          aws.String(destKey),
		Body:         fileReader,
		StorageClass: aws.String(storageClass),
	})

	// 处理上传错误
	if err != nil {
		return 0, fmt.Errorf("failed to upload file %s: %v", filename, err)
	}
	glog.V(1).Infof("file %s uploaded to %s\n", filename, result.Location)

	return
}

// s3UploadProgressedReader 实现带进度跟踪的文件读取器
//
// 改编自 AWS SDK 官方示例：
// - https://github.com/aws/aws-sdk-go/pull/1868
// - https://github.com/aws/aws-sdk-go/blob/main/example/service/s3/putObjectWithProcess/putObjWithProcess.go
//
// 字段说明：
//   - fp: 文件句柄
//   - size: 文件总大小（字节）
//   - read: 已读取的字节数
//   - signMap: 签名请求去重映射（同一偏移量会被读取两次：签名 + 实际上传）
//   - mux: 互斥锁，保护并发访问 read 和 signMap
//   - fn: 进度回调函数
//
// 设计要点：
//   - S3 SDK 在上传前会先读取一次数据计算签名，然后再实际上传
//   - 使用 signMap 去重，只在第二次读取时更新进度
//   - 支持并发分片上传，通过互斥锁保护共享状态
type s3UploadProgressedReader struct {
	fp      *os.File                 // 文件句柄
	size    int64                    // 文件总大小
	read    int64                    // 已读取的字节数
	signMap map[int64]struct{}       // 签名请求去重映射
	mux     sync.Mutex               // 互斥锁
	fn      func(progressed int64, percentage float32) error // 进度回调函数
}

// Read 实现 io.Reader 接口（用于顺序读取）
func (r *s3UploadProgressedReader) Read(p []byte) (int, error) {
	return r.fp.Read(p)
}

// ReadAt 实现 io.ReaderAt 接口（用于分片上传的随机读取）
//
// 参数:
//   - p: 目标缓冲区
//   - off: 读取偏移量
// 返回:
//   - n: 实际读取的字节数
//   - err: 错误信息（如果有）
//
// 实现细节：
//   1. 从指定偏移量读取数据
//   2. 使用 signMap 去重签名请求（同一偏移量会被读取两次）
//   3. 只在第二次读取时更新进度（实际上传时）
//   4. 调用回调函数上报进度
//
// 去重逻辑：
//   - 第一次读取（签名）：记录偏移量到 signMap，不更新进度
//   - 第二次读取（上传）：从 signMap 中找到记录，更新进度
func (r *s3UploadProgressedReader) ReadAt(p []byte, off int64) (int, error) {
	// 从指定偏移量读取数据
	n, err := r.fp.ReadAt(p, off)
	if err != nil {
		return n, err
	}

	// 使用互斥锁保护共享状态（支持并发分片上传）
	r.mux.Lock()
	// 去重签名请求
	// 第一次读取：记录偏移量，不更新进度
	// 第二次读取：更新进度
	if _, ok := r.signMap[off]; ok {
		r.read += int64(n)
	} else {
		r.signMap[off] = struct{}{}
	}
	r.mux.Unlock()

	// 调用进度回调函数
	if r.fn != nil {
		read := r.read
		// 计算百分比：(已上传 / 总大小) × 100
		if err := r.fn(read, float32(read*100)/float32(r.size)); err != nil {
			return n, err
		}
	}

	return n, err
}

// Seek 实现 io.Seeker 接口（用于文件定位）
func (r *s3UploadProgressedReader) Seek(offset int64, whence int) (int64, error) {
	return r.fp.Seek(offset, whence)
}
