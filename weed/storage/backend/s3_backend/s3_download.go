// Package s3_backend 实现基于 AWS S3 的远程存储后端
// 本文件实现 S3 下载功能，支持分片下载和进度跟踪
package s3_backend

import (
	"fmt"
	"os"
	"sync/atomic"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// downloadFromS3 从 S3 下载文件到本地
//
// 参数:
//   - sess: S3 API 客户端
//   - destFileName: 本地目标文件路径
//   - sourceBucket: 源 S3 存储桶
//   - sourceKey: 源 S3 对象 key
//   - fn: 进度回调函数，接收已下载字节数和百分比
// 返回:
//   - fileSize: 下载的文件大小（字节）
//   - err: 错误信息（如果有）
//
// 实现细节：
//   1. 通过 HeadObject 获取文件大小
//   2. 创建本地文件（覆盖模式）
//   3. 创建 S3 下载器，配置分片大小和并发数
//   4. 使用带进度跟踪的写入器下载文件
//   5. 返回文件大小和下载结果
//
// 分片策略：
//   - 分片大小：64MB
//   - 并发下载 5 个分片
//
// 性能考虑：
//   - 大文件使用分片下载，提高下载速度和可靠性
//   - 分片下载失败时可以只重传失败的分片
//   - 并发下载 5 个分片，充分利用网络带宽
func downloadFromS3(sess s3iface.S3API, destFileName string, sourceBucket string, sourceKey string,
	fn func(progressed int64, percentage float32) error) (fileSize int64, err error) {

	// 获取文件大小（通过 HeadObject）
	fileSize, err = getFileSize(sess, sourceBucket, sourceKey)
	if err != nil {
		return
	}

	// 创建本地文件（覆盖模式）
	f, err := os.OpenFile(destFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return 0, fmt.Errorf("failed to open file %q, %v", destFileName, err)
	}
	defer f.Close()

	// 创建 S3 下载器，配置分片大小和并发数
	downloader := s3manager.NewDownloaderWithClient(sess, func(u *s3manager.Downloader) {
		u.PartSize = int64(64 * 1024 * 1024) // 分片大小：64MB
		u.Concurrency = 5                     // 并发下载 5 个分片
	})

	// 创建带进度跟踪的文件写入器
	fileWriter := &s3DownloadProgressedWriter{
		fp:      f,
		size:    fileSize,
		written: 0,
		fn:      fn,
	}

	// 从 S3 下载文件
	fileSize, err = downloader.Download(fileWriter, &s3.GetObjectInput{
		Bucket: aws.String(sourceBucket),
		Key:    aws.String(sourceKey),
	})
	if err != nil {
		return fileSize, fmt.Errorf("failed to download /buckets/%s%s to %s: %v", sourceBucket, sourceKey, destFileName, err)
	}

	glog.V(1).Infof("downloaded file %s\n", destFileName)

	return
}

// s3DownloadProgressedWriter 实现带进度跟踪的文件写入器
//
// 改编自 AWS SDK 官方示例：
// - https://github.com/aws/aws-sdk-go/pull/1868
// - https://petersouter.xyz/s3-download-progress-bar-in-golang/
//
// 字段说明：
//   - size: 文件总大小（字节）
//   - written: 已写入的字节数（使用 atomic 操作保证并发安全）
//   - fn: 进度回调函数
//   - fp: 文件句柄
//
// 设计要点：
//   - 实现 io.WriterAt 接口，支持并发分片写入
//   - 使用 atomic 操作保护 written 字段，避免竞态条件
//   - 每次写入后调用回调函数上报进度
type s3DownloadProgressedWriter struct {
	size    int64    // 文件总大小
	written int64    // 已写入的字节数（原子操作）
	fn      func(progressed int64, percentage float32) error // 进度回调函数
	fp      *os.File // 文件句柄
}

// WriteAt 实现 io.WriterAt 接口（用于分片下载的并发写入）
//
// 参数:
//   - p: 数据缓冲区
//   - off: 写入偏移量
// 返回:
//   - n: 实际写入的字节数
//   - err: 错误信息（如果有）
//
// 实现细节：
//   1. 将数据写入指定偏移量
//   2. 使用原子操作更新已写入的字节数（支持并发写入）
//   3. 调用回调函数上报进度
//
// 并发安全：
//   - WriteAt 会被多个 goroutine 并发调用（分片下载）
//   - 使用 atomic.AddInt64 保证 written 字段的并发安全
func (w *s3DownloadProgressedWriter) WriteAt(p []byte, off int64) (int, error) {
	// 将数据写入指定偏移量
	n, err := w.fp.WriteAt(p, off)
	if err != nil {
		return n, err
	}

	// 使用原子操作更新已写入的字节数
	// 支持并发分片写入，避免竞态条件
	atomic.AddInt64(&w.written, int64(n))

	// 调用进度回调函数
	if w.fn != nil {
		written := w.written
		// 计算百分比：(已下载 / 总大小) × 100
		if err := w.fn(written, float32(written*100)/float32(w.size)); err != nil {
			return n, err
		}
	}

	return n, err
}

// getFileSize 通过 HeadObject 获取 S3 对象的大小
//
// 参数:
//   - svc: S3 API 客户端
//   - bucket: S3 存储桶名称
//   - key: S3 对象 key
// 返回:
//   - filesize: 文件大小（字节）
//   - error: 错误信息（如果有）
//
// 实现细节：
//   调用 S3 HeadObject API 获取对象元数据
//   从响应中提取 ContentLength 字段
func getFileSize(svc s3iface.S3API, bucket string, key string) (filesize int64, error error) {
	// 构造 HeadObject 请求参数
	params := &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	// 调用 HeadObject API 获取对象元数据
	resp, err := svc.HeadObject(params)
	if err != nil {
		return 0, err
	}

	// 返回文件大小
	return *resp.ContentLength, nil
}
