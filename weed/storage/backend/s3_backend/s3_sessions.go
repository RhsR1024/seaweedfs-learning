// Package s3_backend 实现基于 AWS S3 的远程存储后端
// 本文件实现 S3 会话管理和删除功能
package s3_backend

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/util/version"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

// 全局 S3 会话缓存和锁
var (
	s3Sessions   = make(map[string]s3iface.S3API) // S3 会话缓存，key 格式：region|endpoint
	sessionsLock sync.RWMutex                      // 读写锁，保护并发访问
)

// getSession 从缓存中获取 S3 会话
//
// 参数:
//   - region: AWS 区域
// 返回:
//   - s3iface.S3API: S3 API 客户端
//   - bool: 是否找到缓存
//
// 注意：
//   此函数目前未被使用，仅通过 region 查找会话
//   实际使用的是 createSession 函数，通过 region|endpoint 查找
func getSession(region string) (s3iface.S3API, bool) {
	sessionsLock.RLock()
	defer sessionsLock.RUnlock()

	sess, found := s3Sessions[region]
	return sess, found
}

// createSession 创建或获取 S3 会话
//
// 参数:
//   - awsAccessKeyId: AWS 访问密钥 ID
//   - awsSecretAccessKey: AWS 访问密钥 Secret
//   - region: AWS 区域
//   - endpoint: 自定义 endpoint（可选）
//   - forcePathStyle: 是否使用路径风格 URL
// 返回:
//   - s3iface.S3API: S3 API 客户端
//   - error: 错误信息（如果有）
//
// 实现细节：
//   1. 使用 region|endpoint 作为缓存 key，支持多区域多 endpoint
//   2. 如果缓存中存在会话，直接返回（避免重复创建）
//   3. 如果不存在，创建新会话并缓存
//   4. 设置自定义 User-Agent，标识 SeaweedFS 版本
//   5. 支持自定义凭证或使用 AWS 默认凭证链
//
// 配置选项：
//   - S3ForcePathStyle: 使用路径风格 URL（兼容 MinIO 等服务）
//   - S3DisableContentMD5Validation: 禁用 Content-MD5 验证（提高性能）
//
// 并发安全：
//   使用读写锁保护会话缓存，支持并发读取和独占写入
func createSession(awsAccessKeyId, awsSecretAccessKey, region, endpoint string, forcePathStyle bool) (s3iface.S3API, error) {

	sessionsLock.Lock()
	defer sessionsLock.Unlock()

	// 构造缓存 key，格式：region|endpoint
	// 支持同一 region 的多个 endpoint（如多个 MinIO 实例）
	cacheKey := fmt.Sprintf("%s|%s", region, endpoint)

	// 检查缓存中是否已存在会话
	if t, found := s3Sessions[cacheKey]; found {
		return t, nil
	}

	// 构造 AWS SDK 配置
	config := &aws.Config{
		Region:                        aws.String(region),
		Endpoint:                      aws.String(endpoint),
		S3ForcePathStyle:              aws.Bool(forcePathStyle),      // 使用路径风格 URL（兼容 MinIO）
		S3DisableContentMD5Validation: aws.Bool(true),                // 禁用 MD5 验证（提高性能）
	}

	// 如果提供了凭证，使用静态凭证
	// 否则使用 AWS 默认凭证链（环境变量、配置文件、IAM 角色等）
	if awsAccessKeyId != "" && awsSecretAccessKey != "" {
		config.Credentials = credentials.NewStaticCredentials(awsAccessKeyId, awsSecretAccessKey, "")
	}

	// 创建 AWS 会话
	sess, err := session.NewSession(config)
	if err != nil {
		return nil, fmt.Errorf("create aws session in region %s: %v", region, err)
	}

	// 设置自定义 User-Agent，标识 SeaweedFS 版本
	// 格式：SeaweedFS/3.xx
	sess.Handlers.Build.PushBack(func(r *request.Request) {
		r.HTTPRequest.Header.Set("User-Agent", "SeaweedFS/"+version.VERSION_NUMBER)
	})

	// 创建 S3 客户端
	t := s3.New(sess)

	// 缓存会话，避免重复创建
	s3Sessions[cacheKey] = t

	return t, nil

}

// deleteFromS3 从 S3 删除对象
//
// 参数:
//   - sess: S3 API 客户端
//   - sourceBucket: S3 存储桶名称
//   - sourceKey: S3 对象 key
// 返回:
//   - err: 错误信息（如果有）
//
// 实现细节：
//   调用 S3 DeleteObject API 删除对象
//   删除不存在的对象不会报错（幂等操作）
func deleteFromS3(sess s3iface.S3API, sourceBucket string, sourceKey string) (err error) {
	_, err = sess.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(sourceBucket),
		Key:    aws.String(sourceKey),
	})
	return err
}
