// Package weed_server 实现 Volume Server 的 HTTP Range 请求辅助功能
// 本文件提供 HTTP Range 请求的解析和处理能力，支持断点续传和分片下载
//
// 核心功能:
//   - parseRange: 解析 HTTP Range 请求头（RFC 2616 标准）
//   - httpRange: 表示字节范围（start + length）
//   - rangesMIMESize: 计算 multipart 响应的总大小
//   - countingWriter: 计数写入器，用于计算响应大小
//
// HTTP Range 请求格式:
//   Range: bytes=0-499       # 前 500 字节（0-499）
//   Range: bytes=500-999     # 第 500-999 字节
//   Range: bytes=-500        # 最后 500 字节
//   Range: bytes=500-        # 从第 500 字节到文件结尾
//   Range: bytes=0-0,-1      # 第一个和最后一个字节（multipart）
//
// 使用场景:
//   - 视频点播：用户拖动进度条时，从指定位置开始播放
//   - 断点续传：下载中断后，从上次位置继续下载
//   - 并行下载：多线程下载工具，每个线程下载一部分
//   - 大文件预览：只下载文件头部，快速预览内容
//
// RFC 2616 Range 规范:
//   - byte-range-spec = first-byte-pos "-" [last-byte-pos]
//   - first-byte-pos = 1*DIGIT（从 0 开始）
//   - last-byte-pos = 1*DIGIT（包含此字节）
//   - suffix-byte-range-spec = "-" suffix-length
//   - 多个范围用逗号分隔（multipart/byteranges 响应）
//
// 响应格式:
//   - 单范围：206 Partial Content + Content-Range 头
//   - 多范围：206 Partial Content + multipart/byteranges 响应体
//   - 无效范围：416 Requested Range Not Satisfiable
//
// 注意事项:
//   - 范围是左闭右闭区间：[start, end]
//   - 字节位置从 0 开始计数
//   - 超出文件大小的范围会被自动调整
//   - 不满足的范围返回 416 状态码
package weed_server

import (
	"errors"
	"fmt"
	"mime/multipart"
	"net/textproto"
	"strconv"
	"strings"
)

// 本文件大部分代码来自 Go 标准库 src/pkg/net/http/fs.go
// 针对 SeaweedFS 的场景进行了适配

// httpRange 表示要发送给客户端的字节范围
// 定义了文件中的一个连续片段
//
// 字段说明:
//   - start: 起始字节位置（从 0 开始）
//   - length: 范围长度（字节数）
//
// 示例:
//   - {start: 0, length: 100}     → 读取前 100 字节（0-99）
//   - {start: 500, length: 100}   → 读取第 500-599 字节
//   - {start: 1000, length: 500}  → 读取第 1000-1499 字节
//
// 注意:
//   - 不直接存储 end 位置，通过 start + length 计算
//   - length 必须 > 0
type httpRange struct {
	start, length int64
}

// contentRange 生成 Content-Range 响应头的值
// 格式：bytes <start>-<end>/<total>
//
// 参数:
//   - size: 文件总大小（字节）
//
// 返回:
//   - string: Content-Range 头的值
//
// 示例:
//   - r={start:0, length:100}, size=1000
//     → "bytes 0-99/1000"（前 100 字节）
//   - r={start:500, length:200}, size=1000
//     → "bytes 500-699/1000"（第 500-699 字节）
//   - r={start:900, length:100}, size=1000
//     → "bytes 900-999/1000"（最后 100 字节）
//
// HTTP 响应示例:
//   HTTP/1.1 206 Partial Content
//   Content-Range: bytes 0-1023/10240
//   Content-Length: 1024
//   Content-Type: video/mp4
func (r httpRange) contentRange(size int64) string {
	// 计算结束位置：start + length - 1
	// 例如：start=0, length=100 → end=99（字节 0-99）
	return fmt.Sprintf("bytes %d-%d/%d", r.start, r.start+r.length-1, size)
}

// mimeHeader 生成 multipart 响应的 MIME 头
// 用于多范围下载时的每个部分
//
// 参数:
//   - contentType: MIME 类型（如 "video/mp4", "image/jpeg"）
//   - size: 文件总大小（字节）
//
// 返回:
//   - textproto.MIMEHeader: 包含 Content-Range 和 Content-Type 的头
//
// multipart/byteranges 响应示例:
//   HTTP/1.1 206 Partial Content
//   Content-Type: multipart/byteranges; boundary=3d6b6a416f9b5
//
//   --3d6b6a416f9b5
//   Content-Type: video/mp4
//   Content-Range: bytes 0-1023/10240
//
//   [第一部分数据...]
//   --3d6b6a416f9b5
//   Content-Type: video/mp4
//   Content-Range: bytes 5120-6143/10240
//
//   [第二部分数据...]
//   --3d6b6a416f9b5--
func (r httpRange) mimeHeader(contentType string, size int64) textproto.MIMEHeader {
	return textproto.MIMEHeader{
		"Content-Range": {r.contentRange(size)},  // bytes 起始-结束/总大小
		"Content-Type":  {contentType},           // MIME 类型
	}
}

// parseRange 解析 HTTP Range 请求头（RFC 2616 标准）
// 将字符串形式的 Range 头转换为 httpRange 结构体数组
//
// 参数:
//   - s: Range 请求头的值（如 "bytes=0-499,1000-1499"）
//   - size: 文件总大小（字节），用于验证和调整范围
//
// 返回:
//   - []httpRange: 解析后的范围列表
//   - error: 解析错误（格式无效、范围无效等）
//
// Range 格式支持:
//   1. 完整范围：bytes=0-499
//      → {start: 0, length: 500}
//   2. 从指定位置到末尾：bytes=500-
//      → {start: 500, length: size-500}
//   3. 最后 N 字节：bytes=-500
//      → {start: size-500, length: 500}
//   4. 多个范围：bytes=0-499,1000-1499
//      → [{start:0, length:500}, {start:1000, length:500}]
//
// 边界处理:
//   - 超出文件大小的 end 会被调整为 size-1
//   - 后缀范围（如 -500）超过文件大小时，返回整个文件
//   - start > size 时返回错误
//
// 使用示例（视频点播）:
//   // 用户拖动到 10MB 位置，请求后续 1MB 数据
//   ranges, err := parseRange("bytes=10485760-11534335", fileSize)
//   // ranges = [{start: 10485760, length: 1048576}]
//
// 使用示例（断点续传）:
//   // 下载到 50% 时中断，继续下载剩余部分
//   ranges, err := parseRange("bytes=52428800-", fileSize)
//   // ranges = [{start: 52428800, length: fileSize-52428800}]
//
// 错误情况:
//   - "": 返回 nil, nil（无 Range 头）
//   - "files=0-100": 返回 error（不是 bytes=）
//   - "bytes=abc-def": 返回 error（无效数字）
//   - "bytes=1000-500": 返回 error（start > end）
func parseRange(s string, size int64) ([]httpRange, error) {
	// 【检查 Range 头是否存在】
	if s == "" {
		// 没有 Range 头，返回 nil 表示请求整个文件
		return nil, nil
	}

	// 【验证 Range 头格式】
	// RFC 2616 要求 Range 头必须以 "bytes=" 开头
	const b = "bytes="
	if !strings.HasPrefix(s, b) {
		// 不支持其他单位（如 "pages=", "items=" 等）
		return nil, errors.New("invalid range")
	}

	// 【解析多个范围】
	// Range 头可以包含多个范围，用逗号分隔
	// 例如："bytes=0-499,1000-1499,5000-5999"
	var ranges []httpRange
	for _, ra := range strings.Split(s[len(b):], ",") {
		// 去除空格
		ra = strings.TrimSpace(ra)
		if ra == "" {
			continue
		}

		// 【查找 "-" 分隔符】
		// 每个范围格式：<start>-<end>
		i := strings.Index(ra, "-")
		if i < 0 {
			// 没有 "-" 分隔符，格式错误
			return nil, errors.New("invalid range")
		}

		// 【分离 start 和 end】
		start, end := strings.TrimSpace(ra[:i]), strings.TrimSpace(ra[i+1:])
		var r httpRange

		if start == "" {
			// 【后缀范围：bytes=-500】
			// 表示文件的最后 N 字节
			// 例如：bytes=-500 表示最后 500 字节
			i, err := strconv.ParseInt(end, 10, 64)
			if err != nil {
				return nil, errors.New("invalid range")
			}

			// 如果请求的字节数超过文件大小，返回整个文件
			if i > size {
				i = size
			}

			// 计算起始位置和长度
			r.start = size - i         // 起始位置：文件大小 - 后缀长度
			r.length = size - r.start  // 长度：到文件末尾
		} else {
			// 【完整范围或前缀范围】
			// 完整范围：bytes=0-499（字节 0-499）
			// 前缀范围：bytes=500-（从 500 到文件末尾）

			// 解析起始位置
			i, err := strconv.ParseInt(start, 10, 64)
			if err != nil || i > size || i < 0 {
				// start 无效：不是数字、超出文件大小、或为负数
				return nil, errors.New("invalid range")
			}
			r.start = i

			if end == "" {
				// 【前缀范围：bytes=500-】
				// 从指定位置到文件末尾
				r.length = size - r.start
			} else {
				// 【完整范围：bytes=0-499】
				// 有明确的结束位置
				i, err := strconv.ParseInt(end, 10, 64)
				if err != nil || r.start > i {
					// end 无效：不是数字、或 start > end
					return nil, errors.New("invalid range")
				}

				// 如果 end 超出文件大小，调整为文件末尾
				if i >= size {
					i = size - 1
				}

				// 计算长度（包含 end 位置）
				// 例如：bytes=0-499 表示 500 字节（位置 0-499）
				r.length = i - r.start + 1
			}
		}

		// 添加到范围列表
		ranges = append(ranges, r)
	}

	return ranges, nil
}

// countingWriter 计数写入器，用于统计写入的字节数
// 实现 io.Writer 接口，但不实际写入数据，只记录大小
//
// 使用场景:
//   - 计算 multipart 响应的总大小
//   - 在实际写入前预估响应大小
//   - 设置 Content-Length 响应头
//
// 示例:
//   var w countingWriter
//   w.Write([]byte("hello"))  // w = 5
//   w.Write([]byte("world"))  // w = 10
type countingWriter int64

// Write 实现 io.Writer 接口，只累加字节数，不实际写入
//
// 参数:
//   - p: 要"写入"的字节切片
//
// 返回:
//   - n: 写入的字节数（总是等于 len(p)）
//   - err: 错误（总是 nil）
func (w *countingWriter) Write(p []byte) (n int, err error) {
	// 累加字节数
	*w += countingWriter(len(p))
	// 返回写入的字节数（但实际不写入）
	return len(p), nil
}

// rangesMIMESize 计算 multipart/byteranges 响应的总大小
// 用于多范围下载时设置 Content-Length 响应头
//
// 参数:
//   - ranges: 所有要发送的字节范围
//   - contentType: MIME 类型（如 "video/mp4"）
//   - contentSize: 文件总大小
//
// 返回:
//   - encSize: multipart 响应的总字节数
//
// multipart 响应结构:
//   --boundary\r\n
//   Content-Type: video/mp4\r\n
//   Content-Range: bytes 0-1023/10240\r\n
//   \r\n
//   [1024 字节数据]
//   \r\n
//   --boundary\r\n
//   Content-Type: video/mp4\r\n
//   Content-Range: bytes 5120-6143/10240\r\n
//   \r\n
//   [1024 字节数据]
//   \r\n
//   --boundary--\r\n
//
// 总大小 = 所有 MIME 头大小 + 所有数据大小 + 边界符大小
//
// 使用示例:
//   ranges := []httpRange{
//       {start: 0, length: 1024},
//       {start: 5120, length: 1024},
//   }
//   size := rangesMIMESize(ranges, "video/mp4", 10240)
//   // size = MIME头部 + 1024 + MIME头部 + 1024 + 边界符
//   w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
func rangesMIMESize(ranges []httpRange, contentType string, contentSize int64) (encSize int64) {
	// 【使用计数写入器模拟写入】
	var w countingWriter
	mw := multipart.NewWriter(&w)

	// 【计算每个部分的大小】
	for _, ra := range ranges {
		// 创建 MIME 头部（会写入到 countingWriter，累加头部大小）
		mw.CreatePart(ra.mimeHeader(contentType, contentSize))
		// 累加实际数据大小
		encSize += ra.length
	}

	// 【关闭 multipart writer】
	// 会写入结束边界符（--boundary--）
	mw.Close()

	// 【累加所有头部和边界符的大小】
	// countingWriter 记录了所有 MIME 头部和边界符的大小
	encSize += int64(w)

	return
}

// sumRangesSize 计算所有范围的总数据大小（不包含 MIME 头部）
// 用于单范围下载时设置 Content-Length
//
// 参数:
//   - ranges: 字节范围数组
//
// 返回:
//   - size: 所有范围的总长度
//
// 示例:
//   ranges := []httpRange{
//       {start: 0, length: 100},
//       {start: 500, length: 200},
//   }
//   size := sumRangesSize(ranges)  // size = 300
//
// 注意:
//   - 只计算数据大小，不包含 HTTP 头部
//   - 多范围下载应使用 rangesMIMESize（包含 MIME 头部）
func sumRangesSize(ranges []httpRange) (size int64) {
	for _, ra := range ranges {
		size += ra.length
	}
	return
}
