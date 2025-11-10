// Package util 提供了 SeaweedFS 的通用工具函数
// 本文件实现了数据压缩和解压缩相关功能,主要使用 Gzip 压缩算法
package util

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	// "github.com/klauspost/compress/zstd"  // Zstd 压缩支持(已注释,暂未启用)
)

var (
	// UnsupportedCompression 不支持的压缩格式错误
	UnsupportedCompression = fmt.Errorf("unsupported compression")
)

// MaybeGzipData 智能 Gzip 压缩函数
// 自动判断数据是否已压缩,并决定是否执行压缩
// 压缩策略:
//   1. 如果数据已经是 Gzip 格式,直接返回
//   2. 尝试压缩数据
//   3. 只有压缩率 > 10% 时才使用压缩数据(压缩后大小 < 原始大小 * 0.9)
//   4. 如果压缩效果不明显或失败,返回原始数据
// 参数:
//   input: 待压缩的数据
// 返回:
//   压缩后的数据或原始数据
func MaybeGzipData(input []byte) []byte {
	// 检查数据是否已经是 Gzip 格式
	if IsGzippedContent(input) {
		return input
	}
	// 尝试压缩
	gzipped, err := GzipData(input)
	if err != nil {
		return input
	}
	// 检查压缩效果:只有压缩后大小 < 原始大小 * 0.9 才使用压缩数据
	// len(gzipped) * 10 > len(input) * 9  =>  压缩率 < 10%,不值得压缩
	if len(gzipped)*10 > len(input)*9 {
		return input
	}
	return gzipped
}

// MaybeDecompressData 尝试解压缩数据
// 自动检测压缩格式并解压,如果失败或不支持则返回原始数据
// 参数:
//   input: 可能已压缩的数据
// 返回:
//   解压后的数据或原始数据
func MaybeDecompressData(input []byte) []byte {
	uncompressed, err := DecompressData(input)
	if err != nil {
		// 只记录非"不支持压缩"的错误
		if err != UnsupportedCompression {
			glog.Errorf("decompressed data: %v", err)
		}
		return input
	}
	return uncompressed
}

// GzipData 使用 Gzip 算法压缩数据
// 参数:
//   input: 待压缩的原始数据
// 返回:
//   压缩后的数据
//   错误信息
func GzipData(input []byte) ([]byte, error) {
	w := new(bytes.Buffer)
	// 使用流式压缩,将压缩结果写入 Buffer
	_, err := GzipStream(w, bytes.NewReader(input))
	if err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

// ungzipData 解压 Gzip 格式的数据
// 这是一个内部函数,小写开头表示包内私有
// 参数:
//   input: Gzip 压缩的数据
// 返回:
//   解压后的原始数据
//   错误信息
func ungzipData(input []byte) ([]byte, error) {
	w := new(bytes.Buffer)
	// 使用流式解压,将解压结果写入 Buffer
	_, err := GunzipStream(w, bytes.NewReader(input))
	if err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

// DecompressData 解压缩数据,自动检测压缩格式
// 目前支持 Gzip 格式,Zstd 支持已注释
// 参数:
//   input: 压缩的数据
// 返回:
//   解压后的数据
//   错误信息(如果不支持该压缩格式,返回 UnsupportedCompression)
func DecompressData(input []byte) ([]byte, error) {
	// 检查是否为 Gzip 格式(通过 magic number 识别)
	if IsGzippedContent(input) {
		return ungzipData(input)
	}
	/*
		// Zstd 压缩支持(已注释)
		if IsZstdContent(input) {
			return unzstdData(input)
		}
	*/
	// 不是已知的压缩格式,返回错误
	return input, UnsupportedCompression
}

// IsGzippedContent 检查数据是否为 Gzip 格式
// 通过检查 Gzip 的 magic number (文件头标识) 来判断
// Gzip 格式的 magic number 是: 0x1F 0x8B (十进制: 31 139)
// 参数:
//   data: 待检查的数据
// 返回:
//   是否为 Gzip 格式
func IsGzippedContent(data []byte) bool {
	if len(data) < 2 {
		return false
	}
	// 检查前两个字节是否为 Gzip magic number
	return data[0] == 31 && data[1] == 139
}

/*
// Zstd 压缩支持(已注释,暂未启用)
// Zstd 是 Facebook 开源的高性能压缩算法,压缩率和速度都优于 Gzip

var zstdEncoder, _ = zstd.NewWriter(nil)

// ZstdData 使用 Zstd 算法压缩数据
func ZstdData(input []byte) ([]byte, error) {
	return zstdEncoder.EncodeAll(input, nil), nil
}

var decoder, _ = zstd.NewReader(nil)

// unzstdData 解压 Zstd 格式的数据
func unzstdData(input []byte) ([]byte, error) {
	return decoder.DecodeAll(input, nil)
}

// IsZstdContent 检查数据是否为 Zstd 格式
// Zstd 格式的 magic number 是: 0x28 0xB5 0x2F 0xFD
func IsZstdContent(data []byte) bool {
	if len(data) < 4 {
		return false
	}
	// 注意: 字节序是小端序,从后往前检查
	return data[3] == 0xFD && data[2] == 0x2F && data[1] == 0xB5 && data[0] == 0x28
}
*/

/*
 * IsCompressableFileType 判断文件类型是否适合压缩
 * 默认策略:不压缩,因为压缩可以在客户端完成
 *
 * 压缩决策依据:
 *   1. 文本文件(text/*)适合压缩
 *   2. 未压缩的图片格式(SVG, BMP)适合压缩
 *   3. 已压缩的格式(ZIP, JPG, PNG)不需要压缩
 *   4. 源代码文件适合压缩
 *   5. 音频文件根据格式判断(WAV 适合,MP3 不适合)
 *
 * 参数:
 *   ext: 文件扩展名(如 ".txt", ".jpg")
 *   mtype: MIME 类型(如 "text/plain", "image/jpeg")
 *
 * 返回:
 *   shouldBeCompressed: 是否应该压缩
 *   iAmSure: 是否确定(如果不确定,调用者可以选择不压缩)
 */
func IsCompressableFileType(ext, mtype string) (shouldBeCompressed, iAmSure bool) {

	// 文本类型:全部适合压缩
	if strings.HasPrefix(mtype, "text/") {
		return true, true
	}

	// 图片类型:特殊处理
	switch ext {
	case ".svg", ".bmp", ".wav":
		// SVG 是文本格式,BMP 是未压缩位图,WAV 是未压缩音频
		return true, true
	}
	if strings.HasPrefix(mtype, "image/") {
		// 大多数图片格式(JPG, PNG, GIF)已经压缩过,不需要再压缩
		return false, true
	}

	// 根据文件扩展名判断
	switch ext {
	case ".zip", ".rar", ".gz", ".bz2", ".xz", ".zst", ".br":
		// 已压缩的归档格式,不需要再压缩
		return false, true
	case ".pdf", ".txt", ".html", ".htm", ".css", ".js", ".json":
		// 文本文档和 Web 资源,适合压缩
		return true, true
	case ".php", ".java", ".go", ".rb", ".c", ".cpp", ".h", ".hpp":
		// 源代码文件,适合压缩
		return true, true
	case ".png", ".jpg", ".jpeg":
		// 已压缩的图片格式,不需要再压缩
		return false, true
	}

	// 根据 MIME 类型判断
	if strings.HasPrefix(mtype, "application/") {
		if strings.HasSuffix(mtype, "zstd") {
			// 已 Zstd 压缩,不需要再压缩
			return false, true
		}
		if strings.HasSuffix(mtype, "xml") {
			// XML 文本格式,适合压缩
			return true, true
		}
		if strings.HasSuffix(mtype, "script") {
			// 脚本文件(JavaScript 等),适合压缩
			return true, true
		}
		if strings.HasSuffix(mtype, "vnd.rar") {
			// RAR 压缩格式,不需要再压缩
			return false, true
		}
	}

	// 音频类型:只有 WAV 未压缩音频适合压缩
	if strings.HasPrefix(mtype, "audio/") {
		switch strings.TrimPrefix(mtype, "audio/") {
		case "wave", "wav", "x-wav", "x-pn-wav":
			// WAV 是未压缩音频格式,适合压缩
			return true, true
		}
	}

	// 无法确定,默认不压缩
	// iAmSure = false 表示不确定,调用者可以根据实际情况决定
	return false, false
}
