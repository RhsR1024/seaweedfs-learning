package needle

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/images"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
)

const (
	// NeedleChecksumSize CRC32 校验和的大小（字节）
	// 用于验证 Needle 数据的完整性
	NeedleChecksumSize = 4

	// PairNamePrefix 自定义键值对的 HTTP 头前缀
	// 例如：Seaweed-CustomKey: CustomValue
	// 存储时会去掉前缀，只保留 CustomKey: CustomValue
	PairNamePrefix = "Seaweed-"
)

// Needle 表示一个已上传并存储的文件
//
// Needle 是 SeaweedFS 存储架构的核心数据结构，代表存储在卷中的单个文件。
// "Needle"（针）这个名字形象地比喻了在大型卷文件中精确定位小文件的能力。
//
// 设计理念：
//   1. 将多个小文件打包到一个大文件（卷）中，减少文件系统元数据开销
//   2. 使用内存索引快速定位文件在卷中的位置
//   3. 支持文件元数据（文件名、MIME 类型、自定义属性）
//   4. 内置数据完整性校验（CRC32）
//   5. 支持 TTL（生存时间）自动过期
//
// 存储格式版本：
//   - Version 1: 基础版本，只包含 Cookie, Id, Size, Data
//   - Version 2: 添加 Flags, Name, Mime, Pairs, LastModified, Ttl
//   - Version 3: 添加 AppendAtNs（纳秒级追加时间戳）
//
// 大小限制：
//   - 单个 Needle（文件）最大 4GB
//   - 文件名最大 255 字符
//   - MIME 类型最大 255 字符
//   - 自定义键值对最大 64KB
//
// 磁盘存储布局：
//   [Cookie][Id][Size][DataSize][Data][Flags][NameSize][Name]
//   [MimeSize][Mime][PairsSize][Pairs][LastModified][Ttl]
//   [Checksum][AppendAtNs][Padding]
type Needle struct {
	// Cookie 随机数，用于防止暴力破解查找
	// 即使知道 NeedleId，也需要正确的 Cookie 才能访问文件
	// 相当于一个简单的访问令牌
	Cookie Cookie `comment:"random number to mitigate brute force lookups"`

	// Id Needle 的唯一标识符
	// 在整个 Volume 中唯一，用于快速定位文件
	Id NeedleId `comment:"needle id"`

	// Size Needle 的总大小
	// 包含：DataSize + Data + NameSize + Name + MimeSize + Mime + 其他元数据
	// 用于快速跳过整个 Needle 或分配内存
	Size Size `comment:"sum of DataSize,Data,NameSize,Name,MimeSize,Mime"`

	// DataSize 实际文件数据的大小（字节）
	// 注意：如果文件被压缩，这是压缩后的大小
	// Version 2 新增
	DataSize uint32 `comment:"Data size"` //version2

	// Data 实际的文件数据
	// 可能是压缩的，根据 Flags 判断
	Data []byte `comment:"The actual file data"`

	// Flags 布尔标志位，用于存储文件的各种属性
	// 每个 bit 代表一个标志，例如：
	//   - 是否压缩
	//   - 是否有文件名
	//   - 是否有 MIME 类型
	//   - 是否是分块上传的清单文件
	// Version 2 新增
	Flags byte `comment:"boolean flags"` //version2

	// NameSize 文件名的长度（字节）
	// 最大 255 字节（uint8 的最大值）
	// Version 2 新增
	NameSize uint8 //version2

	// Name 文件名
	// 原始上传时的文件名，UTF-8 编码
	// 最大 255 字符
	// Version 2 新增
	Name []byte `comment:"maximum 255 characters"` //version2

	// MimeSize MIME 类型的长度（字节）
	// 最大 255 字节（uint8 的最大值）
	// Version 2 新增
	MimeSize uint8 //version2

	// Mime MIME 类型（内容类型）
	// 例如："image/jpeg", "application/pdf", "text/plain"
	// 用于 HTTP 响应的 Content-Type 头
	// 最大 255 字符
	// Version 2 新增
	Mime []byte `comment:"maximum 255 characters"` //version2

	// PairsSize 自定义键值对的总大小（字节）
	// 最大 64KB（uint16 的最大值 65535）
	// Version 2 新增
	PairsSize uint16 //version2

	// Pairs 自定义键值对，JSON 格式
	// 存储额外的元数据，如：
	//   {"Author": "John", "Department": "Engineering"}
	// HTTP 上传时通过 Seaweed- 前缀的头传递
	// 最大 64KB
	// Version 2 新增
	Pairs []byte `comment:"additional name value pairs, json format, maximum 64kB"`

	// LastModified 文件的最后修改时间（Unix 时间戳，秒）
	// 磁盘上只存储 5 字节（LastModifiedBytesLength）
	// 5 字节可以表示到 2286 年，足够使用
	// Version 2 新增
	LastModified uint64 //only store LastModifiedBytesLength bytes, which is 5 bytes to disk

	// Ttl 文件的生存时间（Time To Live）
	// 指定文件应该保留多长时间
	// 过期后可以被自动清理
	// nil 表示永不过期
	// Version 2 新增
	Ttl *TTL

	// Checksum CRC32 校验和
	// 用于验证文件数据的完整性
	// 在读取时验证，防止数据损坏
	Checksum CRC `comment:"CRC32 to check integrity"`

	// AppendAtNs 追加到卷的时间戳（纳秒）
	// 用于确保时间戳的单调递增
	// 即使在高并发写入时也能保证顺序
	// Version 3 新增
	AppendAtNs uint64 `comment:"append timestamp in nano seconds"` //version3

	// Padding 填充字节，用于对齐到 8 字节边界
	// 提高磁盘读写效率，尤其是在使用 mmap 时
	Padding []byte `comment:"Aligned to 8 bytes"`
}

// String 返回 Needle 的字符串表示形式
//
// 用于调试和日志输出，包含关键信息：
//   - Needle ID 和 Cookie
//   - 总大小和数据大小
//   - 文件名和 MIME 类型
//   - 是否压缩
//
// 示例输出：
//   "3,01637037d6 Size:1024, DataSize:950, Name:photo.jpg, Mime:image/jpeg Compressed:true"
func (n *Needle) String() (str string) {
	str = fmt.Sprintf("%s Size:%d, DataSize:%d, Name:%s, Mime:%s Compressed:%v",
		formatNeedleIdCookie(n.Id, n.Cookie), n.Size, n.DataSize, n.Name, n.Mime, n.IsCompressed())
	return
}

// CreateNeedleFromRequest 从 HTTP 请求创建一个 Needle 对象
//
// 这是处理文件上传的核心函数，负责：
//   1. 解析 multipart/form-data 上传
//   2. 提取文件数据和元数据
//   3. 处理文件压缩
//   4. 修复 JPEG 图片方向（可选）
//   5. 计算 CRC32 校验和
//   6. 解析文件 ID（fid）
//
// 参数：
//   r: HTTP 请求对象，包含上传的文件和元数据
//   fixJpgOrientation: 是否自动修复 JPEG 图片的 EXIF 方向
//     某些相机拍摄的照片方向信息存储在 EXIF 中，而不是真实旋转图片
//   sizeLimit: 文件大小限制（字节），超过此限制将被拒绝
//   bytesBuffer: 用于读取数据的缓冲区，可复用以减少内存分配
//
// 返回：
//   n: 创建的 Needle 对象
//   originalSize: 原始文件大小（压缩前）
//   contentMd5: 文件内容的 MD5 哈希（如果客户端提供）
//   e: 错误信息
//
// URL 格式：
//   POST /volumeId,needleId[.ext]
//   例如：POST /3,01637037d6.jpg
//
// 自定义元数据：
//   通过 HTTP 头传递，格式为 "Seaweed-Key: Value"
//   例如：Seaweed-Author: John
//   存储时去掉 "Seaweed-" 前缀
func CreateNeedleFromRequest(r *http.Request, fixJpgOrientation bool, sizeLimit int64, bytesBuffer *bytes.Buffer) (n *Needle, originalSize int, contentMd5 string, e error) {
	n = new(Needle)

	// 解析上传的文件和元数据
	pu, e := ParseUpload(r, sizeLimit, bytesBuffer)
	if e != nil {
		return
	}

	// 设置文件数据和基本属性
	n.Data = pu.Data
	originalSize = pu.OriginalDataSize
	n.LastModified = pu.ModifiedTime
	n.Ttl = pu.Ttl
	contentMd5 = pu.ContentMd5

	// 设置文件名（最大 255 字符）
	if len(pu.FileName) < 256 {
		n.Name = []byte(pu.FileName)
		n.SetHasName() // 设置标志位，表示有文件名
	}

	// 设置 MIME 类型（最大 255 字符）
	if len(pu.MimeType) < 256 {
		n.Mime = []byte(pu.MimeType)
		n.SetHasMime() // 设置标志位，表示有 MIME 类型
	}

	// 处理自定义键值对
	if len(pu.PairMap) != 0 {
		// 去掉 "Seaweed-" 前缀
		trimmedPairMap := make(map[string]string)
		for k, v := range pu.PairMap {
			trimmedPairMap[k[len(PairNamePrefix):]] = v
		}

		// 序列化为 JSON
		pairs, _ := json.Marshal(trimmedPairMap)
		// 检查大小限制（最大 64KB）
		if len(pairs) < 65536 {
			n.Pairs = pairs
			n.PairsSize = uint16(len(pairs))
			n.SetHasPairs() // 设置标志位，表示有自定义键值对
		}
	}

	// 设置压缩标志
	if pu.IsGzipped {
		// println(r.URL.Path, "is set to compressed", pu.FileName, pu.IsGzipped, "dataSize", pu.OriginalDataSize)
		n.SetIsCompressed()
	}

	// 设置最后修改时间（默认为当前时间）
	if n.LastModified == 0 {
		n.LastModified = uint64(time.Now().Unix())
	}
	n.SetHasLastModifiedDate()

	// 设置 TTL 标志
	if n.Ttl != EMPTY_TTL {
		n.SetHasTtl()
	}

	// 标记是否是分块上传的清单文件
	if pu.IsChunkedFile {
		n.SetIsChunkManifest()
	}

	// 修复 JPEG 图片方向
	// 某些相机拍摄的照片需要根据 EXIF 信息旋转
	if fixJpgOrientation {
		loweredName := strings.ToLower(pu.FileName)
		if pu.MimeType == "image/jpeg" || strings.HasSuffix(loweredName, ".jpg") || strings.HasSuffix(loweredName, ".jpeg") {
			n.Data = images.FixJpgOrientation(n.Data)
		}
	}

	// 计算 CRC32 校验和
	n.Checksum = NewCRC(n.Data)

	// 从 URL 路径中提取文件 ID (fid)
	// 格式：/volumeId,needleId[.ext]
	// 例如：/3,01637037d6.jpg
	commaSep := strings.LastIndex(r.URL.Path, ",")
	dotSep := strings.LastIndex(r.URL.Path, ".")
	fid := r.URL.Path[commaSep+1:]
	if dotSep > 0 {
		// 去掉扩展名
		fid = r.URL.Path[commaSep+1 : dotSep]
	}

	// 解析 fid，提取 NeedleId 和 Cookie
	e = n.ParsePath(fid)

	return
}
// ParsePath 解析文件 ID (fid) 字符串，提取 NeedleId 和 Cookie
//
// fid 格式：
//   基础格式：needleId_cookie
//   带 delta：needleId_cookie_delta
//
// 示例：
//   "01637037d6" -> needleId=01637037d6, cookie=<从末尾提取>
//   "01637037d6_100" -> needleId=01637037d6+100, cookie=<从末尾提取>
//
// Delta 机制：
//   Delta 用于支持同一个文件的多个版本或变体
//   例如：原图的 needleId 是 100，缩略图可以是 100_1
//
// 参数：
//   fid: 文件 ID 字符串
//
// 返回：
//   err: 解析错误
//
// 副作用：
//   设置 n.Id 和 n.Cookie
func (n *Needle) ParsePath(fid string) (err error) {
	length := len(fid)
	// fid 长度必须大于 Cookie 大小（Cookie 占用固定长度）
	if length <= CookieSize*2 {
		return fmt.Errorf("Invalid fid: %s", fid)
	}

	// 检查是否有 delta（版本号或偏移量）
	delta := ""
	deltaIndex := strings.LastIndex(fid, "_")
	if deltaIndex > 0 {
		// 分离 fid 和 delta
		// 例如："01637037d6_100" -> fid="01637037d6", delta="100"
		fid, delta = fid[0:deltaIndex], fid[deltaIndex+1:]
	}

	// 解析 NeedleId 和 Cookie
	n.Id, n.Cookie, err = ParseNeedleIdCookie(fid)
	if err != nil {
		return err
	}

	// 如果有 delta，将其加到 NeedleId 上
	if delta != "" {
		if d, e := strconv.ParseUint(delta, 10, 64); e == nil {
			n.Id += Uint64ToNeedleId(d)
		} else {
			return e
		}
	}

	return err
}

// GetAppendAtNs 获取追加时间戳（纳秒）
//
// 确保返回的时间戳严格大于卷的最后追加时间戳，
// 即使在高并发环境下也能保证时间戳的单调递增。
//
// 工作原理：
//   1. 获取当前系统时间（纳秒）
//   2. 如果当前时间小于等于上次追加时间，使用 lastTime+1
//   3. 否则使用当前时间
//
// 参数：
//   volumeLastAppendAtNs: 卷的最后追加时间戳（纳秒）
//
// 返回：
//   新的追加时间戳，保证大于 volumeLastAppendAtNs
//
// 使用场景：
//   - 创建新 Needle 时
//   - 更新现有 Needle 时
//   - 保证时间戳的单调性，用于排序和同步
func GetAppendAtNs(volumeLastAppendAtNs uint64) uint64 {
	return max(uint64(time.Now().UnixNano()), volumeLastAppendAtNs+1)
}

// UpdateAppendAtNs 更新 Needle 的追加时间戳
//
// 与 GetAppendAtNs 类似，但直接更新 Needle 对象的 AppendAtNs 字段。
//
// 参数：
//   volumeLastAppendAtNs: 卷的最后追加时间戳（纳秒）
//
// 副作用：
//   设置 n.AppendAtNs 为新的时间戳
func (n *Needle) UpdateAppendAtNs(volumeLastAppendAtNs uint64) {
	n.AppendAtNs = max(uint64(time.Now().UnixNano()), volumeLastAppendAtNs+1)
}

// ParseNeedleIdCookie 从字符串解析 NeedleId 和 Cookie
//
// 字符串格式：<needleId><cookie>
// 其中 Cookie 占用固定长度（CookieSize*2 个十六进制字符）
//
// 解析逻辑：
//   1. 从字符串末尾提取 CookieSize*2 个字符作为 Cookie
//   2. 剩余部分作为 NeedleId
//
// 示例：
//   输入："01637037d6a1b2c3d4"
//   假设 CookieSize=4（8个十六进制字符）
//   输出：needleId="01637037d6", cookie="a1b2c3d4"
//
// 参数：
//   key_hash_string: 包含 NeedleId 和 Cookie 的十六进制字符串
//
// 返回：
//   NeedleId: 解析出的 Needle ID
//   Cookie: 解析出的 Cookie
//   error: 解析错误
//
// 错误情况：
//   - 字符串太短（小于等于 Cookie 长度）
//   - 字符串太长（超过 NeedleId + Cookie 的最大长度）
//   - NeedleId 格式错误
//   - Cookie 格式错误
func ParseNeedleIdCookie(key_hash_string string) (NeedleId, Cookie, error) {
	// 检查最小长度（至少要包含 Cookie）
	if len(key_hash_string) <= CookieSize*2 {
		return NeedleIdEmpty, 0, fmt.Errorf("KeyHash is too short.")
	}

	// 检查最大长度（NeedleId + Cookie）
	if len(key_hash_string) > (NeedleIdSize+CookieSize)*2 {
		return NeedleIdEmpty, 0, fmt.Errorf("KeyHash is too long.")
	}

	// 计算分割点：Cookie 从末尾开始占用 CookieSize*2 个字符
	split := len(key_hash_string) - CookieSize*2

	// 解析 NeedleId（前半部分）
	needleId, err := ParseNeedleId(key_hash_string[:split])
	if err != nil {
		return NeedleIdEmpty, 0, fmt.Errorf("Parse needleId error: %w", err)
	}

	// 解析 Cookie（后半部分）
	cookie, err := ParseCookie(key_hash_string[split:])
	if err != nil {
		return NeedleIdEmpty, 0, fmt.Errorf("Parse cookie error: %w", err)
	}

	return needleId, cookie, nil
}

// LastModifiedString 返回最后修改时间的 ISO 8601 格式字符串
//
// 格式：YYYY-MM-DDTHH:MM:SS
// 例如："2024-01-15T14:30:45"
//
// 返回：
//   ISO 8601 格式的时间字符串
//
// 使用场景：
//   - HTTP 响应的 Last-Modified 头
//   - 日志输出
//   - API 响应
func (n *Needle) LastModifiedString() string {
	return time.Unix(int64(n.LastModified), 0).Format("2006-01-02T15:04:05")
}

// max 返回两个 uint64 值中的较大者
//
// 参数：
//   x: 第一个值
//   y: 第二个值
//
// 返回：
//   x 和 y 中的较大值
//
// 注意：
//   这是一个工具函数，用于时间戳比较
func max(x, y uint64) uint64 {
	if x <= y {
		return y
	}
	return x
}
