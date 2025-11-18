// Package needle 实现了 SeaweedFS 中 Needle 的读取功能
// Needle 是 SeaweedFS 存储数据的基本单元，类似于文件系统中的 inode
// 每个 Needle 包含完整的元数据和实际数据，可以独立存储和检索
package needle

import (
	"errors"
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// Needle 标志位常量定义
// 这些标志位用于标识 Needle 中包含哪些可选字段
const (
	FlagIsCompressed        = 0x01 // 数据是否压缩 (bit 0)
	FlagHasName             = 0x02 // 是否包含文件名 (bit 1)
	FlagHasMime             = 0x04 // 是否包含 MIME 类型 (bit 2)
	FlagHasLastModifiedDate = 0x08 // 是否包含最后修改时间 (bit 3)
	FlagHasTtl              = 0x10 // 是否包含 TTL 过期时间 (bit 4)
	FlagHasPairs            = 0x20 // 是否包含自定义键值对 (bit 5)
	FlagIsChunkManifest     = 0x80 // 是否为分块清单 (bit 7)，用于大文件分块存储
	LastModifiedBytesLength = 5    // 最后修改时间字段长度（5字节可表示到2127年）
	TtlBytesLength          = 2    // TTL 字段长度（2字节）
)

// 错误定义
var ErrorSizeMismatch = errors.New("size mismatch") // Needle 大小不匹配错误
var ErrorSizeInvalid = errors.New("size invalid")   // Needle 大小无效错误

// DiskSize 计算 Needle 在磁盘上占用的实际大小
// 包括 Header + Body + Checksum + Padding
// 参数:
//   - version: Volume 版本号，不同版本有不同的存储格式
//
// 返回: Needle 在磁盘上的字节数
func (n *Needle) DiskSize(version Version) int64 {
	return GetActualSize(n.Size, version)
}

// ReadNeedleBlob 从存储后端读取完整的 Needle 二进制数据块
// 这是一个底层函数，读取包括 Header、Body、Checksum 的完整数据
// 参数:
//   - r: 后端存储文件接口
//   - offset: Needle 在文件中的起始偏移量
//   - size: Needle 的 Body 大小（不包括 Header）
//   - version: Volume 版本号
//
// 返回:
//   - dataSlice: 读取的完整二进制数据
//   - err: 错误信息
func ReadNeedleBlob(r backend.BackendStorageFile, offset int64, size Size, version Version) (dataSlice []byte, err error) {

	// 计算 Needle 在磁盘上的实际大小（Header + Body + Checksum + Padding）
	dataSize := GetActualSize(size, version)
	dataSlice = make([]byte, int(dataSize))

	var n int
	// 从指定偏移量读取数据
	n, err = r.ReadAt(dataSlice, offset)
	// 如果读取的字节数等于期望大小，即使返回 EOF 也认为成功
	if err != nil && int64(n) == dataSize {
		err = nil
	}
	// 读取失败时记录详细的错误日志
	if err != nil {
		fileSize, _, _ := r.GetStat()
		glog.Errorf("%s read %d dataSize %d offset %d fileSize %d: %v", r.Name(), n, dataSize, offset, fileSize, err)
	}
	return dataSlice, err

}

// ReadBytes 从字节缓冲区解析 Needle 数据并填充到 Needle 结构体
// 调用前需要先设置 n.Id，该函数会填充其他所有字段
// 参数:
//   - bytes: 包含完整 Needle 数据的字节数组
//   - offset: Needle 在文件中的偏移量（用于错误报告）
//   - size: 期望的 Needle Body 大小
//   - version: Volume 版本号
//
// 返回: 解析错误（如果有）
func (n *Needle) ReadBytes(bytes []byte, offset int64, size Size, version Version) (err error) {
	// 解析 Needle Header（Cookie + Id + Size）
	n.ParseNeedleHeader(bytes)
	// 验证读取的大小是否与期望大小一致
	if n.Size != size {
		// 特殊情况：32位偏移量可能溢出，需要加上最大卷大小后重试
		if OffsetSize == 4 && offset < int64(MaxPossibleVolumeSize) {
			stats.VolumeServerHandlerCounter.WithLabelValues(stats.ErrorSizeMismatchOffsetSize).Inc()
			glog.Errorf("entry not found1: offset %d found id %x size %d, expected size %d", offset, n.Id, n.Size, size)
			return ErrorSizeMismatch
		}
		stats.VolumeServerHandlerCounter.WithLabelValues(stats.ErrorSizeMismatch).Inc()
		return fmt.Errorf("entry not found: offset %d found id %x size %d, expected size %d", offset, n.Id, n.Size, size)
	}
	// 根据版本解析 Body 数据
	if version == Version1 {
		// Version1: Data 直接存储，无元数据字段
		n.Data = bytes[NeedleHeaderSize : NeedleHeaderSize+size]
	} else {
		// Version2/3: Data 前有长度字段，后跟各种元数据
		err := n.readNeedleDataVersion2(bytes[NeedleHeaderSize : NeedleHeaderSize+int(size)])
		if err != nil && err != io.EOF {
			return err
		}
	}
	// 读取 Needle 尾部（Checksum 和 Padding）
	err = n.readNeedleTail(bytes[NeedleHeaderSize+size:], version)
	if err != nil {
		return err
	}
	return nil
}

// ReadData 从存储文件中读取并解析 Needle 数据
// 这是从文件读取 Needle 的主要入口函数，调用前需要先设置 n.Id
// 参数:
//   - r: 后端存储文件接口
//   - offset: Needle 在文件中的偏移量
//   - size: Needle Body 大小
//   - version: Volume 版本号
//
// 返回: 读取或解析错误
//
// 特殊处理：当使用32位偏移量且遇到大小不匹配时，会自动尝试加上 MaxPossibleVolumeSize 后重试
// 这是为了处理大于 4GB 的卷文件
func (n *Needle) ReadData(r backend.BackendStorageFile, offset int64, size Size, version Version) (err error) {
	// 读取原始二进制数据
	bytes, err := ReadNeedleBlob(r, offset, size, version)
	if err != nil {
		return err
	}
	// 解析字节数据到 Needle 结构体
	err = n.ReadBytes(bytes, offset, size, version)
	// 处理32位偏移量溢出的情况
	if err == ErrorSizeMismatch && OffsetSize == 4 {
		// 加上最大卷大小后重试读取
		offset = offset + int64(MaxPossibleVolumeSize)
		bytes, err = ReadNeedleBlob(r, offset, size, version)
		if err != nil {
			return err
		}
		err = n.ReadBytes(bytes, offset, size, version)
	}
	return err
}

// ParseNeedleHeader 从字节数组中解析 Needle 头部信息
// Needle Header 包含 3 个字段（固定 16 字节）：
//   - Cookie (4 字节): 用于验证 Needle 的完整性
//   - Id (8 字节): Needle 的唯一标识符
//   - Size (4 字节): Needle Body 的大小
//
// 参数:
//   - bytes: 至少包含 NeedleHeaderSize (16) 字节的数组
func (n *Needle) ParseNeedleHeader(bytes []byte) {
	n.Cookie = BytesToCookie(bytes[0:CookieSize])                           // 前4字节：Cookie
	n.Id = BytesToNeedleId(bytes[CookieSize : CookieSize+NeedleIdSize])     // 中间8字节：Id
	n.Size = BytesToSize(bytes[CookieSize+NeedleIdSize : NeedleHeaderSize]) // 后4字节：Size
}

// readNeedleDataVersion2 解析 Version2/3 格式的 Needle Body 数据
// Version2 格式布局：
//   - DataSize (4 字节): 实际数据长度
//   - Data (DataSize 字节): 实际文件数据
//   - Flags (1 字节): 标志位，指示后续包含哪些元数据字段
//   - [可选] NameSize + Name: 文件名
//   - [可选] MimeSize + Mime: MIME 类型
//   - [可选] LastModified (5 字节): 最后修改时间
//   - [可选] Ttl (2 字节): 过期时间
//   - [可选] PairsSize + Pairs: 自定义键值对
//
// 参数:
//   - bytes: 包含完整 Body 数据的字节数组
//
// 返回: 解析错误（如果有）
func (n *Needle) readNeedleDataVersion2(bytes []byte) (err error) {
	index, lenBytes := 0, len(bytes)
	// 读取实际数据大小（4字节）
	if index < lenBytes {
		n.DataSize = util.BytesToUint32(bytes[index : index+4])
		index = index + 4
		// 边界检查：确保数据不会越界
		if int(n.DataSize)+index > lenBytes {
			stats.VolumeServerHandlerCounter.WithLabelValues(stats.ErrorIndexOutOfRange).Inc()
			return fmt.Errorf("index out of range %d", 1)
		}
		// 读取实际数据
		n.Data = bytes[index : index+int(n.DataSize)]
		index = index + int(n.DataSize)
	}
	// 读取元数据字段（Flags, Name, Mime, LastModified, Ttl, Pairs）
	_, err = n.readNeedleDataVersion2NonData(bytes[index:])
	return
}

// readNeedleDataVersion2NonData 解析 Version2 格式中除实际数据外的元数据部分
// 根据 Flags 标志位判断哪些字段存在，并依次解析
// 参数:
//   - bytes: 从 Flags 字段开始的字节数组
//
// 返回:
//   - index: 解析后的位置索引
//   - err: 解析错误（如果有）
func (n *Needle) readNeedleDataVersion2NonData(bytes []byte) (index int, err error) {
	lenBytes := len(bytes)
	// 读取 Flags 标志位（1字节）
	if index < lenBytes {
		n.Flags = bytes[index]
		index = index + 1
	}
	// 如果有文件名字段
	if index < lenBytes && n.HasName() {
		n.NameSize = uint8(bytes[index])
		index = index + 1
		if int(n.NameSize)+index > lenBytes {
			stats.VolumeServerHandlerCounter.WithLabelValues(stats.ErrorIndexOutOfRange).Inc()
			return index, fmt.Errorf("index out of range %d", 2)
		}
		n.Name = bytes[index : index+int(n.NameSize)]
		index = index + int(n.NameSize)
	}
	// 如果有 MIME 类型字段
	if index < lenBytes && n.HasMime() {
		n.MimeSize = uint8(bytes[index])
		index = index + 1
		if int(n.MimeSize)+index > lenBytes {
			stats.VolumeServerHandlerCounter.WithLabelValues(stats.ErrorIndexOutOfRange).Inc()
			return index, fmt.Errorf("index out of range %d", 3)
		}
		n.Mime = bytes[index : index+int(n.MimeSize)]
		index = index + int(n.MimeSize)
	}
	// 如果有最后修改时间字段（5字节，可表示到2127年）
	if index < lenBytes && n.HasLastModifiedDate() {
		if LastModifiedBytesLength+index > lenBytes {
			stats.VolumeServerHandlerCounter.WithLabelValues(stats.ErrorIndexOutOfRange).Inc()
			return index, fmt.Errorf("index out of range %d", 4)
		}
		n.LastModified = util.BytesToUint64(bytes[index : index+LastModifiedBytesLength])
		index = index + LastModifiedBytesLength
	}
	// 如果有 TTL 字段（2字节）
	if index < lenBytes && n.HasTtl() {
		if TtlBytesLength+index > lenBytes {
			stats.VolumeServerHandlerCounter.WithLabelValues(stats.ErrorIndexOutOfRange).Inc()
			return index, fmt.Errorf("index out of range %d", 5)
		}
		n.Ttl = LoadTTLFromBytes(bytes[index : index+TtlBytesLength])
		index = index + TtlBytesLength
	}
	// 如果有自定义键值对字段
	if index < lenBytes && n.HasPairs() {
		if 2+index > lenBytes {
			stats.VolumeServerHandlerCounter.WithLabelValues(stats.ErrorIndexOutOfRange).Inc()
			return index, fmt.Errorf("index out of range %d", 6)
		}
		n.PairsSize = util.BytesToUint16(bytes[index : index+2])
		index += 2
		if int(n.PairsSize)+index > lenBytes {
			stats.VolumeServerHandlerCounter.WithLabelValues(stats.ErrorIndexOutOfRange).Inc()
			return index, fmt.Errorf("index out of range %d", 7)
		}
		end := index + int(n.PairsSize)
		n.Pairs = bytes[index:end]
		index = end
	}
	return index, nil
}

// ReadNeedleHeader 从存储文件中只读取 Needle 的头部信息
// 这是一个轻量级函数，用于快速获取 Needle 的基本信息而不读取完整数据
// 参数:
//   - r: 后端存储文件接口
//   - version: Volume 版本号
//   - offset: Needle 在文件中的偏移量
//
// 返回:
//   - n: 包含头部信息的 Needle 对象
//   - bytes: 读取的头部字节数组
//   - bodyLength: Body 部分的长度（不包括 Header）
//   - err: 读取错误
func ReadNeedleHeader(r backend.BackendStorageFile, version Version, offset int64) (n *Needle, bytes []byte, bodyLength int64, err error) {
	n = new(Needle)

	// 分配 Header 大小的缓冲区（16字节）
	bytes = make([]byte, NeedleHeaderSize)

	var count int
	// 读取 Header
	count, err = r.ReadAt(bytes, offset)
	// 如果读取到完整的 Header，即使返回 EOF 也认为成功
	if err == io.EOF && count == NeedleHeaderSize {
		err = nil
	}
	if count <= 0 || err != nil {
		return nil, bytes, 0, err
	}

	// 解析 Header 字段
	n.ParseNeedleHeader(bytes)
	// 计算 Body 长度（包括数据和 Checksum）
	bodyLength = NeedleBodyLength(n.Size, version)

	return
}

// ReadNeedleBody 读取 Needle 的 Body 部分（不包括 Header）
// 调用前需要已经读取并解析 Header，该函数继续读取剩余的 Body 数据
// 参数:
//   - r: 后端存储文件接口
//   - version: Volume 版本号
//   - offset: Body 部分的起始偏移量（Header 之后）
//   - bodyLength: Body 部分的长度
//
// 返回:
//   - bytes: 读取的 Body 字节数组
//   - err: 读取或解析错误
func (n *Needle) ReadNeedleBody(r backend.BackendStorageFile, version Version, offset int64, bodyLength int64) (bytes []byte, err error) {

	if bodyLength <= 0 {
		return nil, nil
	}
	// 分配 Body 大小的缓冲区
	bytes = make([]byte, bodyLength)
	readCount, err := r.ReadAt(bytes, offset)
	// 如果读取到完整的 Body，即使返回 EOF 也认为成功
	if err == io.EOF && int64(readCount) == bodyLength {
		err = nil
	}
	if err != nil {
		glog.Errorf("%s read %d bodyLength %d offset %d: %v", r.Name(), readCount, bodyLength, offset, err)
		return
	}

	// 解析 Body 字节数据到 Needle 结构体
	err = n.ReadNeedleBodyBytes(bytes, version)

	return
}

// ReadNeedleBodyBytes 从字节数组解析 Needle Body 数据
// 这是一个核心解析函数，根据不同版本采用不同的解析策略
// 参数:
//   - needleBody: 包含完整 Body 的字节数组
//   - version: Volume 版本号
//
// 返回: 解析错误（如果有）
func (n *Needle) ReadNeedleBodyBytes(needleBody []byte, version Version) (err error) {

	if len(needleBody) <= 0 {
		return nil
	}
	switch version {
	case Version1:
		// Version1 格式：只有原始数据 + Checksum
		n.Data = needleBody[:n.Size]
		err = n.readNeedleTail(needleBody[n.Size:], version)
	case Version2, Version3:
		// Version2/3 格式：数据 + 元数据 + Checksum
		err = n.readNeedleDataVersion2(needleBody[0:n.Size])
		if err == nil {
			err = n.readNeedleTail(needleBody[n.Size:], version)
		}
	default:
		err = fmt.Errorf("unsupported version %d!", version)
	}
	return
}

// IsCompressed 检查 Needle 数据是否被压缩
// 通过检查 Flags 的 bit 0 来判断
func (n *Needle) IsCompressed() bool {
	return n.Flags&FlagIsCompressed > 0
}

// SetIsCompressed 设置压缩标志位
func (n *Needle) SetIsCompressed() {
	n.Flags = n.Flags | FlagIsCompressed
}

// HasName 检查 Needle 是否包含文件名字段
// 通过检查 Flags 的 bit 1 来判断
// 这是用按位与判断那一位是否为 1。注意：> 0 和 != 0 都等价；!= 0 更常见、更直观，但 > 0 也可以
func (n *Needle) HasName() bool {
	return n.Flags&FlagHasName > 0
}

// SetHasName 设置文件名标志位
// 这是把 FlagHasName 对应位设置为 1（按位或），不会影响其他位
func (n *Needle) SetHasName() {
	n.Flags = n.Flags | FlagHasName
}

// HasMime 检查 Needle 是否包含 MIME 类型字段
// 通过检查 Flags 的 bit 2 来判断
func (n *Needle) HasMime() bool {
	return n.Flags&FlagHasMime > 0
}

// SetHasMime 设置 MIME 类型标志位
func (n *Needle) SetHasMime() {
	n.Flags = n.Flags | FlagHasMime
}

// HasLastModifiedDate 检查 Needle 是否包含最后修改时间字段
// 通过检查 Flags 的 bit 3 来判断
func (n *Needle) HasLastModifiedDate() bool {
	return n.Flags&FlagHasLastModifiedDate > 0
}

// SetHasLastModifiedDate 设置最后修改时间标志位
func (n *Needle) SetHasLastModifiedDate() {
	n.Flags = n.Flags | FlagHasLastModifiedDate
}

// HasTtl 检查 Needle 是否包含 TTL（生存时间）字段
// 通过检查 Flags 的 bit 4 来判断
func (n *Needle) HasTtl() bool {
	return n.Flags&FlagHasTtl > 0
}

// SetHasTtl 设置 TTL 标志位
func (n *Needle) SetHasTtl() {
	n.Flags = n.Flags | FlagHasTtl
}

// IsChunkedManifest 检查 Needle 是否为分块清单
// 大文件会被分成多个块存储，清单记录所有块的位置信息
// 通过检查 Flags 的 bit 7 来判断
func (n *Needle) IsChunkedManifest() bool {
	return n.Flags&FlagIsChunkManifest > 0
}

// SetIsChunkManifest 设置分块清单标志位
func (n *Needle) SetIsChunkManifest() {
	n.Flags = n.Flags | FlagIsChunkManifest
}

// HasPairs 检查 Needle 是否包含自定义键值对字段
// 通过检查 Flags 的 bit 5 来判断
func (n *Needle) HasPairs() bool {
	return n.Flags&FlagHasPairs != 0
}

// SetHasPairs 设置自定义键值对标志位
func (n *Needle) SetHasPairs() {
	n.Flags = n.Flags | FlagHasPairs
}

// GetActualSize 计算 Needle 在磁盘上的实际占用大小
// 包括：Header (16字节) + Body + Checksum + Padding（对齐到8字节边界）
// 参数:
//   - size: Needle Body 的大小
//   - version: Volume 版本号
//
// 返回: Needle 的实际磁盘占用字节数
func GetActualSize(size Size, version Version) int64 {
	return NeedleHeaderSize + NeedleBodyLength(size, version)
}
