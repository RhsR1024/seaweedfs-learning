// Package needle 提供 SeaweedFS 的 Needle 存储结构和相关操作
// 本文件测试 Needle 的写入功能
//
// ============================================================================
// 测试覆盖范围
// ============================================================================
//
// 1. Needle.Append - 测试追加写入到大文件（超过 4GB）
// 2. 新旧实现兼容性 - 验证 writeNeedleV1/V2/V3 与 LegacyPrepareWriteBuffer 输出一致
// 3. Mock 后端写入 - 使用模拟接口测试写入逻辑
//
// ============================================================================
// 大文件支持
// ============================================================================
//
// SeaweedFS 支持单个 Volume 文件超过 4GB（32 位整数上限）。
// 这通过以下方式实现：
// - 使用 64 位偏移量
// - Needle 偏移以 8 字节为单位存储（节省空间）
// - 实际支持的最大 Volume 大小：32GB（使用 32 位索引）或更大（64 位索引）
//
// ============================================================================
package needle

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// TestAppend 测试 Needle 追加写入到大文件
//
// 测试场景：
// - 创建一个超过 4GB 的临时文件
// - 向文件末尾追加 Needle
// - 验证返回的偏移量正确
//
// 这个测试验证了 SeaweedFS 对大文件的支持，
// 确保 64 位偏移量处理正确。
func TestAppend(t *testing.T) {
	// 创建测试用的 Needle
	// 包含所有 Version2/3 支持的字段
	n := &Needle{
		// Cookie: 用于安全验证的随机数，防止 URL 猜测攻击
		Cookie: types.Cookie(123),

		// Id: Needle 的唯一标识符
		Id: types.NeedleId(123),

		// Size: Header 中记录的大小字段
		// 对于 Version2/3，这是所有可选字段的总大小
		Size: 8,

		// DataSize: 实际数据的大小（Version2/3）
		DataSize: 4,

		// Data: 实际的文件数据
		Data: []byte("abcd"),

		// Flags: 标志位，指示哪些可选字段存在
		Flags: 0,

		// NameSize 和 Name: 文件名（可选，最大 256 字符）
		NameSize: 0,
		Name:     nil,

		// MimeSize 和 Mime: MIME 类型（可选，最大 256 字符）
		MimeSize: 0,
		Mime:     nil,

		// PairsSize 和 Pairs: 额外的键值对（可选，JSON 格式）
		PairsSize: 0,
		Pairs:     nil,

		// LastModified: 最后修改时间（只存储 5 字节）
		LastModified: 123,

		// Ttl: 生存时间（可选）
		Ttl: nil,

		// Checksum: CRC32 校验和，用于数据完整性验证
		Checksum: 123,

		// AppendAtNs: 追加时间戳（Version3，纳秒精度）
		AppendAtNs: 123,

		// Padding: 用于对齐到 8 字节边界的填充数据
		Padding: nil,
	}

	// 创建临时文件
	tempFile, err := os.CreateTemp("", ".dat")
	if err != nil {
		t.Errorf("Fail TempFile. %v", err)
		return
	}

	/*
		整数类型范围参考：
		uint8  : 0 to 255
		uint16 : 0 to 65535
		uint32 : 0 to 4294967295（约 4GB）
		uint64 : 0 to 18446744073709551615（约 18EB）
		int8   : -128 to 127
		int16  : -32768 to 32767
		int32  : -2147483648 to 2147483647
		int64  : -9223372036854775808 to 9223372036854775807
	*/

	// 设置文件大小为 4GB + 10000 字节
	// 这超过了 uint32 的最大值，测试 64 位偏移量支持
	fileSize := int64(4294967296) + 10000 // 约 4.0000093GB
	tempFile.Truncate(fileSize)

	// 清理：测试结束后关闭并删除临时文件
	defer func() {
		tempFile.Close()
		os.Remove(tempFile.Name())
	}()

	// 创建磁盘后端存储接口
	datBackend := backend.NewDiskFile(tempFile)
	defer datBackend.Close()

	// 执行追加写入
	// Append 会将 Needle 写入到文件末尾
	offset, _, _, _ := n.Append(datBackend, GetCurrentVersion())

	// 验证偏移量
	// 偏移量应该等于原始文件大小（即新数据追加到文件末尾）
	if offset != uint64(fileSize) {
		t.Errorf("Fail to Append Needle.")
	}
}

// versionString 将 Version 枚举转换为可读字符串
//
// 用于测试输出中显示版本名称
func versionString(v Version) string {
	switch v {
	case Version1:
		return "Version1"
	case Version2:
		return "Version2"
	case Version3:
		return "Version3"
	default:
		return "UnknownVersion"
	}
}

// TestWriteNeedle_CompatibilityWithLegacy 测试新旧写入实现的兼容性
//
// 测试目的：
// 验证新的 writeNeedleV1/V2/V3 函数与旧的 LegacyPrepareWriteBuffer 函数
// 产生完全相同的二进制输出。
//
// 这确保了：
// 1. 新实现不会破坏现有数据格式
// 2. 新旧版本之间的数据可以互相读取
// 3. 重构没有引入 bug
func TestWriteNeedle_CompatibilityWithLegacy(t *testing.T) {
	// 测试所有三个版本
	versions := []Version{Version1, Version2, Version3}

	for _, version := range versions {
		// 使用子测试，便于识别哪个版本失败
		t.Run(versionString(version), func(t *testing.T) {
			// 创建包含所有字段的完整 Needle
			n := &Needle{
				Cookie:       0x12345678,              // 固定的 Cookie 值
				Id:           0x1122334455667788,      // 固定的 NeedleId
				Data:         []byte("hello world"),   // 测试数据
				Flags:        0xFF,                    // 所有标志位都设置
				Name:         []byte("filename.txt"),  // 文件名
				Mime:         []byte("text/plain"),    // MIME 类型
				LastModified: 0x1234567890,            // 最后修改时间
				Ttl:          nil,                     // TTL（可选）
				Pairs:        []byte("key=value"),     // 额外的键值对
				PairsSize:    9,                       // Pairs 长度
				Checksum:     0xCAFEBABE,              // 校验和
				AppendAtNs:   0xDEADBEEF,              // 追加时间戳
			}

			// ========== 使用旧版实现序列化 ==========
			legacyBuf := &bytes.Buffer{}
			_, _, err := n.LegacyPrepareWriteBuffer(version, legacyBuf)
			if err != nil {
				t.Fatalf("LegacyPrepareWriteBuffer failed: %v", err)
			}

			// ========== 使用新版实现序列化 ==========
			newBuf := &bytes.Buffer{}
			offset := uint64(0)

			// 根据版本调用对应的新实现
			switch version {
			case Version1:
				_, _, err = writeNeedleV1(n, offset, newBuf)
			case Version2:
				_, _, err = writeNeedleV2(n, offset, newBuf)
			case Version3:
				_, _, err = writeNeedleV3(n, offset, newBuf)
			}
			if err != nil {
				t.Fatalf("writeNeedleV%d failed: %v", version, err)
			}

			// ========== 比较输出 ==========
			// 新旧实现的输出必须完全相同（字节级别）
			if !bytes.Equal(legacyBuf.Bytes(), newBuf.Bytes()) {
				t.Errorf("Data layout mismatch for version %d\nLegacy: %x\nNew:    %x",
					version, legacyBuf.Bytes(), newBuf.Bytes())
			}
		})
	}
}

// ============================================================================
// Mock 后端存储接口
// ============================================================================
//
// 以下是用于测试的模拟后端存储实现。
// 它实现了 backend.BackendStorageFile 接口的必要方法，
// 但只将数据写入内存缓冲区，不涉及实际的文件 I/O。

// mockBackendWriter 模拟后端写入器
//
// 用于单元测试，避免创建真实的临时文件
type mockBackendWriter struct {
	buf *bytes.Buffer // 内存缓冲区，存储写入的数据
}

// WriteAt 实现 io.WriterAt 接口
//
// 注意：这个简化实现忽略了 offset 参数，
// 直接追加到缓冲区。对于兼容性测试来说这是足够的。
func (m *mockBackendWriter) WriteAt(p []byte, off int64) (n int, err error) {
	return m.buf.Write(p)
}

// GetStat 返回模拟的文件状态
//
// 返回值：
// - size: 始终返回 0（模拟空文件）
// - mtime: 零时间
// - err: 无错误
func (m *mockBackendWriter) GetStat() (int64, time.Time, error) {
	return 0, time.Time{}, nil
}

// Truncate 模拟文件截断
//
// 在测试中不执行实际操作
func (m *mockBackendWriter) Truncate(size int64) error {
	return nil
}

// Name 返回模拟文件的名称
func (m *mockBackendWriter) Name() string {
	return "mock"
}

// Close 关闭模拟文件
//
// 在测试中不执行实际操作
func (m *mockBackendWriter) Close() error {
	return nil
}

// Sync 同步模拟文件
//
// 在测试中不执行实际操作
func (m *mockBackendWriter) Sync() error {
	return nil
}

// ReadAt 实现 io.ReaderAt 接口
//
// 在写入测试中不使用，返回空结果
func (m *mockBackendWriter) ReadAt(p []byte, off int64) (n int, err error) {
	return 0, nil
}
