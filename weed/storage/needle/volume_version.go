// Package needle 提供 SeaweedFS 的核心数据结构和操作
package needle

// Version Volume 数据格式版本号
//
// SeaweedFS 的 Volume 文件格式经历了多次演进，每个版本都引入了新特性和优化。
// Version 使用 uint8 类型，占用 1 字节，存储在 SuperBlock 中。
//
// 版本演进历史：
//
// Version 1（初始版本）：
//   - 基础的 Needle 存储格式
//   - 简单的文件追加写入模式
//   - Needle 格式：Header + Data + Footer
//
// Version 2（引入压缩支持）：
//   - 新增数据压缩支持（gzip）
//   - Needle Header 中增加压缩标志位
//   - 支持透明的压缩/解压缩
//   - 向后兼容 Version 1
//
// Version 3（当前版本，增强功能）：
//   - 支持更多压缩算法（gzip, zstd, lz4）
//   - 优化的索引结构
//   - 改进的空间回收机制
//   - 更好的并发访问性能
//   - 完全向后兼容 Version 1 和 Version 2
//
// 版本兼容性：
//   - 新版本的 SeaweedFS 可以读取旧版本的 Volume
//   - Volume 创建后版本号固定，不会自动升级
//   - 新创建的 Volume 使用当前版本（Version 3）
type Version uint8

const (
	// Version1 初始版本
	// 基础的 Needle 存储格式，无压缩支持
	Version1 = Version(1)

	// Version2 第二版本
	// 引入数据压缩支持（gzip），向后兼容 Version 1
	Version2 = Version(2)

	// Version3 第三版本（当前版本）
	// 支持多种压缩算法（gzip, zstd, lz4），优化的索引和性能
	// 向后兼容 Version 1 和 Version 2
	Version3 = Version(3)
)

// GetCurrentVersion 获取当前使用的 Volume 版本号
//
// 该函数返回新创建的 Volume 所使用的版本号。
// 当前返回 Version3，表示新 Volume 将使用第三版本格式。
//
// 返回：Version3 - 当前使用的 Volume 版本
//
// 使用场景：
//   - 创建新 Volume 时，在 SuperBlock 中写入版本号
//   - 验证 Volume 格式是否为最新版本
//   - 日志记录和调试信息
//
// 示例：
//   version := needle.GetCurrentVersion()
//   // version = Version3
func GetCurrentVersion() Version {
	return Version3
}
