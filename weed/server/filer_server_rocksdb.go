// filer_server_rocksdb.go 是一个条件编译文件，用于在编译时可选地启用 RocksDB 存储后端
//
// 条件编译（Build Tags）:
//   此文件仅在编译时指定 `rocksdb` 标签时才会被包含
//   编译命令示例：go build -tags "rocksdb"
//
// 为什么使用条件编译:
//   1. RocksDB 是可选依赖：RocksDB 是 C++ 库，需要 CGO 支持，增加了编译复杂度
//   2. 减少默认依赖：用户可以选择不使用 RocksDB，使用其他轻量级存储（如 LevelDB、内存存储）
//   3. 跨平台兼容：某些平台可能不支持 RocksDB，条件编译可以避免编译失败
//
// RocksDB 存储后端的特点:
//   - 高性能：基于 LSM-tree，适合写密集型场景
//   - 可配置：支持压缩、缓存、WAL 等高级特性
//   - 成熟稳定：由 Facebook 开发，广泛应用于生产环境
//
// 使用方法:
//   1. 编译时启用：go build -tags "rocksdb"
//   2. 配置文件中指定：filer.toml 中配置 [rocksdb] 段
//   3. 启动时指定：weed filer -filerStore=rocksdb
//
// 相关文件:
//   - weed/filer/rocksdb/rocksdb_store.go：RocksDB 存储实现
//   - weed/command/filer.go：Filer 启动逻辑，注册存储后端

//go:build rocksdb
// +build rocksdb

package weed_server

import (
	// 导入 RocksDB 存储实现
	// 使用 blank import (_) 触发包的 init() 函数，自动注册到 filer 存储工厂
	// init() 函数会调用 filer.Stores.Register() 注册 "rocksdb" 存储类型
	_ "github.com/seaweedfs/seaweedfs/weed/filer/rocksdb"
)
