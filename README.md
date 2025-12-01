# SeaweedFS 学习增强版 🌱

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Go Version](https://img.shields.io/badge/Go-1.24%2B-00ADD8?logo=go)](https://golang.org)
[![原项目](https://img.shields.io/badge/Original-SeaweedFS-green)](https://github.com/seaweedfs/seaweedfs)

> **这是 SeaweedFS 的中文学习增强版本，包含大量详细的中文注释和学习文档，帮助开发者深入理解 SeaweedFS 的设计与实现。**

## 📚 项目简介

本仓库是 [SeaweedFS](https://github.com/seaweedfs/seaweedfs) 的学习型 Fork，专注于为源码添加详细的中文注释和学习文档。

**SeaweedFS** 是一个高性能、易扩展的分布式文件系统，特别适合存储海量（十亿级）小文件：

- ⚡ **O(1) 读取时间** - 一次磁盘操作即可读取文件
- 🏗️ **经典架构** - Facebook Haystack 设计 + f4 纠删码实现
- 🔌 **多种接口** - S3 兼容 API、POSIX FUSE、原生 HTTP
- 📦 **灵活存储** - 支持副本复制、纠删码、冷热分层
- 💾 **存储高效** - 每个文件仅 16 字节元数据开销

### 与官方版本的区别

| 特性 | 官方版本 | 本学习版 |
|------|---------|---------|
| 代码功能 | ✅ 完整功能 | ✅ 完整功能 |
| 英文注释 | ⭕ 部分 | ⭕ 保留 |
| 中文注释 | ❌ 无 | ✅ **详细的中文注释** |
| 学习文档 | ⭕ Wiki | ✅ **集成学习文档** |
| 架构说明 | ⭕ 分散 | ✅ **系统化整理** |
| 适合人群 | 使用者 | **学习者、源码研究者** |

---

## 🎯 适合人群

- 🔍 想深入理解分布式存储系统设计的开发者
- 📖 希望通过阅读优秀开源项目提升技术的工程师
- 🎓 学习 Go 语言分布式系统开发的学生
- 💡 需要为自己的项目选型或设计存储方案的架构师

---

## 🚀 快速开始

### 基础构建

```bash
# 克隆本仓库
git clone https://github.com/RhsR1024/seaweedfs-learning.git
cd seaweedfs-learning

# 构建并安装
cd weed && go install

# 验证安装
weed version
```

### 启动开发服务器

```bash
# 方式 1：使用 Makefile
make server

# 方式 2：手动启动（包含 Master、Volume、Filer、S3 网关）
weed server -s3 -filer -volume.max=0 -master.volumeSizeLimitMB=100
```

### 测试上传下载

```bash
# 1. 请求文件 ID
curl http://localhost:9333/dir/assign
# 返回：{"fid":"3,01637037d6","url":"127.0.0.1:8080","publicUrl":"localhost:8080"}

# 2. 上传文件
curl -F file=@test.jpg http://127.0.0.1:8080/3,01637037d6

# 3. 下载文件
curl http://localhost:8080/3,01637037d6 -o downloaded.jpg

# 4. 删除文件
curl -X DELETE http://127.0.0.1:8080/3,01637037d6
```

---

## 📖 核心架构

SeaweedFS 采用三层架构设计，职责清晰、易于扩展：

```
┌─────────────────────────────────────────────────────────────┐
│                          Client                              │
│              (HTTP API / S3 API / FUSE Mount)                │
└─────────────────────────────────────────────────────────────┘
                              ▲
                              │
         ┌────────────────────┼────────────────────┐
         │                    │                    │
         ▼                    ▼                    ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│  Master Server  │  │  Filer Server   │  │ Volume Server   │
│   (端口 9333)   │  │   (端口 8888)   │  │   (端口 8080)   │
├─────────────────┤  ├─────────────────┤  ├─────────────────┤
│ • Volume 管理   │  │ • 目录树管理    │  │ • 实际文件存储  │
│ • 文件 ID 分配  │  │ • 文件元数据    │  │ • Needle 格式   │
│ • 拓扑维护      │  │ • POSIX 接口    │  │ • 索引管理      │
│ • 心跳检测      │  │ • S3 网关       │  │ • 副本/纠删码   │
└─────────────────┘  └─────────────────┘  └─────────────────┘
         │                    │                    │
         └────────────────────┴────────────────────┘
                              ▼
                    ┌─────────────────┐
                    │  存储层          │
                    │  • .dat 文件     │
                    │  • .idx 索引     │
                    │  • 元数据 DB     │
                    └─────────────────┘
```

### 1. Master Server（元数据管理）

- **核心职责**：Volume 分配和文件 ID 生成
- **不存储**：文件内容和文件元数据
- **关键概念**：
  - 维护 Volume ID → Volume Server 的映射
  - 生成全局唯一的文件 ID（格式：`volumeId,fileKey_cookie`）
  - 拓扑结构：数据中心 → 机架 → 节点 → Volume

### 2. Volume Server（存储层）

- **核心职责**：实际存储文件数据
- **存储格式**：
  - 每个 Volume 是一个 32GB 的 append-only 文件
  - Needle 是基本存储单元（仅 16 字节元数据开销）
- **索引类型**：
  - **Memory**：全内存，最快，高内存占用
  - **LevelDB**：LSM-tree，平衡性能和内存
  - **Sorted File**：最低内存，启动较慢

### 3. Filer Server（文件系统层）

- **核心职责**：提供类 POSIX 文件系统接口
- **功能**：
  - 目录树和文件元数据管理
  - 支持 FUSE Mount、S3 API、WebDAV
  - 可插拔存储后端（MySQL、PostgreSQL、Redis 等）

---

## 🔑 核心概念

### File ID (fid) 格式

```
格式：volumeId,fileKey[_cookie]
示例：3,01e3b0756f_a1b2c3d4

组成部分：
├─ volumeId: 3              (32位无符号整数，标识 Volume)
├─ fileKey: 01e3b0756f      (64位无符号整数，Volume 内唯一标识)
└─ cookie: a1b2c3d4         (32位无符号整数，防止 URL 猜测)
```

### Needle 存储格式

```
┌──────────────────────────────────────────────────────┐
│                    Needle 结构                        │
├─────────┬─────────┬──────┬────────┬──────┬──────────┤
│ Cookie  │ NeedleId│ Size │  Data  │ Flag │ Checksum │
│  4字节  │  8字节  │ 4字节│ N字节  │ 1字节│  4字节   │
└─────────┴─────────┴──────┴────────┴──────┴──────────┘
                    ↑
              仅 16 字节元数据开销
```

### 副本策略（Replication）

```
格式：XYZ（三位数字）

X - 不同数据中心的副本数
Y - 同数据中心不同机架的副本数
Z - 同机架不同服务器的副本数

示例：
000 - 无副本
001 - 同机架复制一次
010 - 同数据中心不同机架复制一次
100 - 不同数据中心复制一次
200 - 不同数据中心复制两次
```

---

## 📂 已注释的核心文件

本项目为以下关键模块添加了详细的中文注释：

### 存储层（Storage Layer）
- [`weed/storage/needle_map.go`](weed/storage/needle_map.go) - Needle 索引接口
- [`weed/storage/super_block/super_block.go`](weed/storage/super_block/super_block.go) - Volume 元数据
- [`weed/storage/super_block/replica_placement.go`](weed/storage/super_block/replica_placement.go) - 副本策略

### 网络层（Network Layer）
- [`weed/pb/server_address.go`](weed/pb/server_address.go) - 服务器地址处理和 DNS SRV 解析
- [`weed/pb/server_discovery.go`](weed/pb/server_discovery.go) - 服务发现机制

### 操作层（Operation Layer）
- [`weed/operation/submit.go`](weed/operation/submit.go) - 文件上传逻辑
- [`weed/operation/lookup_vid_cache.go`](weed/operation/lookup_vid_cache.go) - Volume 位置缓存

### 命令层（Command Layer）
- [`weed/command/volume.go`](weed/command/volume.go) - Volume Server 启动（详细的 4 阶段流程）
- [`weed/command/command.go`](weed/command/command.go) - 命令框架

### 服务层（Server Layer）
- [`weed/server/master_grpc_server.go`](weed/server/master_grpc_server.go) - Master gRPC 服务
- [`weed/server/filer_grpc_server.go`](weed/server/filer_grpc_server.go) - Filer gRPC 服务
- [`weed/server/filer_server_handlers_*.go`](weed/server/) - Filer HTTP 处理器

---

## 📚 学习文档

本仓库提供了系统化的学习文档：

| 文档 | 说明 |
|------|------|
| [`docs/LEARNING_GUIDE_API_FLOW.md`](docs/LEARNING_GUIDE_API_FLOW.md) | 上传/下载/删除 API 完整流程 |
| [`docs/LEARNING_GUIDE_DOWNLOAD_DELETE_STORAGE.md`](docs/LEARNING_GUIDE_DOWNLOAD_DELETE_STORAGE.md) | 下载和删除的存储层详解 |
| [`weed/storage/needle/README.md`](weed/storage/needle/README.md) | Needle 二进制格式详解 |
| [`weed/server/README_volume_server_read_flow.md`](weed/server/README_volume_server_read_flow.md) | Volume Server 读取流程 |
| [`CLAUDE.md`](CLAUDE.md) | 项目开发指南和注释规范 |

---

## 🛠️ 开发命令

```bash
# 构建基础版本
cd weed && go install

# 构建包含所有可选功能的版本
cd weed && go install -tags "elastic gocdk sqlite ydb tarantool tikv rclone"

# 运行测试
make test
cd weed && go test -v ./storage/...

# 运行基准测试
make benchmark
make benchmark_with_pprof

# 启动开发服务器
make server
```

---

## 🎓 学习路径建议

### 1️⃣ 初级：理解基本概念（1-2 天）

1. 阅读本 README 的架构概览部分
2. 阅读 [`docs/LEARNING_GUIDE_API_FLOW.md`](docs/LEARNING_GUIDE_API_FLOW.md) - 理解上传下载流程
3. 动手实践：启动服务器，测试上传下载

### 2️⃣ 中级：深入核心模块（1-2 周）

1. **存储层**：
   - 阅读 [`weed/storage/needle/README.md`](weed/storage/needle/README.md)
   - 研究 [`weed/storage/super_block/`](weed/storage/super_block/) 中的代码注释

2. **网络层**：
   - 研究 [`weed/pb/server_address.go`](weed/pb/server_address.go) 的 DNS SRV 机制
   - 理解 [`weed/pb/server_discovery.go`](weed/pb/server_discovery.go) 的服务发现

3. **服务层**：
   - 阅读 [`weed/server/master_grpc_server.go`](weed/server/master_grpc_server.go) 的注释
   - 研究 [`weed/server/filer_server_handlers_*.go`](weed/server/) 的 HTTP 处理流程

### 3️⃣ 高级：架构设计和优化（2-4 周）

1. 研究 Volume Server 启动流程（[`weed/command/volume.go`](weed/command/volume.go)）
2. 理解副本策略和纠删码实现
3. 研究索引优化（Memory vs LevelDB vs Sorted File）
4. 阅读官方 Wiki 的高级话题

---

## 🤝 贡献指南

欢迎为本学习项目贡献注释和文档！

### 注释规范

1. **必须使用中文**进行所有代码注释
2. **详细程度**：
   - 简单代码：一行注释说明目的
   - 中等复杂：多行注释 + 参数说明
   - 复杂逻辑：分步注释 + 原理说明 + 示例 + 边界情况

3. **注释位置**：
   - 包级别注释
   - 结构体和接口
   - 函数签名和内部步骤
   - 重要变量和参数
   - 关键函数调用
   - 复杂逻辑和算法
   - 错误处理和边界情况

### 提交规范

```bash
git commit -m "为 [模块名] 添加详细中文注释

主要修改：
- 文件1: 添加XX功能的注释
- 文件2: 完善YY逻辑的说明

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>"
```

详细规范请参考 [`CLAUDE.md`](CLAUDE.md)。

---

## 🔗 相关链接

- 🌐 [官方 SeaweedFS 项目](https://github.com/seaweedfs/seaweedfs)
- 📖 [官方 Wiki 文档](https://github.com/seaweedfs/seaweedfs/wiki)
- 📄 [SeaweedFS 白皮书](https://github.com/seaweedfs/seaweedfs/wiki/SeaweedFS_Architecture.pdf)
- 💬 [Slack 社区](https://join.slack.com/t/seaweedfs/shared_invite/enQtMzI4MTMwMjU2MzA3LTEyYzZmZWYzOGQ3MDJlZWMzYmI0OTE4OTJiZjJjODBmMzUxNmYwODg0YjY3MTNlMjBmZDQ1NzQ5NDJhZWI2ZmY)

---

## 📊 与其他分布式文件系统对比

| 特性 | SeaweedFS | HDFS | Ceph | MinIO | GlusterFS |
|------|-----------|------|------|-------|-----------|
| **小文件优化** | ✅ 优秀 | ❌ 差 | ⭕ 一般 | ❌ 差 | ⭕ 一般 |
| **元数据开销** | ✅ 16字节/文件 | ❌ 536字节/文件 | ⭕ 中等 | ❌ 单独文件 | ⭕ 中等 |
| **读取性能** | ✅ O(1) 磁盘操作 | ⭕ O(N) | ⭕ O(N) | ⭕ O(N) | ⭕ O(N) |
| **S3 兼容** | ✅ 是 | ❌ 否 | ✅ 是 | ✅ 是 | ❌ 否 |
| **POSIX 支持** | ✅ FUSE | ✅ 原生 | ✅ 原生 | ❌ 否 | ✅ 原生 |
| **部署复杂度** | ✅ 简单 | ⭕ 中等 | ❌ 复杂 | ✅ 简单 | ⭕ 中等 |
| **适用场景** | 海量小文件 | 大数据分析 | 通用存储 | S3 替代 | 通用存储 |

---

## 📜 开源协议

本项目继承 SeaweedFS 的 Apache License 2.0 协议。

```
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
```

详见 [LICENSE](LICENSE) 文件。

---

## ⭐ Star History

如果本项目对你有帮助，欢迎 Star 支持！

[![Star History Chart](https://api.star-history.com/svg?repos=RhsR1024/seaweedfs-learning&type=Date)](https://star-history.com/#RhsR1024/seaweedfs-learning&Date)

---

## 📮 联系方式

- **GitHub Issues**: [提交问题或建议](https://github.com/RhsR1024/seaweedfs-learning/issues)
- **原项目**: [SeaweedFS Official](https://github.com/seaweedfs/seaweedfs)

---

<div align="center">

**本项目旨在帮助开发者更好地理解分布式存储系统的设计与实现**

如果你觉得有帮助，请给个 ⭐ Star 支持一下！

Made with ❤️ by SeaweedFS Learning Community

</div>
