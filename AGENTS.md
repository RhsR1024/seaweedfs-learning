# AGENTS.md

## 文件说明

本文件为 **CodeX（codex.ai/code）** 在处理此仓库代码时提供指导。

---

# 1. 项目概述（Project Overview）

本仓库为 **SeaweedFS** 的学习增强版，包含大量中文注释，旨在帮助理解 SeaweedFS 的架构与实现细节。

SeaweedFS 是一个高性能、易扩展的分布式文件系统，适合存储海量（十亿级）小文件，提供：

* O(1) 读取时间
* Facebook Haystack 设计 + f4 纠删码（Erasure Coding）实现
* S3 兼容 API、POSIX 接口、HTTP 原生接口
* 支持副本复制、纠删码、冷热分层存储

---

# 2. 构建与开发命令（Build Commands）

## 2.1 基础构建

```bash
cd weed && go install
```

## 2.2 构建所有可选功能

```bash
cd weed && go install -tags "elastic gocdk sqlite ydb tarantool tikv rclone"
```

## 2.3 启动开发服务器

```bash
make server
# 或
weed server -s3 -filer -volume.max=0 -master.volumeSizeLimitMB=100
```

## 2.4 测试

```bash
make test
cd weed && go test -v ./storage/...
cd weed && go test -v ./storage -run TestVolume
```

## 2.5 基准测试（Benchmark）

```bash
make benchmark
make benchmark_with_pprof
```

---

# 3. 架构概览（Architecture Overview）

SeaweedFS 采用三层架构设计：

## 3.1 Master Server (`weed/server/master_*`)

**核心职责**：Volume 元数据管理和文件 ID 分配

**关键概念**：
- 根据容量和副本策略将 volume 分配给 volume server
- 生成文件 ID (fid)，格式：`<volumeId>,<fileKey>[_cookie]`
- 维护拓扑结构（数据中心 → 机架 → 节点 → 卷）
- **不存储**文件元数据或内容

**入口文件**：[weed/command/master.go](weed/command/master.go)
**默认端口**：9333

## 3.2 Volume Server (`weed/server/volume_*`)

**核心职责**：使用 Needle 格式实际存储文件

**关键概念**：
- 每个 volume 是一个 32GB 的 append-only 文件，包含多个 Needle
- Needle = 基本存储单元（文件 + 元数据），仅 16 字节开销
- 支持三种索引类型：
  - **Memory**：最快，高内存使用
  - **LevelDB**：平衡性能/内存
  - **Sorted File**：最低内存，启动较慢
- 支持纠删码（EC）用于温数据存储

**入口文件**：[weed/command/volume.go](weed/command/volume.go)
**启动流程**：详见 `weed/command/volume.go:startVolumeServer` 的 4 个阶段注释
**默认端口**：8080
**读取流程**：详见 [weed/server/README_volume_server_read_flow.md](weed/server/README_volume_server_read_flow.md)

## 3.3 Filer Server (`weed/command/filer.go`)

**核心职责**：在对象存储之上提供类 POSIX 文件系统接口

**关键概念**：
- 在可插拔存储中存储目录树和文件元数据（MySQL、Postgres、Redis 等）
- 实际文件数据以分块形式存储在 volume 中
- 通过 chunk manifest 支持大文件
- 提供 S3/WebDAV/FUSE 接口

**默认端口**：8888

---

# 4. 关键数据结构（Key Data Structures）

## 4.1 Needle (weed/storage/needle/)

基本存储单元。布局随版本变化（v1/v2/v3）：

**版本 1**：Cookie(4) + Id(8) + Size(4) + Data(N) + Checksum(4)
**版本 2**：增加 Flags、可选的 Name/Mime/LastModified/TTL/Pairs
**版本 3**：增加 Timestamp(8) 记录追加时间

详细的二进制布局参见：[weed/storage/needle/README.md](weed/storage/needle/README.md)

## 4.2 Volume (weed/storage/volume.go)

- 单个 append-only 的 .dat 文件中包含多个 Needle
- 配套 .idx 文件用于快速查找（NeedleId → offset）
- SuperBlock（前 8 字节）存储版本、副本策略、TTL 等
- 详见：[weed/storage/super_block/](weed/storage/super_block/)

## 4.3 File ID (fid)

格式：`<volumeId>,<fileKey>[_cookie]`

- **volumeId**：32 位无符号整数，标识存储此文件的 volume
- **fileKey**：64 位无符号整数（十六进制），volume 内唯一
- **cookie**：32 位无符号整数（十六进制），防止 URL 猜测
- 示例：`3,01e3b0756f` 或 `3,01e3b0756f_a1b2c3d4`

---

# 5. 核心流程（Important Code Patterns）

## 5.1 文件上传流程

1. 客户端 → Master：请求 fid 分配 (`/dir/assign`)
2. Master：返回 `{fid, volumeId, url}` 指向可写 volume
3. 客户端 → Volume Server：将文件数据上传到 `url/fid`
4. Volume Server：将 Needle 追加到 volume .dat 文件，更新 .idx

详见：[LEARNING_GUIDE_API_FLOW.md](LEARNING_GUIDE_API_FLOW.md)

## 5.2 文件下载流程

1. 客户端 → Master：通过 volumeId 查询 volume 位置 (`/dir/lookup?volumeId=X`)
2. Master：返回 volume server URL 列表
3. 客户端 → Volume Server：GET `http://volumeServer/volumeId,fileKey`
4. Volume Server：使用 .idx 索引从 .dat 文件读取 Needle

**三种 ReadMode 选项**：
- `local`：仅读取本地数据，找不到返回 404
- `redirect`（默认）：返回 301 重定向到正确的 volume server
- `proxy`：代理请求到正确的 volume server

## 5.3 Needle 索引类型

通过启动 volume server 时的 `-index` 参数配置：

```bash
# Memory 索引（最快，最高内存使用）
weed volume -index=memory

# LevelDB 索引（平衡）
weed volume -index=leveldb

# LevelDB Medium/Large（更低内存，较慢）
weed volume -index=leveldbMedium
weed volume -index=leveldbLarge
```

实现位置：[weed/storage/needle_map.go](weed/storage/needle_map.go)

---

# 6. 已注释的核心文件（Annotated Core Files）

本仓库在关键文件中包含大量中文注释。修改时请遵循现有注释风格：

## 6.1 存储层 (Storage Layer)
- [weed/storage/needle_map.go](weed/storage/needle_map.go) - Needle 索引接口
- [weed/storage/super_block/super_block.go](weed/storage/super_block/super_block.go) - Volume 元数据
- [weed/storage/super_block/replica_placement.go](weed/storage/super_block/replica_placement.go) - 副本策略

## 6.2 操作层 (Operation Layer)
- [weed/operation/submit.go](weed/operation/submit.go) - 文件上传逻辑
- [weed/operation/lookup_vid_cache.go](weed/operation/lookup_vid_cache.go) - Volume 位置缓存

## 6.3 命令层 (Command Layer)
- [weed/command/volume.go](weed/command/volume.go) - Volume server 启动（详细的 4 阶段启动流程）
- [weed/command/command.go](weed/command/command.go) - 命令框架

---

# 7. 文档资源（Documentation Files）

本仓库中的关键学习资源：
- `LEARNING_GUIDE_API_FLOW.md` - 上传/下载/删除 API 流程
- `LEARNING_GUIDE_DOWNLOAD_DELETE_STORAGE.md` - 下载和删除详解
- `weed/server/README_volume_server_read_flow.md` - Volume server 读取内部机制
- `weed/storage/needle/README.md` - Needle 二进制格式
- `DESIGN.md` - 任务分发系统设计（EC、vacuum）
- `SSE-C_IMPLEMENTATION.md` - 服务端加密设计

---

# 8. 常见开发任务（Common Tasks）

## 8.1 添加新的存储后端
1. 在 `weed/storage/` 中实现 `NeedleMapper` 接口
2. 在 `weed/storage/needle_map.go` 工厂中注册
3. 在 `weed/command/volume.go` 中添加命令行参数

## 8.2 添加新的 Filer 存储
1. 在 `weed/filer/<storename>/` 创建新包
2. 实现 `FilerStore` 接口
3. 在 `weed/command/filer.go` 中注册

## 8.3 修改 Volume 启动逻辑
Volume 启动在 `weed/command/volume.go:startVolumeServer` 中有详细的 4 阶段注释：
1. 配置验证和解析
2. 服务组件创建（包含详细的 Volume 加载说明）
3. 服务启动（gRPC、HTTP）
4. 生命周期管理（重载、优雅关闭）

---

# 9. 测试说明（Testing Notes）

- 大多数集成测试需要运行 master/volume servers
- 测试数据目录已在 `.gitignore` 中（例如：`test_data/`、`filerldb2/`）
- EC（纠删码）测试：`docker/admin_integration/`
- S3 兼容性测试：`test/s3/`
- 在测试中使用 `-tags` 标志启用可选存储后端

---

# 10. Go 模块信息（Go Module Information）

- 模块名：`github.com/seaweedfs/seaweedfs`
- Go 版本：1.24.0+
- 主入口：`weed/main.go`

**关键依赖**：
- Raft 共识：`github.com/seaweedfs/raft`
- 云存储：`gocloud.dev`
- S3 SDK：`github.com/aws/aws-sdk-go`
- gRPC：`google.golang.org/grpc`

---

# 11. 中文注释规范（Code Comment Style）

为代码添加注释时：
- 使用中文进行详细解释（这是学习型 fork）
- 包含示例和使用说明
- 解释"为什么"而不只是"是什么"
- 记录参数、返回值和边界情况
- 引用相关文件和概念

**示例（来自现有代码）**：

```go
// Needle 索引映射接口
// 提供从 NeedleId 到磁盘位置的映射关系
// 不同实现有不同的内存/性能权衡:
//   - Memory: 全内存，性能最好，但内存占用高
//   - LevelDB: 基于 LSM-tree，内存占用低，性能适中
type NeedleMapper interface {
    Get(key NeedleId) (element *NeedleValue, ok bool)
    Put(key NeedleId, offset Offset, size Size) error
    Delete(key NeedleId) error
}
```

---

# 12. Git 提交规范（Git Workflow）

本仓库使用中文 commit 消息记录学习文档。最近的提交展示了这种模式：

```
为 SeaweedFS [模块名] 添加详细中文注释

🤖 Generated with [CodeX](https://openai.com/codex)

Co-Authored-By: CodeX <noreply@openai.com>
```

提交注释或文档改进时，请遵循此风格。

---

# 13. CodeX 工作规则（CodeX Working Rules）

> ⚠ 以下内容为 **CodeX 在此仓库中工作时必须遵守的规则**。

## 13.1 CodeX 的核心职责

本仓库是 **SeaweedFS 学习型仓库**，CodeX 的主要任务是：

* 为 SeaweedFS 源码添加详细的中文注释，帮助用户理解和学习
* 解释和回答与 SeaweedFS 代码相关的问题
* 创建学习文档和流程说明（如 LEARNING_GUIDE 系列）
* **不修改、不重构、不优化原有 Go 源码**（除非用户明确要求）
* 保持 SeaweedFS 原有代码逻辑完全不变

## 13.2 中文注释添加规范

### 13.2.1 注释语言
* **必须使用中文**进行所有代码注释
* 专业术语可保留英文，但需附加中文解释
* 示例：`// Needle 索引映射接口 (NeedleMapper interface)`

### 13.2.2 注释覆盖范围

**不仅要在函数/结构体头部添加注释，还必须在以下位置添加注释：**

1. **包级别注释**
   ```go
   // Package storage 实现了 SeaweedFS 的核心存储功能
   // 包含 Volume、Needle、索引等关键组件
   package storage
   ```

2. **结构体和接口**
   ```go
   // NeedleMapper 提供 Needle ID 到磁盘位置的映射
   // 是 SeaweedFS 索引系统的核心接口
   type NeedleMapper interface {
       // Get 根据 NeedleId 查找其在磁盘上的位置
       // 参数:
       //   - key: Needle 的唯一标识符
       // 返回:
       //   - element: Needle 的磁盘位置信息（offset + size）
       //   - ok: 是否找到该 Needle
       Get(key NeedleId) (element *NeedleValue, ok bool)
   }
   ```

3. **函数内部的重要步骤**
   ```go
   func startVolumeServer() {
       // 【阶段 1：配置验证和解析】
       // 解析存储目录配置，支持多目录格式: dir1,dir2,dir3
       locations := parseLocations(*volumeFolders)

       // 验证每个目录是否存在且可写
       for _, loc := range locations {
           // 检查目录权限，确保有读写权限
           if err := checkDirPermission(loc); err != nil {
               glog.Fatalf("目录 %s 权限检查失败: %v", loc, err)
           }
       }

       // 【阶段 2：创建服务组件】
       // 根据 -index 参数选择索引类型
       // memory: 全内存索引，速度最快但内存占用高
       // leveldb: LSM-tree 索引，平衡性能和内存
       indexType := chooseIndexType(*indexType)
   }
   ```

4. **重要变量和参数**
   ```go
   // volumeId 是卷的唯一标识符，32 位无符号整数
   // 取值范围：0 ~ 4,294,967,295
   volumeId := uint32(3)

   // fid 是文件 ID，格式：volumeId,fileKey[_cookie]
   // 例如：3,01e3b0756f 或 3,01e3b0756f_a1b2c3d4
   fid := fmt.Sprintf("%d,%x", volumeId, fileKey)
   ```

5. **关键函数调用**
   ```go
   // 从 Master 请求分配新的 Volume
   // 参数说明：
   //   - replication: 副本策略，如 "001" 表示同机架复制一次
   //   - collection: 集合名称，用于逻辑分组
   //   - dataCenter: 指定数据中心，为空则自动选择
   resp, err := operation.Assign(masterClient, &operation.VolumeAssignRequest{
       Replication: replication,
       Collection:  collection,
       DataCenter:  dataCenter,
   })
   ```

6. **复杂逻辑和算法**
   ```go
   // 计算 Needle 在文件中的实际偏移量
   // SeaweedFS 使用 8 字节对齐，所以需要计算 padding
   // 公式：actualOffset = SuperBlockSize + offset + padding
   actualOffset := int64(SuperBlockSize)  // 跳过 SuperBlock（8 字节）
   actualOffset += int64(offset) * NeedleEntrySize  // Needle 索引偏移

   // 计算对齐 padding，确保 8 字节边界
   // 例如：size=13 时，padding = (8 - 13%8) % 8 = 3
   padding := (NeedlePaddingSize - actualOffset%NeedlePaddingSize) % NeedlePaddingSize
   actualOffset += padding
   ```

7. **错误处理和边界情况**
   ```go
   // 读取 Needle 数据
   n, err := volume.ReadNeedle(needleId)
   if err != nil {
       // 可能的错误情况：
       // 1. Needle 不存在（已删除或从未创建）
       // 2. 磁盘 I/O 错误
       // 3. 数据损坏（CRC 校验失败）
       if err == ErrorNotFound {
           return nil, fmt.Errorf("Needle %d 不存在", needleId)
       }
       return nil, fmt.Errorf("读取失败: %v", err)
   }
   ```

### 13.2.3 注释详细程度

* **简单代码**：一行注释说明目的
* **中等复杂**：多行注释解释逻辑 + 参数说明
* **复杂逻辑**：分步注释 + 原理说明 + 示例 + 边界情况

**示例（复杂函数）**：
```go
// parseReplicaPlacement 解析副本放置策略字符串
// SeaweedFS 使用三位数字表示副本策略：XYZ
//   - X: 不同数据中心的副本数
//   - Y: 不同机架的副本数（同数据中心）
//   - Z: 不同服务器的副本数（同机架）
//
// 示例：
//   - "000": 无副本
//   - "001": 同机架不同服务器 1 个副本
//   - "010": 同数据中心不同机架 1 个副本
//   - "100": 不同数据中心 1 个副本
//   - "200": 不同数据中心 2 个副本
//
// 参数:
//   - rp: 副本策略字符串，必须是 3 位数字
// 返回:
//   - *ReplicaPlacement: 解析后的副本策略对象
//   - error: 格式错误时返回
func parseReplicaPlacement(rp string) (*ReplicaPlacement, error) {
    // 验证格式：必须是 3 位数字
    if len(rp) != 3 {
        return nil, fmt.Errorf("副本策略必须是 3 位数字，当前: %s", rp)
    }

    // 解析每一位数字
    // rp[0] - 数据中心级别副本数（X）
    dataCenterCount := int(rp[0] - '0')
    // rp[1] - 机架级别副本数（Y）
    rackCount := int(rp[1] - '0')
    // rp[2] - 服务器级别副本数（Z）
    serverCount := int(rp[2] - '0')

    // 边界检查：每个级别副本数不能超过 9
    if dataCenterCount > 9 || rackCount > 9 || serverCount > 9 {
        return nil, fmt.Errorf("副本数不能超过 9")
    }

    return &ReplicaPlacement{
        DataCenterCount: dataCenterCount,
        RackCount:       rackCount,
        ServerCount:     serverCount,
    }, nil
}
```

## 13.3 交互规范

为代码添加注释时，CodeX 必须：

* **明确说明**要修改的文件完整路径
* **解释原因**：为什么要添加这些注释，涵盖哪些知识点
* **分块展示**：如果同时修改多个文件，按文件或模块分组说明
* **可直接使用**：生成的代码可直接复制到项目中，无需调整格式

## 13.4 文档创建规则

* 学习文档放入项目根目录，使用中文，命名格式：`LEARNING_GUIDE_*.md`
* 流程图和架构说明文档也放在根目录
* 不创建无关目录或文件
* 所有新文档需要在本 `AGENTS.md` 的第 7 章"文档资源"中更新

## 13.5 安全原则

* **不修改**原有代码逻辑和功能
* **不添加**新的功能代码（除非用户明确要求）
* **不重构**现有代码结构
* **不破坏**编译和运行（添加的注释不能导致语法错误）
* 遵循 Go 语言注释规范（`//` 单行注释，`/* */` 多行注释）

## 13.6 使用方法

将 `AGENTS.md` 放入仓库根目录后：

* CodeX 会自动读取此文件作为工作指南
* 所有代码注释和文档创建会按照上述规则执行
* 用户无需额外配置，直接提出需求即可


