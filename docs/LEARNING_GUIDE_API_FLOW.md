# SeaweedFS API 流程详解

本文档详细说明 SeaweedFS 的主要 API 请求流程，帮助理解代码执行路径。

## 📋 目录

1. [文件上传流程](#文件上传流程)
2. [文件下载流程](#文件下载流程)
3. [文件删除流程](#文件删除流程)
4. [关键数据结构](#关键数据结构)
5. [已注释的源文件](#已注释的源文件)

---

## 文件上传流程

### API 示例
```bash
curl -F file=@/etc/hosts "http://127.0.0.1:9333/submit"
```

### 完整流程图

```
客户端 curl
    │
    ▼
Master Server (localhost:9333)
    │
    ├─ HTTP POST /submit
    │   └─ weed/server/master_server.go:173 (路由注册)
    │       └─ submitFromMasterServerHandler()
    │           └─ weed/server/master_server_handlers_admin.go:143
    │
    ▼
submitForClientHandler()  # 上传核心处理
    │   └─ weed/server/common.go:157
    │
    ├─ 1️⃣ 解析上传文件
    │   └─ needle.ParseUpload(r, 256MB, buffer)
    │       ├─ 读取 multipart/form-data
    │       ├─ 提取文件名、MIME 类型
    │       └─ 读取文件数据到内存
    │
    ├─ 2️⃣ 向 Master 请求分配文件 ID
    │   └─ operation.Assign(ctx, masterFn, grpcDialOption, request)
    │       ├─ Request: collection, replication, ttl, diskType等
    │       └─ Response: fid="3,01e3b0756f", url="localhost:8080"
    │           ├─ fid 格式: <volume_id>,<file_key>
    │           ├─ volume_id=3 (卷ID)
    │           └─ file_key=01e3b0756f (文件键)
    │
    ├─ 3️⃣ 上传文件到 Volume Server
    │   └─ uploader.UploadData(ctx, data, uploadOption)
    │       └─ HTTP POST http://localhost:8080/3,01e3b0756f
    │           └─ Volume Server 接收并存储
    │
    └─ 4️⃣ 返回结果给客户端
        └─ JSON Response:
            {
              "fileName": "hosts",
              "fid": "3,01e3b0756f",
              "fileUrl": "http://localhost:8080/3,01e3b0756f",
              "size": 1024,
              "eTag": "..."
            }
```

### 关键代码路径

| 文件 | 函数 | 行号 | 说明 |
|------|------|------|------|
| `weed/server/master_server.go` | 路由注册 | 173 | 注册 `/submit` 路由 |
| `weed/server/master_server_handlers_admin.go` | `submitFromMasterServerHandler` | 143 | 转发给 Leader |
| `weed/server/common.go` | `submitForClientHandler` | 132 | 上传核心逻辑 |
| `weed/storage/needle/parse_upload.go` | `ParseUpload` | - | 解析上传文件 |
| `weed/operation/submit.go` | `Assign` | - | 请求分配 fid |
| `weed/operation/upload_content.go` | `UploadData` | - | 上传到 Volume |

---

## 文件下载流程

### API 示例
```bash
# 方式1: 直接通过 Volume Server (推荐，性能最好)
curl "http://127.0.0.1:8080/3,01e3b0756f" -o downloaded_file

# 方式2: 通过 Master Server (会重定向到 Volume Server)
curl "http://127.0.0.1:9333/3,01e3b0756f" -o downloaded_file

# 方式3: 通过 Filer (支持文件系统路径)
curl "http://127.0.0.1:8888/path/to/file.txt" -o downloaded_file
```

### 流程图（Volume Server 直接访问）

```
客户端 curl
    │
    ▼
Volume Server (localhost:8080)
    │
    ├─ HTTP GET /3,01e3b0756f
    │   └─ weed/server/volume_server.go (路由注册)
    │       └─ GetOrHeadHandler()
    │
    ├─ 1️⃣ 解析 URL 路径
    │   └─ parseURLPath("/3,01e3b0756f")
    │       ├─ vid = "3"          # 卷 ID
    │       ├─ fid = "01e3b0756f" # 文件键
    │       └─ weed/server/common.go:281
    │
    ├─ 2️⃣ 从磁盘读取文件
    │   └─ storage.ReadNeedle(volumeId, needleId)
    │       ├─ 打开卷文件: /data/3.dat
    │       ├─ 根据索引找到文件位置
    │       └─ 读取文件数据
    │
    └─ 3️⃣ 返回文件给客户端
        └─ HTTP Response:
            ├─ Content-Type: image/jpeg
            ├─ Content-Length: 1024
            └─ Body: [文件数据]
```

### fid (文件ID) 详解

fid 是 SeaweedFS 中文件的唯一标识符，格式为：`<volume_id>,<file_key>[,<file_cookie>][.ext]`

**示例：** `3,01e3b0756f.jpg`
- `3` - Volume ID（卷ID）：文件存储在哪个卷上
- `01e3b0756f` - File Key（文件键）：文件在卷内的唯一标识
- `.jpg` - Extension（扩展名）：可选

**为什么这样设计？**
1. **快速定位**：通过 volume_id 可以直接找到存储文件的 Volume Server
2. **索引高效**：file_key 用于在卷内快速查找文件位置
3. **分布式友好**：不同 Volume Server 可以独立生成 file_key，无需中心化协调

---

## 文件删除流程

### API 示例
```bash
# 方式1: 直接向 Volume Server 发送 DELETE 请求
curl -X DELETE "http://127.0.0.1:8080/3,01e3b0756f"

# 方式2: 通过 Filer 路径删除
curl -X DELETE "http://127.0.0.1:8888/path/to/file.txt"
```

### 流程图（Volume Server 直接删除）

```
客户端 curl
    │
    ▼
Volume Server (localhost:8080)
    │
    ├─ HTTP DELETE /3,01e3b0756f
    │   └─ DeleteHandler()
    │
    ├─ 1️⃣ 解析 fid
    │   └─ vid=3, fid=01e3b0756f
    │
    ├─ 2️⃣ 标记文件为删除
    │   └─ storage.DeleteNeedle(volumeId, needleId)
    │       ├─ 更新索引（标记为已删除）
    │       └─ 实际数据暂不删除（等待压缩）
    │
    └─ 3️⃣ 返回 202 Accepted
        └─ {"size": 28}
```

**注意：**
- 删除操作是**标记删除**，不会立即回收磁盘空间
- 需要运行 `vacuum` 命令来压缩卷并回收空间
- `vacuum` 会创建新卷，复制未删除的文件，然后删除旧卷

---

## 关键数据结构

### 1. Volume（卷）

```go
// 卷是存储文件的基本单位
// 每个卷包含：
// - .dat 文件：实际存储文件数据
// - .idx 文件：文件索引（fid -> offset）
type Volume struct {
    Id            VolumeId    // 卷 ID
    dir           string      // 存储目录
    dataFile      *os.File    // .dat 文件
    nm            NeedleMapper // 索引（内存或 LevelDB）
    ReplicaPlacement *ReplicaPlacement // 复制策略
}
```

### 2. Needle（针）

```go
// Needle 是 SeaweedFS 中存储文件的基本单位
// 一个卷文件 (.dat) 包含多个 Needle
type Needle struct {
    Id          NeedleId  // 文件键（从 fid 解析）
    Size        uint32    // 文件大小
    DataSize    uint32    // 数据大小
    Data        []byte    // 文件数据
    Flags       byte      // 标志位（是否删除、是否压缩等）
    NameSize    uint8     // 文件名长度
    Name        []byte    // 文件名
    MimeSize    uint8     // MIME 类型长度
    Mime        []byte    // MIME 类型
}
```

### 3. FID 解析

```go
// parseURLPath 解析 URL 中的 fid
// 输入: "/3,01e3b0756f.jpg"
// 输出:
//   vid = "3"          // 卷 ID
//   fid = "01e3b0756f" // 文件键
//   ext = ".jpg"       // 扩展名
```

---

## 已注释的源文件

### 命令行入口
- ✅ `weed/weed.go` - 主程序入口
- ✅ `weed/command/command.go` - 命令框架
- ✅ `weed/command/master.go` - Master 启动命令
- ✅ `weed/command/volume.go` - Volume 启动命令
- ✅ `weed/command/filer.go` - Filer 启动命令
- ✅ `weed/command/s3.go` - S3 启动命令

### 服务器实现（上传流程）
- ✅ `weed/server/common.go` - 通用工具函数
  - `submitForClientHandler()` - 上传核心逻辑
  - `parseURLPath()` - URL 路径解析
- ✅ `weed/server/master_server_handlers_admin.go` - Master 管理 API
  - `submitFromMasterServerHandler()` - /submit 处理

### 下一步建议注释的文件

**文件下载流程：**
- `weed/server/volume_server_handlers_read.go` - Volume Server 读取处理
- `weed/server/filer_server_handlers_read.go` - Filer 读取处理
- `weed/storage/needle_read.go` - Needle 读取逻辑

**文件删除流程：**
- `weed/server/volume_server_handlers_write.go` - Volume Server 写入/删除处理
- `weed/storage/needle_delete.go` - Needle 删除逻辑
- `weed/server/volume_server_handlers_admin.go` - Vacuum 压缩逻辑

**存储引擎核心：**
- `weed/storage/volume.go` - 卷的核心实现
- `weed/storage/needle.go` - Needle 数据结构
- `weed/storage/store.go` - 存储管理器

---

## 快速测试

### 1. 启动 SeaweedFS（开发模式）
```bash
# 一键启动 master + volume + filer
weed server -dir=/tmp/data
```

### 2. 上传文件
```bash
# 上传文件并获取 fid
curl -F file=@/etc/hosts "http://localhost:9333/submit" | jq .

# 响应示例：
# {
#   "fileName": "hosts",
#   "fid": "3,01e3b0756f",
#   "fileUrl": "http://localhost:8080/3,01e3b0756f",
#   "size": 1024
# }
```

### 3. 下载文件
```bash
# 使用返回的 fid 下载
curl "http://localhost:8080/3,01e3b0756f" -o downloaded_hosts

# 验证下载的文件
diff /etc/hosts downloaded_hosts
```

### 4. 删除文件
```bash
# 删除文件（仅标记）
curl -X DELETE "http://localhost:8080/3,01e3b0756f"

# 压缩卷以回收空间
curl "http://localhost:9333/vol/vacuum?garbageThreshold=0.3"
```

---

## 学习建议

### 阶段 1：理解整体流程（已完成）
1. ✅ 阅读已注释的命令行入口文件
2. ✅ 理解上传 API 的完整流程
3. ✅ 掌握 fid 的结构和解析

### 阶段 2：深入存储引擎（进行中）
1. 学习 Volume 的实现
2. 理解 Needle 的存储格式
3. 研究索引机制（内存 vs LevelDB）

### 阶段 3：高级特性
1. 复制机制（Replication）
2. 垃圾回收和压缩（Vacuum & Compaction）
3. 集群拓扑管理（Topology）

---

## 常见问题

### Q1: 为什么上传需要两步（Assign + Upload）？
**A:** 分离文件ID分配和数据存储有以下好处：
1. **灵活性**：客户端可以先获取 fid，然后选择合适的时机上传
2. **批量操作**：可以一次分配多个 fid，用于批量上传
3. **负载均衡**：Master 可以根据负载情况分配到不同的 Volume Server

### Q2: 删除文件为什么不立即回收空间？
**A:** 延迟回收有以下原因：
1. **性能**：标记删除比实际删除快得多
2. **批量处理**：Vacuum 可以批量处理多个删除，减少 IO
3. **一致性**：避免频繁的文件移动和索引更新

### Q3: fid 中的 file_key 如何生成？
**A:** file_key 由 Volume Server 生成，通常是一个递增的序列号（用于保证唯一性）

---

## 调试技巧

### 1. 启用详细日志
```bash
# 启动时指定日志级别
weed server -dir=/tmp/data -v=4
```

### 2. 查看卷信息
```bash
# 查看所有卷的状态
curl "http://localhost:9333/dir/status" | jq .

# 查看特定卷
curl "http://localhost:8080/status?pretty=y"
```

### 3. 查看文件元数据
```bash
# 获取文件的元数据
curl -I "http://localhost:8080/3,01e3b0756f"
```

---

## 总结

通过本指南，你应该已经理解了：
1. ✅ SeaweedFS 的文件上传完整流程
2. ✅ fid 的结构和作用
3. ✅ 如何通过源码追踪 API 请求
4. ✅ 主要数据结构（Volume、Needle）

下一步建议深入学习**存储引擎**部分，理解文件如何在磁盘上存储和索引。
