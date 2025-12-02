# PostgreSQL 线协议包（Wire Protocol Package）

本包为 SeaweedFS 实现了 PostgreSQL 线协议支持，使 SeaweedFS 能够与任何 PostgreSQL 客户端、工具和应用程序实现通用兼容。

## 包结构

```
weed/server/postgres/
├── README.md           # 英文文档
├── README_zh.md        # 本文档（中文文档）
├── server.go          # PostgreSQL 服务器主实现
├── protocol.go        # 线协议消息处理器（集成 MQ）
├── DESIGN.md          # 架构和设计文档
└── IMPLEMENTATION.md  # 完整实现指南
```

## 核心组件

### `server.go`
- **PostgreSQLServer**: 主服务器结构，负责连接管理
- **PostgreSQLSession**: 单个客户端会话处理
- **PostgreSQLServerConfig**: 服务器配置选项
- **认证系统**: 支持 Trust、Password、MD5 认证
- **TLS 支持**: 支持自定义证书的加密连接
- **连接池**: 资源管理和自动清理

### `protocol.go`
- **线协议实现**: 完整的 PostgreSQL 3.0 协议支持
- **消息处理器**: 处理启动、查询、parse/bind/execute 序列
- **响应生成**: 行描述、数据行、命令完成
- **数据类型映射**: SeaweedFS 类型到 PostgreSQL 类型的转换
- **SQL 解析器**: 使用 PostgreSQL 原生解析器实现完整方言兼容性
- **错误处理**: 符合 PostgreSQL 规范的错误响应
- **MQ 集成**: 直接集成 SeaweedFS SQL 引擎访问真实 Topic 数据
- **系统查询支持**: 支持基本的 PostgreSQL 系统查询（version、current_user 等）
- **数据库上下文**: 基于会话的数据库切换，支持 USE 命令

## 主要特性

### 真实 MQ Topic 集成
PostgreSQL 服务器现在直接集成 SeaweedFS 消息队列 Topic，提供：

- **实时 Topic 发现**: 自动从 Filer 发现 MQ 命名空间和 Topic
- **真实 Schema 信息**: 从 Broker 配置读取真实的 Topic Schema
- **实际数据访问**: 查询存储在 Parquet 和日志文件中的真实 MQ 数据
- **动态更新**: 自动反映 Topic 添加和 Schema 变更
- **一致的 SQL 引擎**: 与 `weed sql` 命令使用相同的 SQL 引擎

### 数据库上下文管理
- **会话隔离**: 每个 PostgreSQL 连接都有自己的数据库上下文
- **USE 命令支持**: 使用标准 `USE database` 语法在命名空间之间切换
- **自动发现**: Topic 在首次访问时被发现和注册
- **Schema 缓存**: 高效缓存 Topic Schema 和元数据

## 使用方法

### 导入包
```go
import "github.com/seaweedfs/seaweedfs/weed/server/postgres"
```

### 创建并启动服务器
```go
config := &postgres.PostgreSQLServerConfig{
    Host:        "localhost",
    Port:        5432,
    AuthMethod:  postgres.AuthMD5,
    Users:       map[string]string{"admin": "secret"},
    Database:    "default",
    MaxConns:    100,
    IdleTimeout: time.Hour,
}

server, err := postgres.NewPostgreSQLServer(config, "localhost:9333")
if err != nil {
    return err
}

err = server.Start()
if err != nil {
    return err
}

// 服务器现在开始接受 PostgreSQL 连接
```

## 认证方式

本包支持三种认证方式：

### Trust 认证（信任认证）
```go
AuthMethod: postgres.AuthTrust
```
- 无需密码
- 适用于开发/测试环境
- 不推荐用于生产环境

### Password 认证（密码认证）
```go
AuthMethod: postgres.AuthPassword,
Users: map[string]string{"user": "password"}
```
- 明文密码传输
- 简单但安全性较低
- 生产环境需要配合 TLS 使用

### MD5 认证（MD5 哈希认证）
```go
AuthMethod: postgres.AuthMD5,
Users: map[string]string{"user": "password"}
```
- 使用盐值的安全哈希认证
- **推荐用于生产环境**
- 与所有 PostgreSQL 客户端兼容

## TLS 配置

启用 TLS 加密以实现安全连接：

```go
cert, err := tls.LoadX509KeyPair("server.crt", "server.key")
if err != nil {
    return err
}

config.TLSConfig = &tls.Config{
    Certificates: []tls.Certificate{cert},
}
```

## 客户端兼容性

本实现兼容以下工具和库：

### 命令行工具
- `psql` - PostgreSQL 命令行客户端
- `pgcli` - 增强型命令行工具，支持自动补全
- 数据库 IDE（DataGrip、DBeaver）

### 编程语言库
- **Python**: psycopg2、asyncpg
- **Java**: PostgreSQL JDBC 驱动
- **JavaScript**: pg (node-postgres)
- **Go**: lib/pq、pgx
- **.NET**: Npgsql
- **PHP**: pdo_pgsql
- **Ruby**: pg gem

### BI 工具
- Tableau（原生 PostgreSQL 连接器）
- Power BI（PostgreSQL 数据源）
- Grafana（PostgreSQL 插件）
- Apache Superset

## 支持的 SQL 操作

### 数据查询
```sql
SELECT * FROM topic_name;
SELECT id, message FROM topic_name WHERE condition;
SELECT COUNT(*) FROM topic_name;
SELECT MIN(id), MAX(id), AVG(amount) FROM topic_name;
```

### Schema 信息
```sql
SHOW DATABASES;
SHOW TABLES;
DESCRIBE topic_name;
DESC topic_name;
```

### 系统信息
```sql
SELECT version();
SELECT current_database();
SELECT current_user;
```

### 系统列
```sql
SELECT id, message, _timestamp_ns, _key, _source FROM topic_name;
```

## 配置选项

### 服务器配置
- **Host/Port**: 服务器绑定地址和端口
- **Authentication**: 认证方式和用户凭据
- **Database**: 默认数据库/命名空间名称
- **Connections**: 最大并发连接数
- **Timeouts**: 空闲连接超时时间
- **TLS**: 证书和加密设置

### 性能调优
- **连接限制**: 防止资源耗尽
- **空闲超时**: 自动清理未使用的连接
- **内存管理**: 高效的会话处理
- **查询流式传输**: 支持大结果集

## 错误处理

本包提供符合 PostgreSQL 规范的错误响应：

- **连接错误**: 认证失败、网络问题
- **SQL 错误**: 语法无效、表不存在
- **资源错误**: 连接限制、超时
- **安全错误**: 权限拒绝、凭据无效

## 开发和测试

### 单元测试
运行 PostgreSQL 包测试：
```bash
go test ./weed/server/postgres
```

### 集成测试
使用提供的 Python 测试客户端：
```bash
python postgres-examples/test_client.py --host localhost --port 5432
```

### 手动测试
使用 psql 连接：
```bash
psql -h localhost -p 5432 -U seaweedfs -d default
```

## 文档资源

- **DESIGN.md**: 完整的架构和设计概述
- **IMPLEMENTATION.md**: 详细实现指南
- **postgres-examples/**: 客户端示例和测试脚本
- **命令文档**: `weed db -help`

## 安全考虑

### 生产部署
- 使用 MD5 或更强的认证方式
- 启用 TLS 加密
- 配置适当的连接限制
- 监控可疑活动
- 使用强密码
- 实施适当的防火墙规则

### 访问控制
- 创建专用只读用户
- 使用最小权限原则
- 监控连接模式
- 记录认证尝试

## 架构说明

### SQL 解析器方言考虑

**✅ 仅支持 POSTGRESQL**: SeaweedFS SQL 引擎专门支持 PostgreSQL 语法：

- **✅ 核心引擎**: `engine.go` 使用自定义 PostgreSQL 解析器实现适当的方言支持
- **PostgreSQL 服务器**: 使用 PostgreSQL 解析器以实现最佳线协议兼容性
- **解析器**: 自定义轻量级 PostgreSQL 解析器，完全兼容 PostgreSQL
- **支持状态**: 仅支持 PostgreSQL 语法 - 已移除 MySQL 解析

**PostgreSQL 解析器的主要优势**：
- **原生方言支持**: 正确处理 PostgreSQL 特定的语法和语义
- **系统目录兼容性**: 支持 `pg_catalog`、`information_schema` 查询
- **运算符兼容性**: 处理 `||` 字符串连接等 PostgreSQL 特定运算符
- **类型系统对齐**: 更好的 PostgreSQL 类型推断和强制转换
- **减少转换开销**: 消除了方言转换层的需要

**PostgreSQL 语法支持**：
- **标识符引用**: 使用 PostgreSQL 双引号（`"`）引用标识符
- **字符串连接**: 支持 PostgreSQL `||` 运算符
- **系统函数**: 完全支持 PostgreSQL 系统目录（`pg_catalog`）和函数
- **标准合规性**: 遵循 PostgreSQL SQL 标准和方言

**实现特性**：
- `protocol.go` 中的原生 PostgreSQL 查询处理
- 系统查询支持（`SELECT version()`、`BEGIN` 等）
- PostgreSQL 和 SeaweedFS Schema 类型之间的类型映射
- 错误代码映射到 PostgreSQL 标准
- 全面的 PostgreSQL 线协议支持

本包提供企业级 PostgreSQL 兼容性，使 SeaweedFS 能够无缝集成到整个 PostgreSQL 生态系统中。
