// Package postgres 实现了 PostgreSQL 线协议（Wire Protocol）服务器
//
// 这个包使 SeaweedFS 能够通过标准 PostgreSQL 协议接受连接和查询，
// 允许任何 PostgreSQL 客户端（psql、DataGrip、Python psycopg2 等）直接访问 SeaweedFS 数据。
//
// 核心功能：
//   1. 完整的 PostgreSQL 3.0 线协议实现
//   2. 多种认证方式支持（Trust、Password、MD5）
//   3. TLS 加密连接支持
//   4. 会话管理和连接池
//   5. SQL 查询执行（通过 SeaweedFS SQL 引擎）
//   6. 类型映射（SeaweedFS 类型 ↔ PostgreSQL 类型）
//
// 架构设计：
//   ┌─────────────┐        ┌──────────────────┐        ┌────────────────┐
//   │ PG Clients  │───────>│ PostgreSQL Server│───────>│ SQL Engine     │
//   │ (psql, etc) │<───────│ (Wire Protocol)  │<───────│ (Query Parser) │
//   └─────────────┘        └──────────────────┘        └────────────────┘
//                                    │
//                                    ↓
//                          ┌──────────────────┐
//                          │  SeaweedFS Filer │
//                          │  (Topic Data)    │
//                          └──────────────────┘
//
// 使用场景：
//   - 使用 BI 工具（Tableau、Power BI）访问 SeaweedFS 数据
//   - 使用 PostgreSQL 客户端查询 MQ Topic 数据
//   - 在应用中通过标准 PostgreSQL 驱动访问 SeaweedFS
//   - 将 SeaweedFS 集成到现有 PostgreSQL 生态系统
//
// 协议兼容性：
//   - PostgreSQL 3.0 协议（与 PostgreSQL 14+ 兼容）
//   - 支持简单查询和扩展查询协议
//   - 支持 Prepared Statements 和 Portals
//   - 支持事务命令（BEGIN、COMMIT、ROLLBACK）
//
// 详细文档请参考：
//   - README.md: 使用指南和配置说明
//   - README_zh.md: 中文使用指南
//   - protocol.go: 协议消息处理实现
package postgres

import (
	"bufio"
	"crypto/md5"
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/query/engine"
	"github.com/seaweedfs/seaweedfs/weed/util/version"
)

// ═══════════════════════════════════════════════════════════════════════════
// PostgreSQL 协议常量定义
// ═══════════════════════════════════════════════════════════════════════════

// PostgreSQL protocol constants
const (
	// ─── 协议版本 ───────────────────────────────────────────────────────────

	// PG_PROTOCOL_VERSION_3 是 PostgreSQL 3.0 协议版本号
	// 二进制值：0x00030000（十进制 196608）
	// 协议版本在客户端启动消息的前 4 个字节中发送
	PG_PROTOCOL_VERSION_3 = 196608

	// PG_SSL_REQUEST 是 SSL 加密请求的特殊版本号
	// 二进制值：0x04d2162f（十进制 80877103）
	// 客户端发送此值请求建立 TLS 加密连接
	PG_SSL_REQUEST = 80877103

	// PG_GSSAPI_REQUEST 是 GSSAPI 认证请求的特殊版本号
	// 二进制值：0x04d21630（十进制 80877104）
	// 用于 Kerberos 等企业级认证（当前实现不支持）
	PG_GSSAPI_REQUEST = 80877104

	// ─── 客户端消息类型（Client → Server）────────────────────────────────

	PG_MSG_STARTUP   = 0x00 // 启动消息（无类型字节，仅有长度 + 协议版本）
	PG_MSG_QUERY     = 'Q'  // 简单查询消息（包含 SQL 语句）
	PG_MSG_PARSE     = 'P'  // 解析消息（创建 Prepared Statement）
	PG_MSG_BIND      = 'B'  // 绑定消息（绑定参数到 Prepared Statement）
	PG_MSG_EXECUTE   = 'E'  // 执行消息（执行已绑定的 Portal）
	PG_MSG_DESCRIBE  = 'D'  // 描述消息（获取 Statement 或 Portal 的元数据）
	PG_MSG_CLOSE     = 'C'  // 关闭消息（关闭 Statement 或 Portal）
	PG_MSG_FLUSH     = 'H'  // 刷新消息（强制发送缓冲的响应）
	PG_MSG_SYNC      = 'S'  // 同步消息（结束扩展查询协议流程）
	PG_MSG_TERMINATE = 'X'  // 终止消息（客户端断开连接）
	PG_MSG_PASSWORD  = 'p'  // 密码消息（响应认证请求）

	// ─── 服务器响应类型（Server → Client）────────────────────────────────

	PG_RESP_AUTH_OK        = 'R' // 认证响应（OK 或请求密码）
	PG_RESP_BACKEND_KEY    = 'K' // 后端密钥数据（用于取消请求）
	PG_RESP_PARAMETER      = 'S' // 参数状态（server_version 等）
	PG_RESP_READY          = 'Z' // 就绪响应（准备接受新查询）
	PG_RESP_COMMAND        = 'C' // 命令完成（包含命令标签，如 "SELECT 10"）
	PG_RESP_DATA_ROW       = 'D' // 数据行（查询结果的一行）
	PG_RESP_ROW_DESC       = 'T' // 行描述（列名和类型信息）
	PG_RESP_PARSE_COMPLETE = '1' // 解析完成
	PG_RESP_BIND_COMPLETE  = '2' // 绑定完成
	PG_RESP_CLOSE_COMPLETE = '3' // 关闭完成
	PG_RESP_ERROR          = 'E' // 错误消息
	PG_RESP_NOTICE         = 'N' // 通知消息（警告等）

	// ─── 事务状态指示符 ─────────────────────────────────────────────────

	PG_TRANS_IDLE    = 'I' // 空闲状态（无活动事务）
	PG_TRANS_INTRANS = 'T' // 事务中（有活动事务）
	PG_TRANS_ERROR   = 'E' // 错误状态（事务失败，需要 ROLLBACK）

	// ─── 认证方式代码 ───────────────────────────────────────────────────

	AUTH_OK    = 0  // 认证成功
	AUTH_CLEAR = 3  // 明文密码认证
	AUTH_MD5   = 5  // MD5 密码认证（带盐值）
	AUTH_TRUST = 10 // 信任认证（无需密码）

	// ─── PostgreSQL 数据类型 OID（Object Identifier）─────────────────────
	// 这些 OID 对应 PostgreSQL 的 pg_type 系统表中的类型定义

	PG_TYPE_BOOL      = 16   // boolean（布尔值）
	PG_TYPE_BYTEA     = 17   // bytea（字节数组）
	PG_TYPE_INT8      = 20   // bigint（64 位整数）
	PG_TYPE_INT4      = 23   // integer（32 位整数）
	PG_TYPE_TEXT      = 25   // text（文本，无长度限制）
	PG_TYPE_FLOAT4    = 700  // real（32 位浮点数）
	PG_TYPE_FLOAT8    = 701  // double precision（64 位浮点数）
	PG_TYPE_VARCHAR   = 1043 // varchar（可变长度字符串）
	PG_TYPE_TIMESTAMP = 1114 // timestamp（时间戳）
	PG_TYPE_JSON      = 114  // json（JSON 文本格式）
	PG_TYPE_JSONB     = 3802 // jsonb（JSON 二进制格式）

	// ─── 默认配置值 ─────────────────────────────────────────────────────

	DEFAULT_POSTGRES_PORT = 5432 // PostgreSQL 标准端口
)

// ═══════════════════════════════════════════════════════════════════════════
// 认证方法类型定义
// ═══════════════════════════════════════════════════════════════════════════

// AuthMethod 定义 PostgreSQL 服务器支持的认证方式
type AuthMethod int

const (
	// AuthTrust 信任认证 - 无需密码
	// 适用场景：
	//   - 本地开发环境
	//   - 内网测试环境
	// 安全性：低（任何人都可以连接）
	AuthTrust AuthMethod = iota

	// AuthPassword 明文密码认证
	// 适用场景：
	//   - 配合 TLS 使用时的生产环境
	// 安全性：中（需要 TLS 保护，否则密码会被明文传输）
	AuthPassword

	// AuthMD5 MD5 哈希密码认证
	// 适用场景：
	//   - 推荐用于生产环境（即使没有 TLS）
	// 安全性：高（密码经过 MD5 + 盐值哈希，不会明文传输）
	// 哈希算法：md5(md5(password + username) + salt)
	AuthMD5
)

// ═══════════════════════════════════════════════════════════════════════════
// PostgreSQL 服务器配置
// ═══════════════════════════════════════════════════════════════════════════

// PostgreSQLServerConfig 定义 PostgreSQL 服务器的配置参数
type PostgreSQLServerConfig struct {
	// Host 是服务器监听的主机地址
	// 示例：
	//   - "localhost" - 仅本地访问
	//   - "0.0.0.0" - 允许所有网络接口
	//   - "192.168.1.100" - 绑定到特定 IP
	Host string

	// Port 是服务器监听的端口号
	// 默认值：5432（PostgreSQL 标准端口）
	// 生产环境建议使用标准端口以兼容各种客户端工具
	Port int

	// AuthMethod 是认证方式
	// 可选值：AuthTrust、AuthPassword、AuthMD5
	// 生产环境推荐：AuthMD5
	AuthMethod AuthMethod

	// Users 是用户名到密码的映射表
	// 仅在 AuthPassword 或 AuthMD5 模式下使用
	// 示例：map[string]string{"admin": "secret", "readonly": "readonly123"}
	Users map[string]string

	// TLSConfig 是 TLS 加密配置（可选）
	// 如果设置，服务器将支持 TLS 加密连接
	// 生产环境强烈推荐启用 TLS
	TLSConfig *tls.Config

	// MaxConns 是最大并发连接数
	// 用于防止资源耗尽
	// 默认值：100
	// 建议根据服务器资源调整（每个连接约占用 100KB-1MB 内存）
	MaxConns int

	// IdleTimeout 是空闲连接超时时间
	// 超过此时间无活动的连接将被自动关闭
	// 默认值：1 小时
	// 建议值：30 分钟到 2 小时
	IdleTimeout time.Duration

	// StartupTimeout 是客户端启动握手的超时时间
	// 如果客户端在此时间内未完成认证，连接将被关闭
	// 默认值：30 秒
	// 用于防止慢速攻击和挂起连接
	StartupTimeout time.Duration

	// Database 是默认数据库名称
	// 对应 SeaweedFS 的命名空间（Namespace）
	// 客户端可以在连接时指定数据库，或使用 USE 命令切换
	Database string
}

// ═══════════════════════════════════════════════════════════════════════════
// PostgreSQL 服务器主结构
// ═══════════════════════════════════════════════════════════════════════════

// PostgreSQLServer 是 PostgreSQL 协议服务器的主结构
// 负责接受连接、管理会话、执行查询
type PostgreSQLServer struct {
	// config 是服务器配置
	config *PostgreSQLServerConfig

	// listener 是网络监听器（TCP Socket）
	listener net.Listener

	// sqlEngine 是 SeaweedFS SQL 查询引擎
	// 负责解析和执行 SQL 查询，访问 Topic 数据
	sqlEngine *engine.SQLEngine

	// sessions 是所有活动会话的映射表
	// key: 连接 ID（uint32）
	// value: 会话对象指针
	sessions map[uint32]*PostgreSQLSession

	// sessionMux 是保护 sessions 映射表的读写锁
	sessionMux sync.RWMutex

	// shutdown 是关闭信号通道
	// 当服务器停止时，此通道会被关闭
	shutdown chan struct{}

	// wg 是等待组，用于优雅关闭
	// 确保所有 goroutine 完成后才退出
	wg sync.WaitGroup

	// nextConnID 是下一个连接的 ID
	// 原子递增，保证每个连接有唯一 ID
	nextConnID uint32
}

// ═══════════════════════════════════════════════════════════════════════════
// PostgreSQL 会话（每个客户端连接对应一个会话）
// ═══════════════════════════════════════════════════════════════════════════

// PostgreSQLSession 表示一个客户端连接的会话状态
// 每个会话都是独立的，有自己的认证状态、事务状态、Prepared Statements 等
type PostgreSQLSession struct {
	// conn 是底层网络连接
	conn net.Conn

	// reader 是带缓冲的读取器（从客户端读取消息）
	reader *bufio.Reader

	// writer 是带缓冲的写入器（向客户端发送响应）
	writer *bufio.Writer

	// authenticated 标识用户是否已通过认证
	authenticated bool

	// username 是已认证的用户名
	username string

	// database 是当前会话使用的数据库（命名空间）
	// 可以通过 USE 命令切换
	database string

	// parameters 是会话参数键值对
	// 客户端在启动消息中发送的参数（如 application_name）
	parameters map[string]string

	// preparedStmts 是此会话的 Prepared Statements
	// key: statement 名称
	// value: PreparedStatement 对象
	preparedStmts map[string]*PreparedStatement

	// portals 是此会话的 Portals（游标）
	// key: portal 名称
	// value: Portal 对象
	portals map[string]*Portal

	// transactionState 是当前事务状态
	// 值：PG_TRANS_IDLE、PG_TRANS_INTRANS、PG_TRANS_ERROR
	transactionState byte

	// processID 是此会话的唯一进程 ID
	// 用于在 pg_stat_activity 等系统视图中标识会话
	processID uint32

	// secretKey 是此会话的密钥
	// 用于取消请求（CancelRequest）的验证
	secretKey uint32

	// created 是会话创建时间
	created time.Time

	// lastActivity 是最后一次活动时间
	// 用于检测空闲超时
	lastActivity time.Time

	// mutex 是保护会话状态的互斥锁
	mutex sync.Mutex
}

// ═══════════════════════════════════════════════════════════════════════════
// Prepared Statement（预编译语句）
// ═══════════════════════════════════════════════════════════════════════════

// PreparedStatement 表示一个预编译的 SQL 语句
// 客户端使用 Parse 消息创建，使用 Bind 和 Execute 消息执行
type PreparedStatement struct {
	// Name 是 statement 的名称
	// 空字符串表示未命名的 statement
	Name string

	// Query 是 SQL 查询语句
	// 可以包含参数占位符（$1, $2, ...）
	Query string

	// ParamTypes 是参数的 PostgreSQL 类型 OID 数组
	// 长度等于参数个数
	ParamTypes []uint32

	// Fields 是查询结果的字段描述
	// 仅在 Describe 消息后填充
	Fields []FieldDescription
}

// ═══════════════════════════════════════════════════════════════════════════
// Portal（游标/结果集）
// ═══════════════════════════════════════════════════════════════════════════

// Portal 表示一个绑定了参数的查询结果集（类似游标）
// 客户端使用 Bind 消息创建，使用 Execute 消息获取行
type Portal struct {
	// Name 是 portal 的名称
	// 空字符串表示未命名的 portal
	Name string

	// Statement 是关联的 Prepared Statement 名称
	Statement string

	// Parameters 是绑定的参数值（二进制格式）
	Parameters [][]byte

	// Suspended 标识 portal 是否被挂起
	// Execute 消息可以指定最大行数，剩余行会被挂起
	Suspended bool
}

// ═══════════════════════════════════════════════════════════════════════════
// 字段描述（列元数据）
// ═══════════════════════════════════════════════════════════════════════════

// FieldDescription 描述查询结果中的一个列
// 在 RowDescription 消息中发送给客户端
type FieldDescription struct {
	// Name 是列名
	Name string

	// TableOID 是列所属表的 OID
	// 0 表示计算列或没有对应表
	TableOID uint32

	// AttrNum 是列在表中的编号（从 1 开始）
	// 0 表示没有对应表列
	AttrNum int16

	// TypeOID 是列的 PostgreSQL 类型 OID
	// 例如：PG_TYPE_INT4、PG_TYPE_TEXT
	TypeOID uint32

	// TypeSize 是类型的固定大小（字节）
	// -1 表示可变长度类型
	TypeSize int16

	// TypeMod 是类型修饰符
	// 例如：varchar(50) 的修饰符是 54（50 + 4）
	// -1 表示没有修饰符
	TypeMod int32

	// Format 是数据格式代码
	// 0 = 文本格式，1 = 二进制格式
	Format int16
}

// ═══════════════════════════════════════════════════════════════════════════
// 服务器生命周期管理函数
// ═══════════════════════════════════════════════════════════════════════════

// NewPostgreSQLServer 创建一个新的 PostgreSQL 协议服务器实例
//
// 这个函数负责：
//   1. 验证和设置配置参数的默认值
//   2. 创建 SQL 查询引擎（连接到 SeaweedFS Master）
//   3. 初始化服务器内部数据结构（会话映射、关闭通道等）
//
// 参数:
//   - config: 服务器配置（认证、端口、TLS 等）
//   - masterAddr: SeaweedFS Master 服务器地址（例如 "localhost:9333"）
//
// 返回:
//   - *PostgreSQLServer: 配置好的服务器实例
//   - error: 创建过程中的错误（当前实现总是返回 nil）
//
// 配置默认值：
//   - Port: 5432（PostgreSQL 标准端口）
//   - Host: "localhost"
//   - Database: "default"
//   - MaxConns: 100（最大并发连接数）
//   - IdleTimeout: 1 小时
//   - StartupTimeout: 30 秒
//
// 使用示例:
//   config := &PostgreSQLServerConfig{
//       Host:       "0.0.0.0",
//       Port:       5432,
//       AuthMethod: AuthMD5,
//       Users:      map[string]string{"admin": "secret"},
//   }
//   server, err := NewPostgreSQLServer(config, "localhost:9333")
func NewPostgreSQLServer(config *PostgreSQLServerConfig, masterAddr string) (*PostgreSQLServer, error) {
	// 【配置验证和默认值设置】

	// 端口号：如果未设置或无效，使用 PostgreSQL 标准端口 5432
	if config.Port <= 0 {
		config.Port = DEFAULT_POSTGRES_PORT
	}

	// 主机地址：如果未设置，默认只监听本地回环地址
	if config.Host == "" {
		config.Host = "localhost"
	}

	// 默认数据库：如果未设置，使用 "default" 作为默认命名空间
	if config.Database == "" {
		config.Database = "default"
	}

	// 最大连接数：如果未设置，默认允许 100 个并发连接
	// 可根据服务器资源调整（每个连接约占用 100KB-1MB 内存）
	if config.MaxConns <= 0 {
		config.MaxConns = 100
	}

	// 空闲超时：如果未设置，默认 1 小时后关闭空闲连接
	if config.IdleTimeout <= 0 {
		config.IdleTimeout = time.Hour
	}

	// 启动超时：如果未设置，默认 30 秒内必须完成认证握手
	// 防止慢速攻击和挂起连接
	if config.StartupTimeout <= 0 {
		config.StartupTimeout = 30 * time.Second
	}

	// 【创建 SQL 引擎】
	// SQL 引擎负责：
	//   1. 连接到 SeaweedFS Master 获取 Topic 元数据
	//   2. 解析 SQL 查询（使用 PostgreSQL 兼容的解析器）
	//   3. 执行查询并返回结果
	sqlEngine := engine.NewSQLEngine(masterAddr)

	// 【创建服务器实例】
	server := &PostgreSQLServer{
		config:     config,
		sqlEngine:  sqlEngine,
		sessions:   make(map[uint32]*PostgreSQLSession), // 初始化会话映射表
		shutdown:   make(chan struct{}),                 // 初始化关闭信号通道
		nextConnID: 1,                                   // 连接 ID 从 1 开始
	}

	return server, nil
}

// Start 开始监听 PostgreSQL 客户端连接
//
// 这个函数负责：
//   1. 创建 TCP 监听器（支持可选的 TLS 加密）
//   2. 启动连接接受循环（在独立 goroutine 中）
//   3. 启动会话清理循环（定期清理空闲连接）
//
// 返回:
//   - error: 启动失败时的错误（例如端口已被占用）
//
// 注意：
//   - 此函数是非阻塞的，立即返回
//   - 连接处理在后台 goroutine 中进行
//   - 使用 Stop() 方法优雅关闭服务器
//
// 启动流程：
//   1. 根据 TLS 配置创建 TCP 或 TLS 监听器
//   2. 启动 acceptConnections goroutine（接受新连接）
//   3. 启动 cleanupSessions goroutine（清理空闲会话）
func (s *PostgreSQLServer) Start() error {
	// 构建监听地址字符串（例如 "0.0.0.0:5432"）
	addr := fmt.Sprintf("%s:%d", s.config.Host, s.config.Port)

	var listener net.Listener
	var err error

	// 【创建监听器】
	// 根据是否配置了 TLS，创建加密或非加密监听器
	if s.config.TLSConfig != nil {
		// 启用 TLS 加密的监听器
		// 适用于：
		//   - 公网部署
		//   - 需要保护数据传输的场景
		//   - 使用明文密码认证时（AuthPassword）
		listener, err = tls.Listen("tcp", addr, s.config.TLSConfig)
		glog.Infof("PostgreSQL Server with TLS listening on %s", addr)
	} else {
		// 普通 TCP 监听器（未加密）
		// 适用于：
		//   - 内网部署
		//   - 开发测试环境
		//   - 使用 MD5 认证时（密码已哈希）
		listener, err = net.Listen("tcp", addr)
		glog.Infof("PostgreSQL Server listening on %s", addr)
	}

	// 检查监听器创建是否成功
	if err != nil {
		// 常见错误：
		//   - 端口已被占用（另一个进程在使用该端口）
		//   - 权限不足（Unix 系统绑定 < 1024 端口需要 root）
		//   - 地址格式错误
		return fmt.Errorf("failed to start PostgreSQL server on %s: %v", addr, err)
	}

	s.listener = listener

	// 【启动连接接受循环】
	// 在独立 goroutine 中运行，不阻塞当前函数返回
	s.wg.Add(1)
	go s.acceptConnections()

	// 【启动会话清理循环】
	// 定期清理空闲超时的会话，防止资源泄露
	s.wg.Add(1)
	go s.cleanupSessions()

	return nil
}

// Stop 优雅关闭 PostgreSQL 服务器
//
// 这个函数负责：
//   1. 关闭监听器，停止接受新连接
//   2. 关闭所有现有客户端会话
//   3. 等待所有后台 goroutine 退出
//
// 返回:
//   - error: 关闭过程中的错误（当前实现总是返回 nil）
//
// 关闭流程：
//   1. 关闭 shutdown 通道，通知所有 goroutine 退出
//   2. 关闭网络监听器，拒绝新连接
//   3. 遍历并关闭所有活动会话
//   4. 等待所有 goroutine 完成（通过 WaitGroup）
//
// 注意：
//   - 这是一个阻塞操作，会等待所有连接关闭
//   - 建议在收到 SIGTERM 等信号时调用
//   - 确保客户端有足够时间完成当前查询
func (s *PostgreSQLServer) Stop() error {
	// 【关闭 shutdown 通道】
	// 这会通知所有监听此通道的 goroutine 退出
	// acceptConnections 和 cleanupSessions 都会收到信号
	close(s.shutdown)

	// 【关闭网络监听器】
	// 停止接受新的客户端连接
	if s.listener != nil {
		s.listener.Close()
	}

	// 【关闭所有活动会话】
	s.sessionMux.Lock()
	for _, session := range s.sessions {
		// 关闭每个会话的网络连接
		// 客户端会收到连接断开通知
		session.close()
	}
	// 清空会话映射表
	s.sessions = make(map[uint32]*PostgreSQLSession)
	s.sessionMux.Unlock()

	// 【等待所有 goroutine 退出】
	// 阻塞直到所有 wg.Add(1) 对应的 wg.Done() 都被调用
	// 包括：
	//   - acceptConnections goroutine
	//   - cleanupSessions goroutine
	//   - 所有 handleConnection goroutine
	s.wg.Wait()
	glog.Infof("PostgreSQL Server stopped")
	return nil
}

// ═══════════════════════════════════════════════════════════════════════════
// 连接处理函数
// ═══════════════════════════════════════════════════════════════════════════

// acceptConnections 处理传入的 PostgreSQL 客户端连接
//
// 这个函数在独立 goroutine 中运行，负责：
//   1. 循环接受新的客户端连接
//   2. 检查连接数限制
//   3. 为每个连接启动独立的处理 goroutine
//
// 连接限制：
//   - 达到 MaxConns 限制时，拒绝新连接
//   - 被拒绝的连接会被立即关闭
//
// 退出条件：
//   - 收到 shutdown 信号
//   - listener 被关闭（Stop() 调用时）
func (s *PostgreSQLServer) acceptConnections() {
	// 标记此 goroutine 完成（在函数退出时）
	defer s.wg.Done()

	// 【连接接受循环】
	for {
		// 检查是否收到关闭信号
		select {
		case <-s.shutdown:
			// 服务器正在关闭，退出循环
			return
		default:
			// 继续接受连接
		}

		// 【接受新连接】
		// Accept() 是阻塞调用，会等待客户端连接
		conn, err := s.listener.Accept()
		if err != nil {
			// 再次检查是否是因为服务器关闭导致的错误
			select {
			case <-s.shutdown:
				return
			default:
				// 其他错误（例如临时网络问题）
				glog.Errorf("Failed to accept PostgreSQL connection: %v", err)
				continue
			}
		}

		// 【检查连接数限制】
		s.sessionMux.RLock()
		sessionCount := len(s.sessions)
		s.sessionMux.RUnlock()

		if sessionCount >= s.config.MaxConns {
			// 已达到最大连接数，拒绝新连接
			// 这是一种背压机制，防止服务器过载
			glog.Warningf("Maximum connections reached (%d), rejecting connection from %s",
				s.config.MaxConns, conn.RemoteAddr())
			conn.Close()
			continue
		}

		// 【启动连接处理 goroutine】
		// 每个连接在独立 goroutine 中处理，实现并发
		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

// handleConnection 处理单个 PostgreSQL 客户端连接
//
// 这个函数在独立 goroutine 中运行，负责一个客户端连接的完整生命周期：
//   1. 创建会话对象并注册
//   2. 执行启动握手和认证
//   3. 循环处理客户端消息（查询、命令等）
//   4. 清理会话资源
//
// 会话生命周期：
//   创建 → 注册 → 启动/认证 → 消息处理循环 → 注销 → 关闭
//
// 参数:
//   - conn: 客户端的网络连接
//
// 退出条件：
//   - 客户端发送 Terminate 消息
//   - 网络连接断开（读/写错误）
//   - 收到服务器 shutdown 信号
//   - 启动握手失败
func (s *PostgreSQLServer) handleConnection(conn net.Conn) {
	// 标记此 goroutine 完成并确保连接关闭
	defer s.wg.Done()
	defer conn.Close()

	// 【生成唯一标识符】
	// 为此连接生成唯一的进程 ID 和密钥
	connID := s.generateConnectionID()   // 进程 ID（用于日志和 pg_stat_activity）
	secretKey := s.generateSecretKey()   // 密钥（用于取消请求验证）

	// 【创建会话对象】
	session := &PostgreSQLSession{
		conn:             conn,
		reader:           bufio.NewReader(conn),       // 带缓冲的读取器，提高性能
		writer:           bufio.NewWriter(conn),       // 带缓冲的写入器，减少系统调用
		authenticated:    false,                       // 初始未认证状态
		database:         s.config.Database,           // 使用默认数据库
		parameters:       make(map[string]string),     // 客户端参数（application_name 等）
		preparedStmts:    make(map[string]*PreparedStatement), // Prepared Statements 映射
		portals:          make(map[string]*Portal),    // Portals（游标）映射
		transactionState: PG_TRANS_IDLE,               // 初始事务状态：空闲
		processID:        connID,
		secretKey:        secretKey,
		created:          time.Now(),
		lastActivity:     time.Now(),
	}

	// 【注册会话】
	// 将会话添加到服务器的会话映射表中
	s.sessionMux.Lock()
	s.sessions[connID] = session
	s.sessionMux.Unlock()

	// 【注册清理函数】
	// 在函数退出时（无论正常或异常）注销会话
	defer func() {
		s.sessionMux.Lock()
		delete(s.sessions, connID)
		s.sessionMux.Unlock()
	}()

	glog.V(2).Infof("New PostgreSQL connection from %s (ID: %d)", conn.RemoteAddr(), connID)

	// 【执行启动握手】
	// 包括协议版本协商和用户认证
	err := s.handleStartup(session)
	if err != nil {
		// 根据错误类型使用不同的日志级别
		// 健康检查导致的断开是正常的，使用低级别日志
		if strings.Contains(err.Error(), "client disconnected") {
			glog.V(1).Infof("Client startup disconnected from %s (ID: %d): %v", conn.RemoteAddr(), connID, err)
		} else if strings.Contains(err.Error(), "timeout") {
			glog.Warningf("Startup timeout for connection %d from %s: %v", connID, conn.RemoteAddr(), err)
		} else {
			glog.Errorf("Startup failed for connection %d from %s: %v", connID, conn.RemoteAddr(), err)
		}
		return
	}

	// 【消息处理循环】
	// 认证成功后，进入主消息处理循环
	for {
		// 检查服务器关闭信号
		select {
		case <-s.shutdown:
			return
		default:
		}

		// 设置读取超时，防止长时间挂起
		// 30 秒无活动会触发超时，但客户端可以发送心跳保持连接
		conn.SetReadDeadline(time.Now().Add(30 * time.Second))

		// 处理单个消息（查询、命令等）
		err := s.handleMessage(session)
		if err != nil {
			if err == io.EOF {
				// 客户端正常断开连接
				glog.Infof("PostgreSQL client disconnected (ID: %d)", connID)
			} else {
				// 其他错误（协议错误、网络错误等）
				glog.Errorf("Error handling PostgreSQL message (ID: %d): %v", connID, err)
			}
			return
		}

		// 更新最后活动时间，用于空闲超时检测
		session.lastActivity = time.Now()
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// 启动和认证函数
// ═══════════════════════════════════════════════════════════════════════════

// handleStartup 处理 PostgreSQL 启动序列
//
// 这个函数负责处理客户端连接的初始化阶段：
//   1. 读取和验证协议版本
//   2. 处理 SSL/GSSAPI 请求（当前拒绝）
//   3. 解析客户端参数（用户名、数据库等）
//   4. 执行用户认证
//   5. 发送服务器参数和就绪消息
//
// 启动消息格式：
//   - 长度（4 字节）
//   - 协议版本（4 字节）：196608 (0x00030000) 表示 PostgreSQL 3.0
//   - 参数列表：key\0value\0...（以 \0\0 结尾）
//
// 参数:
//   - session: 客户端会话对象
//
// 返回:
//   - error: 启动失败时的错误
//
// 常见错误：
//   - 客户端断开连接（健康检查）
//   - 启动超时（慢速攻击防护）
//   - 协议版本不支持
//   - 认证失败
func (s *PostgreSQLServer) handleStartup(session *PostgreSQLSession) error {
	// Set a startup timeout to prevent hanging connections
	startupTimeout := s.config.StartupTimeout
	session.conn.SetReadDeadline(time.Now().Add(startupTimeout))
	defer session.conn.SetReadDeadline(time.Time{}) // Clear timeout

	for {
		// Read startup message length
		length := make([]byte, 4)
		_, err := io.ReadFull(session.reader, length)
		if err != nil {
			if err == io.EOF {
				// Client disconnected during startup - this is common for health checks
				return fmt.Errorf("client disconnected during startup handshake")
			}
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				return fmt.Errorf("startup handshake timeout after %v", startupTimeout)
			}
			return fmt.Errorf("failed to read message length during startup: %v", err)
		}

		msgLength := binary.BigEndian.Uint32(length) - 4
		if msgLength > 10000 { // Reasonable limit for startup messages
			return fmt.Errorf("startup message too large: %d bytes", msgLength)
		}

		// Read startup message content
		msg := make([]byte, msgLength)
		_, err = io.ReadFull(session.reader, msg)
		if err != nil {
			if err == io.EOF {
				return fmt.Errorf("client disconnected while reading startup message")
			}
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				return fmt.Errorf("startup message read timeout")
			}
			return fmt.Errorf("failed to read startup message: %v", err)
		}

		// Parse protocol version
		protocolVersion := binary.BigEndian.Uint32(msg[0:4])

		switch protocolVersion {
		case PG_SSL_REQUEST:
			// Reject SSL request - send 'N' to indicate SSL not supported
			_, err = session.conn.Write([]byte{'N'})
			if err != nil {
				return fmt.Errorf("failed to reject SSL request: %v", err)
			}
			// Continue loop to read the actual startup message
			continue

		case PG_GSSAPI_REQUEST:
			// Reject GSSAPI request - send 'N' to indicate GSSAPI not supported
			_, err = session.conn.Write([]byte{'N'})
			if err != nil {
				return fmt.Errorf("failed to reject GSSAPI request: %v", err)
			}
			// Continue loop to read the actual startup message
			continue

		case PG_PROTOCOL_VERSION_3:
			// This is the actual startup message, break out of loop
			break

		default:
			return fmt.Errorf("unsupported protocol version: %d", protocolVersion)
		}

		// Parse parameters
		params := strings.Split(string(msg[4:]), "\x00")
		for i := 0; i < len(params)-1; i += 2 {
			if params[i] == "user" {
				session.username = params[i+1]
			} else if params[i] == "database" {
				session.database = params[i+1]
			}
			session.parameters[params[i]] = params[i+1]
		}

		// Break out of the main loop - we have the startup message
		break
	}

	// Handle authentication
	err := s.handleAuthentication(session)
	if err != nil {
		return err
	}

	// Send parameter status messages
	err = s.sendParameterStatus(session, "server_version", fmt.Sprintf("%s (SeaweedFS)", version.VERSION_NUMBER))
	if err != nil {
		return err
	}
	err = s.sendParameterStatus(session, "server_encoding", "UTF8")
	if err != nil {
		return err
	}
	err = s.sendParameterStatus(session, "client_encoding", "UTF8")
	if err != nil {
		return err
	}
	err = s.sendParameterStatus(session, "DateStyle", "ISO, MDY")
	if err != nil {
		return err
	}
	err = s.sendParameterStatus(session, "integer_datetimes", "on")
	if err != nil {
		return err
	}

	// Send backend key data
	err = s.sendBackendKeyData(session)
	if err != nil {
		return err
	}

	// Send ready for query
	err = s.sendReadyForQuery(session)
	if err != nil {
		return err
	}

	session.authenticated = true
	return nil
}

// handleAuthentication 处理客户端认证
//
// 根据服务器配置的认证方式执行相应的认证流程：
//   - AuthTrust: 无条件信任（无需密码）
//   - AuthPassword: 明文密码认证
//   - AuthMD5: MD5 哈希密码认证（推荐）
//
// 参数:
//   - session: 客户端会话对象
//
// 返回:
//   - error: 认证失败时的错误
//
// 认证流程取决于配置的 AuthMethod：
//   1. AuthTrust: 直接发送认证成功消息
//   2. AuthPassword/AuthMD5: 请求密码 → 验证 → 发送结果
func (s *PostgreSQLServer) handleAuthentication(session *PostgreSQLSession) error {
	switch s.config.AuthMethod {
	case AuthTrust:
		// 信任认证：无需密码，直接成功
		return s.sendAuthenticationOk(session)
	case AuthPassword:
		// 明文密码认证：需要 TLS 保护
		return s.handlePasswordAuth(session)
	case AuthMD5:
		// MD5 哈希认证：安全且兼容性好（推荐）
		return s.handleMD5Auth(session)
	default:
		return fmt.Errorf("unsupported authentication method")
	}
}

// sendAuthenticationOk 发送认证成功消息
//
// PostgreSQL 认证消息格式：
//   - 消息类型: 'R' (PG_RESP_AUTH_OK)
//   - 消息长度: 8 字节
//   - 认证结果代码: 0 (AUTH_OK) 表示成功
//
// 参数:
//   - session: 客户端会话对象
//
// 返回:
//   - error: 发送失败时的错误
func (s *PostgreSQLServer) sendAuthenticationOk(session *PostgreSQLSession) error {
	msg := make([]byte, 9)
	msg[0] = PG_RESP_AUTH_OK
	binary.BigEndian.PutUint32(msg[1:5], 8)
	binary.BigEndian.PutUint32(msg[5:9], AUTH_OK)

	_, err := session.writer.Write(msg)
	if err == nil {
		err = session.writer.Flush()
	}
	return err
}

// handlePasswordAuth 处理明文密码认证
//
// 明文密码认证流程：
//   1. 服务器发送密码请求（AUTH_CLEAR）
//   2. 客户端发送明文密码
//   3. 服务器验证密码是否匹配
//   4. 发送认证结果（成功或失败）
//
// 安全性说明：
//   - 密码以明文传输，容易被网络嗅探工具截获
//   - 强烈建议配合 TLS 使用
//   - 生产环境推荐使用 AuthMD5 代替
//
// 参数:
//   - session: 客户端会话对象
//
// 返回:
//   - error: 认证失败或网络错误
func (s *PostgreSQLServer) handlePasswordAuth(session *PostgreSQLSession) error {
	// Send password request
	msg := make([]byte, 9)
	msg[0] = PG_RESP_AUTH_OK
	binary.BigEndian.PutUint32(msg[1:5], 8)
	binary.BigEndian.PutUint32(msg[5:9], AUTH_CLEAR)

	_, err := session.writer.Write(msg)
	if err != nil {
		return err
	}
	err = session.writer.Flush()
	if err != nil {
		return err
	}

	// Read password response
	msgType := make([]byte, 1)
	_, err = io.ReadFull(session.reader, msgType)
	if err != nil {
		return err
	}

	if msgType[0] != PG_MSG_PASSWORD {
		return fmt.Errorf("expected password message, got %c", msgType[0])
	}

	length := make([]byte, 4)
	_, err = io.ReadFull(session.reader, length)
	if err != nil {
		return err
	}

	msgLength := binary.BigEndian.Uint32(length) - 4
	password := make([]byte, msgLength)
	_, err = io.ReadFull(session.reader, password)
	if err != nil {
		return err
	}

	// Verify password
	expectedPassword, exists := s.config.Users[session.username]
	if !exists || string(password[:len(password)-1]) != expectedPassword { // Remove null terminator
		return s.sendError(session, "28P01", "authentication failed for user \""+session.username+"\"")
	}

	return s.sendAuthenticationOk(session)
}

// handleMD5Auth 处理 MD5 哈希密码认证
//
// MD5 认证流程：
//   1. 服务器生成 4 字节随机盐值（salt）
//   2. 服务器发送 MD5 认证请求 + salt
//   3. 客户端计算：md5(md5(password + username) + salt)
//   4. 客户端发送计算结果（以 "md5" 开头的 32 位十六进制字符串）
//   5. 服务器验证哈希值是否匹配
//
// 安全性说明：
//   - 密码不以明文传输，只传输哈希值
//   - 每次连接使用不同的盐值，防止重放攻击
//   - 比明文密码安全，但不如现代加密算法（SCRAM-SHA-256）
//   - 推荐用于生产环境（PostgreSQL 传统认证方式）
//
// 哈希算法：
//   内层 MD5: md5(password + username)
//   外层 MD5: md5(内层哈希的十六进制 + salt)
//   最终格式: "md5" + 外层哈希的十六进制
//
// 参数:
//   - session: 客户端会话对象
//
// 返回:
//   - error: 认证失败或网络错误
func (s *PostgreSQLServer) handleMD5Auth(session *PostgreSQLSession) error {
	// Generate salt
	salt := make([]byte, 4)
	_, err := rand.Read(salt)
	if err != nil {
		return err
	}

	// Send MD5 request
	msg := make([]byte, 13)
	msg[0] = PG_RESP_AUTH_OK
	binary.BigEndian.PutUint32(msg[1:5], 12)
	binary.BigEndian.PutUint32(msg[5:9], AUTH_MD5)
	copy(msg[9:13], salt)

	_, err = session.writer.Write(msg)
	if err != nil {
		return err
	}
	err = session.writer.Flush()
	if err != nil {
		return err
	}

	// Read password response
	msgType := make([]byte, 1)
	_, err = io.ReadFull(session.reader, msgType)
	if err != nil {
		return err
	}

	if msgType[0] != PG_MSG_PASSWORD {
		return fmt.Errorf("expected password message, got %c", msgType[0])
	}

	length := make([]byte, 4)
	_, err = io.ReadFull(session.reader, length)
	if err != nil {
		return err
	}

	msgLength := binary.BigEndian.Uint32(length) - 4
	response := make([]byte, msgLength)
	_, err = io.ReadFull(session.reader, response)
	if err != nil {
		return err
	}

	// Verify MD5 hash
	expectedPassword, exists := s.config.Users[session.username]
	if !exists {
		return s.sendError(session, "28P01", "authentication failed for user \""+session.username+"\"")
	}

	// Calculate expected hash: md5(md5(password + username) + salt)
	inner := md5.Sum([]byte(expectedPassword + session.username))
	expected := fmt.Sprintf("md5%x", md5.Sum(append([]byte(fmt.Sprintf("%x", inner)), salt...)))

	if string(response[:len(response)-1]) != expected { // Remove null terminator
		return s.sendError(session, "28P01", "authentication failed for user \""+session.username+"\"")
	}

	return s.sendAuthenticationOk(session)
}

// ═══════════════════════════════════════════════════════════════════════════
// 辅助函数和会话管理
// ═══════════════════════════════════════════════════════════════════════════

// generateConnectionID 生成唯一的连接 ID
//
// 连接 ID 用途：
//   - 在日志中标识连接
//   - 在 pg_stat_activity 系统视图中显示
//   - 作为会话映射表的键
//
// 实现：
//   - 使用原子递增的计数器
//   - 从 1 开始，每次调用加 1
//   - 线程安全（使用互斥锁保护）
//
// 返回:
//   - uint32: 唯一的连接 ID
func (s *PostgreSQLServer) generateConnectionID() uint32 {
	s.sessionMux.Lock()
	defer s.sessionMux.Unlock()
	id := s.nextConnID
	s.nextConnID++
	return id
}

// generateSecretKey 生成连接的密钥
//
// 密钥用途：
//   - 验证取消请求（CancelRequest）
//   - 防止未授权的查询取消
//
// 实现：
//   - 生成 4 字节随机数
//   - 使用密码学安全的随机数生成器
//
// 返回:
//   - uint32: 随机生成的密钥
func (s *PostgreSQLServer) generateSecretKey() uint32 {
	key := make([]byte, 4)
	rand.Read(key)
	return binary.BigEndian.Uint32(key)
}

// close 关闭会话的网络连接
//
// 这个方法是线程安全的，可以从多个 goroutine 调用。
// 关闭连接后，会话对象的 conn 字段会被设置为 nil，
// 防止重复关闭。
//
// 注意：
//   - 关闭连接会导致客户端收到断开通知
//   - 正在进行的读写操作会立即返回错误
//   - 不会清理 Prepared Statements 和 Portals（由调用方负责）
func (s *PostgreSQLSession) close() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.conn != nil {
		s.conn.Close()
		s.conn = nil
	}
}

// cleanupSessions 定期清理空闲会话
//
// 这个函数在独立 goroutine 中运行，负责：
//   - 每分钟检查一次所有活动会话
//   - 关闭超过空闲超时时间的会话
//   - 释放相关资源
//
// 空闲超时检测：
//   - 空闲时间 = 当前时间 - 最后活动时间
//   - 超过 IdleTimeout 配置值时关闭会话
//
// 退出条件：
//   - 收到 shutdown 信号（服务器停止时）
func (s *PostgreSQLServer) cleanupSessions() {
	defer s.wg.Done()

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-s.shutdown:
			return
		case <-ticker.C:
			s.cleanupIdleSessions()
		}
	}
}

// cleanupIdleSessions 移除空闲时间过长的会话
//
// 这个函数遍历所有活动会话，关闭空闲超时的会话。
//
// 清理逻辑：
//   1. 遍历所有会话
//   2. 计算每个会话的空闲时间
//   3. 超过 IdleTimeout 的会话被关闭并移除
//
// 注意：
//   - 使用写锁保护会话映射表
//   - 关闭会话会导致客户端断开连接
//   - 被清理的会话会记录日志
func (s *PostgreSQLServer) cleanupIdleSessions() {
	now := time.Now()

	s.sessionMux.Lock()
	defer s.sessionMux.Unlock()

	for id, session := range s.sessions {
		// 计算空闲时间
		idleTime := now.Sub(session.lastActivity)

		// 检查是否超过空闲超时阈值
		if idleTime > s.config.IdleTimeout {
			glog.Infof("Closing idle PostgreSQL session %d (idle: %v)", id, idleTime)
			session.close()
			delete(s.sessions, id)
		}
	}
}

// GetAddress 返回服务器监听地址
//
// 返回格式：
//   - "host:port"（例如 "0.0.0.0:5432"）
//
// 用途：
//   - 日志记录
//   - 状态显示
//   - 监控系统
//
// 返回:
//   - string: 服务器地址字符串
func (s *PostgreSQLServer) GetAddress() string {
	return fmt.Sprintf("%s:%d", s.config.Host, s.config.Port)
}
