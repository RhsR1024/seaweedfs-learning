// PostgreSQL 协议消息处理实现（protocol.go）
//
// 本文件实现 PostgreSQL 线协议的核心消息处理逻辑，包括：
//   1. 客户端消息解析和路由
//   2. SQL 查询执行和结果返回
//   3. 错误映射到 PostgreSQL 错误代码
//   4. 类型转换（SeaweedFS Schema → PostgreSQL OID）
//   5. Prepared Statements 和 Portals 支持
//   6. 系统查询处理（version、current_user 等）
//
// PostgreSQL 线协议概述：
//
// ┌───────────────────────────────────────────────────────────────────┐
// │                   PostgreSQL 协议消息流                             │
// ├───────────────────────────────────────────────────────────────────┤
// │                                                                   │
// │  客户端                            服务器                           │
// │  ──────                            ──────                         │
// │                                                                   │
// │  1. 启动阶段（Startup Phase）                                      │
// │  ════════════════════════════                                     │
// │  StartupMessage        ─────>                                     │
// │                        <─────    AuthenticationRequest           │
// │  PasswordMessage       ─────>    (如果需要认证)                    │
// │                        <─────    AuthenticationOk                │
// │                        <─────    ParameterStatus (多个)           │
// │                        <─────    BackendKeyData                  │
// │                        <─────    ReadyForQuery                   │
// │                                                                   │
// │  2. 简单查询协议（Simple Query Protocol）                          │
// │  ══════════════════════════════════                               │
// │  Query                 ─────>                                     │
// │                        <─────    RowDescription                  │
// │                        <─────    DataRow (多行)                   │
// │                        <─────    CommandComplete                 │
// │                        <─────    ReadyForQuery                   │
// │                                                                   │
// │  3. 扩展查询协议（Extended Query Protocol）                        │
// │  ═════════════════════════════════════════                        │
// │  Parse                 ─────>                                     │
// │                        <─────    ParseComplete                   │
// │  Bind                  ─────>                                     │
// │                        <─────    BindComplete                    │
// │  Describe              ─────>    (可选)                           │
// │                        <─────    RowDescription                  │
// │  Execute               ─────>                                     │
// │                        <─────    DataRow (多行)                   │
// │                        <─────    CommandComplete                 │
// │  Sync                  ─────>                                     │
// │                        <─────    ReadyForQuery                   │
// │                                                                   │
// │  4. 终止阶段（Termination Phase）                                  │
// │  ═══════════════════════════════                                  │
// │  Terminate             ─────>                                     │
// │                                                                   │
// └───────────────────────────────────────────────────────────────────┘
//
// 关键函数说明：
//   - handleMessage: 路由和分发客户端消息
//   - handleSimpleQuery: 处理简单查询（最常用）
//   - mapErrorToPostgreSQLCode: 将 SeaweedFS 错误映射到 PostgreSQL 错误代码
//   - getPostgreSQLTypeFromSchema: 从 Schema 推断 PostgreSQL 类型
//   - sendRowDescription: 发送列元数据
//   - sendDataRow: 发送数据行
//
// PostgreSQL 错误代码（SQLSTATE）：
//   - 00000: 成功
//   - 42601: 语法错误
//   - 42P01: 表不存在
//   - 42703: 列不存在
//   - 0A000: 特性不支持
//   - 42883: 函数不存在
//   - 08000: 连接异常
//
// 完整错误代码列表：https://www.postgresql.org/docs/current/errcodes-appendix.html
package postgres

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/seaweedfs/seaweedfs/weed/query/engine"
	"github.com/seaweedfs/seaweedfs/weed/query/sqltypes"
	"github.com/seaweedfs/seaweedfs/weed/util/sqlutil"
	"github.com/seaweedfs/seaweedfs/weed/util/version"
)

// ═══════════════════════════════════════════════════════════════════════════
// 错误映射：SeaweedFS → PostgreSQL
// ═══════════════════════════════════════════════════════════════════════════

// mapErrorToPostgreSQLCode 将 SeaweedFS SQL 引擎错误映射到相应的 PostgreSQL 错误代码
//
// PostgreSQL 使用 5 位字符的 SQLSTATE 错误代码（遵循 SQL:2016 标准）
// 格式：CCSSS
//   - CC: 错误类别（Class）
//   - SSS: 具体子类别（Subclass）
//
// 常见错误类别：
//   - 00: 成功完成
//   - 08: 连接异常
//   - 0A: 特性不支持
//   - 42: 语法错误或访问规则违规
//
// 参数:
//   - err: SeaweedFS SQL 引擎返回的错误
//
// 返回:
//   - string: 5 位字符的 PostgreSQL SQLSTATE 代码
//
// 映射表：
//   engine.ParseError           → "42601" (syntax_error)
//   engine.TableNotFoundError   → "42P01" (undefined_table)
//   engine.ColumnNotFoundError  → "42703" (undefined_column)
//   engine.UnsupportedFeature   → "0A000" (feature_not_supported)
//   engine.AggregationError     → "42883" (undefined_function)
//   engine.DataSourceError      → "08000" (connection_exception)
//   engine.OptimizationError    → "0A000" (feature_not_supported)
//   其他错误                     → "42000" (syntax_error_or_access_rule_violation)
func mapErrorToPostgreSQLCode(err error) string {
	if err == nil {
		return "00000" // Success
	}

	// Use typed errors for robust error mapping
	switch err.(type) {
	case engine.ParseError:
		return "42601" // Syntax error

	case engine.TableNotFoundError:
		return "42P01" // Undefined table

	case engine.ColumnNotFoundError:
		return "42703" // Undefined column

	case engine.UnsupportedFeatureError:
		return "0A000" // Feature not supported

	case engine.AggregationError:
		// Aggregation errors are usually function-related issues
		return "42883" // Undefined function (aggregation function issues)

	case engine.DataSourceError:
		// Data source errors are usually access or connection issues
		return "08000" // Connection exception

	case engine.OptimizationError:
		// Optimization failures are usually feature limitations
		return "0A000" // Feature not supported

	case engine.NoSchemaError:
		// Topic exists but no schema available
		return "42P01" // Undefined table (treat as table not found)
	}

	// Fallback: analyze error message for backward compatibility with non-typed errors
	errLower := strings.ToLower(err.Error())

	// Parsing and syntax errors
	if strings.Contains(errLower, "parse error") || strings.Contains(errLower, "syntax") {
		return "42601" // Syntax error
	}

	// Unsupported features
	if strings.Contains(errLower, "unsupported") || strings.Contains(errLower, "not supported") {
		return "0A000" // Feature not supported
	}

	// Table/topic not found
	if strings.Contains(errLower, "not found") ||
		(strings.Contains(errLower, "topic") && strings.Contains(errLower, "available")) {
		return "42P01" // Undefined table
	}

	// Column-related errors
	if strings.Contains(errLower, "column") || strings.Contains(errLower, "field") {
		return "42703" // Undefined column
	}

	// Multi-table or complex query limitations
	if strings.Contains(errLower, "single table") || strings.Contains(errLower, "join") {
		return "0A000" // Feature not supported
	}

	// Default to generic syntax/access error
	return "42000" // Syntax error or access rule violation
}

// ═══════════════════════════════════════════════════════════════════════════
// 消息路由和处理（Message Routing）
// ═══════════════════════════════════════════════════════════════════════════

// handleMessage 处理单个 PostgreSQL 协议消息
//
// 职责：
//   1. 读取消息类型标识（1 字节）
//   2. 读取消息长度（4 字节，大端序）
//   3. 读取消息体（长度 - 4 字节）
//   4. 根据消息类型分发到对应的处理函数
//
// 参数:
//   - session: 当前会话上下文
//
// 返回:
//   - error: 处理错误或 io.EOF（表示连接终止）
//
// 支持的消息类型：
//   - 'Q' (Query): 简单查询
//   - 'P' (Parse): 准备语句解析
//   - 'B' (Bind): 绑定参数到 Portal
//   - 'E' (Execute): 执行 Portal
//   - 'D' (Describe): 描述语句或 Portal
//   - 'C' (Close): 关闭语句或 Portal
//   - 'H' (Flush): 刷新输出缓冲区
//   - 'S' (Sync): 同步事务状态
//   - 'X' (Terminate): 终止连接
func (s *PostgreSQLServer) handleMessage(session *PostgreSQLSession) error {
	// 【步骤 1：读取消息类型】
	// PostgreSQL 协议中，每个消息以 1 字节的类型标识开始
	// 常见类型：'Q'=Query, 'P'=Parse, 'B'=Bind, 'E'=Execute 等
	msgType := make([]byte, 1)
	_, err := io.ReadFull(session.reader, msgType)
	if err != nil {
		return err // 读取失败，通常表示连接断开
	}

	// 【步骤 2：读取消息长度】
	// 消息长度字段为 4 字节（int32，大端序）
	// 长度包含自身的 4 字节，但不包含消息类型的 1 字节
	length := make([]byte, 4)
	_, err = io.ReadFull(session.reader, length)
	if err != nil {
		return err
	}

	// 【步骤 3：读取消息体】
	// 实际消息体长度 = 消息长度字段值 - 4（减去长度字段自身）
	msgLength := binary.BigEndian.Uint32(length) - 4
	msgBody := make([]byte, msgLength)
	if msgLength > 0 {
		_, err = io.ReadFull(session.reader, msgBody)
		if err != nil {
			return err
		}
	}

	// 【步骤 4：根据消息类型分发处理】
	// 使用 switch 语句将消息路由到对应的处理函数
	switch msgType[0] {
	case PG_MSG_QUERY: // 'Q' - 简单查询协议
		// 移除消息体末尾的 null 终止符（PostgreSQL 协议要求）
		return s.handleSimpleQuery(session, string(msgBody[:len(msgBody)-1]))
	case PG_MSG_PARSE: // 'P' - 解析准备语句
		return s.handleParse(session, msgBody)
	case PG_MSG_BIND: // 'B' - 绑定参数到 Portal
		return s.handleBind(session, msgBody)
	case PG_MSG_EXECUTE: // 'E' - 执行 Portal
		return s.handleExecute(session, msgBody)
	case PG_MSG_DESCRIBE: // 'D' - 描述语句或 Portal
		return s.handleDescribe(session, msgBody)
	case PG_MSG_CLOSE: // 'C' - 关闭语句或 Portal
		return s.handleClose(session, msgBody)
	case PG_MSG_FLUSH: // 'H' - 刷新输出缓冲区
		return s.handleFlush(session)
	case PG_MSG_SYNC: // 'S' - 同步（扩展查询协议结束标志）
		return s.handleSync(session)
	case PG_MSG_TERMINATE: // 'X' - 客户端请求断开连接
		return io.EOF // 返回 EOF 信号连接正常终止
	default:
		// 未知消息类型，返回协议错误
		// 错误代码 08P01 = protocol_violation
		return s.sendError(session, "08P01", fmt.Sprintf("unknown message type: %c", msgType[0]))
	}
}

// ───────────────────────────────────────────────────────────────────────────
// 简单查询协议（Simple Query Protocol）
// ───────────────────────────────────────────────────────────────────────────

// handleSimpleQuery 处理简单查询消息
//
// 职责：
//   1. 处理 USE database 命令，切换会话数据库上下文
//   2. 将多语句查询拆分为单个语句顺序执行
//   3. 处理 PostgreSQL 系统查询（SELECT version() 等）
//   4. 使用 PostgreSQL 解析器执行常规 SQL 查询
//   5. 发送查询结果（RowDescription + DataRow + CommandComplete）
//   6. 发送 ReadyForQuery 表示准备接受新查询
//
// 参数:
//   - session: 当前会话上下文
//   - query: SQL 查询字符串（可能包含多个语句，用分号分隔）
//
// 返回:
//   - error: 处理错误（发送错误消息后仍保持连接）
//
// 查询处理流程：
//   1. 检查 USE 命令 → 切换数据库上下文 → 发送 CommandComplete
//   2. 拆分多语句查询 → 逐个处理
//   3. 对每个语句：
//      a. 尝试系统查询处理（version、current_user 等）
//      b. 如果非系统查询，使用 SQL 引擎执行
//      c. 发送结果集（RowDescription + DataRow）
//      d. 发送 CommandComplete
//   4. 所有语句执行完成 → 发送 ReadyForQuery
//
// Panic 恢复：
//   - 在函数和 SQL 执行层面都有 recover()，防止崩溃
//   - 出错时发送错误消息，尝试保持连接存活
//
// 示例：
//   handleSimpleQuery(session, "SELECT * FROM topic1; SELECT COUNT(*) FROM topic2;")
//   会依次执行两个查询，分别返回结果
func (s *PostgreSQLServer) handleSimpleQuery(session *PostgreSQLSession, query string) error {
	// 记录查询日志（仅在日志级别 >=2 时输出）
	glog.V(2).Infof("PostgreSQL Query (ID: %d): %s", session.processID, query)

	// 【全局 Panic 恢复】
	// 添加最外层的 panic 恢复机制，防止任何未预期的崩溃导致服务器进程退出
	// 即使查询处理失败，也要尽可能保持连接存活
	defer func() {
		if r := recover(); r != nil {
			glog.Errorf("Panic in handleSimpleQuery (ID: %d): %v", session.processID, r)
			// 尝试发送错误消息给客户端（错误代码 XX000 = internal_error）
			s.sendError(session, "XX000", fmt.Sprintf("Internal error: %v", r))
			// 尝试发送 ReadyForQuery 保持连接存活
			s.sendReadyForQuery(session)
		}
	}()

	// 【特殊处理：USE database 命令】
	// PostgreSQL 本身不支持 USE 命令，但为了兼容性我们提供支持
	// 允许客户端切换当前会话的数据库上下文
	parts := strings.Fields(strings.TrimSpace(query))
	if len(parts) >= 2 && strings.ToUpper(parts[0]) == "USE" {
		// 提取数据库名称（支持带空格的名称）
		// 例如："USE my_database" 或 "USE \"my database\""
		dbName := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(query), parts[0]))

		// 移除引号（支持双引号和反引号）
		// PostgreSQL 使用双引号，MySQL 使用反引号
		if len(dbName) > 1 && dbName[0] == '"' && dbName[len(dbName)-1] == '"' {
			dbName = dbName[1 : len(dbName)-1] // 去除双引号
		} else if len(dbName) > 1 && dbName[0] == '`' && dbName[len(dbName)-1] == '`' {
			dbName = dbName[1 : len(dbName)-1] // 去除反引号（MySQL 兼容）
		}

		// 更新会话的数据库上下文
		session.database = dbName
		s.sqlEngine.GetCatalog().SetCurrentDatabase(dbName)

		// 发送命令完成消息
		err := s.sendCommandComplete(session, "USE")
		if err != nil {
			return err
		}
		// 发送 ReadyForQuery 并提前返回（不继续执行后续逻辑）
		return s.sendReadyForQuery(session)
	}

	// 【同步数据库上下文】
	// 如果会话有指定数据库，确保 SQL 引擎使用相同的数据库上下文
	// 这对于多数据库查询很重要
	if session.database != "" && session.database != s.sqlEngine.GetCatalog().GetCurrentDatabase() {
		s.sqlEngine.GetCatalog().SetCurrentDatabase(session.database)
	}

	// 【拆分多语句查询】
	// PostgreSQL 允许在一个 Query 消息中发送多个以分号分隔的语句
	// 例如："SELECT 1; SELECT 2; SELECT 3;"
	// 我们需要逐个执行这些语句
	queries := sqlutil.SplitStatements(query)

	// 【逐个执行语句】
	for _, singleQuery := range queries {
		// 去除空白字符
		cleanQuery := strings.TrimSpace(singleQuery)
		if cleanQuery == "" {
			continue // 跳过空语句（例如：";;;" 会产生空语句）
		}

		// 【系统查询快速路径】
		// 某些 PostgreSQL 系统查询可以直接处理，无需经过 SQL 引擎
		// 这样可以提高性能并简化实现
		// 例如：SELECT version(), SELECT current_user 等
		if systemResult := s.handleSystemQuery(session, cleanQuery); systemResult != nil {
			// 发送系统查询结果
			err := s.sendSystemQueryResult(session, systemResult, cleanQuery)
			if err != nil {
				return err
			}
			continue // 继续处理下一个语句
		}

		// 【通过 SQL 引擎执行查询】
		ctx := context.Background()
		var result *engine.QueryResult
		var err error

		// 【SQL 执行 Panic 恢复】
		// 为 SQL 引擎执行添加独立的 panic 恢复
		// 这样即使 SQL 引擎内部出现问题，也不会影响连接
		func() {
			defer func() {
				if r := recover(); r != nil {
					glog.Errorf("Panic in SQL execution (ID: %d, Query: %s): %v", session.processID, cleanQuery, r)
					err = fmt.Errorf("internal error during SQL execution: %v", r)
				}
			}()

			// 使用 SQL 引擎执行查询
			// SQL 引擎使用 PostgreSQL 解析器（CockroachDB parser），确保方言兼容性
			result, err = s.sqlEngine.ExecuteSQL(ctx, cleanQuery)
		}()

		// 【错误处理：ExecuteSQL 返回错误】
		if err != nil {
			// 将 SeaweedFS 错误映射为 PostgreSQL SQLSTATE 代码
			// 例如：ParseError → "42601"（语法错误）
			errorCode := mapErrorToPostgreSQLCode(err)
			sendErr := s.sendError(session, errorCode, err.Error())
			if sendErr != nil {
				return sendErr // 发送错误消息失败，断开连接
			}
			// 发送 ReadyForQuery 保持连接存活
			// 这样客户端可以继续发送新的查询
			return s.sendReadyForQuery(session)
		}

		// 【错误处理：QueryResult 包含错误】
		if result.Error != nil {
			// QueryResult.Error 表示查询执行过程中的错误
			errorCode := mapErrorToPostgreSQLCode(result.Error)
			sendErr := s.sendError(session, errorCode, result.Error.Error())
			if sendErr != nil {
				return sendErr
			}
			// 发送 ReadyForQuery 保持连接存活
			return s.sendReadyForQuery(session)
		}

		// 【发送查询结果】
		if len(result.Columns) > 0 {
			// 有列定义，说明是返回数据的查询（SELECT、SHOW 等）

			// 1. 发送列元数据（RowDescription）
			err = s.sendRowDescription(session, result)
			if err != nil {
				return err
			}

			// 2. 逐行发送数据（DataRow）
			for _, row := range result.Rows {
				err = s.sendDataRow(session, row)
				if err != nil {
					return err
				}
			}
		}

		// 【发送命令完成】
		// 为此语句生成命令标签（例如："SELECT 10" 表示返回 10 行）
		tag := s.getCommandTag(cleanQuery, len(result.Rows))
		err = s.sendCommandComplete(session, tag)
		if err != nil {
			return err
		}
	}

	// 【所有语句执行完毕】
	// 发送 ReadyForQuery 告知客户端可以发送新的命令
	return s.sendReadyForQuery(session)
}

// ───────────────────────────────────────────────────────────────────────────
// 系统查询处理（System Query Handling）
// ───────────────────────────────────────────────────────────────────────────

// SystemQueryResult 表示系统查询的结果
//
// 系统查询是 PostgreSQL 协议中的特殊查询，用于获取服务器信息
// 这些查询不经过 SQL 引擎，直接在协议层处理以提高性能
type SystemQueryResult struct {
	Columns []string   // 列名列表
	Rows    [][]string // 结果行，每行是字符串数组
}

// handleSystemQuery 直接处理 PostgreSQL 系统查询
//
// 职责：
//   识别并处理常见的 PostgreSQL 系统查询，无需经过 SQL 引擎
//
// 参数:
//   - session: 当前会话（未使用，但保留用于未来扩展）
//   - query: SQL 查询字符串
//
// 返回:
//   - *SystemQueryResult: 系统查询结果；如果不是系统查询则返回 nil
//
// 支持的系统查询：
//   1. 版本信息：
//      - SELECT version()
//   2. 数据库上下文：
//      - SELECT current_database()
//      - SELECT current_user
//   3. 服务器设置：
//      - SELECT current_setting('server_version')
//      - SELECT current_setting('server_encoding')
//      - SELECT current_setting('client_encoding')
//   4. 事务命令（只读模式，实际不执行）：
//      - BEGIN / START TRANSACTION
//      - COMMIT
//      - ROLLBACK
//   5. 设置命令（不支持修改，返回成功以保持兼容性）：
//      - SET ...
//
// 设计考虑：
//   - 系统查询直接返回结果，避免 SQL 引擎解析开销
//   - 事务命令返回成功但不实际执行（SeaweedFS 为只读）
//   - SET 命令忽略但不报错，以兼容 PostgreSQL 客户端初始化流程
func (s *PostgreSQLServer) handleSystemQuery(session *PostgreSQLSession, query string) *SystemQueryResult {
	// 【规范化查询字符串】
	// 移除前后空白和尾部分号，然后转小写用于匹配
	query = strings.TrimSpace(query)
	query = strings.TrimSuffix(query, ";")
	queryLower := strings.ToLower(query)

	// 【处理基本 PostgreSQL 系统查询】
	// 这些查询由客户端在连接时频繁调用，直接返回结果避免 SQL 引擎开销
	switch queryLower {
	case "select version()":
		// 返回服务器版本信息
		// 格式：SeaweedFS <版本> (PostgreSQL 14.0 compatible)
		return &SystemQueryResult{
			Columns: []string{"version"},
			Rows:    [][]string{{fmt.Sprintf("SeaweedFS %s (PostgreSQL 14.0 compatible)", version.VERSION_NUMBER)}},
		}
	case "select current_database()":
		// 返回当前数据库名称
		return &SystemQueryResult{
			Columns: []string{"current_database"},
			Rows:    [][]string{{s.config.Database}},
		}
	case "select current_user":
		// 返回当前用户名（SeaweedFS 总是返回 "seaweedfs"）
		return &SystemQueryResult{
			Columns: []string{"current_user"},
			Rows:    [][]string{{"seaweedfs"}},
		}
	case "select current_setting('server_version')":
		// 返回服务器版本配置
		return &SystemQueryResult{
			Columns: []string{"server_version"},
			Rows:    [][]string{{fmt.Sprintf("%s (SeaweedFS)", version.VERSION_NUMBER)}},
		}
	case "select current_setting('server_encoding')":
		// 返回服务器字符编码（总是 UTF8）
		return &SystemQueryResult{
			Columns: []string{"server_encoding"},
			Rows:    [][]string{{"UTF8"}},
		}
	case "select current_setting('client_encoding')":
		// 返回客户端字符编码（总是 UTF8）
		return &SystemQueryResult{
			Columns: []string{"client_encoding"},
			Rows:    [][]string{{"UTF8"}},
		}
	}

	// 【处理事务命令】
	// SeaweedFS 是只读系统，不支持真正的事务
	// 但为了兼容性，我们接受这些命令并返回成功
	switch queryLower {
	case "begin", "start transaction":
		// 返回 BEGIN 成功（实际不开启事务）
		return &SystemQueryResult{
			Columns: []string{"status"},
			Rows:    [][]string{{"BEGIN"}},
		}
	case "commit":
		// 返回 COMMIT 成功（实际无事务可提交）
		return &SystemQueryResult{
			Columns: []string{"status"},
			Rows:    [][]string{{"COMMIT"}},
		}
	case "rollback":
		// 返回 ROLLBACK 成功（实际无事务可回滚）
		return &SystemQueryResult{
			Columns: []string{"status"},
			Rows:    [][]string{{"ROLLBACK"}},
		}
	}

	// 【处理 SET 命令】
	// PostgreSQL 客户端连接时经常发送 SET 命令配置会话参数
	// 例如：SET extra_float_digits = 3, SET DateStyle = 'ISO, MDY'
	// 我们接受但忽略这些命令，保持客户端兼容性
	if strings.HasPrefix(queryLower, "set ") {
		return &SystemQueryResult{
			Columns: []string{"status"},
			Rows:    [][]string{{"SET"}},
		}
	}

	// 【不是系统查询】
	// 返回 nil 表示这不是系统查询，应该由 SQL 引擎处理
	return nil
}

// sendSystemQueryResult 发送系统查询的结果
//
// 职责：
//   1. 将 SystemQueryResult 转换为 PostgreSQL 线协议格式
//   2. 发送 RowDescription（列定义）
//   3. 发送 DataRow（数据行）
//   4. 发送 CommandComplete（命令完成标识）
//   5. 发送 ReadyForQuery（准备接受新查询）
//
// 参数:
//   - session: 当前会话上下文
//   - result: 系统查询结果（列名和数据行）
//   - query: 原始查询字符串（用于生成 CommandTag）
//
// 返回:
//   - error: 发送错误
//
// 实现细节：
//   - 将字符串结果转换为 sqltypes.Value 格式以复用现有发送函数
//   - 创建临时 QueryResult 对象以统一处理流程
//   - 包含 panic 恢复机制，防止系统查询崩溃
func (s *PostgreSQLServer) sendSystemQueryResult(session *PostgreSQLSession, result *SystemQueryResult, query string) error {
	// Add panic recovery to prevent crashes in system query results
	defer func() {
		if r := recover(); r != nil {
			glog.Errorf("Panic in sendSystemQueryResult (ID: %d, Query: %s): %v", session.processID, query, r)
			// Try to send error and continue
			s.sendError(session, "XX000", fmt.Sprintf("Internal error in system query: %v", r))
		}
	}()

	// Create column descriptions for system query results
	columns := make([]string, len(result.Columns))
	for i, col := range result.Columns {
		columns[i] = col
	}

	// Convert to sqltypes.Value format
	var sqlRows [][]sqltypes.Value
	for _, row := range result.Rows {
		sqlRow := make([]sqltypes.Value, len(row))
		for i, cell := range row {
			sqlRow[i] = sqltypes.NewVarChar(cell)
		}
		sqlRows = append(sqlRows, sqlRow)
	}

	// Send row description (create a temporary QueryResult for consistency)
	tempResult := &engine.QueryResult{
		Columns: columns,
		Rows:    sqlRows,
	}
	err := s.sendRowDescription(session, tempResult)
	if err != nil {
		return err
	}

	// Send data rows
	for _, row := range sqlRows {
		err = s.sendDataRow(session, row)
		if err != nil {
			return err
		}
	}

	// Send command complete
	tag := s.getCommandTag(query, len(result.Rows))
	err = s.sendCommandComplete(session, tag)
	if err != nil {
		return err
	}

	// Send ready for query
	return s.sendReadyForQuery(session)
}

// ───────────────────────────────────────────────────────────────────────────
// 扩展查询协议（Extended Query Protocol）
// ───────────────────────────────────────────────────────────────────────────

// handleParse 处理 Parse 消息（准备语句解析）
//
// 职责：
//   1. 解析 Parse 消息格式（语句名 + SQL 查询 + 参数类型）
//   2. 创建 PreparedStatement 对象并存储到会话中
//   3. 发送 ParseComplete 响应
//
// 参数:
//   - session: 当前会话上下文
//   - msgBody: Parse 消息体（不包含消息类型和长度）
//
// 返回:
//   - error: 解析或发送错误
//
// Parse 消息格式：
//   statement_name\0query\0param_count(int16)[param_type(int32)...]
//   - statement_name: 准备语句的名称（空字符串表示未命名语句）
//   - query: SQL 查询字符串
//   - param_count: 参数个数（2 字节，大端序）
//   - param_type: 每个参数的 PostgreSQL 类型 OID（4 字节，大端序）
//
// 实现说明：
//   - 当前实现简化处理，只存储语句名和查询
//   - 参数类型暂未解析（TODO: 完整实现参数类型处理）
//   - 准备语句存储在 session.preparedStmts map 中
func (s *PostgreSQLServer) handleParse(session *PostgreSQLSession, msgBody []byte) error {
	// Parse message format: statement_name\0query\0param_count(int16)[param_type(int32)...]
	parts := strings.Split(string(msgBody), "\x00")
	if len(parts) < 2 {
		return s.sendError(session, "08P01", "invalid Parse message format")
	}

	stmtName := parts[0]
	query := parts[1]

	// Create prepared statement
	stmt := &PreparedStatement{
		Name:       stmtName,
		Query:      query,
		ParamTypes: []uint32{},
		Fields:     []FieldDescription{},
	}

	session.preparedStmts[stmtName] = stmt

	// Send parse complete
	return s.sendParseComplete(session)
}

// handleBind 处理 Bind 消息
//
// 职责：
//   1. 将准备语句绑定到 Portal（命名或未命名）
//   2. 绑定参数值到准备语句的参数占位符
//   3. 指定结果列的格式（文本或二进制）
//   4. 发送 BindComplete 响应
//
// 参数:
//   - session: 当前会话上下文
//   - msgBody: Bind 消息体
//
// 返回:
//   - error: 绑定或发送错误
//
// Bind 消息格式：
//   portal_name\0statement_name\0
//   param_format_count(int16)[format_code(int16)...]
//   param_count(int16)[param_length(int32)param_value...]
//   result_format_count(int16)[format_code(int16)...]
//
// 实现状态：
//   - 当前为简化实现，仅发送 BindComplete
//   - TODO: 解析参数值并创建 Portal 对象
//   - TODO: 存储参数值用于后续 Execute 命令
func (s *PostgreSQLServer) handleBind(session *PostgreSQLSession, msgBody []byte) error {
	// For now, simple implementation
	// In full implementation, would parse parameters and create portal

	// Send bind complete
	return s.sendBindComplete(session)
}

// handleExecute 处理 Execute 消息
//
// 职责：
//   1. 执行之前通过 Bind 创建的 Portal
//   2. 支持限制返回的行数（用于分批获取结果）
//   3. 发送查询结果（RowDescription + DataRow）
//   4. 发送 CommandComplete
//
// 参数:
//   - session: 当前会话上下文
//   - msgBody: Execute 消息体
//
// 返回:
//   - error: 执行或发送错误
//
// Execute 消息格式：
//   portal_name\0max_rows(int32)
//   - portal_name: Portal 名称（空字符串表示未命名 Portal）
//   - max_rows: 最大返回行数（0 表示无限制）
//
// 实现状态：
//   - 当前为简化实现，直接返回 "SELECT 0"
//   - TODO: 从 Portal 获取准备语句和参数
//   - TODO: 执行查询并返回实际结果
//   - TODO: 支持 max_rows 限制和游标功能
func (s *PostgreSQLServer) handleExecute(session *PostgreSQLSession, msgBody []byte) error {
	// Parse portal name
	parts := strings.Split(string(msgBody), "\x00")
	if len(parts) == 0 {
		return s.sendError(session, "08P01", "invalid Execute message format")
	}

	portalName := parts[0]

	// For now, execute as simple query
	// In full implementation, would use portal with parameters
	glog.V(2).Infof("PostgreSQL Execute portal (ID: %d): %s", session.processID, portalName)

	// Send command complete
	err := s.sendCommandComplete(session, "SELECT 0")
	if err != nil {
		return err
	}

	return nil
}

// handleDescribe 处理 Describe 消息
//
// 职责：
//   1. 描述准备语句（'S'）或 Portal（'P'）的元数据
//   2. 对于语句：返回参数描述和结果列描述
//   3. 对于 Portal：返回结果列描述
//   4. 发送 RowDescription（或 NoData）
//
// 参数:
//   - session: 当前会话上下文
//   - msgBody: Describe 消息体
//
// 返回:
//   - error: 描述或发送错误
//
// Describe 消息格式：
//   object_type(byte)object_name\0
//   - object_type: 'S' 表示准备语句，'P' 表示 Portal
//   - object_name: 对象名称（空字符串表示未命名对象）
//
// 实现状态：
//   - 当前为简化实现，返回空的 RowDescription
//   - TODO: 从 preparedStmts/portals 中获取实际的列信息
//   - TODO: 对于准备语句，发送 ParameterDescription
//   - TODO: 如果没有结果列，发送 NoData 消息
func (s *PostgreSQLServer) handleDescribe(session *PostgreSQLSession, msgBody []byte) error {
	if len(msgBody) < 2 {
		return s.sendError(session, "08P01", "invalid Describe message format")
	}

	objectType := msgBody[0] // 'S' for statement, 'P' for portal
	objectName := string(msgBody[1:])

	glog.V(2).Infof("PostgreSQL Describe %c (ID: %d): %s", objectType, session.processID, objectName)

	// For now, send empty row description
	tempResult := &engine.QueryResult{
		Columns: []string{},
		Rows:    [][]sqltypes.Value{},
	}
	return s.sendRowDescription(session, tempResult)
}

// handleClose 处理 Close 消息
//
// 职责：
//   1. 关闭并释放准备语句（'S'）或 Portal（'P'）资源
//   2. 从会话中移除对应的对象
//   3. 发送 CloseComplete 响应
//
// 参数:
//   - session: 当前会话上下文
//   - msgBody: Close 消息体
//
// 返回:
//   - error: 关闭或发送错误
//
// Close 消息格式：
//   object_type(byte)object_name\0
//   - object_type: 'S' 表示准备语句，'P' 表示 Portal
//   - object_name: 对象名称（空字符串表示未命名对象）
//
// 行为：
//   - 关闭准备语句：从 session.preparedStmts 中删除
//   - 关闭 Portal：从 session.portals 中删除
//   - 如果对象不存在，不报错（PostgreSQL 协议行为）
func (s *PostgreSQLServer) handleClose(session *PostgreSQLSession, msgBody []byte) error {
	if len(msgBody) < 2 {
		return s.sendError(session, "08P01", "invalid Close message format")
	}

	objectType := msgBody[0] // 'S' for statement, 'P' for portal
	objectName := string(msgBody[1:])

	switch objectType {
	case 'S':
		delete(session.preparedStmts, objectName)
	case 'P':
		delete(session.portals, objectName)
	}

	// Send close complete
	return s.sendCloseComplete(session)
}

// ───────────────────────────────────────────────────────────────────────────
// 流控制和同步（Flow Control）
// ───────────────────────────────────────────────────────────────────────────

// handleFlush 处理 Flush 消息
//
// 职责：
//   强制刷新输出缓冲区，确保之前的消息立即发送到客户端
//
// 参数:
//   - session: 当前会话上下文
//
// 返回:
//   - error: 刷新错误
//
// 使用场景：
//   - 在扩展查询协议中，客户端可能需要立即查看服务器响应
//   - 例如：Parse → Flush（等待 ParseComplete）→ Bind → Flush → ...
//   - 不影响事务状态，仅影响网络 I/O
func (s *PostgreSQLServer) handleFlush(session *PostgreSQLSession) error {
	return session.writer.Flush()
}

// handleSync 处理 Sync 消息
//
// 职责：
//   1. 标记扩展查询协议的边界
//   2. 重置事务状态为 IDLE（如果没有显式事务）
//   3. 发送 ReadyForQuery 消息
//
// 参数:
//   - session: 当前会话上下文
//
// 返回:
//   - error: 发送错误
//
// 使用场景：
//   - 扩展查询协议必须以 Sync 消息结束
//   - 例如：Parse → Bind → Execute → Sync
//   - Sync 后服务器发送 ReadyForQuery，表示可以开始新的查询
//
// 事务状态：
//   - 重置为 PG_TRANS_IDLE（'I' - 空闲，无事务）
//   - 如果在显式事务中，应保持 PG_TRANS_INTRANS（'T' - 事务中）
func (s *PostgreSQLServer) handleSync(session *PostgreSQLSession) error {
	// Reset transaction state if needed
	session.transactionState = PG_TRANS_IDLE

	// Send ready for query
	return s.sendReadyForQuery(session)
}

// ═══════════════════════════════════════════════════════════════════════════
// 启动阶段响应消息（Startup Phase Responses）
// ═══════════════════════════════════════════════════════════════════════════

// sendParameterStatus 发送参数状态消息
//
// 职责：
//   在启动阶段向客户端发送服务器参数信息
//
// 参数:
//   - session: 当前会话上下文
//   - name: 参数名称（例如："server_version"、"client_encoding"）
//   - value: 参数值（例如："14.0"、"UTF8"）
//
// 返回:
//   - error: 发送错误
//
// 消息格式（'S'）：
//   'S' + length(int32) + name\0 + value\0
//
// 常见参数：
//   - server_version: PostgreSQL 服务器版本
//   - server_encoding: 服务器字符编码
//   - client_encoding: 客户端字符编码
//   - application_name: 应用程序名称
//   - TimeZone: 时区设置
//   - integer_datetimes: 日期时间格式（on/off）
//
// 发送时机：
//   在 AuthenticationOk 之后，ReadyForQuery 之前
func (s *PostgreSQLServer) sendParameterStatus(session *PostgreSQLSession, name, value string) error {
	msg := make([]byte, 0)
	msg = append(msg, PG_RESP_PARAMETER)

	// Calculate length
	length := 4 + len(name) + 1 + len(value) + 1
	lengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBytes, uint32(length))
	msg = append(msg, lengthBytes...)

	// Add name and value
	msg = append(msg, []byte(name)...)
	msg = append(msg, 0) // null terminator
	msg = append(msg, []byte(value)...)
	msg = append(msg, 0) // null terminator

	_, err := session.writer.Write(msg)
	if err == nil {
		err = session.writer.Flush()
	}
	return err
}

// sendBackendKeyData 发送后端密钥数据
//
// 职责：
//   向客户端发送进程 ID 和密钥，用于取消查询请求
//
// 参数:
//   - session: 当前会话上下文
//
// 返回:
//   - error: 发送错误
//
// 消息格式（'K'）：
//   'K' + length(12) + process_id(int32) + secret_key(int32)
//
// 用途：
//   - 客户端可以使用这两个值发送 CancelRequest 来取消正在执行的查询
//   - process_id: 唯一标识此会话
//   - secret_key: 防止恶意取消其他会话的查询
//
// 安全考虑：
//   - secret_key 必须使用加密随机数生成
//   - 客户端必须保密这两个值，仅用于自己的查询取消
//
// 发送时机：
//   在 ParameterStatus 消息之后，ReadyForQuery 之前
func (s *PostgreSQLServer) sendBackendKeyData(session *PostgreSQLSession) error {
	msg := make([]byte, 13)
	msg[0] = PG_RESP_BACKEND_KEY
	binary.BigEndian.PutUint32(msg[1:5], 12)
	binary.BigEndian.PutUint32(msg[5:9], session.processID)
	binary.BigEndian.PutUint32(msg[9:13], session.secretKey)

	_, err := session.writer.Write(msg)
	if err == nil {
		err = session.writer.Flush()
	}
	return err
}

// sendReadyForQuery 发送准备接受查询消息
//
// 职责：
//   告知客户端服务器已准备好接受新的查询命令
//
// 参数:
//   - session: 当前会话上下文
//
// 返回:
//   - error: 发送错误
//
// 消息格式（'Z'）：
//   'Z' + length(5) + transaction_state(byte)
//
// 事务状态标识：
//   - 'I' (PG_TRANS_IDLE): 空闲，没有活动事务
//   - 'T' (PG_TRANS_INTRANS): 在事务块中
//   - 'E' (PG_TRANS_ERROR): 在失败的事务块中（需要 ROLLBACK）
//
// 发送时机：
//   - 启动阶段完成后（BackendKeyData 之后）
//   - 每个查询命令完成后（CommandComplete 之后）
//   - Sync 消息处理后
//   - 错误发生后（保持连接存活）
//
// 重要性：
//   - 这是客户端判断是否可以发送下一个命令的关键信号
//   - 如果不发送此消息，客户端会一直等待
func (s *PostgreSQLServer) sendReadyForQuery(session *PostgreSQLSession) error {
	msg := make([]byte, 6)
	msg[0] = PG_RESP_READY
	binary.BigEndian.PutUint32(msg[1:5], 5)
	msg[5] = session.transactionState

	_, err := session.writer.Write(msg)
	if err == nil {
		err = session.writer.Flush()
	}
	return err
}

// ═══════════════════════════════════════════════════════════════════════════
// 查询结果响应消息（Query Result Responses）
// ═══════════════════════════════════════════════════════════════════════════

// sendRowDescription 发送行描述消息
//
// 职责：
//   在发送查询结果数据行之前，发送列元数据描述
//
// 参数:
//   - session: 当前会话上下文
//   - result: 查询结果（包含列名和类型信息）
//
// 返回:
//   - error: 发送错误
//
// 消息格式（'T'）：
//   'T' + length(int32) + field_count(int16) + [field_description...]
//
// 每个字段描述包含：
//   - name\0: 列名（null 终止字符串）
//   - table_oid(int32): 表的 OID（0 表示无表或虚拟列）
//   - attr_num(int16): 列在表中的属性编号（从 1 开始）
//   - type_oid(int32): PostgreSQL 类型 OID（例如：23=INT4, 25=TEXT）
//   - type_size(int16): 类型大小（-1 表示可变长度）
//   - type_mod(int32): 类型修饰符（-1 表示默认）
//   - format(int16): 格式代码（0=文本，1=二进制）
//
// 类型推断：
//   1. 优先从 Schema 获取类型（最准确）
//   2. 识别系统列（_timestamp_ns、_key、_source）
//   3. 回退到数据推断（从第一行数据判断类型）
//
// 发送时机：
//   在 DataRow 消息之前，CommandComplete 之前
func (s *PostgreSQLServer) sendRowDescription(session *PostgreSQLSession, result *engine.QueryResult) error {
	// 【构建消息】
	msg := make([]byte, 0)
	msg = append(msg, PG_RESP_ROW_DESC) // 消息类型：'T' (RowDescription)

	// 【计算消息长度】
	// 长度 = 长度字段本身(4) + 字段数量(2) + 所有字段描述
	length := 4 + 2 // length + field count
	for _, col := range result.Columns {
		// 每个字段描述的长度：
		// name(可变) + null(1) + tableOID(4) + attrNum(2) + typeOID(4) + typeSize(2) + typeMod(4) + format(2)
		length += len(col) + 1 + 4 + 2 + 4 + 2 + 4 + 2
	}

	// 写入消息长度（大端序）
	lengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBytes, uint32(length))
	msg = append(msg, lengthBytes...)

	// 【写入字段数量】
	fieldCountBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(fieldCountBytes, uint16(len(result.Columns)))
	msg = append(msg, fieldCountBytes...)

	// 【逐个写入字段描述】
	for i, col := range result.Columns {
		// 1. 字段名称（null 终止字符串）
		msg = append(msg, []byte(col)...)
		msg = append(msg, 0) // null terminator

		// 2. 表的 OID（0 表示无关联表或虚拟列）
		// SeaweedFS 查询结果不关联特定的 PostgreSQL 表，所以使用 0
		tableOID := make([]byte, 4)
		binary.BigEndian.PutUint32(tableOID, 0)
		msg = append(msg, tableOID...)

		// 3. 列在表中的属性编号（从 1 开始）
		// 即使没有真实的表，也需要提供递增的属性编号
		attrNum := make([]byte, 2)
		binary.BigEndian.PutUint16(attrNum, uint16(i+1))
		msg = append(msg, attrNum...)

		// 4. PostgreSQL 类型 OID（关键字段）
		// 优先从 Schema 推断，回退到数据推断
		// 例如：23=INT4, 20=INT8, 25=TEXT, 701=FLOAT8
		typeOID := s.getPostgreSQLTypeFromSchema(result, col, i)
		typeOIDBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(typeOIDBytes, typeOID)
		msg = append(msg, typeOIDBytes...)

		// 5. 类型大小（-1 表示可变长度）
		// 大多数 PostgreSQL 类型都是可变长度，所以使用 -1
		typeSize := make([]byte, 2)
		binary.BigEndian.PutUint16(typeSize, 0xFFFF) // -1 as uint16
		msg = append(msg, typeSize...)

		// 6. 类型修饰符（-1 表示使用默认值）
		// 用于 VARCHAR(n)、NUMERIC(p,s) 等类型的长度/精度信息
		// 我们不使用修饰符，所以设为 -1
		typeMod := make([]byte, 4)
		binary.BigEndian.PutUint32(typeMod, 0xFFFFFFFF) // -1 as uint32
		msg = append(msg, typeMod...)

		// 7. 格式代码（0=文本格式，1=二进制格式）
		// 当前实现仅支持文本格式
		format := make([]byte, 2)
		binary.BigEndian.PutUint16(format, 0)
		msg = append(msg, format...)
	}

	// 【发送消息】
	_, err := session.writer.Write(msg)
	if err == nil {
		err = session.writer.Flush()
	}
	return err
}

// sendDataRow 发送数据行消息
//
// 职责：
//   发送查询结果的单行数据
//
// 参数:
//   - session: 当前会话上下文
//   - row: 单行数据（sqltypes.Value 数组）
//
// 返回:
//   - error: 发送错误
//
// 消息格式（'D'）：
//   'D' + length(int32) + field_count(int16) + [field_value...]
//
// 每个字段值格式：
//   - value_length(int32): 值的字节长度（-1 表示 NULL）
//   - value_data(bytes): 实际数据（如果不是 NULL）
//
// NULL 值处理：
//   - NULL 值长度为 -1 (0xFFFFFFFF)
//   - 不包含 value_data 部分
//
// 数据格式：
//   - 当前实现使用文本格式（format=0）
//   - 所有值通过 ToString() 转换为字符串
//   - 二进制格式（format=1）暂未实现
//
// 发送时机：
//   在 RowDescription 之后，每行数据发送一次
//   所有行发送完毕后发送 CommandComplete
func (s *PostgreSQLServer) sendDataRow(session *PostgreSQLSession, row []sqltypes.Value) error {
	// 【构建消息】
	msg := make([]byte, 0)
	msg = append(msg, PG_RESP_DATA_ROW) // 消息类型：'D' (DataRow)

	// 【计算消息长度】
	// 长度 = 长度字段(4) + 字段数量(2) + 所有字段值
	length := 4 + 2 // length + field count
	for _, value := range row {
		if value.IsNull() {
			length += 4 // NULL 值仅需要 4 字节表示长度 -1
		} else {
			valueStr := value.ToString()
			length += 4 + len(valueStr) // 4 字节长度 + 实际数据
		}
	}

	// 写入消息长度（大端序）
	lengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBytes, uint32(length))
	msg = append(msg, lengthBytes...)

	// 【写入字段数量】
	fieldCountBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(fieldCountBytes, uint16(len(row)))
	msg = append(msg, fieldCountBytes...)

	// 【逐个写入字段值】
	for _, value := range row {
		if value.IsNull() {
			// NULL 值处理
			// PostgreSQL 使用长度 -1 (0xFFFFFFFF) 表示 NULL
			// 不写入任何数据，仅写入长度标识
			nullLength := make([]byte, 4)
			binary.BigEndian.PutUint32(nullLength, 0xFFFFFFFF) // -1 as uint32
			msg = append(msg, nullLength...)
		} else {
			// 非 NULL 值处理
			// 1. 将值转换为文本格式（当前仅支持文本格式）
			valueStr := value.ToString()
			// 2. 写入值的字节长度
			valueLength := make([]byte, 4)
			binary.BigEndian.PutUint32(valueLength, uint32(len(valueStr)))
			msg = append(msg, valueLength...)
			// 3. 写入实际数据
			msg = append(msg, []byte(valueStr)...)
		}
	}

	// 【发送消息】
	_, err := session.writer.Write(msg)
	if err == nil {
		err = session.writer.Flush()
	}
	return err
}

// ───────────────────────────────────────────────────────────────────────────
// 命令完成和状态消息（Command Completion Messages）
// ───────────────────────────────────────────────────────────────────────────

// sendCommandComplete 发送命令完成消息
//
// 职责：
//   标识一个 SQL 命令已成功完成，并提供命令标签和影响的行数
//
// 参数:
//   - session: 当前会话上下文
//   - tag: 命令标签字符串
//
// 返回:
//   - error: 发送错误
//
// 消息格式（'C'）：
//   'C' + length(int32) + tag\0
//
// 命令标签格式（由 getCommandTag 生成）：
//   - "SELECT n": SELECT 查询返回 n 行
//   - "INSERT 0 n": INSERT 插入了 n 行
//   - "UPDATE n": UPDATE 更新了 n 行
//   - "DELETE n": DELETE 删除了 n 行
//   - "BEGIN": 开始事务
//   - "COMMIT": 提交事务
//   - "ROLLBACK": 回滚事务
//   - "USE": 切换数据库
//   - "SET": 设置参数
//
// 发送时机：
//   在所有 DataRow 消息发送完毕后，ReadyForQuery 之前
func (s *PostgreSQLServer) sendCommandComplete(session *PostgreSQLSession, tag string) error {
	msg := make([]byte, 0)
	msg = append(msg, PG_RESP_COMMAND)

	length := 4 + len(tag) + 1
	lengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBytes, uint32(length))
	msg = append(msg, lengthBytes...)

	msg = append(msg, []byte(tag)...)
	msg = append(msg, 0) // null terminator

	_, err := session.writer.Write(msg)
	if err == nil {
		err = session.writer.Flush()
	}
	return err
}

// ───────────────────────────────────────────────────────────────────────────
// 扩展查询协议响应消息（Extended Query Protocol Responses）
// ───────────────────────────────────────────────────────────────────────────

// sendParseComplete 发送解析完成消息
//
// 职责：
//   响应 Parse 消息，确认准备语句已成功解析并存储
//
// 参数:
//   - session: 当前会话上下文
//
// 返回:
//   - error: 发送错误
//
// 消息格式（'1'）：
//   '1' + length(4)
//   这是一个固定长度的简单响应消息
//
// 发送时机：
//   Parse 消息处理成功后立即发送
func (s *PostgreSQLServer) sendParseComplete(session *PostgreSQLSession) error {
	msg := make([]byte, 5)
	msg[0] = PG_RESP_PARSE_COMPLETE
	binary.BigEndian.PutUint32(msg[1:5], 4)

	_, err := session.writer.Write(msg)
	if err == nil {
		err = session.writer.Flush()
	}
	return err
}

// sendBindComplete 发送绑定完成消息
//
// 职责：
//   响应 Bind 消息，确认参数已成功绑定到 Portal
//
// 参数:
//   - session: 当前会话上下文
//
// 返回:
//   - error: 发送错误
//
// 消息格式（'2'）：
//   '2' + length(4)
//   这是一个固定长度的简单响应消息
//
// 发送时机：
//   Bind 消息处理成功后立即发送
func (s *PostgreSQLServer) sendBindComplete(session *PostgreSQLSession) error {
	msg := make([]byte, 5)
	msg[0] = PG_RESP_BIND_COMPLETE
	binary.BigEndian.PutUint32(msg[1:5], 4)

	_, err := session.writer.Write(msg)
	if err == nil {
		err = session.writer.Flush()
	}
	return err
}

// sendCloseComplete 发送关闭完成消息
//
// 职责：
//   响应 Close 消息，确认准备语句或 Portal 已成功关闭
//
// 参数:
//   - session: 当前会话上下文
//
// 返回:
//   - error: 发送错误
//
// 消息格式（'3'）：
//   '3' + length(4)
//   这是一个固定长度的简单响应消息
//
// 发送时机：
//   Close 消息处理成功后立即发送
func (s *PostgreSQLServer) sendCloseComplete(session *PostgreSQLSession) error {
	msg := make([]byte, 5)
	msg[0] = PG_RESP_CLOSE_COMPLETE
	binary.BigEndian.PutUint32(msg[1:5], 4)

	_, err := session.writer.Write(msg)
	if err == nil {
		err = session.writer.Flush()
	}
	return err
}

// ───────────────────────────────────────────────────────────────────────────
// 错误响应消息（Error Response）
// ───────────────────────────────────────────────────────────────────────────

// sendError 发送错误消息
//
// 职责：
//   向客户端发送 SQL 错误或协议错误
//
// 参数:
//   - session: 当前会话上下文
//   - code: PostgreSQL SQLSTATE 错误代码（5 位字符）
//   - message: 错误消息描述
//
// 返回:
//   - error: 发送错误（如果发送失败）
//
// 消息格式（'E'）：
//   'E' + length(int32) + [error_field...]
//   每个错误字段格式：field_type(byte) + value\0
//   字段列表以 \0 结束
//
// 错误字段类型：
//   - 'S': Severity（严重程度） - "ERROR"、"FATAL"、"PANIC"、"WARNING"、"NOTICE"
//   - 'C': Code（SQLSTATE 代码） - 5 位字符，例如 "42601"（语法错误）
//   - 'M': Message（错误消息） - 人类可读的错误描述
//   - 'D': Detail（详细信息） - 可选的额外细节
//   - 'H': Hint（提示） - 可选的修复建议
//   - 'P': Position（位置） - SQL 查询中的错误位置
//
// 当前实现：
//   仅包含 S、C、M 三个必需字段
//
// SQLSTATE 代码示例（由 mapErrorToPostgreSQLCode 生成）：
//   - "00000": 成功
//   - "42601": 语法错误
//   - "42P01": 表不存在
//   - "42703": 列不存在
//   - "0A000": 特性不支持
//
// 发送后行为：
//   - 简单查询协议：发送 ReadyForQuery 保持连接
//   - 扩展查询协议：等待 Sync 消息
func (s *PostgreSQLServer) sendError(session *PostgreSQLSession, code, message string) error {
	msg := make([]byte, 0)
	msg = append(msg, PG_RESP_ERROR)

	// Build error fields
	fields := fmt.Sprintf("S%s\x00C%s\x00M%s\x00\x00", "ERROR", code, message)
	length := 4 + len(fields)

	lengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBytes, uint32(length))
	msg = append(msg, lengthBytes...)
	msg = append(msg, []byte(fields)...)

	_, err := session.writer.Write(msg)
	if err == nil {
		err = session.writer.Flush()
	}
	return err
}

// ═══════════════════════════════════════════════════════════════════════════
// 辅助函数（Helper Functions）
// ═══════════════════════════════════════════════════════════════════════════

// getCommandTag 为查询生成适当的命令标签
//
// 职责：
//   根据 SQL 命令类型和影响的行数，生成符合 PostgreSQL 规范的命令标签
//
// 参数:
//   - query: SQL 查询字符串
//   - rowCount: 受影响的行数
//
// 返回:
//   - string: 命令标签（用于 CommandComplete 消息）
//
// 标签格式：
//   - SELECT: "SELECT n" - 返回 n 行
//   - INSERT: "INSERT 0 n" - 插入 n 行（中间的 0 是 OID，已过时但保留）
//   - UPDATE: "UPDATE n" - 更新 n 行
//   - DELETE: "DELETE n" - 删除 n 行
//   - SHOW/DESCRIBE: "SELECT n" - 被视为 SELECT 查询
//   - 其他: "SELECT 0" - 默认值
//
// 实现逻辑：
//   1. 规范化查询字符串（转大写、去空格）
//   2. 检查查询前缀确定命令类型
//   3. 根据命令类型格式化标签字符串
func (s *PostgreSQLServer) getCommandTag(query string, rowCount int) string {
	queryUpper := strings.ToUpper(strings.TrimSpace(query))

	if strings.HasPrefix(queryUpper, "SELECT") {
		return fmt.Sprintf("SELECT %d", rowCount)
	} else if strings.HasPrefix(queryUpper, "INSERT") {
		return fmt.Sprintf("INSERT 0 %d", rowCount)
	} else if strings.HasPrefix(queryUpper, "UPDATE") {
		return fmt.Sprintf("UPDATE %d", rowCount)
	} else if strings.HasPrefix(queryUpper, "DELETE") {
		return fmt.Sprintf("DELETE %d", rowCount)
	} else if strings.HasPrefix(queryUpper, "SHOW") {
		return fmt.Sprintf("SELECT %d", rowCount)
	} else if strings.HasPrefix(queryUpper, "DESCRIBE") || strings.HasPrefix(queryUpper, "DESC") {
		return fmt.Sprintf("SELECT %d", rowCount)
	}

	return "SELECT 0"
}

// ───────────────────────────────────────────────────────────────────────────
// 类型映射（Type Mapping）
// ───────────────────────────────────────────────────────────────────────────

// getPostgreSQLTypeFromSchema 从 Schema 信息确定 PostgreSQL 类型 OID，回退到数据推断
//
// 职责：
//   确定每个结果列的 PostgreSQL 类型 OID，用于 RowDescription 消息
//
// 参数:
//   - result: 查询结果（包含数据库、表名和列信息）
//   - columnName: 列名
//   - colIndex: 列索引（用于数据推断）
//
// 返回:
//   - uint32: PostgreSQL 类型 OID
//
// 类型推断策略（按优先级）：
//   1. **Schema 推断**（最准确）：
//      - 如果 result.Database 和 result.Table 可用
//      - 从 Catalog 获取 TableInfo 和 Schema
//      - 使用 mapSchemaTypeToPostgreSQL 映射 SeaweedFS 类型到 PostgreSQL OID
//
//   2. **系统列识别**：
//      - _timestamp_ns → PG_TYPE_INT8 (BIGINT)
//      - _key → PG_TYPE_BYTEA (二进制数据)
//      - _source → PG_TYPE_TEXT (文本)
//
//   3. **数据推断**（回退方案）：
//      - 调用 getPostgreSQLTypeFromData
//      - 从第一行非 NULL 数据推断类型
//
// PostgreSQL 类型 OID（常用）：
//   - 16: BOOL
//   - 23: INT4 (INTEGER)
//   - 20: INT8 (BIGINT)
//   - 700: FLOAT4 (REAL)
//   - 701: FLOAT8 (DOUBLE PRECISION)
//   - 25: TEXT
//   - 17: BYTEA
//   - 3802: JSONB
func (s *PostgreSQLServer) getPostgreSQLTypeFromSchema(result *engine.QueryResult, columnName string, colIndex int) uint32 {
	// 【策略 1：从 Schema 推断类型】
	// 这是最准确的方法，因为 Schema 包含了列的真实类型信息
	if result.Database != "" && result.Table != "" {
		// 从 Catalog 获取表信息
		if tableInfo, err := s.sqlEngine.GetCatalog().GetTableInfo(result.Database, result.Table); err == nil {
			if tableInfo.Schema != nil && tableInfo.Schema.RecordType != nil {
				// 在 Schema 的字段列表中查找匹配的列名
				for _, field := range tableInfo.Schema.RecordType.Fields {
					if field.Name == columnName {
						// 找到匹配的字段，使用 Schema 类型映射
						return s.mapSchemaTypeToPostgreSQL(field.Type)
					}
				}
			}
		}
	}

	// 【策略 2：识别系统列】
	// SeaweedFS MQ 查询会返回特殊的系统列
	// 这些列有固定的类型，无需从 Schema 或数据推断
	switch columnName {
	case "_timestamp_ns":
		// 纳秒级时间戳，使用 BIGINT 类型
		return PG_TYPE_INT8 // PostgreSQL BIGINT for nanosecond timestamps
	case "_key":
		// 消息的二进制键，使用 BYTEA 类型
		return PG_TYPE_BYTEA // PostgreSQL BYTEA for binary keys
	case "_source":
		// 消息来源信息，使用 TEXT 类型
		return PG_TYPE_TEXT // PostgreSQL TEXT for source information
	}

	// 【策略 3：回退到数据推断】
	// 如果 Schema 不可用且不是系统列，从第一行数据推断类型
	// 这是最不准确的方法，但总好过返回错误类型
	return s.getPostgreSQLTypeFromData(result.Columns, result.Rows, colIndex)
}

// mapSchemaTypeToPostgreSQL 将 SeaweedFS Schema 类型映射到 PostgreSQL 类型 OID
//
// 职责：
//   将 SeaweedFS protobuf Schema 类型转换为对应的 PostgreSQL 类型 OID
//
// 参数:
//   - fieldType: SeaweedFS Schema 类型定义（schema_pb.Type）
//
// 返回:
//   - uint32: PostgreSQL 类型 OID
//
// 类型映射表：
//
//   **标量类型（ScalarType）**：
//   - BOOL → PG_TYPE_BOOL (16)
//   - INT32 → PG_TYPE_INT4 (23)
//   - INT64 → PG_TYPE_INT8 (20)
//   - FLOAT → PG_TYPE_FLOAT4 (700)
//   - DOUBLE → PG_TYPE_FLOAT8 (701)
//   - BYTES → PG_TYPE_BYTEA (17)
//   - STRING → PG_TYPE_TEXT (25)
//
//   **复合类型**：
//   - ListType（数组） → PG_TYPE_JSONB (3802) - 表示为 JSON 数组
//   - RecordType（嵌套记录） → PG_TYPE_JSONB (3802) - 表示为 JSON 对象
//
//   **默认值**：
//   - 未知类型或 nil → PG_TYPE_TEXT (25)
//
// 设计考虑：
//   - 复合类型映射为 JSONB 以保留结构信息
//   - JSONB 比 JSON 更高效，支持索引和操作符
//   - 默认使用 TEXT 类型确保兼容性
func (s *PostgreSQLServer) mapSchemaTypeToPostgreSQL(fieldType *schema_pb.Type) uint32 {
	if fieldType == nil {
		return PG_TYPE_TEXT
	}

	switch kind := fieldType.Kind.(type) {
	case *schema_pb.Type_ScalarType:
		switch kind.ScalarType {
		case schema_pb.ScalarType_BOOL:
			return PG_TYPE_BOOL
		case schema_pb.ScalarType_INT32:
			return PG_TYPE_INT4
		case schema_pb.ScalarType_INT64:
			return PG_TYPE_INT8
		case schema_pb.ScalarType_FLOAT:
			return PG_TYPE_FLOAT4
		case schema_pb.ScalarType_DOUBLE:
			return PG_TYPE_FLOAT8
		case schema_pb.ScalarType_BYTES:
			return PG_TYPE_BYTEA
		case schema_pb.ScalarType_STRING:
			return PG_TYPE_TEXT
		default:
			return PG_TYPE_TEXT
		}
	case *schema_pb.Type_ListType:
		// For list types, we'll represent them as JSON text
		return PG_TYPE_JSONB
	case *schema_pb.Type_RecordType:
		// For nested record types, we'll represent them as JSON text
		return PG_TYPE_JSONB
	default:
		return PG_TYPE_TEXT
	}
}

// getPostgreSQLTypeFromData 从数据推断 PostgreSQL 类型 OID（传统回退方法）
//
// 职责：
//   当 Schema 信息不可用时，通过采样数据推断列的 PostgreSQL 类型
//
// 参数:
//   - columns: 列名列表（未使用，但保留用于未来扩展）
//   - rows: 数据行列表
//   - colIndex: 需要推断类型的列索引
//
// 返回:
//   - uint32: PostgreSQL 类型 OID
//
// 推断策略：
//   1. 如果没有数据行或列索引越界，返回 PG_TYPE_TEXT
//   2. 遍历所有行，找到第一个非 NULL 值
//   3. 根据 sqltypes.Value 类型判断：
//      - Int8/Int16/Int32 → PG_TYPE_INT4 (INTEGER)
//      - Int64 → PG_TYPE_INT8 (BIGINT)
//      - Float32/Float64 → PG_TYPE_FLOAT8 (DOUBLE PRECISION)
//      - Bit → PG_TYPE_BOOL (BOOLEAN)
//      - Timestamp/Datetime → PG_TYPE_TIMESTAMP
//      - 其他类型 → 尝试字符串解析
//   4. 字符串解析尝试：
//      - 尝试解析为 int32 → PG_TYPE_INT4
//      - 尝试解析为 int64 → PG_TYPE_INT8
//      - 尝试解析为 float64 → PG_TYPE_FLOAT8
//      - "true"/"false" → PG_TYPE_BOOL
//      - 其他 → PG_TYPE_TEXT
//   5. 如果所有行都是 NULL，返回 PG_TYPE_TEXT
//
// 局限性：
//   - 仅采样第一个非 NULL 值，可能不准确
//   - 无法区分 VARCHAR 和 TEXT
//   - 无法推断复合类型
//   - 推荐使用 Schema 推断而不是数据推断
func (s *PostgreSQLServer) getPostgreSQLTypeFromData(columns []string, rows [][]sqltypes.Value, colIndex int) uint32 {
	// 【边界检查】
	// 如果没有数据或列索引无效，默认使用 TEXT 类型
	if len(rows) == 0 || colIndex >= len(rows[0]) {
		return PG_TYPE_TEXT // Default to text
	}

	// 【采样第一个非 NULL 值】
	// 遍历所有行，找到指定列的第一个非 NULL 值进行类型推断
	// 注意：这只是启发式方法，可能不准确（例如第一行是 "123"，但后续行可能是 "abc"）
	for _, row := range rows {
		// 确保列索引有效且值不是 NULL
		if colIndex < len(row) && !row[colIndex].IsNull() {
			value := row[colIndex]

			// 【方法 1：基于 sqltypes 类型推断】
			switch value.Type() {
			case sqltypes.Int8, sqltypes.Int16, sqltypes.Int32:
				// 小整数类型，映射到 PostgreSQL INTEGER
				return PG_TYPE_INT4
			case sqltypes.Int64:
				// 大整数类型，映射到 PostgreSQL BIGINT
				return PG_TYPE_INT8
			case sqltypes.Float32, sqltypes.Float64:
				// 浮点类型，映射到 PostgreSQL DOUBLE PRECISION
				return PG_TYPE_FLOAT8
			case sqltypes.Bit:
				// 布尔类型
				return PG_TYPE_BOOL
			case sqltypes.Timestamp, sqltypes.Datetime:
				// 时间戳类型
				return PG_TYPE_TIMESTAMP
			default:
				// 【方法 2：基于字符串内容推断】
				// 对于未知类型，尝试解析字符串内容判断实际类型
				valueStr := value.ToString()

				// 尝试解析为 32 位整数
				if _, err := strconv.ParseInt(valueStr, 10, 32); err == nil {
					return PG_TYPE_INT4
				}
				// 尝试解析为 64 位整数
				if _, err := strconv.ParseInt(valueStr, 10, 64); err == nil {
					return PG_TYPE_INT8
				}
				// 尝试解析为浮点数
				if _, err := strconv.ParseFloat(valueStr, 64); err == nil {
					return PG_TYPE_FLOAT8
				}
				// 检查是否为布尔值
				if valueStr == "true" || valueStr == "false" {
					return PG_TYPE_BOOL
				}
				// 无法识别，使用 TEXT 类型
				return PG_TYPE_TEXT
			}
		}
	}

	// 【所有行都是 NULL】
	// 无法推断类型，默认使用 TEXT
	return PG_TYPE_TEXT // Default to text
}
