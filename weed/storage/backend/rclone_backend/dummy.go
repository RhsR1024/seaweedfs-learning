// Package rclone_backend 提供基于 Rclone 的远程存储后端实现
//
// 本文件是一个占位文件（dummy file），用于在未启用 rclone 构建标签时提供包定义。
//
// 构建标签说明：
//   - 主要实现在 rclone_backend.go 中，需要 `rclone` 构建标签
//   - 本文件无构建标签限制，确保包在任何情况下都能编译
//
// 作用：
//   - 防止在没有 rclone 标签时出现 "no buildable Go source files" 错误
//   - 提供空包定义，满足 Go 编译器的包结构要求
//
// 使用场景：
//   - 默认编译：go build（不包含 rclone 功能，使用本文件）
//   - 启用 rclone：go build -tags rclone（使用完整实现）
package rclone_backend
