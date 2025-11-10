package security

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// EncodedJwt JWT 编码后的字符串类型
// 表示经过签名的 JWT token 字符串，格式为：header.payload.signature
type EncodedJwt string

// SigningKey JWT 签名密钥类型
// 用于对 JWT token 进行签名和验证的密钥
// 使用 []byte 类型以支持任意字节序列作为密钥
type SigningKey []byte

// SeaweedFileIdClaims 文件级别的 JWT Claims
//
// 这个 Claims 结构由 Master 服务器创建，Volume 服务器验证。
// 它限制了 JWT 只能访问单个特定的文件。
//
// 工作流程：
//  1. 客户端请求上传文件
//  2. Master 分配文件 ID (fid) 和 Volume 服务器地址
//  3. Master 生成包含该 fid 的 JWT token
//  4. 客户端使用 JWT token 向 Volume 服务器上传文件
//  5. Volume 服务器验证 JWT 中的 fid 是否匹配
//
// 字段说明：
//
//	Fid: 文件 ID，格式通常为 "volumeId,needleId"（如 "3,01637037d6"）
//	RegisteredClaims: 标准 JWT claims（exp, nbf, iat 等）
type SeaweedFileIdClaims struct {
	Fid                  string `json:"fid"` // 文件 ID，限制此 token 只能访问特定文件
	jwt.RegisteredClaims        // 嵌入标准 claims
}

// SeaweedFilerClaims Filer 级别的 JWT Claims
//
// 这个 Claims 结构用于 Filer 服务器的 API 认证。
// 例如，由 S3 代理服务器创建，Filer 服务器验证。
//
// 当前实现：
// 目前只包含标准的 JWT claims，但设计上预留了扩展空间，
// 未来可能添加更细粒度的权限控制，如：
//   - 允许访问的路径范围
//   - 读写权限控制
//   - 配额限制
//
// 使用场景：
//   - S3 API 认证
//   - WebDAV 认证
//   - Filer HTTP API 认证
type SeaweedFilerClaims struct {
	jwt.RegisteredClaims // 标准 JWT claims
}

// GenJwtForVolumeServer 为 Volume 服务器生成 JWT token
//
// 此函数生成一个限制了文件 ID 的 JWT token，用于授权客户端
// 访问 Volume 服务器上的特定文件。
//
// 安全机制：
//  1. 使用 HMAC-SHA256 算法签名
//  2. Token 绑定到特定的文件 ID (fid)
//  3. 设置过期时间，防止 token 被长期滥用
//  4. 如果签名密钥为空，返回空字符串（开发模式）
//
// 参数：
//
//	signingKey: JWT 签名密钥，为空时不生成 token
//	expiresAfterSec: token 有效期（秒），0 或负数表示永不过期（不推荐）
//	fileId: 文件 ID，格式为 "volumeId,needleId"
//
// 返回：
//
//	EncodedJwt: 编码后的 JWT token 字符串，格式为 "header.payload.signature"
//	  如果签名失败或密钥为空，返回空字符串
//
// 使用场景：
//  1. Master 分配文件 ID 后生成 token
//  2. 客户端上传文件时携带 token
//  3. Volume 服务器验证 token 和文件 ID 匹配
//
// 示例：
//
//	token := GenJwtForVolumeServer(
//	    SigningKey("secret"),
//	    10,              // 10 秒有效期
//	    "3,01637037d6",  // 文件 ID
//	)
//	// token 格式: "eyJhbGc...header.eyJma...payload.SflKx...signature"
func GenJwtForVolumeServer(signingKey SigningKey, expiresAfterSec int, fileId string) EncodedJwt {
	// 如果没有配置签名密钥，返回空字符串（开发模式，不启用认证）
	if len(signingKey) == 0 {
		return ""
	}

	// 创建 claims，包含文件 ID 和标准字段
	claims := SeaweedFileIdClaims{
		fileId,
		jwt.RegisteredClaims{},
	}

	// 设置过期时间
	if expiresAfterSec > 0 {
		// 计算过期时间点：当前时间 + 有效期
		claims.ExpiresAt = jwt.NewNumericDate(time.Now().Add(time.Second * time.Duration(expiresAfterSec)))
	}
	// 注意：如果 expiresAfterSec <= 0，则不设置过期时间，token 永久有效

	// 创建 JWT token，使用 HS256 (HMAC-SHA256) 算法
	t := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)

	// 使用密钥签名 token
	encoded, e := t.SignedString([]byte(signingKey))
	if e != nil {
		// 签名失败，记录错误并返回空字符串
		glog.V(0).Infof("Failed to sign claims %+v: %v", t.Claims, e)
		return ""
	}
	// 类型转换（type conversion），将 string 类型的 encoded 变量转换为 EncodedJwt 类型
	return EncodedJwt(encoded)
}

// GenJwtForFilerServer 为 Filer 服务器生成 JWT token
//
// 此函数创建一个用于 Filer API 认证的 JSON Web Token。
// 与 GenJwtForVolumeServer 不同，这个 token 不绑定到特定文件，
// 而是授权访问整个 Filer API。
//
// 安全机制：
//  1. 使用 HMAC-SHA256 算法签名
//  2. 设置过期时间限制 token 有效期
//  3. 如果签名密钥为空，返回空字符串（开发模式）
//
// 参数：
//
//	signingKey: JWT 签名密钥，为空时不生成 token
//	expiresAfterSec: token 有效期（秒），0 或负数表示永不过期（不推荐）
//
// 返回：
//
//	EncodedJwt: 编码后的 JWT token 字符串
//	  如果签名失败或密钥为空，返回空字符串
//
// 使用场景：
//   - S3 API 网关：生成 token 用于访问 Filer
//   - WebDAV 服务：认证后生成 token
//   - HTTP API：长期访问 Filer 的客户端
//
// 与 GenJwtForVolumeServer 的区别：
//   - VolumeServer token：绑定到单个文件，短期有效（10秒）
//   - FilerServer token：授权访问整个 API，较长有效期（60秒或更长）
//
// 示例：
//
//	token := GenJwtForFilerServer(
//	    SigningKey("filer-secret"),
//	    3600,  // 1 小时有效期
//	)
func GenJwtForFilerServer(signingKey SigningKey, expiresAfterSec int) EncodedJwt {
	// 如果没有配置签名密钥，返回空字符串（开发模式）
	if len(signingKey) == 0 {
		return ""
	}

	// 创建 Filer claims，只包含标准字段
	claims := SeaweedFilerClaims{
		jwt.RegisteredClaims{},
	}

	// 设置过期时间
	if expiresAfterSec > 0 {
		claims.ExpiresAt = jwt.NewNumericDate(time.Now().Add(time.Second * time.Duration(expiresAfterSec)))
	}

	// 创建 JWT token，使用 HS256 算法
	t := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)

	// 使用密钥签名 token
	encoded, e := t.SignedString([]byte(signingKey))
	if e != nil {
		// 签名失败，记录错误并返回空字符串
		glog.V(0).Infof("Failed to sign claims %+v: %v", t.Claims, e)
		return ""
	}
	// 类型转换（type conversion），将 string 类型的 encoded 变量转换为 EncodedJwt 类型
	return EncodedJwt(encoded)
}

// GetJwt 从 HTTP 请求中提取 JWT token
//
// 此函数按照优先级顺序从三个位置尝试获取 JWT token：
//  1. URL 查询参数 "jwt"（最高优先级）
//  2. HTTP Authorization 头（Bearer token）
//  3. HTTP-only Cookie "AT"（最低优先级）
//
// 优先级设计考虑：
//   - URL 参数：便于浏览器直接访问，如分享链接
//   - Authorization 头：标准的 HTTP 认证方式，适合 API 调用
//   - Cookie：适合 Web 应用，自动携带，更安全（HttpOnly）
//
// 参数：
//
//	r: HTTP 请求对象
//
// 返回：
//
//	EncodedJwt: 提取到的 JWT token 字符串，未找到时返回空字符串
//
// 使用示例：
//
//  1. URL 参数方式：
//     GET /3,01637037d6.jpg?jwt=eyJhbGc...
//
//  2. Authorization 头方式：
//     GET /3,01637037d6.jpg
//     Authorization: Bearer eyJhbGc...
//
//  3. Cookie 方式：
//     GET /3,01637037d6.jpg
//     Cookie: AT=eyJhbGc...
//
// 安全注意事项：
//   - URL 参数会出现在日志中，可能泄露 token
//   - 推荐生产环境使用 Authorization 头或 Cookie
//   - Cookie 应设置 HttpOnly 和 Secure 标志
func GetJwt(r *http.Request) EncodedJwt {

	// 第一优先级：从 URL 查询参数获取 token
	// 示例：?jwt=eyJhbGc...
	tokenStr := r.URL.Query().Get("jwt")

	// 第二优先级：从 Authorization 头获取 token
	if tokenStr == "" {
		bearer := r.Header.Get("Authorization")
		// 检查是否是 Bearer token 格式
		// 格式：Authorization: Bearer <token>
		if len(bearer) > 7 && strings.ToUpper(bearer[0:6]) == "BEARER" {
			// 提取 "Bearer " 之后的 token 部分
			tokenStr = bearer[7:]
		}
	}

	// 第三优先级：从 HTTP-only Cookie 获取 token
	if tokenStr == "" {
		// Cookie 名称为 "AT" (Access Token)
		token, err := r.Cookie("AT")
		if err == nil {
			tokenStr = token.Value
		}
	}
	// 类型转换（type conversion），将 string 类型的 encoded 变量转换为 EncodedJwt 类型
	return EncodedJwt(tokenStr)
}

// DecodeJwt 解码并验证 JWT token
//
// 此函数解析 JWT token 字符串，验证签名，并提取 claims。
// 它会自动检查标准的时间相关 claims：
//   - exp (Expiration Time): 过期时间，token 不能在此时间之后使用
//   - nbf (Not Before): 生效时间，token 不能在此时间之前使用
//
// 安全验证：
//  1. 验证签名算法是否为 HMAC 系列（防止算法替换攻击）
//  2. 使用签名密钥验证 token 完整性
//  3. 自动验证 exp 和 nbf claims
//
// 参数：
//
//	signingKey: 用于验证签名的密钥（必须与生成 token 时的密钥相同）
//	tokenString: 编码的 JWT token 字符串
//	claims: 用于存储解析结果的 claims 对象指针
//	  可以是 SeaweedFileIdClaims 或 SeaweedFilerClaims
//
// 返回：
//
//	token: 解析后的 JWT token 对象（如果成功）
//	err: 错误信息，可能的错误包括：
//	  - 签名验证失败
//	  - token 已过期 (exp)
//	  - token 尚未生效 (nbf)
//	  - 格式错误
//	  - 算法不匹配
//
// 使用示例：
//
//  1. 验证 Volume Server token：
//     var claims SeaweedFileIdClaims
//     token, err := DecodeJwt(signingKey, jwtString, &claims)
//     if err != nil {
//     // token 无效
//     }
//     // 检查 claims.Fid 是否匹配请求的文件 ID
//
//  2. 验证 Filer Server token：
//     var claims SeaweedFilerClaims
//     token, err := DecodeJwt(signingKey, jwtString, &claims)
//     if err != nil {
//     // token 无效
//     }
//
// 安全注意事项：
//   - 始终检查返回的 error，不要使用未验证的 claims
//   - 确保使用正确的签名密钥
//   - 验证 claims 中的业务逻辑（如 fid 是否匹配）
func DecodeJwt(signingKey SigningKey, tokenString EncodedJwt, claims jwt.Claims) (token *jwt.Token, err error) {
	// 解析 JWT token，同时验证签名和标准 claims (exp, nbf)
	return jwt.ParseWithClaims(string(tokenString), claims, func(token *jwt.Token) (interface{}, error) {
		// 验证签名算法
		// 必须是 HMAC 系列算法 (HS256, HS384, HS512)
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			// 算法不匹配，可能是攻击尝试（如算法替换攻击）
			return nil, fmt.Errorf("unknown token method")
		}
		// 返回签名密钥用于验证
		return []byte(signingKey), nil
	})
}
