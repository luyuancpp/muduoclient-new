package muduo

import "errors"

// 全局错误定义（对外暴露，供业务层判断错误类型）
// 命名规则：Err+错误场景描述，描述需清晰定位错误原因
var (
	ErrInsufficientData = errors.New("rpc codec: insufficient data for decode")  // 数据不足（拆包场景，如长度前缀未读全）
	ErrInvalidChecksum  = errors.New("rpc codec: checksum mismatch")             // 校验和不匹配（数据完整性校验失败）
	ErrInvalidTag       = errors.New("rpc codec: invalid tag (expected 'RPC0')") // RPC 标签错误（协议头标签不符合约定）
	ErrEmptyMsgType     = errors.New("rpc codec: Msg type is not initialized")   // 未初始化 Protobuf 消息类型（编码前未设置消息体）
	ErrConsumeFailed    = errors.New("rpc codec: consume packet failed")         // 消费数据包失败（Discard/Read 操作失败）
	// 新增 ErrInvalidMsgName：Protobuf 消息名无效（如长度不合法、FullName 不存在）
	ErrInvalidMsgName = errors.New("rpc codec: invalid proto message name (length illegal or fullname not found)")
)
