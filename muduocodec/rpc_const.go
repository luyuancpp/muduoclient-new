package muduocodec

import "errors"

// 全局错误定义（对外暴露，供业务层判断错误类型）
var (
	ErrInsufficientData = errors.New("rpc codec: insufficient data for decode")  // 数据不足（拆包）
	ErrInvalidChecksum  = errors.New("rpc codec: checksum mismatch")             // 校验和不匹配
	ErrInvalidTag       = errors.New("rpc codec: invalid tag (expected 'RPC0')") // RPC 标签错误
	ErrEmptyMsgType     = errors.New("rpc codec: msg type is not initialized")   // 未初始化 Protobuf 消息类型
	ErrConsumeFailed    = errors.New("rpc codec: consume packet failed")         // 消费数据包失败
)
