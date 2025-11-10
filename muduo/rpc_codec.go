package muduo

import (
	"encoding/binary"
	"errors"
	"github.com/panjf2000/gnet/v2"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoregistry"
	"hash/adler32"
	"log"
)

// ------------------------------ 连接上下文与编解码器结构体 ------------------------------
// rpcConnContext：gnet连接上下文，仅存储解码结果（不存数据缓存）
type rpcConnContext struct {
	msg proto.Message // 解码成功后的Protobuf消息
}

// RpcCodec：RPC协议编解码器（基于Protobuf+TCP）
type RpcCodec struct {
	MsgType proto.Message // 预定义的RPC消息类型（如&pb.UserRequest{}）
}

// ------------------------------ 编解码器构造函数 ------------------------------
func NewRpcCodec(msgType proto.Message) *RpcCodec {
	if msgType == nil {
		log.Panic("rpc codec: message type cannot be nil") // Panic：构造函数不允许空类型
	}
	return &RpcCodec{MsgType: msgType}
}

// ------------------------------ 编码逻辑（Encode） ------------------------------
// 保留原有逻辑，协议格式不变：[4字节总长度] + [4字节"RPC0"标签] + [消息体] + [4字节校验和]
func (c *RpcCodec) Encode(msg proto.Message) ([]byte, error) {
	// 1. 校验消息合法性
	if msg == nil {
		return nil, errors.New("rpc codec: encode nil message")
	}

	// 2. 固定RPC标签（4字节"RPC0"，用于解码时识别RPC包）
	tag := []byte("RPC0")

	// 3. 序列化Protobuf消息体（确定性序列化，确保跨语言/版本兼容性）
	body, err := proto.MarshalOptions{
		AllowPartial:  false, // 不允许部分序列化（确保消息完整）
		Deterministic: true,  // 相同消息生成相同字节流（避免哈希不一致）
	}.Marshal(msg)
	if err != nil {
		return nil, errors.Join(errors.New("rpc codec: marshal body failed"), err)
	}

	// 4. 构建Payload（标签+消息体）：校验和仅计算此部分，确保数据完整性
	payload := make([]byte, 0, len(tag)+len(body))
	payload = append(payload, tag...)
	payload = append(payload, body...)

	// 5. 计算Adler32校验和（轻量、高效，适合TCP数据完整性校验）
	checksum := adler32.Checksum(payload)
	checksumBuf := uint32BufPool.Get().([]byte)
	defer uint32BufPool.Put(checksumBuf) // 延迟归还内存池
	binary.BigEndian.PutUint32(checksumBuf, checksum)

	// 6. 计算总长度（Payload长度 + 4字节校验和）
	totalDataLen := uint32(len(payload) + 4)
	totalLenBuf := uint32BufPool.Get().([]byte)
	defer uint32BufPool.Put(totalLenBuf)
	binary.BigEndian.PutUint32(totalLenBuf, totalDataLen)

	// 7. 组装完整TCP数据包：[4字节总长度] + [Payload] + [4字节校验和]
	fullPacket := make([]byte, 0, 4+len(payload)+4)
	fullPacket = append(fullPacket, totalLenBuf...)
	fullPacket = append(fullPacket, payload...)
	fullPacket = append(fullPacket, checksumBuf...)

	return fullPacket, nil
}

// ------------------------------ 解码逻辑（Decode） ------------------------------
// 重构核心：完全基于gnet缓冲区操作，无本地缓存，解决Discard同步问题
func (c *RpcCodec) Decode(conn gnet.Conn) error {
	// 1. 初始化/获取连接上下文（仅存解码结果，无数据缓存）
	ctx, ok := conn.Context().(*rpcConnContext)
	if !ok {
		ctx = &rpcConnContext{}
		conn.SetContext(ctx)
	}

	// 2. 校验预定义消息类型（避免解码时无目标类型）
	if c.MsgType == nil {
		return ErrEmptyMsgType
	}

	// 3. 循环解析：处理粘包/拆包（一次解析一个完整包）
	for {
		// 3.1 第一步：检查缓冲区是否有足够数据解析总长度前缀（4字节）
		if conn.InboundBuffered() < 4 {
			return ErrInsufficientData
		}

		// 3.2 解析总长度前缀（不消费数据）
		totalLenBuf, err := conn.Peek(4)
		if err != nil {
			return errors.Join(errors.New("rpc codec: peek total length failed"), err)
		}
		totalDataLen := binary.BigEndian.Uint32(totalLenBuf)
		requiredTotalLen := 4 + int(totalDataLen) // 完整包总长度（4字节前缀 + 数据部分）

		// 3.3 校验总长度合理性（避免恶意数据或协议错误）
		if totalDataLen == 0 || requiredTotalLen > 10*1024*1024 { // 最大10MB
			log.Printf("rpc codec: invalid totalDataLen %d (conn: %s)", totalDataLen, conn.RemoteAddr())
			conn.Discard(4) // 丢弃无效的长度前缀
			return ErrInvalidPacketLen
		}

		// 3.4 检查缓冲区是否有完整包（实时状态，无同步问题）
		if conn.InboundBuffered() < requiredTotalLen {
			return ErrInsufficientData
		}

		// 3.5 Peek完整包数据（不消费，仅读取）
		fullPacket, err := conn.Peek(requiredTotalLen)
		if err != nil {
			return errors.Join(errors.New("rpc codec: peek full packet failed"), err)
		}

		// 3.6 提取Payload和校验和（与编码逻辑严格对齐）
		payloadEnd := 4 + int(totalDataLen) - 4 // Payload结束位置（排除4字节校验和）
		payload := fullPacket[4:payloadEnd]
		checksumExpected := binary.BigEndian.Uint32(fullPacket[payloadEnd:requiredTotalLen])

		// 3.7 校验和验证（检测数据篡改或传输错误）
		if adler32.Checksum(payload) != checksumExpected {
			log.Printf("rpc codec: invalid checksum (conn: %s)", conn.RemoteAddr())
			conn.Discard(requiredTotalLen) // 立即丢弃无效包
			return ErrInvalidChecksum
		}

		// 3.8 校验RPC标签（确保是目标RPC包，过滤无效数据）
		if len(payload) < 4 || string(payload[:4]) != "RPC0" {
			log.Printf("rpc codec: invalid tag (conn: %s)", conn.RemoteAddr())
			conn.Discard(requiredTotalLen) // 立即丢弃无效包
			return ErrInvalidTag
		}

		// 3.9 提取消息体并反序列化
		body := payload[4:] // 去掉"RPC0"标签，获取纯消息体
		// 从Protobuf全局注册表查找消息类型（基于预定义的MsgType）
		msgTypeName := c.MsgType.ProtoReflect().Descriptor().FullName()
		msgType, err := protoregistry.GlobalTypes.FindMessageByName(msgTypeName)
		if err != nil {
			log.Printf("rpc codec: find message type failed (name=%s, conn: %s): %v", msgTypeName, conn.RemoteAddr(), err)
			conn.Discard(requiredTotalLen)
			return errors.Join(errors.New("rpc codec: find message type failed"), err)
		}

		// 创建消息实例并反序列化（忽略未知字段，提高版本兼容性）
		msg := msgType.New().Interface()
		unmarshalOptions := proto.UnmarshalOptions{
			AllowPartial:   false, // 不允许部分反序列化（确保消息完整）
			DiscardUnknown: true,  // 忽略未知字段（提高兼容性）
		}
		if err := unmarshalOptions.Unmarshal(body, msg); err != nil {
			log.Printf("rpc codec: unmarshal body failed (conn: %s): %v", conn.RemoteAddr(), err)
			conn.Discard(requiredTotalLen)
			return errors.Join(errors.New("rpc codec: unmarshal body failed"), err)
		}

		// 3.10 关键：解析成功后，立即消费缓冲区数据（无延迟，无同步问题）
		if _, err := conn.Discard(requiredTotalLen); err != nil {
			log.Printf("rpc codec: discard packet failed (conn: %s): %v", conn.RemoteAddr(), err)
			return errors.Join(errors.New("rpc codec: discard packet failed"), err)
		}

		// 3.11 保存解码结果到上下文
		ctx.msg = msg
		break // 解析成功一个包，退出循环（避免阻塞事件循环）
	}

	return nil
}
