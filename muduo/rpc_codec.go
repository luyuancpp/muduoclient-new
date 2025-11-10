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
// rpcConnContext：仅用于辅助（可选，若不需要可删除）
type rpcConnContext struct{} // 不再存储msg，仅保留结构兼容旧代码（若无需兼容可直接删除）

// RpcCodec：RPC协议编解码器（基于Protobuf+TCP）
type RpcCodec struct {
	MsgType proto.Message // 预定义的RPC消息类型（如&pb.UserRequest{}）
}

// ------------------------------ 编解码器构造函数 ------------------------------
func NewRpcCodec(msgType proto.Message) *RpcCodec {
	if msgType == nil {
		log.Panic("rpc codec: message type cannot be nil")
	}
	return &RpcCodec{MsgType: msgType}
}

// ------------------------------ 编码逻辑（Encode） ------------------------------
// 保留原有逻辑，协议格式不变
func (c *RpcCodec) Encode(msg proto.Message) ([]byte, error) {
	if msg == nil {
		return nil, errors.New("rpc codec: encode nil message")
	}

	tag := []byte("RPC0")
	body, err := proto.MarshalOptions{
		AllowPartial:  false,
		Deterministic: true,
	}.Marshal(msg)
	if err != nil {
		return nil, errors.Join(errors.New("rpc codec: marshal body failed"), err)
	}

	payload := make([]byte, 0, len(tag)+len(body))
	payload = append(payload, tag...)
	payload = append(payload, body...)

	checksum := adler32.Checksum(payload)
	checksumBuf := uint32BufPool.Get().([]byte)
	defer uint32BufPool.Put(checksumBuf)
	binary.BigEndian.PutUint32(checksumBuf, checksum)

	totalDataLen := uint32(len(payload) + 4)
	totalLenBuf := uint32BufPool.Get().([]byte)
	defer uint32BufPool.Put(totalLenBuf)
	binary.BigEndian.PutUint32(totalLenBuf, totalDataLen)

	fullPacket := make([]byte, 0, 4+len(payload)+4)
	fullPacket = append(fullPacket, totalLenBuf...)
	fullPacket = append(fullPacket, payload...)
	fullPacket = append(fullPacket, checksumBuf...)

	return fullPacket, nil
}

// ------------------------------ 解码逻辑（Decode） ------------------------------
// 核心修改：直接返回proto.Message，不依赖上下文存储
func (c *RpcCodec) Decode(conn gnet.Conn) (proto.Message, error) {
	// 1. 校验预定义消息类型
	if c.MsgType == nil {
		return nil, ErrEmptyMsgType
	}

	// 2. 循环解析gnet缓冲区（处理粘包/拆包）
	for {
		// 2.1 检查总长度前缀（4字节）
		if conn.InboundBuffered() < 4 {
			return nil, ErrInsufficientData
		}

		// 2.2 解析总长度
		totalLenBuf, err := conn.Peek(4)
		if err != nil {
			return nil, errors.Join(errors.New("rpc codec: peek total length failed"), err)
		}
		totalDataLen := binary.BigEndian.Uint32(totalLenBuf)
		requiredTotalLen := 4 + int(totalDataLen)

		// 2.3 校验总长度合理性
		if totalDataLen == 0 || requiredTotalLen > 10*1024*1024 {
			remoteAddr := conn.RemoteAddr().String()
			log.Printf("rpc codec: invalid totalDataLen %d (conn: %s)", totalDataLen, remoteAddr)
			conn.Discard(4)
			return nil, ErrInvalidPacketLen
		}

		// 2.4 检查完整包数据
		if conn.InboundBuffered() < requiredTotalLen {
			return nil, ErrInsufficientData
		}

		// 2.5 Peek完整包
		fullPacket, err := conn.Peek(requiredTotalLen)
		if err != nil {
			return nil, errors.Join(errors.New("rpc codec: peek full packet failed"), err)
		}

		// 2.6 提取Payload和校验和
		payloadEnd := 4 + int(totalDataLen) - 4
		payload := fullPacket[4:payloadEnd]
		checksumExpected := binary.BigEndian.Uint32(fullPacket[payloadEnd:requiredTotalLen])

		// 2.7 校验和验证
		if adler32.Checksum(payload) != checksumExpected {
			remoteAddr := conn.RemoteAddr().String()
			log.Printf("rpc codec: invalid checksum (conn: %s)", remoteAddr)
			conn.Discard(requiredTotalLen)
			return nil, ErrInvalidChecksum
		}

		// 2.8 校验RPC标签
		if len(payload) < 4 || string(payload[:4]) != "RPC0" {
			remoteAddr := conn.RemoteAddr().String()
			log.Printf("rpc codec: invalid tag (conn: %s)", remoteAddr)
			conn.Discard(requiredTotalLen)
			return nil, ErrInvalidTag
		}

		// 2.9 反序列化消息体
		body := payload[4:]
		msgTypeName := c.MsgType.ProtoReflect().Descriptor().FullName()
		msgType, err := protoregistry.GlobalTypes.FindMessageByName(msgTypeName)
		if err != nil {
			remoteAddr := conn.RemoteAddr().String()
			log.Printf("rpc codec: find message type failed (name=%s, conn: %s): %v", msgTypeName, remoteAddr, err)
			conn.Discard(requiredTotalLen)
			return nil, errors.Join(errors.New("rpc codec: find message type failed"), err)
		}

		msg := msgType.New().Interface()
		unmarshalOptions := proto.UnmarshalOptions{
			AllowPartial:   false,
			DiscardUnknown: true,
		}
		if err := unmarshalOptions.Unmarshal(body, msg); err != nil {
			remoteAddr := conn.RemoteAddr().String()
			log.Printf("rpc codec: unmarshal body failed (conn: %s): %v", remoteAddr, err)
			conn.Discard(requiredTotalLen)
			return nil, errors.Join(errors.New("rpc codec: unmarshal body failed"), err)
		}

		// 2.10 消费缓冲区数据
		if _, err := conn.Discard(requiredTotalLen); err != nil {
			remoteAddr := conn.RemoteAddr().String()
			log.Printf("rpc codec: discard packet failed (conn: %s): %v", remoteAddr, err)
			return nil, errors.Join(errors.New("rpc codec: discard packet failed"), err)
		}

		// 2.11 直接返回解码后的消息
		return msg, nil
	}
}
