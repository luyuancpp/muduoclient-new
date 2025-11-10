package muduo

import (
	"encoding/binary"
	"errors"
	"github.com/panjf2000/gnet/v2"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"hash/adler32"
	"log"
)

// -----------------------------------------------------------------------------
// TcpCodec: 通用Protobuf编解码器（基于gnet缓冲区原生操作）
// -----------------------------------------------------------------------------

type TcpCodec struct{}

func NewTcpCodec() *TcpCodec {
	return &TcpCodec{}
}

// Encode: 编码逻辑不变，保持协议兼容性
func (c *TcpCodec) Encode(msg proto.Message) ([]byte, error) {
	// 1. 获取Proto消息描述符（使用FullName避免冲突）
	desc := msg.ProtoReflect().Descriptor()
	if desc == nil {
		return nil, errors.New("empty message descriptor")
	}
	msgTypeName := string(desc.FullName()) + " " // 保留末尾空格（与Decode逻辑对应）
	msgTypeNameData := []byte(msgTypeName)
	nameLen := uint32(len(msgTypeNameData))

	// 2. 序列化Proto消息体
	msgBody, err := proto.MarshalOptions{}.Marshal(msg)
	if err != nil {
		return nil, err
	}

	// 3. 构建msg数据部分（nameLen + typeName + body）
	msgData := make([]byte, 0, 4+len(msgTypeNameData)+len(msgBody))
	nameLenBuf := uint32ToBytes(nameLen)
	msgData = append(msgData, nameLenBuf...)
	uint32BufPool.Put(nameLenBuf) // 归还切片

	msgData = append(msgData, msgTypeNameData...)
	msgData = append(msgData, msgBody...)

	// 4. 计算校验和并拼接
	checksum := adler32.Checksum(msgData)
	checksumBuf := uint32ToBytes(checksum)
	defer uint32BufPool.Put(checksumBuf)
	msgData = append(msgData, checksumBuf...)

	// 5. 拼接总长度前缀（4字节）
	totalLen := uint32(len(msgData))
	totalLenBuf := uint32ToBytes(totalLen)
	defer uint32BufPool.Put(totalLenBuf)
	fullPacket := append(totalLenBuf, msgData...)

	return fullPacket, nil
}

// Decode: 核心修改！直接返回解析后的proto.Message，无需依赖ConnContext
func (c *TcpCodec) Decode(conn gnet.Conn) (proto.Message, error) {
	// 1. 获取连接上下文（仅用于获取ConnID，不存储消息）
	ctx, ok := conn.Context().(*ConnContext)
	if !ok {
		ctx = &ConnContext{} // 即使未初始化，也能正常解析（仅ConnID为空）
		conn.SetContext(ctx)
	}
	connID := ctx.ConnID // 提前获取连接ID，方便日志定位

	// 2. 循环解析gnet缓冲区（处理粘包/拆包）
	for {
		// 2.1 检查缓冲区是否有足够数据解析总长度前缀（4字节）
		if conn.InboundBuffered() < 4 {
			return nil, ErrInsufficientData
		}

		// 2.2 Peek并解析总长度（仅读取4字节，不消费）
		lenPrefix, err := conn.Peek(4)
		if err != nil {
			log.Printf("Decode error (connID: %s): peek length prefix failed: %v", connID, err)
			return nil, err
		}
		totalLen := binary.BigEndian.Uint32(lenPrefix)
		requiredTotalLen := 4 + int(totalLen) // 完整包总长度

		// 2.3 校验总长度合理性
		if totalLen == 0 || requiredTotalLen > 10*1024*1024 { // 最大10MB
			log.Printf("Decode error (connID: %s): invalid totalLen %d", connID, totalLen)
			conn.Discard(4) // 丢弃无效前缀
			return nil, ErrInvalidPacketLen
		}

		// 2.4 检查缓冲区是否有完整包
		if conn.InboundBuffered() < requiredTotalLen {
			return nil, ErrInsufficientData
		}

		// 2.5 Peek完整包数据（不消费）
		fullPacket, err := conn.Peek(requiredTotalLen)
		if err != nil {
			log.Printf("Decode error (connID: %s): peek full packet failed: %v", connID, err)
			return nil, err
		}

		// 2.6 分离msgData和校验和
		msgDataEnd := 4 + int(totalLen) - 4 // 排除4字节校验和
		msgData := fullPacket[4:msgDataEnd]
		checksumExpected := binary.BigEndian.Uint32(fullPacket[msgDataEnd:requiredTotalLen])

		// 2.7 校验和验证
		if adler32.Checksum(msgData) != checksumExpected {
			log.Printf("Decode error (connID: %s): invalid checksum (expected %d, actual %d)",
				connID, checksumExpected, adler32.Checksum(msgData))
			conn.Discard(requiredTotalLen)
			return nil, ErrInvalidChecksum
		}

		// 2.8 解析类型名长度和类型名
		if len(msgData) < 4 {
			log.Printf("Decode error (connID: %s): insufficient data for nameLen", connID)
			conn.Discard(requiredTotalLen)
			return nil, ErrInsufficientData
		}
		nameLen := binary.BigEndian.Uint32(msgData[:4])
		if nameLen < 1 || int(nameLen+4) > len(msgData) {
			log.Printf("Decode error (connID: %s): invalid nameLen %d (msgData len %d)",
				connID, nameLen, len(msgData))
			conn.Discard(requiredTotalLen)
			return nil, ErrInvalidMsgName
		}

		// 2.9 解析Proto消息类型
		typeNameStr := string(msgData[4 : 4+nameLen-1]) // 去掉末尾空格
		msgType, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName(typeNameStr))
		if err != nil {
			log.Printf("Decode error (connID: %s): find message type failed (name=%s): %v",
				connID, typeNameStr, err)
			conn.Discard(requiredTotalLen)
			return nil, err
		}

		// 2.10 反序列化消息体
		bodyStart := 4 + nameLen
		msg := msgType.New().Interface()
		if err := proto.Unmarshal(msgData[bodyStart:], msg); err != nil {
			log.Printf("Decode error (connID: %s): unmarshal message failed: %v", connID, err)
			conn.Discard(requiredTotalLen)
			return nil, err
		}

		// 2.11 消费缓冲区数据
		if _, err := conn.Discard(requiredTotalLen); err != nil {
			log.Printf("Decode error (connID: %s): discard packet failed: %v", connID, err)
			return nil, err
		}

		// 2.12 直接返回解析后的消息（无需存入ctx）
		return msg, nil
	}
}
