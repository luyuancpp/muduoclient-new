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

// 全局错误定义（确保与项目其他模块兼容）
var (
	ErrInvalidPacketLen = errors.New("invalid packet total length")
)

// -----------------------------------------------------------------------------
// TcpCodec: 通用Protobuf编解码器（基于gnet缓冲区原生操作）
// -----------------------------------------------------------------------------

type TcpCodec struct{}

func NewTcpCodec() *TcpCodec {
	return &TcpCodec{}
}

// Encode: 编码Proto消息为TCP协议格式（保留原逻辑，优化类型名避免冲突）
func (c *TcpCodec) Encode(msg proto.Message) ([]byte, error) {
	// 1. 获取Proto消息描述符（优先用FullName避免类型冲突）
	desc := msg.ProtoReflect().Descriptor()
	if desc == nil {
		return nil, errors.New("empty message descriptor")
	}
	// 保留原格式的空格，但使用完整类型名（包路径+消息名）
	msgTypeName := string(desc.FullName()) + " "
	msgTypeNameData := []byte(msgTypeName)
	nameLen := uint32(len(msgTypeNameData))

	// 2. 序列化Proto消息体（v2接口）
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
	checksum := adler32.Checksum(msgData) // 校验和仅计算msgData部分
	checksumBuf := uint32ToBytes(checksum)
	defer uint32BufPool.Put(checksumBuf)
	msgData = append(msgData, checksumBuf...)

	// 5. 拼接总长度前缀（4字节）
	totalLen := uint32(len(msgData)) // 总长度 = nameLen(4) + typeName + body + checksum(4)
	totalLenBuf := uint32ToBytes(totalLen)
	defer uint32BufPool.Put(totalLenBuf)
	fullPacket := append(totalLenBuf, msgData...)

	return fullPacket, nil
}

// Decode: 重构核心！完全基于gnet缓冲区操作，无本地缓存同步问题
func (c *TcpCodec) Decode(conn gnet.Conn) error {
	// 1. 获取/初始化连接上下文（仅存解码结果，不存数据缓存）
	ctx, ok := conn.Context().(*ConnContext)
	if !ok {
		ctx = &ConnContext{}
		conn.SetContext(ctx)
	}

	// 2. 循环解析gnet缓冲区（处理粘包/拆包）
	for {
		// 2.1 第一步：检查缓冲区是否有足够数据解析总长度前缀（4字节）
		if conn.InboundBuffered() < 4 {
			return ErrInsufficientData
		}

		// 2.2 Peek并解析总长度（仅读取4字节，不消费）
		lenPrefix, err := conn.Peek(4)
		if err != nil {
			log.Printf("Decode error (connID: %s): peek length prefix failed: %v", ctx.ConnID, err)
			return err
		}
		totalLen := binary.BigEndian.Uint32(lenPrefix)
		requiredTotalLen := 4 + int(totalLen) // 完整包总长度（前缀4字节 + 数据部分）

		// 2.3 校验总长度合理性（避免恶意数据或协议错误）
		if totalLen == 0 || requiredTotalLen > 10*1024*1024 { // 最大10MB
			log.Printf("Decode error (connID: %s): invalid totalLen %d", ctx.ConnID, totalLen)
			conn.Discard(4) // 丢弃无效的长度前缀
			return ErrInvalidPacketLen
		}

		// 2.4 检查缓冲区是否有完整包（实时状态，无同步问题）
		if conn.InboundBuffered() < requiredTotalLen {
			return ErrInsufficientData
		}

		// 2.5 Peek完整包数据（不消费，仅读取）
		fullPacket, err := conn.Peek(requiredTotalLen)
		if err != nil {
			log.Printf("Decode error (connID: %s): peek full packet failed: %v", ctx.ConnID, err)
			return err
		}

		// 2.6 分离msgData和校验和（遵循协议格式）
		msgDataEnd := 4 + int(totalLen) - 4 // 排除最后4字节校验和
		msgData := fullPacket[4:msgDataEnd]
		checksumExpected := binary.BigEndian.Uint32(fullPacket[msgDataEnd:requiredTotalLen])

		// 2.7 校验和验证（失败则直接丢弃无效包）
		if adler32.Checksum(msgData) != checksumExpected {
			log.Printf("Decode error (connID: %s): invalid checksum (expected %d, actual %d)",
				ctx.ConnID, checksumExpected, adler32.Checksum(msgData))
			conn.Discard(requiredTotalLen)
			return ErrInvalidChecksum
		}

		// 2.8 解析类型名长度和类型名
		if len(msgData) < 4 {
			log.Printf("Decode error (connID: %s): insufficient data for nameLen", ctx.ConnID)
			conn.Discard(requiredTotalLen)
			return ErrInsufficientData
		}
		nameLen := binary.BigEndian.Uint32(msgData[:4])
		if nameLen < 1 || int(nameLen+4) > len(msgData) {
			log.Printf("Decode error (connID: %s): invalid nameLen %d (msgData len %d)",
				ctx.ConnID, nameLen, len(msgData))
			conn.Discard(requiredTotalLen)
			return ErrInvalidMsgName
		}

		// 2.9 解析Proto消息（处理原格式的末尾空格）
		typeNameStr := string(msgData[4 : 4+nameLen-1]) // 去掉Encode时加的空格
		msgType, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName(typeNameStr))
		if err != nil {
			log.Printf("Decode error (connID: %s): find message type failed (name=%s): %v",
				ctx.ConnID, typeNameStr, err)
			conn.Discard(requiredTotalLen)
			return err
		}

		// 2.10 反序列化消息体
		bodyStart := 4 + nameLen
		msg := msgType.New().Interface()
		unmarshalOptions := proto.UnmarshalOptions{}
		if err := unmarshalOptions.Unmarshal(msgData[bodyStart:], msg); err != nil {
			log.Printf("Decode error (connID: %s): unmarshal message failed: %v", ctx.ConnID, err)
			conn.Discard(requiredTotalLen)
			return err
		}

		// 2.11 关键：解析成功后，立即消费缓冲区数据（无延迟，无同步问题）
		if _, err := conn.Discard(requiredTotalLen); err != nil {
			log.Printf("Decode error (connID: %s): discard packet failed: %v", ctx.ConnID, err)
			return err
		}

		// 2.12 保存解码结果（供业务层回调使用）
		ctx.Msg = msg
		break
	}

	return nil
}
