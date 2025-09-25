package muduo

import (
	"encoding/binary"
	"errors"
	"github.com/panjf2000/gnet/v2"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"hash/adler32"
	"io"
	"log"
)

// -----------------------------------------------------------------------------
// TcpCodec: 通用Protobuf编解码器（基于gnet.Peek接口）
// -----------------------------------------------------------------------------

type TcpCodec struct{}

func NewTcpCodec() *TcpCodec {
	return &TcpCodec{}
}

// Encode: 编码Proto消息为TCP协议格式
func (c *TcpCodec) Encode(msg proto.Message) ([]byte, error) {
	// 1. 获取Proto消息描述符和类型名
	desc := msg.ProtoReflect().Descriptor()
	if desc == nil {
		return nil, errors.New("empty message descriptor")
	}
	msgTypeName := desc.Name() + " " // 保留原格式的空格
	msgTypeNameData := []byte(msgTypeName)
	nameLen := uint32(len(msgTypeNameData))

	// 2. 序列化Proto消息体（v2接口）
	msgBody, err := proto.MarshalOptions{}.Marshal(msg)
	if err != nil {
		return nil, err
	}

	// 3. 构建msg数据部分（nameLen + typeName + body）
	// 修正：初始长度计算错误，不需要提前加校验和长度
	msgData := make([]byte, 0, 4+len(msgTypeNameData)+len(msgBody))
	nameLenBuf := uint32ToBytes(nameLen)
	msgData = append(msgData, nameLenBuf...)
	uint32BufPool.Put(nameLenBuf) // 归还切片

	msgData = append(msgData, msgTypeNameData...)
	msgData = append(msgData, msgBody...)

	// 4. 计算校验和并拼接
	checksum := adler32.Checksum(msgData) // 修正：校验和仅计算msgData部分
	checksumBuf := uint32ToBytes(checksum)
	defer uint32BufPool.Put(checksumBuf)
	msgData = append(msgData, checksumBuf...)

	// 5. 拼接总长度前缀（4字节）
	totalLen := uint32(len(msgData)) // 修正：总长度是msgData+checksum的长度
	totalLenBuf := uint32ToBytes(totalLen)
	defer uint32BufPool.Put(totalLenBuf)
	fullPacket := append(totalLenBuf, msgData...)

	return fullPacket, nil
}

func (c *TcpCodec) Decode(conn gnet.Conn) error {
	// 1. 获取/初始化连接上下文
	ctx, ok := conn.Context().(*ConnContext)
	if !ok {
		ctx = &ConnContext{cachedData: make([]byte, 0, 4096)}
		conn.SetContext(ctx)
	}

	// 2. 循环解析：处理粘包和拆包
	for {
		// 2.1 合并缓存数据和新Peek数据（Peek所有可用数据，不消费）
		newData, err := conn.Peek(0)
		if err != nil && err != io.EOF {
			log.Println("Peek new data error:", err)
			return err
		}
		fullData := append(ctx.cachedData, newData...)
		ctx.cachedData = ctx.cachedData[:0] // 清空旧缓存

		// 2.2 解析总长度前缀（4字节）
		if len(fullData) < 4 {
			ctx.cachedData = append(ctx.cachedData, fullData...)
			return ErrInsufficientData
		}
		totalLen := binary.BigEndian.Uint32(fullData[:4])
		requiredTotalLen := 4 + int(totalLen) // 完整包总长度（长度前缀+数据+校验和）

		// 2.3 检查是否有完整包
		if len(fullData) < requiredTotalLen {
			ctx.cachedData = append(ctx.cachedData, fullData...)
			return ErrInsufficientData
		}

		// -------------------------- 核心修改：用 Discard 替代 readFixedLength --------------------------
		// 无论解析成功/失败，最终都要 Discard 已处理的 requiredTotalLen 字节
		defer func() {
			// 调用 Discard 消费数据（从缓冲区移除 requiredTotalLen 字节）
			discarded, discardErr := conn.Discard(requiredTotalLen)
			if discardErr != nil {
				log.Printf("Discard failed: required %d, discarded %d, err: %v", requiredTotalLen, discarded, discardErr)
			} else if discarded != requiredTotalLen {
				// 极端情况：缓冲区数据被意外修改，Discard 长度不匹配（需告警）
				log.Printf("Discard length mismatch: required %d, discarded %d", requiredTotalLen, discarded)
			}
		}()

		// 2.4 分离msgData和校验和（原有逻辑不变）
		msgData := fullData[4 : 4+int(totalLen)-4]
		checksumExpected := binary.BigEndian.Uint32(fullData[4+int(totalLen)-4 : 4+int(totalLen)])

		// 校验和验证（失败直接返回，defer会自动Discard）
		if adler32.Checksum(msgData) != checksumExpected {
			log.Println("TcpCodec:", ErrInvalidChecksum)
			return ErrInvalidChecksum
		}

		// 2.5 解析类型名长度和类型名（失败直接返回，defer会自动Discard）
		if len(msgData) < 4 {
			log.Println("TcpCodec: insufficient data for nameLen")
			return ErrInsufficientData
		}
		nameLen := binary.BigEndian.Uint32(msgData[:4])
		if nameLen < 1 || int(nameLen+4) > len(msgData) {
			log.Println("TcpCodec: invalid nameLen:", nameLen)
			return ErrInvalidMsgName
		}

		// 2.6 解析Proto消息并反序列化（失败直接返回，defer会自动Discard）
		typeNameStr := string(msgData[4 : 4+nameLen-1]) // 注意：建议去掉末尾空格的逻辑，用FullName
		msgType, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName(typeNameStr))
		if err != nil {
			log.Printf("rpc codec: find message type failed (name=%s): %v", typeNameStr, err)
			return err
		}
		bodyStart := 4 + nameLen
		msg := msgType.New().Interface()
		unmarshalOptions := proto.UnmarshalOptions{}
		if err := unmarshalOptions.Unmarshal(msgData[bodyStart:], msg); err != nil {
			log.Println("TcpCodec: unmarshal error:", err)
			return err
		}

		// 2.7 缓存剩余未处理数据（若fullData包含多个包）
		if len(fullData) > requiredTotalLen {
			ctx.cachedData = append(ctx.cachedData, fullData[requiredTotalLen:]...)
		}

		// ----------------------------------------------------------------------------------------------
		// 2.8 保存解码结果
		ctx.Msg = msg
		break
	}

	return nil
}
