package muduocodec

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

// ConnContext: TcpCodec的连接上下文（缓存未消费数据+解码后的消息）
type ConnContext struct {
	cachedData []byte // 暂存未消费的Peek数据
	Msg        proto.Message
}

type TcpCodec struct{}

func NewTcpCodec() *TcpCodec {
	return &TcpCodec{}
}

// Encode: 编码Proto消息为TCP协议格式
func (c *TcpCodec) Encode(conn gnet.Conn, _ []byte) ([]byte, error) {
	ctx, ok := conn.Context().(*ConnContext)
	if !ok || ctx.Msg == nil {
		return nil, nil // 无待发送消息
	}

	// 1. 获取Proto消息描述符和类型名
	desc := ctx.Msg.ProtoReflect().Descriptor()
	if desc == nil {
		return nil, errors.New("empty message descriptor")
	}
	msgTypeName := desc.Name() + " " // 保留原格式的空格
	msgTypeNameData := []byte(msgTypeName)
	nameLen := uint32(len(msgTypeNameData))

	// 2. 序列化Proto消息体（v2接口）
	msgBody, err := proto.MarshalOptions{}.Marshal(ctx.Msg)
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

// Decode: 基于Peek接口解码TCP协议包
func (c *TcpCodec) Decode(conn gnet.Conn) ([]byte, error) {
	// 1. 获取/初始化连接上下文
	ctx, ok := conn.Context().(*ConnContext)
	if !ok {
		ctx = &ConnContext{cachedData: make([]byte, 0, 4096)} // 预分配缓存
		conn.SetContext(ctx)
	}

	// 2. 循环解析：处理粘包和拆包
	for {
		// 2.1 合并缓存数据和新Peek数据
		newData, err := conn.Peek(0)     // Peek所有可用新数据（n=0表示全部）
		if err != nil && err != io.EOF { // EOF暂时忽略（对端关闭时后续会处理）
			log.Println("Peek new data error:", err)
			return nil, err
		}
		fullData := append(ctx.cachedData, newData...)
		ctx.cachedData = ctx.cachedData[:0] // 清空缓存

		// 2.2 第一步：解析总长度前缀（4字节）
		if len(fullData) < 4 {
			ctx.cachedData = append(ctx.cachedData, fullData...) // 缓存不足数据
			return nil, ErrInsufficientData
		}
		totalLen := binary.BigEndian.Uint32(fullData[:4])
		requiredTotalLen := 4 + int(totalLen) // 完整包总长度（4字节长度+totalLen）

		// 2.3 第二步：检查是否有完整包
		if len(fullData) < requiredTotalLen {
			ctx.cachedData = append(ctx.cachedData, fullData...) // 缓存不足数据
			return nil, ErrInsufficientData
		}

		// 2.4 第三步：分离msgData和校验和
		msgData := fullData[4 : 4+int(totalLen)-4] // 修正：msgData部分
		checksumExpected := binary.BigEndian.Uint32(fullData[4+int(totalLen)-4 : 4+int(totalLen)])

		// 校验和验证
		if adler32.Checksum(msgData) != checksumExpected {
			log.Println("TcpCodec:", ErrInvalidChecksum)
			// 消费错误包（读取并丢弃）
			if _, readErr := readFixedLength(conn, requiredTotalLen); readErr != nil && readErr != ErrInsufficientData {
				log.Println("Read invalid packet error:", readErr)
			}
			fullData = fullData[requiredTotalLen:] // 跳过错误包
			continue
		}

		// 2.5 第四步：解析类型名长度和类型名
		if len(msgData) < 4 { // 至少需要4字节nameLen
			log.Println("TcpCodec: insufficient data for nameLen")
			if _, readErr := readFixedLength(conn, requiredTotalLen); readErr != nil && readErr != ErrInsufficientData {
				log.Println("Read invalid nameLen packet error:", readErr)
			}
			fullData = fullData[requiredTotalLen:]
			continue
		}
		nameLen := binary.BigEndian.Uint32(msgData[:4])
		if nameLen < 1 || int(nameLen+4) > len(msgData) { // 校验类型名长度合法性
			log.Println("TcpCodec: invalid nameLen:", nameLen)
			if _, readErr := readFixedLength(conn, requiredTotalLen); readErr != nil && readErr != ErrInsufficientData {
				log.Println("Read invalid nameLen packet error:", readErr)
			}
			fullData = fullData[requiredTotalLen:]
			continue
		}

		// 2.6 第五步：解析Proto消息并反序列化
		typeNameStr := string(msgData[4 : 4+nameLen-1]) // 去掉末尾空格
		msgType, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName(typeNameStr))
		if err != nil {
			log.Printf("rpc codec: find message type failed (name=%s): %v", typeNameStr, err)
			if _, readErr := readFixedLength(conn, requiredTotalLen); readErr != nil && readErr != ErrInsufficientData {
				log.Println("Read invalid proto type packet error:", readErr)
			}
			fullData = fullData[requiredTotalLen:]
			continue
		}

		// 反序列化（使用v2标准接口）
		bodyStart := 4 + nameLen
		msg := msgType.New().Interface()
		unmarshalOptions := proto.UnmarshalOptions{}
		if err := unmarshalOptions.Unmarshal(msgData[bodyStart:], msg); err != nil {
			log.Println("TcpCodec: unmarshal error:", err)
			if _, readErr := readFixedLength(conn, requiredTotalLen); readErr != nil && readErr != ErrInsufficientData {
				log.Println("Read unmarshal error packet error:", readErr)
			}
			fullData = fullData[requiredTotalLen:]
			continue
		}

		// 2.7 第六步：消费完整包+缓存剩余数据
		if _, readErr := readFixedLength(conn, requiredTotalLen); readErr != nil {
			log.Println("Read valid packet error:", readErr)
			return nil, readErr
		}
		// 缓存剩余未处理数据（供下次解码）
		if len(fullData) > requiredTotalLen {
			ctx.cachedData = append(ctx.cachedData, fullData[requiredTotalLen:]...)
		}
		// 保存解码结果
		ctx.Msg = msg
		break // 解析成功一个包，退出循环
	}

	return nil, nil
}
