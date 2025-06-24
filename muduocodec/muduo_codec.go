package muduocodec

import (
	"encoding/binary"
	"github.com/golang/protobuf/proto"
	"github.com/panjf2000/gnet"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"hash/adler32"
	"log"
)

//from muduo codec.cc
// struct ProtobufTransportFormat __attribute__ ((__packed__))
// {
//   int32_t  len;
//   int32_t  nameLen;
//   char     typeName[nameLen];
//   char     protobufData[len-nameLen-8];
//   int32_t  checkSum; // adler32 of nameLen, typeName and protobufData
// }

type connContext struct {
	buffer []byte
	msg    proto.Message
}

type TcpCodec struct{}

func NewTcpCodec() *TcpCodec {
	return &TcpCodec{}
}
func GetDescriptor(m proto.Message) protoreflect.MessageDescriptor {
	if m == nil {
		return nil
	}
	return proto.MessageReflect(m).Descriptor()
}

func uint32ToBytes(n uint32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, n)
	return buf
}

func (c *TcpCodec) Encode(conn gnet.Conn, buf []byte) ([]byte, error) {
	ctx, ok := conn.Context().(*connContext)
	if !ok || ctx.msg == nil {
		return nil, nil // 没有要发送的数据
	}

	d := GetDescriptor(ctx.msg)
	if d == nil {
		return nil, nil
	}

	msgTypeName := d.Name() + " "
	msgTypeNameData := []byte(msgTypeName)
	nameLen := uint32(len(msgTypeNameData))

	// marshal message
	msgBody, err := proto.Marshal(ctx.msg)
	if err != nil {
		return nil, err
	}

	// Build payload: nameLen + typeName + body
	payload := make([]byte, 0, 4+len(msgTypeNameData)+len(msgBody)+4)
	payload = append(payload, uint32ToBytes(nameLen)...)
	payload = append(payload, msgTypeNameData...)
	payload = append(payload, msgBody...)

	checksum := adler32.Checksum(payload)
	payload = append(payload, uint32ToBytes(checksum)...)

	total := append(uint32ToBytes(uint32(len(payload))), payload...)
	return total, nil
}

func (c *TcpCodec) Decode(conn gnet.Conn) ([]byte, error) {
	// 正确方式：读取 gnet 已收数据
	data := conn.Read()

	ctx, ok := conn.Context().(*connContext)
	if !ok {
		ctx = &connContext{}
		conn.SetContext(ctx)
	}
	ctx.buffer = append(ctx.buffer, data...)

	for {
		if len(ctx.buffer) < 4 {
			return nil, nil // not enough data
		}
		msgLen := binary.BigEndian.Uint32(ctx.buffer[:4])
		totalLen := 4 + int(msgLen) + 4 // len + body + checksum

		if len(ctx.buffer) < totalLen {
			return nil, nil // wait more
		}

		payload := ctx.buffer[4 : 4+msgLen]
		checksumExpected := binary.BigEndian.Uint32(ctx.buffer[4+msgLen : totalLen])
		checksumActual := adler32.Checksum(payload)
		if checksumActual != checksumExpected {
			log.Println("⚠️ Checksum mismatch")
			ctx.buffer = ctx.buffer[totalLen:]
			continue
		}

		// parse nameLen + typeName
		if len(payload) < 4 {
			ctx.buffer = ctx.buffer[totalLen:]
			continue
		}
		nameLen := binary.BigEndian.Uint32(payload[:4])
		if nameLen < 1 || int(nameLen+4) > len(payload) {
			ctx.buffer = ctx.buffer[totalLen:]
			continue
		}

		typeName := string(payload[4 : 4+nameLen-1])
		bodyStart := 4 + nameLen

		msgType, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName(typeName))
		if err != nil {
			log.Println("Proto type not found:", typeName)
			ctx.buffer = ctx.buffer[totalLen:]
			continue
		}

		msg := proto.MessageV1(msgType.New())
		err = proto.Unmarshal(payload[bodyStart:], msg)
		if err != nil {
			log.Println("Unmarshal error:", err)
			ctx.buffer = ctx.buffer[totalLen:]
			continue
		}

		ctx.msg = msg
		ctx.buffer = ctx.buffer[totalLen:]
		break
	}

	return nil, nil // data now in conn.Context()
}

// wire format
//
// # Field     Length  Content
//
// size      4-byte  N+8
// "RPC0"    4-byte
// payload   N-byte
// checksum  4-byte  adler32 of "RPC0"+payload

// 每个连接的上下文，缓存 buffer 和 message
type rpcConnContext struct {
	buffer []byte
	msg    proto.Message
}

// RpcCodec 实现 gnet.Codec 接口
type RpcCodec struct {
	MsgType proto.Message // 要解析的 proto 类型，例如 &YourProtoMessage{}
}

func NewRpcCodec(msgType proto.Message) *RpcCodec {
	return &RpcCodec{MsgType: msgType}
}

// Encode 被 gnet 调用，用于将 message 编码为协议格式
func (c *RpcCodec) Encode(conn gnet.Conn, _ []byte) ([]byte, error) {
	ctx, ok := conn.Context().(*rpcConnContext)
	if !ok || ctx.msg == nil {
		return nil, nil
	}

	tag := []byte("RPC0")

	// 编码 proto 消息体
	body, err := proto.Marshal(ctx.msg)
	if err != nil {
		return nil, err
	}

	// 拼接 tag + body
	payload := append(tag, body...)

	// 计算 checksum
	checksum := adler32.Checksum(payload)
	checksumBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(checksumBytes, checksum)

	// payload + checksum
	data := append(payload, checksumBytes...)

	// 总长度（不含 length 字段本身）
	length := uint32(len(data))
	lengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBytes, length)

	// 返回完整包：长度 + 数据
	return append(lengthBytes, data...), nil
}

// Decode 被 gnet 调用，负责拆包、校验、解码
func (c *RpcCodec) Decode(conn gnet.Conn) ([]byte, error) {
	raw := conn.Read()

	ctx, ok := conn.Context().(*rpcConnContext)
	if !ok {
		ctx = &rpcConnContext{}
		conn.SetContext(ctx)
	}

	ctx.buffer = append(ctx.buffer, raw...)

	for {
		if len(ctx.buffer) < 4 {
			return nil, nil // 不够长度字段
		}

		length := binary.BigEndian.Uint32(ctx.buffer[:4])
		total := 4 + int(length) // 包括 length 字段自身

		if len(ctx.buffer) < total {
			return nil, nil // 包未接收完整
		}

		// 读取 tag 和 body
		payload := ctx.buffer[4 : 4+length-4] // 去掉 checksum
		checksumData := ctx.buffer[4+length-4 : total]
		checksumActual := adler32.Checksum(payload)
		checksumExpected := binary.BigEndian.Uint32(checksumData)

		if checksumActual != checksumExpected {
			log.Println("⚠️ RpcCodec checksum mismatch")
			ctx.buffer = ctx.buffer[total:]
			continue
		}

		if string(payload[:4]) != "RPC0" {
			log.Println("⚠️ RpcCodec invalid tag")
			ctx.buffer = ctx.buffer[total:]
			continue
		}

		// 反序列化 proto 数据
		body := payload[4:]
		desc := GetDescriptor(c.MsgType)
		msgType, err := protoregistry.GlobalTypes.FindMessageByName(desc.FullName())
		if err != nil {
			log.Println(" RpcCodec cannot find proto type:", desc.FullName())
			ctx.buffer = ctx.buffer[total:]
			continue
		}

		msg := proto.MessageV1(msgType.New())
		err = proto.Unmarshal(body, msg)
		if err != nil {
			log.Println(" RpcCodec unmarshal error:", err)
			ctx.buffer = ctx.buffer[total:]
			continue
		}

		ctx.msg = msg
		ctx.buffer = ctx.buffer[total:]
		break // 只处理一个包
	}

	return nil, nil
}
