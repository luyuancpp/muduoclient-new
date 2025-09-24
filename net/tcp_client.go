package net

import (
	"context"
	"errors"
	"github.com/luyuancpp/muduo-client-go/muduocodec"
	"github.com/panjf2000/gnet/v2"
	"google.golang.org/protobuf/proto"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type Codec interface {
	Encode(conn gnet.Conn, msg proto.Message) ([]byte, error)

	Decode(conn gnet.Conn) (err error)
}

// TcpClient: gnet 异步读写连接封装
type TcpClient struct {
	codec      Codec              // 编解码器（处理 Protobuf 序列化/反序列化）
	closed     atomic.Bool        // 连接关闭状态（原子变量，线程安全）
	ctx        context.Context    // 生命周期管理上下文
	cancel     context.CancelFunc // 关闭上下文的函数
	incoming   chan proto.Message // 业务层异步接收消息的通道
	outgoing   chan []byte        // 业务层异步发送数据的通道（解耦业务与网络）
	receiveBuf []byte             // 读缓冲（处理 TCP 粘包，暂存未完整解码的数据）
	bufMutex   sync.Mutex         // 保护 receiveBuf 的线程安全（多协程可能操作）
	wg         sync.WaitGroup     // 协程等待组（确保关闭时所有协程退出）
	*gnet.BuiltinEventEngine
	client        *gnet.Client
	network       string
	addr          string
	multicore     bool
	async         bool
	nclients      int
	started       int32
	connected     int32
	clientActive  int32
	disconnected  int32
	udpReadHeader int32
}

// clientEvents 实现gnet的事件接口，处理客户端各种事件
type tcpClientEvents struct {
	*gnet.BuiltinEventEngine
	client *TcpClient
}

// OnConnect 当与服务器建立连接时调用
func (ev *tcpClientEvents) OnOpen(gnet.Conn) (out []byte, action gnet.Action) {
	return
}

func (ev *tcpClientEvents) OnClose(gnet.Conn, error) gnet.Action {
	if ev.client != nil {
		if atomic.AddInt32(&ev.client.clientActive, -1) == 0 {
			return gnet.Shutdown
		}
	}
	return gnet.None
}

func (ev *tcpClientEvents) OnTraffic(c gnet.Conn) (action gnet.Action) {
	tcpClient := ev.client

	// 1. 将新到达的数据追加到读缓冲（处理粘包：可能包含多个包或不完整包）
	tempBuf, _ := c.Peek(0) // 临时变量，避免持有锁期间操作原缓冲

	// 2. 循环解码：处理缓冲中的所有完整包（粘包场景）
	for len(tempBuf) > 0 {

		cc := &muduocodec.ConnContext{}
		c.SetContext(*cc)

		// 2.1 调用编解码器解码（返回完整消息+已解码长度）
		err := tcpClient.codec.Decode(c)

		// 2.2 处理解码结果
		switch {
		case err != nil:
			return gnet.None
		default:
			// 2.3 解码成功：将消息发送到业务层的 incoming 通道（异步）
			if cc.Msg != nil {
				select {
				case tcpClient.incoming <- cc.Msg:
					// 消息成功投递到业务层
				default:
					// 业务层通道满：丢弃消息（或根据业务需求重试）
					log.Println("onData: incoming channel full, drop message")
				}
			}
		}
	}

	return gnet.None
}

func (ev *tcpClientEvents) OnTick() (delay time.Duration, action gnet.Action) {
	delay = 200 * time.Millisecond
	return
}

func NewTcpClient(addr string, codec Codec) *TcpClient {
	// 初始化上下文（用于主动关闭连接）
	ctx, cancel := context.WithCancel(context.Background())

	clientConfig := &testConf{
		et:        false,
		etChunk:   0,
		reuseport: false,
		multicore: false,
		async:     false,
		writev:    false,
		clients:   10,
		lb:        gnet.RoundRobin,
	}

	c := &TcpClient{
		codec:      codec,
		ctx:        ctx,
		cancel:     cancel,
		incoming:   make(chan proto.Message, 200), // 接收通道缓冲（避免业务层阻塞）
		outgoing:   make(chan []byte, 200),        // 发送通道缓冲（避免业务层阻塞）
		receiveBuf: make([]byte, 0, 4096),         // 读缓冲预分配 4KB（减少扩容开销）
		network:    "tcp",
		addr:       addr,
		multicore:  clientConfig.multicore,
		async:      clientConfig.async,
		nclients:   clientConfig.clients,
	}

	// 启动 2 个核心协程：1. 异步发送协程 2. gnet 事件循环协程
	c.wg.Add(2)
	go c.asyncWriteLoop()                           // 协程1：处理 outgoing 通道，实现异步写
	go c.startGnetClient("tcp", addr, clientConfig) // 协程2：启动 gnet 客户端，管理连接和事件循环

	return c
}

// ------------------------------ 异步写实现 ------------------------------
// asyncWriteLoop: 异步发送协程（从 outgoing 通道取数据，调用 gnet.AsyncWrite 发送）
// 核心作用：解耦业务层与 gnet 网络层，避免业务代码直接操作 gnet.Conn
func (c *TcpClient) asyncWriteLoop() {
	defer c.wg.Done() // 协程退出时通知等待组

	for {
		select {
		case <-c.ctx.Done(): // 连接主动关闭（如调用 Close()）
			log.Println("asyncWriteLoop: context canceled, exit")
			return

		case data, ok := <-c.outgoing: // 从业务层接收待发送数据
			if !ok { // outgoing 通道被关闭（主动退出）
				log.Println("asyncWriteLoop: outgoing channel closed, exit")
				return
			}

			// 关键：调用 gnet.Conn.AsyncWrite 实现异步写（不阻塞当前协程）
			// 数据会写入 gnet 内置的出站缓冲区，由 gnet 事件循环调度发送
			err := c.conn.AsyncWrite(data, func(c gnet.Conn, err error) error {
				// 写操作完成后的回调（成功/失败都触发）
				if err != nil {
					log.Printf("asyncWrite callback: failed to send data (len=%d), err=%v", len(data), err)
				}
				return err
			})

			// 检查 AsyncWrite 初始化是否失败（如连接已关闭）
			if err != nil {
				log.Printf("asyncWrite init failed: len=%d, err=%v", len(data), err)
			}
		}
	}
}

// Send: 业务层调用的发送接口（将消息放入 outgoing 通道，实现异步发送）
func (c *TcpClient) Send(msg proto.Message) error {
	// 1. 调用编解码器编码消息（生成完整 TCP 数据包）
	data, err := c.codec.Encode(msg)
	if err != nil {
		return errors.Join(errors.New("encode message failed"), err)
	}

	// 2. 将编码后的数据放入 outgoing 通道（非阻塞，避免业务层阻塞）
	select {
	case c.outgoing <- data:
		return nil // 发送成功（实际发送由 asyncWriteLoop 处理）
	default:
		return errors.New("send buffer full (outgoing channel is full)")
	}
}

// ------------------------------ 异步读实现 ------------------------------

// Recv: 业务层调用的接收接口（从 incoming 通道取消息，实现异步读）
func (c *TcpClient) Recv() (proto.Message, error) {
	// 从 incoming 通道接收消息（阻塞直到有消息或通道关闭）
	msg, ok := <-c.incoming
	if !ok {
		// 通道关闭：说明连接已关闭
		return nil, errors.New("connection is closed, cannot recv")
	}
	return msg, nil
}

// runClient 初始化并启动gnet客户端
func (c *TcpClient) startGnetClient(network, addr string, conf *testConf) {
	defer c.wg.Done() // 协程退出时通知等待组

	clientEV := &tcpClientEvents{client: c}

	// 创建客户端实例
	var err error
	c.client, err = gnet.NewClient(
		clientEV,
		gnet.WithMulticore(conf.multicore),
		gnet.WithEdgeTriggeredIO(conf.et),
		gnet.WithEdgeTriggeredIOChunk(conf.etChunk),
		gnet.WithTCPNoDelay(gnet.TCPNoDelay),
		gnet.WithLockOSThread(true),
		gnet.WithTicker(true),
	)

	if err != nil {
		log.Fatalf("创建客户端失败: %v", err)
	}

	// 启动客户端引擎
	if err := c.client.Start(); err != nil {
		log.Fatalf("启动客户端失败: %v", err)
	}
	defer c.client.Stop() // 确保程序退出时关闭客户端

	// 连接到服务器
	_, err = c.client.Dial(network, addr)
	if err != nil {
		log.Fatalf("无法连接到服务器 %s: %v", addr, err)
	}

	log.Printf("客户端已启动，正在连接到 %s", addr)

	// 保持客户端运行（等待事件）
	select {}
}
