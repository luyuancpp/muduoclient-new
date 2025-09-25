package net

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/luyuancpp/muduo-client-go/muduocodec"
	"github.com/panjf2000/gnet/v2"
	"google.golang.org/protobuf/proto"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// ------------------------------ 核心接口与结构体定义（精简版） ------------------------------
// Codec: 编解码器接口（不变）
type Codec interface {
	Encode(msg proto.Message) ([]byte, error)
	Decode(conn gnet.Conn) error
}

// ConnMeta: 连接元数据（不变，仍绑定到conn）
type ConnMeta struct {
	ConnID   string
	CreateAt time.Time
}

// TcpClient: 精简后结构体（删除connID、connIDMutex）
type TcpClient struct {
	codec     Codec              // 编解码器
	closed    atomic.Bool        // 客户端关闭状态（原子变量）
	ctx       context.Context    // 生命周期上下文
	cancel    context.CancelFunc // 上下文取消函数
	incoming  chan proto.Message // 接收通道
	outgoing  chan []byte        // 发送通道
	conn      atomic.Value       // 活跃连接（用原子变量替代connMutex）
	wg        sync.WaitGroup     // 协程等待组
	client    *gnet.Client       // gnet客户端实例
	network   string             // 网络类型（tcp）
	addr      string             // 服务器地址
	multicore bool               // gnet多核模式
	connected atomic.Bool        // 连接状态（原子变量）
}

// tcpClientEvents: 事件处理器（不变）
type tcpClientEvents struct {
	*gnet.BuiltinEventEngine
	client *TcpClient
}

// ------------------------------ 事件回调（简化锁与connID读取） ------------------------------
// OnOpen: 连接建立（删除connIDMutex，直接绑定元数据）
func (ev *tcpClientEvents) OnOpen(conn gnet.Conn) (out []byte, action gnet.Action) {
	tcpClient := ev.client
	if tcpClient.closed.Load() { // 客户端已关闭，直接拒绝连接
		return nil, gnet.Close
	}

	// 绑定元数据（无需同步到TcpClient的connID字段）
	connMeta := &ConnMeta{
		ConnID:   uuid.NewString(),
		CreateAt: time.Now(),
	}
	conn.SetContext(connMeta)

	// 原子变量存储conn（替代connMutex.Lock/Unlock）
	tcpClient.conn.Store(conn)
	tcpClient.connected.Store(true)

	// 日志直接从元数据读connID
	log.Printf("connected to server: %s (connID: %s)", tcpClient.addr, connMeta.ConnID)
	return out, gnet.None
}

// OnClose: 连接关闭（删除connIDMutex，从元数据读connID）
func (ev *tcpClientEvents) OnClose(conn gnet.Conn, err error) gnet.Action {
	tcpClient := ev.client

	// 从元数据读connID（无需依赖TcpClient的connID字段）
	connMeta, ok := conn.Context().(*ConnMeta)
	connID := "unknown"
	if ok {
		connID = connMeta.ConnID
	}

	// 仅当关闭的是当前活跃连接时，更新状态（单连接场景下基本必成立）
	if currentConn := tcpClient.conn.Load(); currentConn == conn {
		tcpClient.conn.Store(nil)
		tcpClient.connected.Store(false)
	}

	// 主动关闭场景：不重连
	if tcpClient.closed.Load() {
		log.Printf("conn %s closed (active shutdown)", connID)
		return gnet.Shutdown
	}

	// 服务器正常关闭：不重连
	if err == nil {
		log.Printf("conn %s closed by server (normal)", connID)
		return gnet.None
	}

	// 意外断连：启动重连（简化日志）
	log.Printf("conn %s closed (error: %v), start reconnect", connID, err)
	go tcpClient.reconnectLoop()

	return gnet.None
}

// OnTraffic: 数据接收（简化connID读取）
func (ev *tcpClientEvents) OnTraffic(conn gnet.Conn) (action gnet.Action) {
	tcpClient := ev.client

	// 同时获取编解码器上下文和元数据（避免二次类型断言）
	codecCtx, ok := conn.Context().(*muduocodec.ConnContext)
	if !ok {
		connMeta := conn.Context().(*ConnMeta)
		log.Printf("OnTraffic: invalid ConnContext (connID: %s)", connMeta.ConnID)
		return gnet.None
	}
	connMeta := conn.Context().(*ConnMeta) // muduocodec.ConnContext 应嵌入 ConnMeta 或支持类型断言（若不支持，需调整元数据存储方式，见备注）

	// 解码逻辑不变
	decodeErr := tcpClient.codec.Decode(conn)
	if decodeErr != nil && !errors.Is(decodeErr, muduocodec.ErrInsufficientData) {
		log.Printf("OnTraffic: decode failed (connID: %s): %v", connMeta.ConnID, decodeErr)
		return gnet.None
	}

	// 投递消息不变
	if codecCtx.Msg != nil {
		select {
		case tcpClient.incoming <- codecCtx.Msg:
			codecCtx.Msg = nil
		default:
			log.Printf("OnTraffic: incoming full (connID: %s), drop msg", connMeta.ConnID)
		}
	}

	return gnet.None
}

// 备注：若 muduocodec.ConnContext 不支持嵌入 ConnMeta，可调整元数据存储方式：
// 在 OnOpen 时，将 ConnMeta 存储到 codecCtx 中（如 codecCtx.Meta = connMeta），读取时通过 codecCtx.Meta 获取。

// OnTick: 定时心跳（简化conn读取）
func (ev *tcpClientEvents) OnTick() (delay time.Duration, action gnet.Action) {
	delay = 10 * time.Second
	tcpClient := ev.client

	if !tcpClient.connected.Load() {
		return delay, gnet.None
	}

	// 原子变量读取conn（无需锁）
	if conn, ok := tcpClient.conn.Load().(gnet.Conn); ok && conn != nil {
		connMeta := conn.Context().(*ConnMeta)
		// 心跳逻辑不变（示例）
		// heartbeatMsg := &pb.Heartbeat{ConnID: connMeta.ConnID}
		// if err := tcpClient.Send(heartbeatMsg); err != nil {
		// 	log.Printf("OnTick: send heartbeat failed (connID: %s): %v", connMeta.ConnID, err)
		// }
	}

	return delay, gnet.None
}

// ------------------------------ 重连逻辑（大幅简化） ------------------------------
// reconnectLoop: 简化日志，删除冗余的closedConnID参数
func (c *TcpClient) reconnectLoop() {
	reconnectDelay := 3 * time.Second
	maxDelay := 30 * time.Second
	retryCount := 0

	for {
		// 客户端已关闭：终止重连
		if c.closed.Load() {
			log.Println("reconnect: client closed, stop")
			return
		}

		// 简化日志：仅打印核心信息
		log.Printf("reconnect: retry %d, delay %v", retryCount+1, reconnectDelay)
		time.Sleep(reconnectDelay)

		// 尝试重连（逻辑不变）
		newConn, err := c.client.Dial(c.network, c.addr)
		if err == nil {
			log.Printf("reconnect: success (new connID: %s)", newConn.Context().(*ConnMeta).ConnID)
			return
		}

		// 退避算法不变
		retryCount++
		reconnectDelay *= 2
		if reconnectDelay > maxDelay {
			reconnectDelay = maxDelay
		}
		log.Printf("reconnect: failed (retry %d): %v, next delay %v", retryCount, err, reconnectDelay)
	}
}

// ------------------------------ 客户端初始化与启动（不变，仅适配原子变量） ------------------------------
type ClientConfig struct {
	Multicore bool
	Async     bool
	Writev    bool
}

func DefaultClientConfig() *ClientConfig {
	return &ClientConfig{Multicore: false, Async: false, Writev: false}
}

func NewTcpClient(addr string, codec Codec) *TcpClient {
	return NewTcpClientWithConfig(addr, codec, DefaultClientConfig())
}

func NewTcpClientWithConfig(addr string, codec Codec, conf *ClientConfig) *TcpClient {
	ctx, cancel := context.WithCancel(context.Background())
	client := &TcpClient{
		codec:     codec,
		ctx:       ctx,
		cancel:    cancel,
		incoming:  make(chan proto.Message, 200),
		outgoing:  make(chan []byte, 200),
		network:   "tcp",
		addr:      addr,
		multicore: conf.Multicore,
	}

	client.wg.Add(2)
	go client.asyncWriteLoop()
	go client.startGnetClient(conf)
	return client
}

func (c *TcpClient) startGnetClient(conf *ClientConfig) {
	defer c.wg.Done()

	gnetEvents := &tcpClientEvents{
		BuiltinEventEngine: &gnet.BuiltinEventEngine{},
		client:             c,
	}

	// 创建gnet客户端（不变）
	gnetClient, err := gnet.NewClient(
		gnetEvents,
		gnet.WithMulticore(conf.Multicore),
		gnet.WithTCPNoDelay(gnet.TCPNoDelay),
		gnet.WithLockOSThread(true),
		gnet.WithTicker(true),
	)
	if err != nil {
		log.Fatalf("startGnetClient: create failed: %v", err)
	}
	c.client = gnetClient

	// 启动gnet（不变）
	if err := c.client.Start(); err != nil {
		log.Fatalf("startGnetClient: start failed: %v", err)
	}

	// 延迟关闭（不变）
	defer func() {
		if err := c.client.Stop(); err != nil {
			log.Printf("startGnetClient: stop failed: %v", err)
		}
	}()

	// 初始连接（不变）
	initialConn, err := c.client.Dial(c.network, c.addr)
	if err != nil {
		log.Fatalf("startGnetClient: initial dial failed: %v", err)
	}
	log.Printf("startGnetClient: initial conn success (connID: %s)", initialConn.Context().(*ConnMeta).ConnID)

	// 等待关闭（不变）
	<-c.ctx.Done()
	log.Println("startGnetClient: exiting")
}

// ------------------------------ 异步发送与关闭（简化锁） ------------------------------
// asyncWriteLoop: 用原子变量读取conn，删除connMutex
func (c *TcpClient) asyncWriteLoop() {
	defer c.wg.Done()

	for {
		select {
		case <-c.ctx.Done():
			log.Println("asyncWriteLoop: exiting (ctx closed)")
			return
		case data, ok := <-c.outgoing:
			if !ok {
				log.Println("asyncWriteLoop: exiting (outgoing closed)")
				return
			}

			// 双重校验：先判断状态，再读conn（避免无效操作）
			if c.closed.Load() || !c.connected.Load() {
				log.Println("asyncWriteLoop: skip send (client closed or not connected)")
				continue
			}

			// 原子变量读取conn（无需锁）
			conn, ok := c.conn.Load().(gnet.Conn)
			if !ok || conn == nil {
				log.Println("asyncWriteLoop: skip send (no conn)")
				continue
			}

			// 异步写（不变，简化connID读取）
			connMeta := conn.Context().(*ConnMeta)
			err := conn.AsyncWrite(data, func(_ gnet.Conn, err error) error {
				if err != nil {
					log.Printf("asyncWriteLoop: send failed (connID: %s, len: %d): %v", connMeta.ConnID, len(data), err)
				}
				return err
			})
			if err != nil {
				log.Printf("asyncWriteLoop: init send failed (connID: %s, len: %d): %v", connMeta.ConnID, len(data), err)
			}
		}
	}
}

// Send: 业务层发送接口（非阻塞，原子状态判断）
func (c *TcpClient) Send(msg proto.Message) error {
	// 1. 快速状态校验（原子操作，无锁）
	if c.closed.Load() {
		return errors.New("发送失败：客户端已关闭")
	}
	if !c.connected.Load() {
		return errors.New("发送失败：未连接服务器")
	}

	// 2. 编码Protobuf消息
	data, err := c.codec.Encode(msg)
	if err != nil {
		return errors.Join(errors.New("消息编码失败"), err)
	}

	// 3. 非阻塞投递到发送通道（避免阻塞业务层）
	select {
	case c.outgoing <- data:
		return nil
	default:
		return errors.New("发送失败：发送缓冲区已满（业务层发送过快）")
	}
}

// Recv: 业务层接收接口（阻塞，支持优雅退出）
func (c *TcpClient) Recv() (proto.Message, error) {
	select {
	case msg, ok := <-c.incoming:
		// 接收通道关闭（Close函数触发）
		if !ok {
			return nil, errors.New("接收失败：客户端已关闭")
		}
		return msg, nil

	case <-c.ctx.Done():
		// 上下文关闭（主动退出信号）
		return nil, errors.New("接收失败：客户端退出中")
	}
}

// Close: 优雅关闭（原子控制，无冗余清理）
func (c *TcpClient) Close() {
	// 1. 原子判断：确保只关闭一次（避免重复调用）
	if !c.closed.CompareAndSwap(false, true) {
		log.Println("客户端已关闭，跳过重复操作")
		return
	}
	log.Println("开始关闭客户端")

	// 2. 触发所有协程退出信号
	c.cancel()

	// 3. 关闭发送通道（通知asyncWriteLoop退出）
	close(c.outgoing)

	// 4. 关闭活跃连接（仅日志，不中断后续清理）
	if conn, ok := c.conn.Load().(gnet.Conn); ok && conn != nil {
		if err := conn.Close(); err != nil {
			log.Printf("连接关闭异常: %v", err)
		}
	}

	// 5. 等待所有协程退出（确保资源清理完成）
	c.wg.Wait()

	// 6. 关闭接收通道（通知业务层Recv退出）
	close(c.incoming)

	log.Println("客户端完全关闭")
}

// IsConnected: 连接状态查询（原子操作，无锁）
func (c *TcpClient) IsConnected() bool {
	return c.connected.Load()
}

// IsClosed: 检查是否已关闭（线程安全）
func (c *TcpClient) IsClosed() bool {
	return c.closed.Load()
}
