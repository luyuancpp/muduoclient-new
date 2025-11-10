package muduo

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/panjf2000/gnet/v2"
	"google.golang.org/protobuf/proto"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// Codec: 编解码器接口（业务层实现）
type Codec interface {
	Encode(msg proto.Message) ([]byte, error)     // 编码Protobuf消息
	Decode(conn gnet.Conn) (proto.Message, error) // 解码返回消息
}

// MessageCallback: 消息接收回调（业务层注册，接收完整消息后触发）
type MessageCallback func(msg proto.Message, connCtx *ConnContext)

// HeartbeatCallback: 心跳回调（业务层注册，定时触发，与OnTraffic同线程执行）
// 入参为连接上下文，业务层可在此构造并发送心跳消息
type HeartbeatCallback func(connCtx *ConnContext)

// ------------------------------ 核心结构体 ------------------------------
// ConnMeta: 连接元数据（嵌入到ConnContext）
type ConnMeta struct {
	ConnID   string    // 唯一连接标识（UUID）
	CreateAt time.Time // 连接建立时间
}

// ConnContext: 连接上下文（绑定到gnet.Conn，仅存储元数据）
type ConnContext struct {
	ConnMeta // 嵌入元数据
}

// TcpClient: 精简TCP客户端（新增心跳回调相关字段）
type TcpClient struct {
	codec             Codec              // 编解码器
	closed            atomic.Bool        // 关闭状态（原子变量）
	ctx               context.Context    // 生命周期上下文
	cancel            context.CancelFunc // 上下文取消函数
	conn              atomic.Value       // 活跃连接（gnet.Conn）
	wg                sync.WaitGroup     // 协程等待组（优雅关闭）
	client            *gnet.Client       // gnet客户端实例
	network           string             // 网络类型（固定tcp）
	addr              string             // 服务器地址（ip:port）
	multicore         bool               // gnet多核模式
	connected         atomic.Bool        // 连接状态（原子变量）
	msgCallback       MessageCallback    // 消息回调（业务层注册）
	heartbeatCallback HeartbeatCallback  // 心跳回调（业务层注册）
	heartbeatInterval time.Duration      // 心跳间隔（业务层配置）
}

// tcpClientEvents: gnet事件处理器（绑定TcpClient）
type tcpClientEvents struct {
	*gnet.BuiltinEventEngine // 嵌入默认事件实现
	client                   *TcpClient
}

// ------------------------------ gnet事件回调 ------------------------------
// OnOpen: 连接建立时初始化上下文
func (ev *tcpClientEvents) OnOpen(conn gnet.Conn) (out []byte, action gnet.Action) {
	tcpClient := ev.client
	if tcpClient.closed.Load() {
		return nil, gnet.Close
	}

	// 初始化连接上下文（仅元数据）
	connCtx := &ConnContext{
		ConnMeta: ConnMeta{
			ConnID:   uuid.NewString(),
			CreateAt: time.Now(),
		},
	}
	conn.SetContext(connCtx)

	// 原子更新连接状态
	tcpClient.conn.Store(conn)
	tcpClient.connected.Store(true)
	log.Printf("connected to server: %s (connID: %s)", tcpClient.addr, connCtx.ConnID)

	return out, gnet.None
}

// OnClose: 连接关闭时处理（重连/资源清理）
func (ev *tcpClientEvents) OnClose(conn gnet.Conn, err error) gnet.Action {
	tcpClient := ev.client
	connCtx, _ := conn.Context().(*ConnContext)
	connID := "unknown"
	if connCtx != nil {
		connID = connCtx.ConnID
	}

	// 更新连接状态（清空无效连接）
	tcpClient.connected.Store(false)
	tcpClient.conn.Store(nil)

	// 主动关闭：不重连
	if tcpClient.closed.Load() {
		log.Printf("conn closed: %s (active close)", connID)
		return gnet.Shutdown
	}

	// 服务器正常关闭：不重连
	if err == nil {
		log.Printf("conn closed: %s (server normal close)", connID)
		return gnet.None
	}

	// 异常断连：启动重连（提交到独立协程，避免阻塞gnet事件循环）
	log.Printf("conn closed abnormally: %s (err: %v) - starting reconnect", connID, err)
	go tcpClient.reconnectLoop()

	return gnet.None
}

// OnTraffic: 数据接收（解码成功直接触发回调）
func (ev *tcpClientEvents) OnTraffic(conn gnet.Conn) (action gnet.Action) {
	tcpClient := ev.client
	connCtx, ok := conn.Context().(*ConnContext)
	if !ok {
		connCtx = &ConnContext{}
		conn.SetContext(connCtx)
		return gnet.None
	}

	for {
		// 调用编解码器解码
		msg, decodeErr := tcpClient.codec.Decode(conn)

		switch {
		case decodeErr == nil:
			if msg != nil && tcpClient.msgCallback != nil {
				tcpClient.msgCallback(msg, connCtx)
			}
			continue

		case errors.Is(decodeErr, ErrInsufficientData):
			return gnet.None

		default:
			remoteAddr := conn.RemoteAddr().String()
			log.Printf("decode error (conn: %s): %v", remoteAddr, decodeErr)
			return gnet.None
		}
	}
}

// OnTick: 定时回调（触发心跳回调，与事件循环同线程）
func (ev *tcpClientEvents) OnTick() (delay time.Duration, action gnet.Action) {
	tcpClient := ev.client

	// 使用业务层配置的心跳间隔（默认10秒）
	delay = tcpClient.heartbeatInterval
	if delay <= 0 {
		delay = 10 * time.Second
	}

	// 快速校验：客户端关闭/未连接/无心跳回调 → 跳过
	if tcpClient.closed.Load() || !tcpClient.connected.Load() || tcpClient.heartbeatCallback == nil {
		return delay, gnet.None
	}

	// 获取活跃连接和上下文
	connVal := tcpClient.conn.Load()
	conn, ok := connVal.(gnet.Conn)
	if !ok || conn == nil {
		log.Println("heartbeat skipped: no active connection")
		return delay, gnet.None
	}

	connCtx, ok := conn.Context().(*ConnContext)
	if !ok {
		log.Println("heartbeat skipped: invalid connection context")
		return delay, gnet.None
	}

	// 触发业务层心跳回调（在gnet事件循环线程执行）
	tcpClient.heartbeatCallback(connCtx)

	return delay, gnet.None
}

// ------------------------------ 核心拨号方法 ------------------------------
func (c *TcpClient) dial() (gnet.Conn, error) {
	ctx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
	defer cancel()
	return c.client.DialContext(c.network, c.addr, ctx)
}

// ------------------------------ 重连逻辑 ------------------------------
func (c *TcpClient) reconnectLoop() {
	reconnectDelay := 3 * time.Second // 初始间隔
	maxDelay := 30 * time.Second      // 最大间隔
	retryCount := 0

	for {
		if c.closed.Load() {
			log.Println("reconnect stopped: client closed")
			return
		}

		log.Printf("reconnect attempt %d (delay: %v) - target: %s", retryCount+1, reconnectDelay, c.addr)
		time.Sleep(reconnectDelay)

		newConn, err := c.dial()
		if err == nil {
			log.Printf("reconnect success (connID: %s)", newConn.Context().(*ConnContext).ConnID)
			return
		}

		retryCount++
		reconnectDelay *= 2
		if reconnectDelay > maxDelay {
			reconnectDelay = maxDelay
		}
		log.Printf("reconnect failed (attempt %d): %v", retryCount, err)
	}
}

// ------------------------------ 客户端配置与初始化 ------------------------------
type ClientConfig struct {
	Multicore bool // 是否启用gnet多核模式（单连接建议关闭）
}

func DefaultClientConfig() *ClientConfig {
	return &ClientConfig{Multicore: false}
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
		network:   "tcp",
		addr:      addr,
		multicore: conf.Multicore,
	}

	client.wg.Add(1)
	go client.startGnetClient()

	return client
}

func (c *TcpClient) startGnetClient() {
	defer c.wg.Done()

	gnetEvents := &tcpClientEvents{
		BuiltinEventEngine: &gnet.BuiltinEventEngine{},
		client:             c,
	}

	gnetClient, err := gnet.NewClient(
		gnetEvents,
		gnet.WithMulticore(c.multicore),
		gnet.WithTCPNoDelay(gnet.TCPNoDelay),
		gnet.WithLockOSThread(true),
		gnet.WithTicker(true), // 启用定时回调（支持心跳）
	)
	if err != nil {
		log.Fatalf("failed to create gnet client: %v", err)
	}
	c.client = gnetClient

	defer func() {
		if err := c.client.Stop(); err != nil {
			log.Printf("failed to stop gnet client: %v", err)
		} else {
			log.Println("gnet client stopped normally")
		}
	}()

	if err := c.client.Start(); err != nil {
		log.Fatalf("failed to start gnet engine: %v", err)
	}

	if _, err := c.dial(); err != nil {
		log.Fatalf("failed to establish initial connection: %v", err)
	}

	<-c.ctx.Done()
	log.Println("gnet client coroutine exited")
}

// ------------------------------ 业务层核心接口 ------------------------------
// SetMessageCallback: 注册消息回调（业务层调用）
func (c *TcpClient) SetMessageCallback(callback MessageCallback) {
	c.msgCallback = callback
}

// SetHeartbeatCallback: 注册心跳回调（业务层调用，与消息回调风格一致）
// 间隔为0时使用默认10秒
func (c *TcpClient) SetHeartbeatCallback(interval time.Duration, callback HeartbeatCallback) {
	if callback == nil {
		log.Panic("heartbeat callback cannot be nil")
	}
	c.heartbeatInterval = interval
	c.heartbeatCallback = callback
}

// Send: 发送消息（非阻塞，线程安全）
func (c *TcpClient) Send(msg proto.Message) error {
	if c.closed.Load() {
		return errors.New("client closed")
	}
	if !c.connected.Load() {
		return errors.New("not connected to server")
	}

	data, err := c.codec.Encode(msg)
	if err != nil {
		return errors.Join(errors.New("encode message failed"), err)
	}

	connVal := c.conn.Load()
	conn, ok := connVal.(gnet.Conn)
	if !ok || conn == nil {
		return errors.New("no active connection")
	}
	connCtx := conn.Context().(*ConnContext)

	err = conn.AsyncWrite(data, func(_ gnet.Conn, writeErr error) error {
		if writeErr != nil {
			log.Printf("send failed (connID: %s, dataLen: %d): %v", connCtx.ConnID, len(data), writeErr)
		}
		return writeErr
	})
	if err != nil {
		return errors.Join(errors.New("failed to start async write"), err)
	}

	return nil
}

// Close: 优雅关闭客户端（线程安全）
func (c *TcpClient) Close() {
	if !c.closed.CompareAndSwap(false, true) {
		log.Println("client already closed")
		return
	}
	log.Println("starting to close client")

	c.cancel()

	connVal := c.conn.Load()
	if connVal != nil {
		conn, ok := connVal.(gnet.Conn)
		if ok && conn != nil {
			conn.Close()
		}
	}

	c.wg.Wait()

	c.connected.Store(false)
	log.Println("client closed completely")
}

// IsConnected: 查询连接状态（线程安全）
func (c *TcpClient) IsConnected() bool {
	return c.connected.Load()
}

// IsClosed: 查询客户端是否已关闭（线程安全）
func (c *TcpClient) IsClosed() bool {
	return c.closed.Load()
}

// Ctx: 暴露上下文（供业务层退出判断）
func (c *TcpClient) Ctx() context.Context {
	return c.ctx
}
