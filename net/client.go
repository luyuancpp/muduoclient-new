package net

import (
	gnet "github.com/panjf2000/gnet/v2"
	"log"
	"sync/atomic"
	"time"
)

// testClient 存储客户端的状态和配置信息
type testClient struct {
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
type clientEvents struct {
	*gnet.BuiltinEventEngine
	svr *testClient
}

// OnConnect 当与服务器建立连接时调用
func (ev *clientEvents) OnOpen(gnet.Conn) (out []byte, action gnet.Action) {
	return
}

func (ev *clientEvents) OnClose(gnet.Conn, error) gnet.Action {
	if ev.svr != nil {
		if atomic.AddInt32(&ev.svr.clientActive, -1) == 0 {
			return gnet.Shutdown
		}
	}
	return gnet.None
}

func (ev *clientEvents) OnTraffic(c gnet.Conn) (action gnet.Action) {
	return
}

func (ev *clientEvents) OnTick() (delay time.Duration, action gnet.Action) {
	delay = 200 * time.Millisecond
	return
}

// testConf 客户端配置
type testConf struct {
	et        bool
	etChunk   int
	reuseport bool
	multicore bool
	async     bool
	writev    bool
	clients   int
	lb        gnet.LoadBalancing
}

// runClient 初始化并启动gnet客户端
func runClient(network, addr string, conf *testConf) {
	ts := &testClient{
		network:   network,
		addr:      addr,
		multicore: conf.multicore,
		async:     conf.async,
		nclients:  conf.clients,
	}

	clientEV := &clientEvents{svr: ts}

	// 创建客户端实例
	var err error
	ts.client, err = gnet.NewClient(
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
	if err := ts.client.Start(); err != nil {
		log.Fatalf("启动客户端失败: %v", err)
	}
	defer ts.client.Stop() // 确保程序退出时关闭客户端

	// 连接到服务器
	_, err = ts.client.Dial(network, addr)
	if err != nil {
		log.Fatalf("无法连接到服务器 %s: %v", addr, err)
	}

	log.Printf("客户端已启动，正在连接到 %s", addr)

	// 保持客户端运行（等待事件）
	select {}
}

func test() {
	// 启动客户端
	go func() {
		runClient("tcp", "172.23.208.1:10013", &testConf{
			et:        false,
			etChunk:   0,
			reuseport: false,
			multicore: false,
			async:     false,
			writev:    false,
			clients:   10,
			lb:        gnet.RoundRobin,
		})
	}()

	// 主goroutine阻塞，防止程序退出
	select {}
}
