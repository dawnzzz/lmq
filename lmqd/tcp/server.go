package tcp

import (
	"github.com/dawnzzz/hamble-tcp-server/conf"
	"github.com/dawnzzz/hamble-tcp-server/hamble"
	serveriface "github.com/dawnzzz/hamble-tcp-server/iface"
	"github.com/dawnzzz/lmq/config"
	"github.com/dawnzzz/lmq/iface"
	"github.com/dawnzzz/lmq/internel/protocol"
	"github.com/dawnzzz/lmq/logger"
	"sync"
	"sync/atomic"
)

type TcpServer struct {
	lmqDaemon iface.ILmqDaemon
	server    serveriface.IServer

	IsClosing atomic.Bool
	ExitChan  chan struct{}

	clientMap     map[uint64]*TcpClient
	clientMapLock sync.RWMutex
}

func NewTcpServer(lmqDaemon iface.ILmqDaemon) *TcpServer {

	server := hamble.NewServerWithOption(&conf.Profile{
		Name:             "LMQD TCP Server",
		Host:             config.GlobalLmqdConfig.TcpHost,
		Port:             config.GlobalLmqdConfig.TcpPort,
		TcpVersion:       "tcp4",
		MaxConn:          config.GlobalLmqdConfig.TcpServerMaxConn,
		MaxPacketSize:    uint32(config.GlobalLmqdConfig.MaxMessageSize + 8),
		WorkerPoolSize:   config.GlobalLmqdConfig.TcpServerWorkerPoolSize,
		MaxWorkerTaskLen: config.GlobalLmqdConfig.TcpServerMaxWorkerTaskLen,
		MaxMsgChanLen:    config.GlobalLmqdConfig.TcpServerMaxMsgChanLen,
		LogFileName:      "",
		MaxHeartbeatTime: 0,
		CrtFileName:      "",
		KeyFileName:      "",
		PrintBanner:      false,
	})

	tcpServer := &TcpServer{
		lmqDaemon: lmqDaemon,
		server:    server,

		clientMap: make(map[uint64]*TcpClient),

		ExitChan: make(chan struct{}),
	}

	registerHandler(tcpServer, server, lmqDaemon)

	// 连接开始时的Hook函数
	server.SetOnConnStart(func(conn serveriface.IConnection) {
		logger.Infof("on conn start hook func from %s", conn.RemoteAddr())
		// 连接开始时记录客户端状态
		clientID := lmqDaemon.GenerateClientID(conn)
		tcpClient := NewTcpClient(clientID, conn)

		tcpServer.clientMapLock.Lock()
		logger.Infof("clientID=%v", clientID)
		tcpServer.clientMap[clientID] = tcpClient
		tcpServer.clientMapLock.Unlock()

		conn.SetProperty("clientID", clientID)
		conn.SetProperty("client", tcpClient)
	})

	// 连接结束时的hook函数
	server.SetOnConnStop(func(conn serveriface.IConnection) {
		logger.Infof("on conn stop hook func from %s", conn.RemoteAddr())
		// 连接结束时销毁TcpClient对象
		// 获取clientID
		raw := conn.GetProperty("clientID")
		clientID, _ := raw.(uint64)

		// 获取TcpClient对象
		client, ok := tcpServer.clientMap[clientID]
		if !ok {
			return
		}

		_ = client.Close()
		// 从订阅的channel中移除该对象
		if client.channel != nil {
			client.channel.RemoveClient(clientID)
		}

		// 销毁对象
		DestroyTcpClient(client)
	})

	return tcpServer
}

func (tcpServer *TcpServer) Start() {
	tcpServer.server.Start()

	tcpServer.server.Stop()
}

func (tcpServer *TcpServer) Stop() {
	if tcpServer.IsClosing.Load() {
		return
	}

	close(tcpServer.ExitChan)
	tcpServer.server.Stop()
}

func registerHandler(tcpServer *TcpServer, server serveriface.IServer, lmqDaemon iface.ILmqDaemon) {
	/*
		Pub and Sub
	*/
	server.RegisterHandler(protocol.PubID, &PubHandler{
		BaseHandler: RegisterBaseHandler(protocol.PubID, lmqDaemon),
		tcpServer:   tcpServer,
	})

	server.RegisterHandler(protocol.SubID, &SubHandler{
		BaseHandler: RegisterBaseHandler(protocol.SubID, lmqDaemon),
		tcpServer:   tcpServer,
	})

	/*
		protocol
	*/
	server.RegisterHandler(protocol.RydID, &RydHandler{
		BaseHandler: RegisterBaseHandler(protocol.RydID, lmqDaemon),
	})

	server.RegisterHandler(protocol.FinID, &FinHandler{
		BaseHandler: RegisterBaseHandler(protocol.FinID, lmqDaemon),
	})

	server.RegisterHandler(protocol.ReqID, &ReqHandler{
		BaseHandler: RegisterBaseHandler(protocol.ReqID, lmqDaemon),
	})

	/*
		Topic Handler
	*/
	server.RegisterHandler(protocol.CreateTopicID, &CreateChannelHandler{
		BaseHandler: RegisterBaseHandler(protocol.CreateTopicID, lmqDaemon),
	})

	server.RegisterHandler(protocol.DeleteTopicID, &DeleteTopicHandler{
		BaseHandler: RegisterBaseHandler(protocol.DeleteTopicID, lmqDaemon),
	})

	server.RegisterHandler(protocol.EmptyTopicID, &EmptyTopicHandler{
		BaseHandler: RegisterBaseHandler(protocol.EmptyTopicID, lmqDaemon),
	})

	server.RegisterHandler(protocol.PauseTopicID, &PauseTopicHandler{
		BaseHandler: RegisterBaseHandler(protocol.PauseTopicID, lmqDaemon),
	})

	server.RegisterHandler(protocol.UnPauseTopicID, &UnPauseTopicHandler{
		BaseHandler: RegisterBaseHandler(protocol.UnPauseTopicID, lmqDaemon),
	})

	/*
		Channel Handler
	*/
	server.RegisterHandler(protocol.CreateChannelID, &CreateChannelHandler{
		BaseHandler: RegisterBaseHandler(protocol.CreateChannelID, lmqDaemon),
	})

	server.RegisterHandler(protocol.DeleteChannelID, &DeleteChannelHandler{
		BaseHandler: RegisterBaseHandler(protocol.DeleteChannelID, lmqDaemon),
	})

	server.RegisterHandler(protocol.EmptyChannelID, &EmptyChannelHandler{
		BaseHandler: RegisterBaseHandler(protocol.EmptyChannelID, lmqDaemon),
	})

	server.RegisterHandler(protocol.PauseChannelID, &PauseChannelHandler{
		BaseHandler: RegisterBaseHandler(protocol.PauseChannelID, lmqDaemon),
	})

	server.RegisterHandler(protocol.UnPauseChannelID, &UnPauseChannelHandler{
		BaseHandler: RegisterBaseHandler(protocol.UnPauseChannelID, lmqDaemon),
	})
}
