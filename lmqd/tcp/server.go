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
		BaseHandler: BaseHandler{
			LmqDaemon: lmqDaemon,
			TaskID:    protocol.PubID,
		},
		tcpServer: tcpServer,
	})

	server.RegisterHandler(protocol.SubID, &SubHandler{
		BaseHandler: BaseHandler{
			LmqDaemon: lmqDaemon,
			TaskID:    protocol.SubID,
		},
		tcpServer: tcpServer,
	})

	/*
		protocol
	*/
	server.RegisterHandler(protocol.RydID, &RydHandler{
		BaseHandler: BaseHandler{
			LmqDaemon: lmqDaemon,
			TaskID:    protocol.RydID,
		},
	})

	server.RegisterHandler(protocol.FinID, &FinHandler{
		BaseHandler: BaseHandler{
			LmqDaemon: lmqDaemon,
			TaskID:    protocol.FinID,
		},
	})

	server.RegisterHandler(protocol.ReqID, &ReqHandler{
		BaseHandler: BaseHandler{
			LmqDaemon: lmqDaemon,
			TaskID:    protocol.ReqID,
		},
	})

	/*
		Topic Handler
	*/
	server.RegisterHandler(protocol.CreateTopicID, &CreateChannelHandler{
		BaseHandler: BaseHandler{
			LmqDaemon: lmqDaemon,
			TaskID:    protocol.CreateTopicID,
		},
	})

	server.RegisterHandler(protocol.DeleteTopicID, &DeleteTopicHandler{
		BaseHandler: BaseHandler{
			LmqDaemon: lmqDaemon,
			TaskID:    protocol.DeleteTopicID,
		},
	})

	server.RegisterHandler(protocol.EmptyTopicID, &EmptyTopicHandler{
		BaseHandler: BaseHandler{
			LmqDaemon: lmqDaemon,
			TaskID:    protocol.EmptyTopicID,
		},
	})

	server.RegisterHandler(protocol.PauseTopicID, &PauseTopicHandler{
		BaseHandler: BaseHandler{
			LmqDaemon: lmqDaemon,
			TaskID:    protocol.PauseTopicID,
		},
	})

	server.RegisterHandler(protocol.UnPauseTopicID, &UnPauseTopicHandler{
		BaseHandler: BaseHandler{
			LmqDaemon: lmqDaemon,
			TaskID:    protocol.UnPauseTopicID,
		},
	})

	/*
		Channel Handler
	*/
	server.RegisterHandler(protocol.CreateChannelID, &CreateChannelHandler{
		BaseHandler: BaseHandler{
			LmqDaemon: lmqDaemon,
			TaskID:    protocol.CreateChannelID,
		},
	})

	server.RegisterHandler(protocol.DeleteChannelID, &DeleteChannelHandler{
		BaseHandler: BaseHandler{
			LmqDaemon: lmqDaemon,
			TaskID:    protocol.DeleteChannelID,
		},
	})

	server.RegisterHandler(protocol.EmptyChannelID, &EmptyChannelHandler{
		BaseHandler: BaseHandler{
			LmqDaemon: lmqDaemon,
			TaskID:    protocol.EmptyChannelID,
		},
	})

	server.RegisterHandler(protocol.PauseChannelID, &PauseChannelHandler{
		BaseHandler: BaseHandler{
			LmqDaemon: lmqDaemon,
			TaskID:    protocol.PauseChannelID,
		},
	})

	server.RegisterHandler(protocol.UnPauseChannelID, &UnPauseChannelHandler{
		BaseHandler: BaseHandler{
			LmqDaemon: lmqDaemon,
			TaskID:    protocol.UnPauseChannelID,
		},
	})
}
