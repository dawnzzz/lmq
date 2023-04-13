package tcp

import (
	"github.com/dawnzzz/hamble-tcp-server/conf"
	"github.com/dawnzzz/hamble-tcp-server/hamble"
	serveriface "github.com/dawnzzz/hamble-tcp-server/iface"
	"github.com/dawnzzz/lmq/config"
	"github.com/dawnzzz/lmq/iface"
	"sync"
)

type TcpServer struct {
	lmqDaemon iface.ILmqDaemon
	server    serveriface.IServer

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
	}

	registerHandler(tcpServer, server, lmqDaemon)

	// 连接开始时的Hook函数
	server.SetOnConnStart(func(conn serveriface.IConnection) {
		// 连接开始时记录客户端状态
		clientID := lmqDaemon.GenerateClientID(conn)
		tcpServer.clientMapLock.Lock()
		tcpClient := NewTcpClient(clientID, conn)
		tcpServer.clientMap[clientID] = tcpClient
		conn.SetProperty("clientID", clientID)
		conn.SetProperty("client", tcpClient)
	})

	// 连接结束时的hook函数
	server.SetOnConnStop(func(conn serveriface.IConnection) {
		// 连接结束时销毁TcpClient对象
		// 获取clientID
		raw := conn.GetProperty("clientID")
		clientID, _ := raw.(uint64)

		// 获取TcpClient对象
		tcpServer.clientMapLock.Lock()
		client, ok := tcpServer.clientMap[clientID]
		if !ok {
			return
		}

		_ = client.Close()
		// 从订阅的channel中移除该对象
		client.channel.RemoveClient(clientID)

		// 销毁对象
		DestroyTcpClient(client)
	})

	return tcpServer
}

func (tcpServer *TcpServer) Start() {
	tcpServer.server.Start()
}

func (tcpServer *TcpServer) Stop() {
	tcpServer.server.Stop()
}

func registerHandler(tcpServer *TcpServer, server serveriface.IServer, lmqDaemon iface.ILmqDaemon) {
	server.RegisterHandler(PubID, &PubHandler{
		BaseHandler: BaseHandler{
			LmqDaemon: lmqDaemon,
		},
		tcpServer: tcpServer,
	})

	server.RegisterHandler(SubID, &SubHandler{
		BaseHandler: BaseHandler{
			LmqDaemon: lmqDaemon,
		},
		tcpServer: tcpServer,
	})

	/*
		Topic Handler
	*/
	server.RegisterHandler(CreateTopicID, &CreateChannelHandler{BaseHandler: BaseHandler{
		LmqDaemon: lmqDaemon,
	}})

	server.RegisterHandler(DeleteTopicID, &DeleteTopicHandler{
		BaseHandler: BaseHandler{
			LmqDaemon: lmqDaemon,
		},
	})

	server.RegisterHandler(EmptyTopicID, &EmptyTopicHandler{
		BaseHandler: BaseHandler{
			LmqDaemon: lmqDaemon,
		},
	})

	server.RegisterHandler(PauseTopicID, &PauseTopicHandler{
		BaseHandler: BaseHandler{
			LmqDaemon: lmqDaemon,
		},
	})

	server.RegisterHandler(UnPauseTopicID, &UnPauseTopicHandler{
		BaseHandler: BaseHandler{
			LmqDaemon: lmqDaemon,
		},
	})

	/*
		Channel Handler
	*/
	server.RegisterHandler(CreateChannelID, &CreateChannelHandler{
		BaseHandler: BaseHandler{
			LmqDaemon: lmqDaemon,
		},
	})

	server.RegisterHandler(DeleteChannelID, &DeleteChannelHandler{
		BaseHandler: BaseHandler{
			LmqDaemon: lmqDaemon,
		},
	})

	server.RegisterHandler(EmptyChannelID, &EmptyChannelHandler{
		BaseHandler: BaseHandler{
			LmqDaemon: lmqDaemon,
		},
	})

	server.RegisterHandler(PauseChannelID, &PauseChannelHandler{
		BaseHandler: BaseHandler{
			LmqDaemon: lmqDaemon,
		},
	})

	server.RegisterHandler(UnPauseChannelID, &UnPauseChannelHandler{
		BaseHandler: BaseHandler{
			LmqDaemon: lmqDaemon,
		},
	})
}
