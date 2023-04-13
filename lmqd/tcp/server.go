package tcp

import (
	"github.com/dawnzzz/hamble-tcp-server/conf"
	"github.com/dawnzzz/hamble-tcp-server/hamble"
	serveriface "github.com/dawnzzz/hamble-tcp-server/iface"
	"github.com/dawnzzz/lmq/config"
	"github.com/dawnzzz/lmq/iface"
)

type TcpServer struct {
	lmqDaemon iface.ILmqDaemon
	server    serveriface.IServer
}

func NewTcpServer(lmqDaemon iface.ILmqDaemon) *TcpServer {
	server := hamble.NewServerWithOption(&conf.Profile{
		Name:             "LMQD TCP Server",
		Host:             "0.0.0.0",
		Port:             6199,
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

	registerHandler(server, lmqDaemon)

	return &TcpServer{
		lmqDaemon: lmqDaemon,
		server:    server,
	}
}

func (tcpServer *TcpServer) Start() {
	tcpServer.server.Start()
}

func (tcpServer *TcpServer) Stop() {
	tcpServer.server.Stop()
}

func registerHandler(server serveriface.IServer, lmqDaemon iface.ILmqDaemon) {
	server.RegisterHandler(PubID, &PubHandler{BaseHandler{
		lmqDaemon: lmqDaemon,
	}})

	/*
		Topic Handler
	*/
	server.RegisterHandler(CreateTopicID, &CreateChannelHandler{BaseHandler{
		lmqDaemon: lmqDaemon,
	}})

	server.RegisterHandler(DeleteTopicID, &DeleteTopicHandler{BaseHandler{
		lmqDaemon: lmqDaemon,
	}})

	server.RegisterHandler(EmptyTopicID, &EmptyTopicHandler{BaseHandler{
		lmqDaemon: lmqDaemon,
	}})

	server.RegisterHandler(PauseTopicID, &PauseTopicHandler{BaseHandler{
		lmqDaemon: lmqDaemon,
	}})

	server.RegisterHandler(UnPauseTopicID, &UnPauseTopicHandler{BaseHandler{
		lmqDaemon: lmqDaemon,
	}})

	/*
		Channel Handler
	*/
	server.RegisterHandler(CreateChannelID, &CreateChannelHandler{BaseHandler{
		lmqDaemon: lmqDaemon,
	}})

	server.RegisterHandler(DeleteChannelID, &DeleteChannelHandler{BaseHandler{
		lmqDaemon: lmqDaemon,
	}})

	server.RegisterHandler(EmptyChannelID, &EmptyChannelHandler{BaseHandler{
		lmqDaemon: lmqDaemon,
	}})

	server.RegisterHandler(PauseChannelID, &PauseChannelHandler{BaseHandler{
		lmqDaemon: lmqDaemon,
	}})

	server.RegisterHandler(UnPauseChannelID, &UnPauseChannelHandler{BaseHandler{
		lmqDaemon: lmqDaemon,
	}})
}
