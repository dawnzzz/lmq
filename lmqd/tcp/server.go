package tcp

import (
	"github.com/dawnzzz/hamble-tcp-server/conf"
	"github.com/dawnzzz/hamble-tcp-server/hamble"
	serveriface "github.com/dawnzzz/hamble-tcp-server/iface"
	"github.com/dawnzzz/lmq/config"
	"github.com/dawnzzz/lmq/iface"
	"github.com/dawnzzz/lmq/lmqd/tcp/handler"
)

type TcpServer struct {
	lmqDaemon iface.ILmqDaemon
	server    serveriface.IServer
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
	server.RegisterHandler(handler.PubID, &handler.PubHandler{handler.BaseHandler{
		LmqDaemon: lmqDaemon,
	}})

	/*
		Topic Handler
	*/
	server.RegisterHandler(handler.CreateTopicID, &handler.CreateChannelHandler{handler.BaseHandler{
		LmqDaemon: lmqDaemon,
	}})

	server.RegisterHandler(handler.DeleteTopicID, &handler.DeleteTopicHandler{handler.BaseHandler{
		LmqDaemon: lmqDaemon,
	}})

	server.RegisterHandler(handler.EmptyTopicID, &handler.EmptyTopicHandler{handler.BaseHandler{
		LmqDaemon: lmqDaemon,
	}})

	server.RegisterHandler(handler.PauseTopicID, &handler.PauseTopicHandler{handler.BaseHandler{
		LmqDaemon: lmqDaemon,
	}})

	server.RegisterHandler(handler.UnPauseTopicID, &handler.UnPauseTopicHandler{handler.BaseHandler{
		LmqDaemon: lmqDaemon,
	}})

	/*
		Channel Handler
	*/
	server.RegisterHandler(handler.CreateChannelID, &handler.CreateChannelHandler{handler.BaseHandler{
		LmqDaemon: lmqDaemon,
	}})

	server.RegisterHandler(handler.DeleteChannelID, &handler.DeleteChannelHandler{handler.BaseHandler{
		LmqDaemon: lmqDaemon,
	}})

	server.RegisterHandler(handler.EmptyChannelID, &handler.EmptyChannelHandler{handler.BaseHandler{
		LmqDaemon: lmqDaemon,
	}})

	server.RegisterHandler(handler.PauseChannelID, &handler.PauseChannelHandler{handler.BaseHandler{
		LmqDaemon: lmqDaemon,
	}})

	server.RegisterHandler(handler.UnPauseChannelID, &handler.UnPauseChannelHandler{handler.BaseHandler{
		LmqDaemon: lmqDaemon,
	}})
}
