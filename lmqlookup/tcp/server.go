package tcp

import (
	"github.com/dawnzzz/hamble-tcp-server/conf"
	"github.com/dawnzzz/hamble-tcp-server/hamble"
	serveriface "github.com/dawnzzz/hamble-tcp-server/iface"
	"github.com/dawnzzz/lmq/config"
	"sync/atomic"
)

type TcpServer struct {
	server serveriface.IServer

	IsClosing atomic.Bool
	ExitChan  chan struct{}
}

func NewTcpServer() *TcpServer {
	server := hamble.NewServerWithOption(&conf.Profile{
		Name:             "LMQLookup TCP Server",
		Host:             config.GlobalLmqLookupConfig.TcpHost,
		Port:             config.GlobalLmqLookupConfig.TcpPort,
		TcpVersion:       "tcp4",
		MaxConn:          config.GlobalLmqLookupConfig.TcpServerMaxConn,
		WorkerPoolSize:   config.GlobalLmqLookupConfig.TcpServerWorkerPoolSize,
		MaxWorkerTaskLen: config.GlobalLmqLookupConfig.TcpServerMaxWorkerTaskLen,
		MaxMsgChanLen:    config.GlobalLmqLookupConfig.TcpServerMaxMsgChanLen,
		LogFileName:      "",
		MaxHeartbeatTime: int(config.GlobalLmqLookupConfig.InactiveProducerTimeout.Seconds()),
		CrtFileName:      "",
		KeyFileName:      "",
		PrintBanner:      false,
	})

	server.SetOnConnStart(func(conn serveriface.IConnection) {

	})

	server.SetOnConnStop(func(conn serveriface.IConnection) {

	})

	tcpServer := &TcpServer{
		server:   server,
		ExitChan: make(chan struct{}),
	}

	return tcpServer
}
