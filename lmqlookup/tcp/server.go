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

type status uint8

const (
	statusInit   = status(iota) // lmqd或者客户端连接了lmq lookup，还未表露身份
	statusLmqd                  // lmqd已经发送了identity命令，开始与lmq lookup进行同步
	statusClient                // 是客户端连接，禁止发送lmqd与lookup的相关命令
)

const (
	statusPropertyKey   = "status"
	producerPropertyKey = "producer"
)

type TcpServer struct {
	server serveriface.IServer

	registrationDB iface.IRegistrationDB // lmq lookup中用于记录lmqd拓扑的结构

	connStatusMap     map[serveriface.IConnection]status // 记录连接的状态
	connStatusMapLock sync.RWMutex

	IsClosing atomic.Bool
	ExitChan  chan struct{}
}

func NewTcpServer(registrationDB iface.IRegistrationDB) *TcpServer {
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

	// 注册路由
	registerHandler(server, registrationDB)

	tcpServer := &TcpServer{
		registrationDB: registrationDB,
		server:         server,
		ExitChan:       make(chan struct{}),
	}

	server.SetOnConnStart(func(conn serveriface.IConnection) {
		logger.Infof("on conn start hook func %s", conn.RemoteAddr())
		// 连接开始时记录连接状态
		tcpServer.connStatusMapLock.Lock()
		tcpServer.connStatusMap[conn] = statusInit
		tcpServer.connStatusMapLock.Unlock()

		conn.SetProperty(statusPropertyKey, statusInit)
	})

	server.SetOnConnStop(func(conn serveriface.IConnection) {
		logger.Infof("on conn end hook func from %s", conn.RemoteAddr())
		// 连接结束后删除连接状态
		tcpServer.connStatusMapLock.Lock()
		delete(tcpServer.connStatusMap, conn)
		tcpServer.connStatusMapLock.Unlock()
	})

	return tcpServer
}

func (tcpServer *TcpServer) Start() {
	tcpServer.server.Start()
}

func (tcpServer *TcpServer) Stop() {
	if !tcpServer.IsClosing.CompareAndSwap(false, true) {
		// 如果已经关闭，直接返回
		return
	}

	// 停止tcp服务器
	tcpServer.server.Stop()

	close(tcpServer.ExitChan)
}

func registerHandler(server serveriface.IServer, registrationDB iface.IRegistrationDB) {
	// identity
	server.RegisterHandler(protocol.IdentityID, &IdentityHandler{
		RegisterBaseHandler(protocol.IdentityID, registrationDB),
	})

	// register
	server.RegisterHandler(protocol.RegisterID, &RegisterHandler{
		RegisterBaseHandler(protocol.RegisterID, registrationDB),
	})

	// unregister
	server.RegisterHandler(protocol.UnRegisterID, &UnRegisterHandler{
		RegisterBaseHandler(protocol.UnRegisterID, registrationDB),
	})

	// topics
	server.RegisterHandler(protocol.TopicsID, &TopicsHandler{
		RegisterBaseHandler(protocol.TopicsID, registrationDB),
	})

	// channels
	server.RegisterHandler(protocol.ChannelsID, &ChannelsHandlers{
		RegisterBaseHandler(protocol.ChannelsID, registrationDB),
	})

	// lookup
	server.RegisterHandler(protocol.LookupID, &LookupHandler{
		RegisterBaseHandler(protocol.LookupID, registrationDB),
	})

	// create topic
	server.RegisterHandler(protocol.CreateTopicID, &CreateTopicHandler{
		RegisterBaseHandler(protocol.CreateTopicID, registrationDB),
	})
}
