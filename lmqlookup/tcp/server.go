package tcp

import (
	"errors"
	"github.com/dawnzzz/hamble-tcp-server/conf"
	"github.com/dawnzzz/hamble-tcp-server/hamble"
	serveriface "github.com/dawnzzz/hamble-tcp-server/iface"
	"github.com/dawnzzz/lmq/config"
	"github.com/dawnzzz/lmq/iface"
	"github.com/dawnzzz/lmq/internel/protocol"
	"github.com/dawnzzz/lmq/logger"
	"sync/atomic"
	"time"
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
		conn.SetProperty(statusPropertyKey, statusInit)
	})

	server.SetOnConnStop(func(conn serveriface.IConnection) {
		logger.Infof("on conn end hook func from %s", conn.RemoteAddr())
		// 连接结束后，如果是lmqd，则删除producer
		if conn.GetProperty(statusPropertyKey) == statusLmqd {
			producer, ok := conn.GetProperty(producerPropertyKey).(iface.ILmqdProducer)
			if !ok {
				return
			}

			// 从所有的registration中删除producer
			registrationDB.RemoveProducerFromAllRegistrations(producer.GetLmqdInfo().GetID())
		}
	})

	return tcpServer
}

func (tcpServer *TcpServer) Start() {
	// 开启心跳检测
	tcpServer.server.StartHeartbeatWithOption(serveriface.CheckerOption{
		Interval: config.GlobalLmqLookupConfig.InactiveProducerTimeout,
		HeartBeatFunc: func(_ serveriface.IConnection) error {
			// lmq lookup不会主动发送心跳消息
			return nil
		},
		MsgID: protocol.PingID,
		Handler: &pingHandler{
			RegisterBaseHandler(protocol.PingID, tcpServer.registrationDB),
		},
	})

	tcpServer.server.Start()
}

// 定义收到心跳消息后的行为
type pingHandler struct {
	*BaseHandler
}

func (h *pingHandler) Handle(request serveriface.IRequest) {
	// 更新活跃时间
	now := time.Now()
	producer, ok := request.GetConnection().GetProperty(producerPropertyKey).(iface.ILmqdProducer)
	if !ok {
		_ = h.SendErrResponse(request, errors.New("not identify"))
		return
	}
	request.GetConnection().SetProperty(statusPropertyKey, statusLmqd) // 连接身份更改为lmqd
	producer.GetLmqdInfo().SetLastUpdate(now)

	// 收到ping后，返回ok
	_ = h.SendOkResponse(request)
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

	// delete topic
	server.RegisterHandler(protocol.DeleteTopicID, &DeleteTopicHandler{
		RegisterBaseHandler(protocol.DeleteTopicID, registrationDB),
	})

	// create channel
	server.RegisterHandler(protocol.CreateChannelID, &CreateChannel{
		RegisterBaseHandler(protocol.CreateChannelID, registrationDB),
	})

	// delete channel
	server.RegisterHandler(protocol.DeleteChannelID, &DeleteChannel{
		RegisterBaseHandler(protocol.DeleteChannelID, registrationDB),
	})

	// tombstone topic
	server.RegisterHandler(protocol.TombstoneTopicID, &TombstoneHandler{
		RegisterBaseHandler(protocol.TombstoneTopicID, registrationDB),
	})

	// nodes
	server.RegisterHandler(protocol.NodesID, &NodesHandler{
		RegisterBaseHandler(protocol.NodesID, registrationDB),
	})
}
