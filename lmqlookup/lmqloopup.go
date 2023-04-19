package lmqlookup

import (
	"github.com/dawnzzz/lmq/iface"
	"github.com/dawnzzz/lmq/lmqlookup/tcp"
	"github.com/dawnzzz/lmq/lmqlookup/topology"
	"sync"
)

type LmqLookup struct {
	sync.RWMutex

	tcpServer *tcp.TcpServer

	registrationDB *topology.RegistrationDB // 用于存储拓扑结构
}

func NewLmqLookUp() iface.ILmqLookup {
	lmqLookup := &LmqLookup{
		registrationDB: topology.NewRegistrationDB(),
	}

	lmqLookup.tcpServer = tcp.NewTcpServer()

	return lmqLookup
}

func (lmqLookup *LmqLookup) Stop() {
	//TODO implement me
	panic("implement me")
}

func (lmqLookup *LmqLookup) Main() {
	//TODO implement me
	panic("implement me")
}
