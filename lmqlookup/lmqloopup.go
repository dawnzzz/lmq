package lmqlookup

import (
	"github.com/dawnzzz/lmq/lmqlookup/tcp"
	"sync"
)

type LmqLookup struct {
	sync.RWMutex

	tcpServer tcp.TcpServer
}
