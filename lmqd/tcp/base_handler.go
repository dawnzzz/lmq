package tcp

import (
	"github.com/dawnzzz/lmq/iface"
	"github.com/dawnzzz/lmq/internel/protocol"
)

// BaseHandler Lmqd的基础handler，lmqd中所有的handler都是继承于此的
type BaseHandler struct {
	protocol.BaseHandler
	LmqDaemon iface.ILmqDaemon
}
