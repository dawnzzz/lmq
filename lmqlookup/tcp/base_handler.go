package tcp

import (
	"github.com/dawnzzz/lmq/iface"
	"github.com/dawnzzz/lmq/internel/protocol"
)

type BaseHandler struct {
	protocol.BaseHandler
	registrationDB iface.IRegistrationDB
}

func RegisterBaseHandler(taskID uint32, registrationDB iface.IRegistrationDB) *BaseHandler {
	h := &BaseHandler{}
	h.TaskID = taskID
	h.registrationDB = registrationDB

	return h
}
