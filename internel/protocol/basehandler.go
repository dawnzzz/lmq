package protocol

import (
	"github.com/dawnzzz/hamble-tcp-server/hamble"
	serveriface "github.com/dawnzzz/hamble-tcp-server/iface"
	"github.com/dawnzzz/lmq/iface"
)

type BaseHandler struct {
	hamble.BaseHandler
	TaskID uint32
}

func (h *BaseHandler) SendClientResponse(request serveriface.IRequest, msg iface.IMessage, err error) error {
	return request.GetConnection().SendBufMsg(h.TaskID, MakeClientResponse(h.TaskID, msg, err))
}

func (h *BaseHandler) SendOkResponse(request serveriface.IRequest) error {
	return h.SendClientResponse(request, nil, nil)
}

func (h *BaseHandler) SendErrResponse(request serveriface.IRequest, err error) error {
	return h.SendClientResponse(request, nil, err)
}
