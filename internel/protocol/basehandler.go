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

func (h *BaseHandler) SendMessageResponse(request serveriface.IRequest, msg iface.IMessage) error {
	return request.GetConnection().SendBufMsg(h.TaskID, MakeMessageResponse(h.TaskID, msg))
}

func (h *BaseHandler) SendDataResponse(request serveriface.IRequest, data interface{}) error {
	return request.GetConnection().SendBufMsg(h.TaskID, MakeDataResponse(h.TaskID, data))
}

func (h *BaseHandler) SendStatusResponse(request serveriface.IRequest, err error) error {
	return request.GetConnection().SendBufMsg(h.TaskID, MakeStatusResponse(h.TaskID, err))
}

func (h *BaseHandler) SendOkResponse(request serveriface.IRequest) error {
	return h.SendStatusResponse(request, nil)
}

func (h *BaseHandler) SendErrResponse(request serveriface.IRequest, err error) error {
	return h.SendStatusResponse(request, err)
}
