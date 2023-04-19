package tcp

import (
	"errors"
	serveriface "github.com/dawnzzz/hamble-tcp-server/iface"
	"github.com/dawnzzz/lmq/internel/protocol"
)

type RydHandler struct {
	BaseHandler
}

func (handler *RydHandler) Handle(request serveriface.IRequest) {
	// 反序列化，获取count
	requestBody, err := protocol.GetRequestBody(request)
	if err != nil {
		_ = handler.SendErrResponse(request, err)
		return
	}

	raw := request.GetConnection().GetProperty("client")
	client, ok := raw.(*TcpClient)
	if !ok {
		_ = handler.SendErrResponse(request, errors.New("server internal error"))
		return
	}

	count := requestBody.Count
	client.UpdateReady(count)

	_ = handler.SendOkResponse(request)
}

type FinHandler struct {
	BaseHandler
}

func (handler *FinHandler) Handle(request serveriface.IRequest) {
	// 反序列化，获取message id
	requestBody, err := protocol.GetRequestBody(request)
	if err != nil {
		_ = handler.SendErrResponse(request, err)
		return
	}

	raw := request.GetConnection().GetProperty("client")
	client, ok := raw.(*TcpClient)
	if !ok {
		_ = handler.SendErrResponse(request, errors.New("server internal error"))
		return
	}

	rawID := request.GetConnection().GetProperty("clientID")
	clientID, ok := rawID.(uint64)
	if !ok {
		_ = handler.SendErrResponse(request, errors.New("server internal error"))
		return
	}

	err = client.channel.FinishMessage(clientID, requestBody.MessageID)
	if err != nil {
		_ = handler.SendErrResponse(request, err)
		return
	}
	client.InFlightCount.Add(-1)

	_ = handler.SendOkResponse(request)
}

type ReqHandler struct {
	BaseHandler
}

func (handler *ReqHandler) Handle(request serveriface.IRequest) {
	// 反序列化，获取message id
	requestBody, err := protocol.GetRequestBody(request)
	if err != nil {
		_ = handler.SendErrResponse(request, err)
		return
	}

	raw := request.GetConnection().GetProperty("client")
	client, ok := raw.(*TcpClient)
	if !ok {
		_ = handler.SendErrResponse(request, errors.New("server internal error"))
		return
	}

	rawID := request.GetConnection().GetProperty("clientID")
	clientID, ok := rawID.(uint64)
	if !ok {
		_ = handler.SendErrResponse(request, errors.New("server internal error"))
		return
	}

	err = client.channel.RequeueMessage(clientID, requestBody.MessageID)
	if err != nil {
		_ = handler.SendErrResponse(request, err)
		return
	}
	client.RequeueCount.Add(1)

	_ = handler.SendOkResponse(request)
}
