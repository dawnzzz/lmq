package tcp

import (
	serveriface "github.com/dawnzzz/hamble-tcp-server/iface"
)

type RydHandler struct {
	BaseHandler
}

func (handler *RydHandler) Handle(request serveriface.IRequest) {
	// 反序列化，获取count
	requestBody, err := getRequestBody(request)
	if err != nil {
		_ = request.GetConnection().SendBufMsg(ErrID, []byte(err.Error()))
		return
	}

	raw := request.GetConnection().GetProperty("client")
	client, ok := raw.(*TcpClient)
	if !ok {
		_ = request.GetConnection().SendBufMsg(ErrID, []byte("server internal error"))
		return
	}

	count := requestBody.Count
	client.UpdateReady(count)

	_ = request.GetConnection().SendBufMsg(OkID, []byte("OK"))
}

type FinHandler struct {
	BaseHandler
}

func (handler *FinHandler) Handle(request serveriface.IRequest) {
	// 反序列化，获取message id
	requestBody, err := getRequestBody(request)
	if err != nil {
		_ = request.GetConnection().SendBufMsg(ErrID, []byte(err.Error()))
		return
	}

	raw := request.GetConnection().GetProperty("client")
	client, ok := raw.(*TcpClient)
	if !ok {
		_ = request.GetConnection().SendBufMsg(ErrID, []byte("server internal error"))
		return
	}

	rawID := request.GetConnection().GetProperty("clientID")
	clientID, ok := rawID.(uint64)
	if !ok {
		_ = request.GetConnection().SendBufMsg(ErrID, []byte("server internal error"))
		return
	}

	err = client.channel.FinishMessage(clientID, requestBody.MessageID)
	if err != nil {
		_ = request.GetConnection().SendBufMsg(ErrID, []byte(err.Error()))
		return
	}
	client.InFlightCount.Add(-1)

	_ = request.GetConnection().SendBufMsg(OkID, []byte("OK"))
}

type ReqHandler struct {
	BaseHandler
}

func (handler *ReqHandler) Handle(request serveriface.IRequest) {
	// 反序列化，获取message id
	requestBody, err := getRequestBody(request)
	if err != nil {
		_ = request.GetConnection().SendBufMsg(ErrID, []byte(err.Error()))
		return
	}

	raw := request.GetConnection().GetProperty("client")
	client, ok := raw.(*TcpClient)
	if !ok {
		_ = request.GetConnection().SendBufMsg(ErrID, []byte("server internal error"))
		return
	}

	rawID := request.GetConnection().GetProperty("clientID")
	clientID, ok := rawID.(uint64)
	if !ok {
		_ = request.GetConnection().SendBufMsg(ErrID, []byte("server internal error"))
		return
	}

	err = client.channel.RequeueMessage(clientID, requestBody.MessageID)
	if err != nil {
		_ = request.GetConnection().SendBufMsg(ErrID, []byte(err.Error()))
		return
	}
	client.RequeueCount.Add(1)

	_ = request.GetConnection().SendBufMsg(OkID, []byte("OK"))
}
