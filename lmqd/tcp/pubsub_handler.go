package tcp

import (
	"errors"
	serveriface "github.com/dawnzzz/hamble-tcp-server/iface"
	"github.com/dawnzzz/lmq/lmqd/message"
)

/*
	包括发布和订阅的handler
*/

// PubHandler 向一个topic中发送消息
type PubHandler struct {
	BaseHandler
	tcpServer *TcpServer
}

func (handler *PubHandler) Handle(request serveriface.IRequest) {
	// 获取client
	client, _, err := getClient(handler.tcpServer, request)
	if err != nil {
		_ = request.GetConnection().SendBufMsg(ErrID, []byte(err.Error()))
		return
	}

	if !client.IsReadyPub() {
		_ = request.GetConnection().SendBufMsg(ErrID, []byte("not ready for pub"))
		return
	}

	// 反序列化，获取topic name
	requestBody, err := getRequestBody(request)
	if err != nil {
		_ = request.GetConnection().SendBufMsg(ErrID, []byte(err.Error()))
		return
	}

	// 获取topic
	topic := handler.LmqDaemon.GetTopic(requestBody.TopicName)

	// 新建消息
	msg := message.NewMessage(topic.GenerateGUID(), requestBody.MessageData)

	// 发布消息
	err = topic.PutMessage(msg)
	if err != nil {
		_ = request.GetConnection().SendBufMsg(ErrID, []byte(err.Error()))
		return
	}

	client.MessageCount.Add(1)
	_ = request.GetConnection().SendBufMsg(OkID, []byte("OK"))
}

// SubHandler 订阅一个topic、channel
type SubHandler struct {
	BaseHandler
	tcpServer *TcpServer
}

func (handler *SubHandler) Handle(request serveriface.IRequest) {
	// 获取client
	client, clientID, err := getClient(handler.tcpServer, request)
	if err != nil {
		_ = request.GetConnection().SendBufMsg(ErrID, []byte(err.Error()))
		return
	}

	if !client.IsReadySub() {
		_ = request.GetConnection().SendBufMsg(ErrID, []byte("not ready for sub"))
		return
	}

	// 反序列化，获取topic name
	requestBody, err := getRequestBody(request)
	if err != nil {
		_ = request.GetConnection().SendBufMsg(ErrID, []byte(err.Error()))
		return
	}

	// 获取topic
	topic, err := handler.LmqDaemon.GetExistingTopic(requestBody.TopicName)
	if err != nil {
		_ = request.GetConnection().SendBufMsg(ErrID, []byte(err.Error()))
		return
	}

	// 获取channel
	c, err := topic.GetExistingChannel(requestBody.ChannelName)
	if err != nil {
		_ = request.GetConnection().SendBufMsg(ErrID, []byte(err.Error()))
		return
	}

	// 将用户添加到channel的用户组中
	err = c.AddClient(clientID, client)
	if err != nil {
		_ = request.GetConnection().SendBufMsg(ErrID, []byte(err.Error()))
		return
	}

	_ = request.GetConnection().SendBufMsg(OkID, []byte("OK"))
}

func getClient(tcpServer *TcpServer, request serveriface.IRequest) (*TcpClient, uint64, error) {
	raw := request.GetConnection().GetProperty("clientID")
	clientID, ok := raw.(uint64)
	if !ok {
		return nil, 0, errors.New("server internal error")
	}

	tcpServer.clientMapLock.RLock()
	client, ok := tcpServer.clientMap[clientID]
	if !ok {
		return nil, 0, errors.New("server internal error")
	}
	tcpServer.clientMapLock.RUnlock()

	return client, clientID, nil
}
