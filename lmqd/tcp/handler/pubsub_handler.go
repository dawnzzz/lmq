package handler

import (
	serveriface "github.com/dawnzzz/hamble-tcp-server/iface"
	"github.com/dawnzzz/lmq/lmqd/message"
)

/*
	包括发布和订阅的handler
*/

// PubHandler 向一个topic中发送消息
type PubHandler struct {
	BaseHandler
}

func (handler *PubHandler) Handle(request serveriface.IRequest) {
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

	_ = request.GetConnection().SendBufMsg(OkID, []byte("OK"))
}
