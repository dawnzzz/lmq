package tcp

import (
	"errors"
	serveriface "github.com/dawnzzz/hamble-tcp-server/iface"
	"github.com/dawnzzz/lmq/iface"
	"github.com/dawnzzz/lmq/internel/protocol"
	"github.com/dawnzzz/lmq/lmqlookup/topology"
)

type RegisterHandler struct {
	*BaseHandler
}

func (h *RegisterHandler) Handle(request serveriface.IRequest) {
	if statusLmqd != request.GetConnection().GetProperty(statusPropertyKey).(status) {
		// 发起identity身份段不对，只有lmqd才能发起这个命令
		_ = h.SendErrResponse(request, errors.New("lookup status invalid"))
		return
	}

	// 反序列化
	requestBody, err := protocol.GetRequestBody(request)
	if err != nil {
		_ = h.SendErrResponse(request, err)
		return
	}

	if requestBody.TopicName == "" {
		_ = h.SendErrResponse(request, errors.New("register/unregister command topic name is not empty"))
		return
	}

	// 注册topic
	topicName := requestBody.TopicName
	topicReg := topology.MakeRegistration(iface.TopicCategory, topicName, "")
	producer := request.GetConnection().GetProperty(producerPropertyKey).(iface.ILmqdProducer)
	h.registrationDB.AddProducer(topicReg, producer)

	// 注册channel
	channelName := requestBody.ChannelName
	if channelName != "" {
		channelReg := topology.MakeRegistration(iface.TopicCategory, topicName, channelName)
		h.registrationDB.AddProducer(channelReg, producer)
	}

	_ = h.SendOkResponse(request)
}

type UnRegisterHandler struct {
	*BaseHandler
}

func (h *UnRegisterHandler) Handle(request serveriface.IRequest) {
	if statusLmqd != request.GetConnection().GetProperty(statusPropertyKey).(status) {
		// 发起identity身份段不对，只有lmqd才能发起这个命令
		_ = h.SendErrResponse(request, errors.New("lookup status invalid"))
		return
	}
	// 反序列化
	requestBody, err := protocol.GetRequestBody(request)
	if err != nil {
		_ = h.SendErrResponse(request, err)
		return
	}

	if requestBody.TopicName == "" {
		_ = h.SendErrResponse(request, errors.New("register/unregister command topic name is not empty"))
		return
	}

	topicName := requestBody.TopicName
	producer := request.GetConnection().GetProperty(producerPropertyKey).(iface.ILmqdProducer)

	// 取消注册channel
	channelName := requestBody.ChannelName
	if channelName != "" {
		channelReg := topology.MakeRegistration(iface.TopicCategory, topicName, channelName)
		h.registrationDB.RemoveProducer(channelReg, producer.GetLmqdInfo().GetID())

		_ = h.SendOkResponse(request)
		return
	}

	// channel name为空，取消注册topic
	topicReg := topology.MakeRegistration(iface.TopicCategory, topicName, "*")
	h.registrationDB.RemoveProducer(topicReg, producer.GetLmqdInfo().GetID())

	_ = h.SendOkResponse(request)
}
