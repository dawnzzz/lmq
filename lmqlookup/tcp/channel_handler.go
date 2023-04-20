package tcp

import (
	"errors"
	serveriface "github.com/dawnzzz/hamble-tcp-server/iface"
	"github.com/dawnzzz/lmq/iface"
	"github.com/dawnzzz/lmq/internel/protocol"
	"github.com/dawnzzz/lmq/internel/utils"
	"github.com/dawnzzz/lmq/lmqlookup/topology"
	"github.com/dawnzzz/lmq/pkg/e"
)

type ChannelsHandlers struct {
	*BaseHandler
}

func (h *ChannelsHandlers) Handle(request serveriface.IRequest) {
	// 反序列化，得到topic name
	requestBody, err := protocol.GetRequestBody(request)
	if err != nil {
		_ = h.SendErrResponse(request, err)
		return
	}

	if requestBody.TopicName == "" {
		_ = h.SendErrResponse(request, errors.New("channels command topic name is not empty"))
		return
	}

	// 查血channels
	registrations := h.registrationDB.FindRegistrations(iface.ChannelCategory, requestBody.TopicName, "*")
	channels := registrations.SubKeys()

	_ = h.SendDataResponse(request, channels)
}

type CreateChannel struct {
	*BaseHandler
}

func (h *CreateChannel) Handle(request serveriface.IRequest) {
	// 反序列化
	requestBody, err := protocol.GetRequestBody(request)
	if err != nil {
		_ = h.SendErrResponse(request, err)
		return
	}

	// 验证topic name是否有效
	if !utils.TopicOrChannelNameIsValid(requestBody.TopicName) {
		_ = h.SendErrResponse(request, e.ErrTopicNameInValid)
		return
	}

	// 验证channel name是否有效
	if !utils.TopicOrChannelNameIsValid(requestBody.ChannelName) {
		_ = h.SendErrResponse(request, e.ErrTopicNameInValid)
		return
	}

	// 添加channel和topic
	chanReg := topology.MakeRegistration(iface.ChannelCategory, requestBody.TopicName, requestBody.ChannelName)
	h.registrationDB.AddRegistration(chanReg)
	topicReg := topology.MakeRegistration(iface.TopicCategory, requestBody.TopicName, "")
	h.registrationDB.AddRegistration(topicReg)

	_ = h.SendOkResponse(request)
}

type DeleteChannel struct {
	*BaseHandler
}

func (h *DeleteChannel) Handle(request serveriface.IRequest) {
	// 反序列化
	requestBody, err := protocol.GetRequestBody(request)
	if err != nil {
		_ = h.SendErrResponse(request, err)
		return
	}

	// 验证topic name是否有效
	if !utils.TopicOrChannelNameIsValid(requestBody.TopicName) {
		_ = h.SendErrResponse(request, e.ErrTopicNameInValid)
		return
	}

	// 验证channel name是否有效
	if !utils.TopicOrChannelNameIsValid(requestBody.ChannelName) {
		_ = h.SendErrResponse(request, e.ErrTopicNameInValid)
		return
	}

	// 查找channel
	chanRegs := h.registrationDB.FindRegistrations(iface.ChannelCategory, requestBody.TopicName, requestBody.ChannelName)
	if chanRegs.Len() == 0 {
		_ = h.SendErrResponse(request, e.ErrChannelNotFound)
		return
	}

	// 删除channel
	for i := 0; i < chanRegs.Len(); i++ {
		chanReg := chanRegs.GetItem(i)
		h.registrationDB.RemoveRegistration(chanReg)
	}

	_ = h.SendOkResponse(request)
}
