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

type TopicsHandler struct {
	*BaseHandler
}

func (h *TopicsHandler) Handle(request serveriface.IRequest) {
	// 查询所有的topic
	registrations := h.registrationDB.FindRegistrations(iface.TopicCategory, "*", "")
	topics := registrations.Keys()

	_ = h.SendDataResponse(request, topics)
}

type CreateTopicHandler struct {
	*BaseHandler
}

func (h *CreateTopicHandler) Handle(request serveriface.IRequest) {
	// 反序列化，得到topic name
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

	// 创建topic registration
	topicReg := topology.MakeRegistration(iface.TopicCategory, requestBody.TopicName, "")
	// 添加topic
	h.registrationDB.AddRegistration(topicReg)

	_ = h.SendOkResponse(request)
}

type DeleteTopicHandler struct {
	*BaseHandler
}

func (h *DeleteTopicHandler) Handle(request serveriface.IRequest) {
	// 反序列化，得到topic name
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

	// 删除topic下所有的channels
	chanRegs := h.registrationDB.FindRegistrations(iface.ChannelCategory, requestBody.TopicName, "*")
	for i := 0; i < chanRegs.Len(); i++ {
		chanReg := chanRegs.GetItem(i)
		h.registrationDB.RemoveRegistration(chanReg)
	}

	// 删除topic
	topicRegs := h.registrationDB.FindRegistrations(iface.TopicCategory, requestBody.TopicName, "")
	for i := 0; i < topicRegs.Len(); i++ {
		topicReg := topicRegs.GetItem(i)
		h.registrationDB.RemoveRegistration(topicReg)
	}

	_ = h.SendOkResponse(request)
}

type TombstoneHandler struct {
	*BaseHandler
}

func (h *TombstoneHandler) Handle(request serveriface.IRequest) {
	// 反序列化，得到topic name, remoteAddress, hostname, port
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

	// remoteAddress hostname port为空
	if requestBody.RemoteAddress == "" || requestBody.Hostname == "" || requestBody.TcpPort == 0 {
		_ = h.SendErrResponse(request, errors.New("tombstone topic command args invalid"))
		return
	}

	// tombstone
	producers := h.registrationDB.FindProducers(iface.TopicCategory, requestBody.TopicName, "")
	for i := 0; i < producers.Len(); i++ {
		p := producers.GetItem(i)
		info := p.GetLmqdInfo()
		if info.GetRemoteAddress() == requestBody.RemoteAddress && info.GetHostName() == requestBody.Hostname && info.GetTcpPort() == requestBody.TcpPort {
			p.Tombstone()
		}
	}

	_ = h.SendOkResponse(request)
}
