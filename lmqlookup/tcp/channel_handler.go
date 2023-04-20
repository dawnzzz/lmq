package tcp

import (
	"errors"
	serveriface "github.com/dawnzzz/hamble-tcp-server/iface"
	"github.com/dawnzzz/lmq/iface"
	"github.com/dawnzzz/lmq/internel/protocol"
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
