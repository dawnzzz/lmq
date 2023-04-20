package tcp

import (
	"errors"
	serveriface "github.com/dawnzzz/hamble-tcp-server/iface"
	"github.com/dawnzzz/lmq/config"
	"github.com/dawnzzz/lmq/iface"
	"github.com/dawnzzz/lmq/internel/protocol"
)

type LookupHandler struct {
	*BaseHandler
}

func (h *LookupHandler) Handle(request serveriface.IRequest) {
	// 反序列化，得到topic name
	requestBody, err := protocol.GetRequestBody(request)
	if err != nil {
		_ = h.SendErrResponse(request, err)
		return
	}

	if requestBody.TopicName == "" {
		_ = h.SendErrResponse(request, errors.New("lookup command topic name is not empty"))
		return
	}

	// 查询topic下的所有channels
	channels := h.registrationDB.FindRegistrations(iface.ChannelCategory, requestBody.TopicName, "*").SubKeys()

	// 查询topic下所有活跃的producers
	producers := h.registrationDB.FindProducers(iface.TopicCategory, requestBody.TopicName, "")
	producers = producers.FilterByActive(config.GlobalLmqLookupConfig.InactiveProducerTimeout, config.GlobalLmqLookupConfig.TombstoneLifetime)

	_ = h.SendDataResponse(request, map[string]interface{}{
		"channels":  channels,
		"producers": producers,
	})
}
