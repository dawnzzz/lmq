package tcp

import (
	serveriface "github.com/dawnzzz/hamble-tcp-server/iface"
	"github.com/dawnzzz/lmq/iface"
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
