package tcp

import (
	"errors"
	serveriface "github.com/dawnzzz/hamble-tcp-server/iface"
	"github.com/dawnzzz/lmq/iface"
	"github.com/dawnzzz/lmq/internel/protocol"
	"github.com/dawnzzz/lmq/lmqlookup/topology"
)

type IdentityHandler struct {
	*BaseHandler
}

func (h *IdentityHandler) Handle(request serveriface.IRequest) {
	if statusInit != request.GetConnection().GetProperty(statusPropertyKey).(status) {
		// 发起identity的阶段不对
		_ = h.SendErrResponse(request, errors.New("lookup status invalid"))
		return
	}

	// 进行反序列化
	requestBody, err := protocol.GetRequestBody(request)
	if err != nil {
		_ = h.SendErrResponse(request, err)
		return
	}

	// 校验参数
	if requestBody.RemoteAddress == "" || requestBody.Hostname == "" || requestBody.TcpPort == 0 {
		_ = h.SendErrResponse(request, errors.New("lookup identity command args invalid"))
		return
	}

	// 生成lmqd info信息
	info := topology.NewLmqdInfo(request.GetConnection().RemoteAddr(), requestBody.RemoteAddress, requestBody.Hostname, requestBody.TcpPort)
	// info存入上下文中，设置连接身份
	producer := topology.NewLmqProducer(info)
	h.registrationDB.AddProducer(topology.MakeRegistration(iface.LmqdCategory, "", ""), producer) // 存入db中
	request.GetConnection().SetProperty(producerPropertyKey, producer)
	request.GetConnection().SetProperty(statusPropertyKey, statusLmqd)

	_ = h.SendOkResponse(request)
}
