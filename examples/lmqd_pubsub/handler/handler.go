package handler

import (
	"encoding/json"
	"fmt"
	"github.com/dawnzzz/hamble-tcp-server/hamble"
	"github.com/dawnzzz/hamble-tcp-server/iface"
	"github.com/dawnzzz/lmq/lmqd/message"
	"github.com/dawnzzz/lmq/lmqd/tcp"
)

type RecvHandler struct {
	hamble.BaseHandler
}

func (h *RecvHandler) Handle(response iface.IRequest) {
	data := response.GetData()
	resp := &tcp.ResponseBody{}
	_ = json.Unmarshal(data, resp)
	fmt.Printf("recv:%#v\n", resp)
}

type RecvMessageHandler struct {
	hamble.BaseHandler
}

func (h *RecvMessageHandler) Handle(response iface.IRequest) {
	data := response.GetData()
	resp := &tcp.ResponseBody{Message: &message.Message{}}
	_ = json.Unmarshal(data, resp)
	fmt.Printf("recv msg:%s\n", resp.Message.GetData())

	msg := resp.Message
	msgResp, _ := json.Marshal(tcp.RequestBody{
		MessageID: msg.GetID(),
	})

	_ = response.GetConnection().SendBufMsg(tcp.FinID, msgResp)
}
