package handler

import (
	"encoding/json"
	"fmt"
	"github.com/dawnzzz/hamble-tcp-server/hamble"
	"github.com/dawnzzz/hamble-tcp-server/iface"
	"github.com/dawnzzz/lmq/internel/protocol"
	"github.com/dawnzzz/lmq/lmqd/message"
)

type RecvHandler struct {
	hamble.BaseHandler
}

func (h *RecvHandler) Handle(response iface.IRequest) {
	data := response.GetData()
	resp := &protocol.ResponseBody{}
	_ = json.Unmarshal(data, resp)
	fmt.Printf("recv:%#v\n", resp)
}

type RecvMessageHandler struct {
	hamble.BaseHandler
}

func (h *RecvMessageHandler) Handle(response iface.IRequest) {
	data := response.GetData()
	resp := &protocol.ResponseBody{Message: &message.Message{}}
	_ = json.Unmarshal(data, resp)
	fmt.Printf("recv msg:%s\n", resp.Message.GetData())

	msg := resp.Message
	msgResp, _ := json.Marshal(protocol.RequestBody{
		MessageID: msg.GetID(),
	})

	_ = response.GetConnection().SendBufMsg(protocol.FinID, msgResp)
}
