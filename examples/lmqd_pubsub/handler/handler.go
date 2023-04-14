package handler

import (
	"encoding/json"
	"fmt"
	"github.com/dawnzzz/hamble-tcp-server/hamble"
	"github.com/dawnzzz/hamble-tcp-server/iface"
	"github.com/dawnzzz/lmq/lmqd/message"
	"github.com/dawnzzz/lmq/lmqd/tcp"
)

type OkHandler struct {
	hamble.BaseHandler
}

func (h *OkHandler) Handle(response iface.IRequest) {
	fmt.Printf("recv:%s\n", response.GetData())
}

type ErrHandler struct {
	hamble.BaseHandler
}

func (h *ErrHandler) Handle(response iface.IRequest) {
	fmt.Printf("recv:%s\n", response.GetData())
}

type RecvMessageHandler struct {
	hamble.BaseHandler
}

func (h *RecvMessageHandler) Handle(response iface.IRequest) {
	data := response.GetData()
	msg := &message.Message{}
	_ = json.Unmarshal(data, msg)
	fmt.Printf("recv msg:%#s\n", msg.Data)

	resp, _ := json.Marshal(tcp.RequestBody{
		MessageID: msg.ID,
	})

	_ = response.GetConnection().SendBufMsg(tcp.FinID, resp)
}
