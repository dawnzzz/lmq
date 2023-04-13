package tcp

import (
	"encoding/json"
	"github.com/dawnzzz/hamble-tcp-server/hamble"
	serveriface "github.com/dawnzzz/hamble-tcp-server/iface"
	"github.com/dawnzzz/lmq/iface"
)

const (
	OkID = iota
	ErrID
	PubID
	SubID
	SendMsgID
	CreateTopicID
	DeleteTopicID
	EmptyTopicID
	PauseTopicID
	UnPauseTopicID
	CreateChannelID
	DeleteChannelID
	EmptyChannelID
	PauseChannelID
	UnPauseChannelID
	RydID
	FinID
	ReqID
)

type BaseHandler struct {
	hamble.BaseHandler
	LmqDaemon iface.ILmqDaemon
}

type RequestBody struct {
	TopicName   string          `json:"topic_name"`
	ChannelName string          `json:"channel_name"`
	MessageData []byte          `json:"message_data"`
	Count       int64           `json:"count"`
	MessageID   iface.MessageID `json:"message_id"`
}

func getRequestBody(request serveriface.IRequest) (*RequestBody, error) {
	data := request.GetData()

	requestBody := RequestBody{}
	err := json.Unmarshal(data, &requestBody)
	if err != nil {

		return nil, err
	}

	return &requestBody, nil
}
