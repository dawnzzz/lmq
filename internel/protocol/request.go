package protocol

import (
	"encoding/json"
	iface2 "github.com/dawnzzz/hamble-tcp-server/iface"
	"github.com/dawnzzz/lmq/iface"
)

type RequestBody struct {
	TopicName   string          `json:"topic_name,omitempty"`
	ChannelName string          `json:"channel_name,omitempty"`
	MessageData []byte          `json:"message_data,omitempty"`
	Count       int64           `json:"count,omitempty"`
	MessageID   iface.MessageID `json:"message_id,omitempty"`
}

func GetRequestBody(request iface2.IRequest) (*RequestBody, error) {
	data := request.GetData()

	requestBody := RequestBody{}
	err := json.Unmarshal(data, &requestBody)
	if err != nil {

		return nil, err
	}

	return &requestBody, nil
}
