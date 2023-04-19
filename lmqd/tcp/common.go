package tcp

import (
	"encoding/json"
	"github.com/dawnzzz/hamble-tcp-server/hamble"
	serveriface "github.com/dawnzzz/hamble-tcp-server/iface"
	"github.com/dawnzzz/lmq/iface"
)

type BaseHandler struct {
	hamble.BaseHandler
	LmqDaemon iface.ILmqDaemon
	TaskID    uint32
}

func (h *BaseHandler) SendResponse(request serveriface.IRequest, msg iface.IMessage, err error) error {
	return request.GetConnection().SendBufMsg(h.TaskID, makeResponse(h.TaskID, msg, err))
}

func (h *BaseHandler) SendOkResponse(request serveriface.IRequest) error {
	return h.SendResponse(request, nil, nil)
}

func (h *BaseHandler) SendErrResponse(request serveriface.IRequest, err error) error {
	return h.SendResponse(request, nil, err)
}

type RequestBody struct {
	TopicName   string          `json:"topic_name"`
	ChannelName string          `json:"channel_name"`
	MessageData []byte          `json:"message_data"`
	Count       int64           `json:"count"`
	MessageID   iface.MessageID `json:"message_id"`
}

type ResponseBody struct {
	TaskID  uint32         `json:"task_id"`
	IsError bool           `json:"is_error"`
	Status  string         `json:"status"`  // 当发生错误时，为错误提示信息，否则为OK
	Message iface.IMessage `json:"message"` // 数据
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

func makeResponse(taskID uint32, msg iface.IMessage, err error) []byte {
	responseBody := &ResponseBody{
		TaskID: taskID,
	}
	if err != nil {
		responseBody.IsError = true
		responseBody.Status = err.Error()
	} else if msg != nil {
		responseBody.Status = "OK"
		responseBody.Message = msg
	} else {
		responseBody.Status = "OK"
	}

	bytes, _ := json.Marshal(&responseBody)

	return bytes
}
