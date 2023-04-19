package protocol

import (
	"encoding/json"
	"github.com/dawnzzz/lmq/iface"
)

type ResponseBody struct {
	TaskID    uint32         `json:"task_id"`
	IsError   bool           `json:"is_error"`
	StatusMsg string         `json:"status_msg"`        // 当发生错误时，为错误提示信息，否则为OK
	Message   iface.IMessage `json:"message,omitempty"` // 消息数据
}

func MakeResponse(taskID uint32, msg iface.IMessage, err error) []byte {
	responseBody := &ResponseBody{
		TaskID: taskID,
	}
	if err != nil {
		responseBody.IsError = true
		responseBody.StatusMsg = err.Error()
	} else if msg != nil {
		responseBody.StatusMsg = "OK"
		responseBody.Message = msg
	} else {
		responseBody.StatusMsg = "OK"
	}

	bytes, _ := json.Marshal(&responseBody)

	return bytes
}
