package protocol

import (
	"bytes"
	"encoding/json"
	"github.com/dawnzzz/lmq/iface"
	"sync"
)

// 利用buf可以避免对象的重复分配
var bufPool = sync.Pool{
	New: func() interface{} {
		return &bytes.Buffer{}
	},
}

type ResponseBody struct {
	TaskID    uint32         `json:"task_id"`
	IsError   bool           `json:"is_error"`
	StatusMsg string         `json:"status_msg"`        // 当发生错误时，为错误提示信息，否则为OK
	Message   iface.IMessage `json:"message,omitempty"` // 消息数据
	Data      interface{}    `json:"data,omitempty"`    // 其他数据，如lookup topics中，就是topic列表。
}

func MakeStatusResponse(taskID uint32, err error) []byte {
	responseBody := &ResponseBody{
		TaskID: taskID,
	}

	if err != nil {
		responseBody.IsError = true
		responseBody.StatusMsg = err.Error()
	} else {
		responseBody.StatusMsg = "OK"
	}

	buffer := bufPool.Get().(*bytes.Buffer)
	defer bufPool.Put(buffer)
	buffer.Reset()

	_ = json.NewEncoder(buffer).Encode(&responseBody)

	return buffer.Bytes()
}

func MakeMessageResponse(taskID uint32, msg iface.IMessage) []byte {
	responseBody := &ResponseBody{
		TaskID:    taskID,
		StatusMsg: "OK",
		Message:   msg,
	}

	buffer := bufPool.Get().(*bytes.Buffer)
	defer bufPool.Put(buffer)
	buffer.Reset()

	_ = json.NewEncoder(buffer).Encode(&responseBody)

	return buffer.Bytes()
}

func MakeDataResponse(taskID uint32, data interface{}) []byte {
	responseBody := &ResponseBody{
		TaskID:    taskID,
		StatusMsg: "OK",
		Data:      data,
	}

	buffer := bufPool.Get().(*bytes.Buffer)
	defer bufPool.Put(buffer)
	buffer.Reset()

	_ = json.NewEncoder(buffer).Encode(&responseBody)

	return buffer.Bytes()
}
