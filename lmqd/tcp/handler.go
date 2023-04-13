package tcp

import (
	"encoding/json"
	"github.com/dawnzzz/hamble-tcp-server/hamble"
	serveriface "github.com/dawnzzz/hamble-tcp-server/iface"
	"github.com/dawnzzz/lmq/iface"
	"github.com/dawnzzz/lmq/lmqd/message"
)

type BaseHandler struct {
	hamble.BaseHandler
	lmqDaemon iface.ILmqDaemon
}

type RequestBody struct {
	TopicName   string `json:"topic_name"`
	ChannelName string `json:"channel_name"`
	MessageData []byte `json:"message_data"`
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

const (
	OkID = iota
	ErrID
	PubID
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
)

// PubHandler 向一个topic中发送消息
type PubHandler struct {
	BaseHandler
}

func (handler *PubHandler) Handle(request serveriface.IRequest) {
	// 反序列化，获取topic name
	requestBody, err := getRequestBody(request)
	if err != nil {
		_ = request.GetConnection().SendBufMsg(ErrID, []byte(err.Error()))
		return
	}

	// 获取topic
	topic := handler.lmqDaemon.GetTopic(requestBody.TopicName)

	// 新建消息
	msg := message.NewMessage(topic.GenerateGUID(), requestBody.MessageData)

	// 发布消息
	err = topic.PutMessage(msg)
	if err != nil {
		_ = request.GetConnection().SendBufMsg(ErrID, []byte(err.Error()))
		return
	}

	_ = request.GetConnection().SendBufMsg(OkID, []byte("OK"))
}

// CreateTopicHandler 创建topic
type CreateTopicHandler struct {
	BaseHandler
}

func (handler *CreateTopicHandler) Handle(request serveriface.IRequest) {
	// 反序列化，获取topic name
	requestBody, err := getRequestBody(request)
	if err != nil {
		_ = request.GetConnection().SendBufMsg(ErrID, []byte(err.Error()))
		return
	}

	// 创建新的topic
	handler.BaseHandler.lmqDaemon.GetTopic(requestBody.TopicName)

	_ = request.GetConnection().SendBufMsg(OkID, []byte("OK"))
}

// DeleteTopicHandler 删除topic
type DeleteTopicHandler struct {
	BaseHandler
}

func (handler *DeleteTopicHandler) Handle(request serveriface.IRequest) {
	// 反序列化，获取topic name
	requestBody, err := getRequestBody(request)
	if err != nil {
		_ = request.GetConnection().SendBufMsg(ErrID, []byte(err.Error()))
		return
	}

	// 删除topic
	err = handler.lmqDaemon.DeleteExistingTopic(requestBody.TopicName)
	if err != nil {
		// 发生错误，返回错误信息
		_ = request.GetConnection().SendBufMsg(ErrID, []byte(err.Error()))
		return
	}

	_ = request.GetConnection().SendBufMsg(OkID, []byte("OK"))
}

// EmptyTopicHandler 清空topic
type EmptyTopicHandler struct {
	BaseHandler
}

func (handler *EmptyTopicHandler) Handle(request serveriface.IRequest) {
	// 反序列化，获取topic name
	requestBody, err := getRequestBody(request)
	if err != nil {
		_ = request.GetConnection().SendBufMsg(ErrID, []byte(err.Error()))
		return
	}

	// 清空topic
	topic, err := handler.lmqDaemon.GetExistingTopic(requestBody.TopicName)
	if err != nil {
		_ = request.GetConnection().SendBufMsg(ErrID, []byte(err.Error()))
		return
	}

	err = topic.Empty()
	if err != nil {
		_ = request.GetConnection().SendBufMsg(ErrID, []byte(err.Error()))
		return
	}

	_ = request.GetConnection().SendBufMsg(OkID, []byte("OK"))
}

// PauseTopicHandler 暂停topic
type PauseTopicHandler struct {
	BaseHandler
}

func (handler *PauseTopicHandler) Handle(request serveriface.IRequest) {
	// 反序列化，获取topic name
	requestBody, err := getRequestBody(request)
	if err != nil {
		_ = request.GetConnection().SendBufMsg(ErrID, []byte(err.Error()))
		return
	}

	// 暂停topic
	topic, err := handler.lmqDaemon.GetExistingTopic(requestBody.TopicName)
	if err != nil {
		_ = request.GetConnection().SendBufMsg(ErrID, []byte(err.Error()))
		return
	}

	err = topic.Pause()
	if err != nil {
		_ = request.GetConnection().SendBufMsg(ErrID, []byte(err.Error()))
		return
	}

	_ = request.GetConnection().SendBufMsg(OkID, []byte("OK"))
}

// UnPauseTopicHandler 恢复topic
type UnPauseTopicHandler struct {
	BaseHandler
}

func (handler *UnPauseTopicHandler) Handle(request serveriface.IRequest) {
	// 反序列化，获取topic name
	requestBody, err := getRequestBody(request)
	if err != nil {
		_ = request.GetConnection().SendBufMsg(ErrID, []byte(err.Error()))
		return
	}

	// 恢复topic
	topic, err := handler.lmqDaemon.GetExistingTopic(requestBody.TopicName)
	if err != nil {
		_ = request.GetConnection().SendBufMsg(ErrID, []byte(err.Error()))
		return
	}

	err = topic.UnPause()
	if err != nil {
		_ = request.GetConnection().SendBufMsg(ErrID, []byte(err.Error()))
		return
	}

	_ = request.GetConnection().SendBufMsg(OkID, []byte("OK"))
}

// CreateChannelHandler 创建channel
type CreateChannelHandler struct {
	BaseHandler
}

func (handler *CreateChannelHandler) Handle(request serveriface.IRequest) {
	// 反序列化，获取topic name, channel name
	requestBody, err := getRequestBody(request)
	if err != nil {
		_ = request.GetConnection().SendBufMsg(ErrID, []byte(err.Error()))
		return
	}

	// 创建新的channel
	topic := handler.BaseHandler.lmqDaemon.GetTopic(requestBody.TopicName)
	topic.GetChannel(requestBody.ChannelName)

	_ = request.GetConnection().SendBufMsg(OkID, []byte("OK"))
}

// DeleteChannelHandler 删除channel
type DeleteChannelHandler struct {
	BaseHandler
}

func (handler *DeleteChannelHandler) Handle(request serveriface.IRequest) {
	// 反序列化，获取channel name
	requestBody, err := getRequestBody(request)
	if err != nil {
		_ = request.GetConnection().SendBufMsg(ErrID, []byte(err.Error()))
		return
	}

	// 删除channel
	topic, err := handler.lmqDaemon.GetExistingTopic(requestBody.TopicName)
	if err != nil {
		// 发生错误，返回错误信息
		_ = request.GetConnection().SendBufMsg(ErrID, []byte(err.Error()))
		return
	}

	err = topic.DeleteExistingChannel(requestBody.ChannelName)
	if err != nil {
		// 发生错误，返回错误信息
		_ = request.GetConnection().SendBufMsg(ErrID, []byte(err.Error()))
		return
	}

	_ = request.GetConnection().SendBufMsg(OkID, []byte("OK"))
}

// EmptyChannelHandler 清空channel
type EmptyChannelHandler struct {
	BaseHandler
}

func (handler *EmptyChannelHandler) Handle(request serveriface.IRequest) {
	// 反序列化，获取channel name
	requestBody, err := getRequestBody(request)
	if err != nil {
		_ = request.GetConnection().SendBufMsg(1, []byte(err.Error()))
		return
	}

	// 清空channel
	topic, err := handler.lmqDaemon.GetExistingTopic(requestBody.TopicName)
	if err != nil {
		// 发生错误，返回错误信息
		_ = request.GetConnection().SendBufMsg(ErrID, []byte(err.Error()))
		return
	}

	channel, err := topic.GetExistingChannel(requestBody.ChannelName)
	if err != nil {
		// 发生错误，返回错误信息
		_ = request.GetConnection().SendBufMsg(ErrID, []byte(err.Error()))
		return
	}

	err = channel.Empty()
	if err != nil {
		// 发生错误，返回错误信息
		_ = request.GetConnection().SendBufMsg(ErrID, []byte(err.Error()))
		return
	}

	_ = request.GetConnection().SendBufMsg(OkID, []byte("OK"))
}

// PauseChannelHandler 暂停channel
type PauseChannelHandler struct {
	BaseHandler
}

func (handler *PauseChannelHandler) Handle(request serveriface.IRequest) {
	// 反序列化，获取channel name
	requestBody, err := getRequestBody(request)
	if err != nil {
		_ = request.GetConnection().SendBufMsg(ErrID, []byte(err.Error()))
		return
	}

	// 暂停channel
	topic, err := handler.lmqDaemon.GetExistingTopic(requestBody.TopicName)
	if err != nil {
		// 发生错误，返回错误信息
		_ = request.GetConnection().SendBufMsg(ErrID, []byte(err.Error()))
		return
	}

	channel, err := topic.GetExistingChannel(requestBody.ChannelName)
	if err != nil {
		// 发生错误，返回错误信息
		_ = request.GetConnection().SendBufMsg(ErrID, []byte(err.Error()))
		return
	}

	err = channel.Pause()
	if err != nil {
		// 发生错误，返回错误信息
		_ = request.GetConnection().SendBufMsg(ErrID, []byte(err.Error()))
		return
	}

	_ = request.GetConnection().SendBufMsg(OkID, []byte("OK"))
}

// UnPauseChannelHandler 恢复channel
type UnPauseChannelHandler struct {
	BaseHandler
}

func (handler *UnPauseChannelHandler) Handle(request serveriface.IRequest) {
	// 反序列化，获取channel name
	requestBody, err := getRequestBody(request)
	if err != nil {
		_ = request.GetConnection().SendBufMsg(ErrID, []byte(err.Error()))
		return
	}

	// 恢复channel
	topic, err := handler.lmqDaemon.GetExistingTopic(requestBody.TopicName)
	if err != nil {
		// 发生错误，返回错误信息
		_ = request.GetConnection().SendBufMsg(ErrID, []byte(err.Error()))
		return
	}

	channel, err := topic.GetExistingChannel(requestBody.ChannelName)
	if err != nil {
		// 发生错误，返回错误信息
		_ = request.GetConnection().SendBufMsg(ErrID, []byte(err.Error()))
		return
	}

	err = channel.UnPause()
	if err != nil {
		// 发生错误，返回错误信息
		_ = request.GetConnection().SendBufMsg(ErrID, []byte(err.Error()))
		return
	}

	_ = request.GetConnection().SendBufMsg(OkID, []byte("OK"))
}
