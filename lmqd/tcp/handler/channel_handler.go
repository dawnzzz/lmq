package handler

import (
	serveriface "github.com/dawnzzz/hamble-tcp-server/iface"
)

/*
	关于操作channel的handler
*/

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
	topic := handler.BaseHandler.LmqDaemon.GetTopic(requestBody.TopicName)
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
	topic, err := handler.LmqDaemon.GetExistingTopic(requestBody.TopicName)
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
	topic, err := handler.LmqDaemon.GetExistingTopic(requestBody.TopicName)
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
	topic, err := handler.LmqDaemon.GetExistingTopic(requestBody.TopicName)
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
	topic, err := handler.LmqDaemon.GetExistingTopic(requestBody.TopicName)
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
