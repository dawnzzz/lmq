package tcp

import (
	serveriface "github.com/dawnzzz/hamble-tcp-server/iface"
	"github.com/dawnzzz/lmq/internel/protocol"
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
	requestBody, err := protocol.GetRequestBody(request)
	if err != nil {
		_ = handler.SendErrResponse(request, err)
		return
	}

	// 创建新的channel
	topic, err := handler.BaseHandler.LmqDaemon.GetTopic(requestBody.TopicName)
	if err != nil {
		_ = handler.SendErrResponse(request, err)
		return
	}
	_, err = topic.GetChannel(requestBody.ChannelName)
	if err != nil {
		_ = handler.SendErrResponse(request, err)
		return
	}

	_ = handler.SendOkResponse(request)
}

// DeleteChannelHandler 删除channel
type DeleteChannelHandler struct {
	BaseHandler
}

func (handler *DeleteChannelHandler) Handle(request serveriface.IRequest) {
	// 反序列化，获取channel name
	requestBody, err := protocol.GetRequestBody(request)
	if err != nil {
		_ = handler.SendErrResponse(request, err)
		return
	}

	// 删除channel
	topic, err := handler.LmqDaemon.GetExistingTopic(requestBody.TopicName)
	if err != nil {
		// 发生错误，返回错误信息
		_ = handler.SendErrResponse(request, err)
		return
	}

	err = topic.DeleteExistingChannel(requestBody.ChannelName)
	if err != nil {
		// 发生错误，返回错误信息
		_ = handler.SendErrResponse(request, err)
		return
	}

	_ = handler.SendOkResponse(request)
}

// EmptyChannelHandler 清空channel
type EmptyChannelHandler struct {
	BaseHandler
}

func (handler *EmptyChannelHandler) Handle(request serveriface.IRequest) {
	// 反序列化，获取channel name
	requestBody, err := protocol.GetRequestBody(request)
	if err != nil {
		_ = handler.SendErrResponse(request, err)
		return
	}

	// 清空channel
	topic, err := handler.LmqDaemon.GetExistingTopic(requestBody.TopicName)
	if err != nil {
		// 发生错误，返回错误信息
		_ = handler.SendErrResponse(request, err)
		return
	}

	channel, err := topic.GetExistingChannel(requestBody.ChannelName)
	if err != nil {
		// 发生错误，返回错误信息
		_ = handler.SendErrResponse(request, err)
		return
	}

	err = channel.Empty()
	if err != nil {
		// 发生错误，返回错误信息
		_ = handler.SendErrResponse(request, err)
		return
	}

	_ = handler.SendOkResponse(request)
}

// PauseChannelHandler 暂停channel
type PauseChannelHandler struct {
	BaseHandler
}

func (handler *PauseChannelHandler) Handle(request serveriface.IRequest) {
	// 反序列化，获取channel name
	requestBody, err := protocol.GetRequestBody(request)
	if err != nil {
		_ = handler.SendErrResponse(request, err)
		return
	}

	// 暂停channel
	topic, err := handler.LmqDaemon.GetExistingTopic(requestBody.TopicName)
	if err != nil {
		// 发生错误，返回错误信息
		_ = handler.SendErrResponse(request, err)
		return
	}

	channel, err := topic.GetExistingChannel(requestBody.ChannelName)
	if err != nil {
		// 发生错误，返回错误信息
		_ = handler.SendErrResponse(request, err)
		return
	}

	err = channel.Pause()
	if err != nil {
		// 发生错误，返回错误信息
		_ = handler.SendErrResponse(request, err)
		return
	}

	_ = handler.SendOkResponse(request)
}

// UnPauseChannelHandler 恢复channel
type UnPauseChannelHandler struct {
	BaseHandler
}

func (handler *UnPauseChannelHandler) Handle(request serveriface.IRequest) {
	// 反序列化，获取channel name
	requestBody, err := protocol.GetRequestBody(request)
	if err != nil {
		_ = handler.SendErrResponse(request, err)
		return
	}

	// 恢复channel
	topic, err := handler.LmqDaemon.GetExistingTopic(requestBody.TopicName)
	if err != nil {
		// 发生错误，返回错误信息
		_ = handler.SendErrResponse(request, err)
		return
	}

	channel, err := topic.GetExistingChannel(requestBody.ChannelName)
	if err != nil {
		// 发生错误，返回错误信息
		_ = handler.SendErrResponse(request, err)
		return
	}

	err = channel.UnPause()
	if err != nil {
		// 发生错误，返回错误信息
		_ = handler.SendErrResponse(request, err)
		return
	}

	_ = handler.SendOkResponse(request)
}
