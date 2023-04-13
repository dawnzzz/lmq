package tcp

import (
	serveriface "github.com/dawnzzz/hamble-tcp-server/iface"
)

/*
	关于操作topic的handler
*/

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
	handler.BaseHandler.LmqDaemon.GetTopic(requestBody.TopicName)

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
	err = handler.LmqDaemon.DeleteExistingTopic(requestBody.TopicName)
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
	topic, err := handler.LmqDaemon.GetExistingTopic(requestBody.TopicName)
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
	topic, err := handler.LmqDaemon.GetExistingTopic(requestBody.TopicName)
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
	topic, err := handler.LmqDaemon.GetExistingTopic(requestBody.TopicName)
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
