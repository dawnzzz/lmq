package iface

import (
	"github.com/dawnzzz/lmq/lmqd/backendqueue"
	"time"
)

type IChannel interface {
	Pause() error    // 暂停channel
	UnPause() error  // 恢复channel
	Empty() error    // 清空channel
	Close() error    // 关闭channel
	Delete() error   // 关闭并删除channel
	IsPausing() bool // 返回是否处于暂停状态
	IsExiting() bool // 是否退出

	GetMemoryMsgChan() chan IMessage
	GetBackendQueue() backendqueue.BackendQueue

	AddClient(clientID uint64, client IConsumer) error // 添加一个订阅的用户
	RemoveClient(clientID uint64)                      // 移除一个订阅的用户

	GetName() string                   // 获取一个channel的name
	GetTopicName() string              // 获取channel得topic name
	PutMessage(message IMessage) error // 向channel发布一个消息
	FinishMessage(clientID uint64, messageID MessageID) error
	RequeueMessage(clientID uint64, messageID MessageID) error
	StartInFlightTimeout(message IMessage, clientID uint64, timeout time.Duration) error
}

type IConsumer interface {
	Pause()
	UnPause()
	Close() error
	Empty()
	TimeoutMessage()
}
