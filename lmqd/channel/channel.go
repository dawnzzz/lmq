package channel

import (
	"github.com/dawnzzz/lmq/iface"
	"strings"
	"sync"
	"sync/atomic"
)

type Channel struct {
	topicName   string      // topic名称
	name        string      // channel名称
	isTemporary bool        // 标记是否是临时的topic
	isExiting   atomic.Bool // 是否退出
	isPausing   atomic.Bool // 是否已经暂停

	memoryMsgChan chan iface.IMessage // 内存chan

	deleteCallback func(topic iface.IChannel)
	deleter        sync.Once

	clients map[int64]Consumer

	inFlightMessages         map[iface.MessageID]iface.IMessage // 在给客户端发送过程中的message
	inFlightMessagesPriQueue inFlightPriQueue                   // 在给客户端发送过程中的message，优先队列
	inFlightMessagesLock     sync.Mutex
}

func NewChannel(topicName, name string, deleteCallback func(topic iface.IChannel)) iface.IChannel {
	channel := &Channel{
		topicName: topicName,
		name:      name,

		memoryMsgChan: make(chan iface.IMessage, 1024), // TODO：配置文件可定义

		deleteCallback: deleteCallback,
	}
	channel.isTemporary = strings.HasSuffix(channel.name, "#temp")

	// 初始化优先队列
	channel.initPQ()

	return channel
}

func (channel *Channel) initPQ() {
	priQueueSize := 1024 / 10 // TODO：配置文件可定义

	channel.inFlightMessagesLock.Lock()
	channel.inFlightMessages = map[iface.MessageID]iface.IMessage{}
	channel.inFlightMessagesPriQueue = newInFlightPriQueue(priQueueSize)
	channel.inFlightMessagesLock.Unlock()
}

func (channel *Channel) Pause() error {
	//TODO implement me
	panic("implement me")
}

func (channel *Channel) UnPause() error {
	//TODO implement me
	panic("implement me")
}

func (channel *Channel) Empty() error {
	//TODO implement me
	panic("implement me")
}

func (channel *Channel) Close() error {
	//TODO implement me
	panic("implement me")
}

func (channel *Channel) Delete() error {
	//TODO implement me
	panic("implement me")
}

func (channel *Channel) IsPausing() bool {
	//TODO implement me
	panic("implement me")
}

func (channel *Channel) IsExiting() bool {
	//TODO implement me
	panic("implement me")
}

func (channel *Channel) GetName() string {
	return channel.name
}

// PutMessage 投递一个消息
func (channel *Channel) PutMessage(message iface.IMessage) error {
	//TODO implement me
	panic("implement me")
}

// FinishMessage 结束消息的投递
func (channel *Channel) FinishMessage(clientID int64, messageID iface.MessageID) error {
	//TODO implement me
	panic("implement me")
}

// RequeueMessage 将message重新入队发送
func (channel *Channel) RequeueMessage(clientID int64, messageID iface.MessageID) error {
	//TODO implement me
	panic("implement me")
}
