package channel

import (
	"github.com/dawnzzz/lmq/iface"
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

	deleteCallback func(topic iface.ITopic)
	deleter        sync.Once

	clients map[int64]Consumer

	inFlightMessages         map[iface.MessageID]iface.IMessage
	inFlightMessagesPriQueue inFlightPriQueue
	inFlightMessagesLock     sync.Mutex
}

func (channel *Channel) Start() {
	//TODO implement me
	panic("implement me")
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

func (channel *Channel) PutMessage(message iface.IMessage) error {
	//TODO implement me
	panic("implement me")
}

func (channel *Channel) FinishMessage(clientID int64, messageID iface.MessageID) error {
	//TODO implement me
	panic("implement me")
}

func (channel *Channel) RequeueMessage(clientID int64, messageID iface.MessageID) error {
	//TODO implement me
	panic("implement me")
}

func NewChannel(name string) iface.IChannel {
	channel := &Channel{
		name:          name,
		memoryMsgChan: make(chan iface.IMessage, 1024), // TODO：配置文件可配置
	}

	return channel
}
