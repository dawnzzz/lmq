package basestructure

import (
	"github.com/dawnzzz/lmq/iface"
)

type Channel struct {
	base

	name string // channel名称

	memoryMsgChan chan iface.IMessage // 内存chan
}

func NewChannel(name string) iface.IChannel {
	channel := &Channel{
		base:          base{},
		name:          name,
		memoryMsgChan: make(chan iface.IMessage, 1024), // TODO：配置文件可配置
	}
	channel.status.Store(starting)

	return channel
}

func (channel *Channel) Empty() {
	//TODO implement me
	panic("implement me")
}

func (channel *Channel) Delete() {
	// 关闭自己
	channel.Close()
}

func (channel *Channel) GetName() string {
	return channel.name
}

func (channel *Channel) Publish(message iface.IMessage) error {
	//TODO implement me
	panic("implement me")
}
