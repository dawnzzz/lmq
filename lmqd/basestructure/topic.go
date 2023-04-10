package basestructure

import (
	"github.com/dawnzzz/lmq/iface"
	"github.com/dawnzzz/lmq/logger"
	"github.com/dawnzzz/lmq/pkg/e"
	"sync"
	"sync/atomic"
)

type Topic struct {
	base

	name         string
	status       atomic.Uint32             // 当前运行状态：starting、running、pausing、closing、closed
	channels     map[string]iface.IChannel // 保存所有的channel字典
	channelsLock sync.RWMutex              // 控制对channel字典的互斥访问

	memoryMsgChan chan iface.IMessage // 内存chan
}

func NewTopic(name string) iface.ITopic {
	topic := &Topic{
		base: newBase(),

		name:     name,
		channels: map[string]iface.IChannel{},

		memoryMsgChan: make(chan iface.IMessage, 1024), // TODO：memory chan size 通过配置文件配置

	}
	topic.status.Store(starting)

	go topic.messagePump()

	return topic
}

// Delete 关闭并删除topic
func (topic *Topic) Delete() {
	topic.channelsLock.Lock()
	defer topic.channelsLock.Unlock()

	// 关闭自己
	topic.Close()

	// 删除所有的channel
	for _, channel := range topic.channels {
		channel.Delete()
	}
}

// GetName 获取topic的名称
func (topic *Topic) GetName() string {
	return topic.name
}

func (topic *Topic) Empty() {
	//TODO implement me
	panic("implement me")
}

// GetChannel 获取一个channel，如果没有就新建一个
func (topic *Topic) GetChannel(name string) iface.IChannel {
	// 查询channel是否已经存在
	topic.channelsLock.RLock()
	if t, exist := topic.channels[name]; exist {
		// topic已经存在，直接返回
		topic.channelsLock.RUnlock()
		return t
	}
	topic.channelsLock.RUnlock()

	// 不存在则新建一个topic
	// 换一个更细粒度的锁
	topic.channelsLock.Lock()
	defer topic.channelsLock.Unlock()
	if t, exist := topic.channels[name]; exist {
		// topic已经存在，直接返回
		return t
	}

	c := NewChannel(name)
	c.Start()
	topic.channels[name] = c

	return c
}

// GetExistingChannel 根据名字获取一个已存在的channel
func (topic *Topic) GetExistingChannel(name string) (iface.IChannel, error) {
	topic.channelsLock.RLock()
	defer topic.channelsLock.RUnlock()

	channel, exist := topic.channels[name]
	if !exist {
		return nil, e.ErrChannelNotFound
	}

	return channel, nil
}

// DeleteExistingChannel 删除一个已经存在的channel
func (topic *Topic) DeleteExistingChannel(name string) error {
	//TODO implement me
	panic("implement me")
}

func (topic *Topic) Publish(message iface.IMessage) error {
	//TODO implement me
	panic("implement me")
}

func (topic *Topic) messagePump() {
	var memoryMsgChan chan iface.IMessage
	var msg iface.IMessage

	for {
		select {
		case <-topic.pauseChan:
			continue
		case <-topic.updateChan:
			continue
		case <-topic.closingChan:
			goto Exit
		case <-topic.startChan:
		}
		break
	}

	// 改为运行中状态
	topic.status.Store(running)
	logger.Infof("topic [%] is running", topic.name)
	memoryMsgChan = topic.memoryMsgChan

	for {
		select {
		case msg = <-memoryMsgChan: // 获取msg
		case <-topic.closingChan: // 退出
			goto Exit
		case <-topic.pauseChan: // 暂停/恢复
			if topic.IsPausing() {
				memoryMsgChan = topic.memoryMsgChan
			} else {
				memoryMsgChan = nil
			}

			continue
		}

		// TODO：向所有channel发送msg
		for _, channel := range topic.channels {
			_ = channel.Publish(msg)
		}
	}

Exit:
	topic.closedChan <- struct{}{}
	topic.status.Store(Closed)
	logger.Infof("topic [%s] is exited", topic.name)
}
