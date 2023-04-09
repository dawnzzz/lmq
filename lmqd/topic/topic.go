package topic

import (
	"github.com/dawnzzz/lmq/iface"
	"github.com/dawnzzz/lmq/logger"
	"sync"
	"sync/atomic"
)

const (
	starting = uint32(iota)
	running
	pausing
	closing
	closed
)

type Topic struct {
	name         string
	status       atomic.Uint32             // 当前运行状态：starting、running、pausing、closing、closed
	channels     map[string]iface.IChannel // 保存所有的channel字典
	channelsLock sync.RWMutex              // 控制对channel字典的互斥访问

	memoryMsgChan chan iface.IMessage // 内存chan

	startChan   chan struct{}
	updateChan  chan struct{}
	pauseChan   chan struct{}
	closingChan chan struct{}
	closedChan  chan struct{}
}

func NewTopic(name string) iface.ITopic {
	topic := &Topic{
		name:     name,
		channels: map[string]iface.IChannel{},

		memoryMsgChan: make(chan iface.IMessage, 1024), // TODO：memory chan size 通过配置文件配置

		startChan:   make(chan struct{}),
		updateChan:  make(chan struct{}),
		pauseChan:   make(chan struct{}),
		closingChan: make(chan struct{}),
		closedChan:  make(chan struct{}, 1),
	}

	go topic.messagePump()

	return topic
}

// Start 启动topic
func (topic *Topic) Start() {
	select {
	case topic.startChan <- struct{}{}:
	default:
	}
}

// Pause 暂停topic
func (topic *Topic) Pause() {
	if topic.status.CompareAndSwap(running, pausing) {
		return
	}

	select {
	case topic.pauseChan <- struct{}{}:
		topic.status.CompareAndSwap(running, pausing)
	}
}

// UnPause 恢复topic
func (topic *Topic) UnPause() {
	if topic.status.CompareAndSwap(pausing, running) {
		return
	}

	select {
	case topic.pauseChan <- struct{}{}:
		topic.status.CompareAndSwap(pausing, running)
	}
}

// Close 关闭topic
func (topic *Topic) Close() {
	// 检查是否处于关闭状态
	if topic.status.Load() == closing {
		return
	}

	select {
	case topic.closingChan <- struct{}{}:
		topic.status.Store(closing)
	default:
	}
}

// Delete 关闭并删除topic
func (topic *Topic) Delete() {
	//TODO implement me
	panic("implement me")
}

// GetName 获取topic的名称
func (topic *Topic) GetName() string {
	return topic.name
}

// IsPausing 返回是否处于暂停状态
func (topic *Topic) IsPausing() bool {
	return topic.status.Load() == pausing
}

// GetChannel 根据名称获取channel
func (topic *Topic) GetChannel(name string) (iface.IChannel, bool) {
	topic.channelsLock.RLock()
	defer topic.channelsLock.RUnlock()

	channel, exist := topic.channels[name]

	return channel, exist
}

// CloseChannel 关闭channel
func (topic *Topic) CloseChannel(channel iface.IChannel) {
	//TODO implement me
	panic("implement me")
}

// DeleteChannel 删除channel
func (topic *Topic) DeleteChannel(channel iface.IChannel) {
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
	logger.Infof("topic [%s] is exited", topic.name)
}
