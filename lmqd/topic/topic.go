package topic

import (
	"encoding/binary"
	"errors"
	"github.com/dawnzzz/lmq/config"
	"github.com/dawnzzz/lmq/iface"
	"github.com/dawnzzz/lmq/lmqd/channel"
	"github.com/dawnzzz/lmq/lmqd/message"
	"github.com/dawnzzz/lmq/logger"
	"github.com/dawnzzz/lmq/pkg/e"
	"strings"
	"sync"
	"sync/atomic"
)

type Topic struct {
	name         string
	isTemporary  bool                      // 标记是否是临时的topic
	isPausing    atomic.Bool               // 标记是否已经暂停
	isExiting    atomic.Bool               // 标记是否已经退出
	channels     map[string]iface.IChannel // 保存所有的channel字典
	channelsLock sync.RWMutex              // 控制对channel字典的互斥访问

	guidFactory *GUIDFactory // message id 生成器

	memoryMsgChan chan iface.IMessage // 内存chan

	deleteCallback func(topic iface.ITopic)
	deleter        sync.Once

	startChan   chan struct{}
	updateChan  chan struct{}
	pauseChan   chan struct{}
	closingChan chan struct{}
	closedChan  chan struct{}

	messageCount atomic.Uint64
	messageBytes atomic.Uint64
}

func NewTopic(name string, deleteCallback func(topic iface.ITopic)) iface.ITopic {
	topic := &Topic{
		name:     name,
		channels: map[string]iface.IChannel{},

		memoryMsgChan: make(chan iface.IMessage, config.GlobalLmqdConfig.MemQueueSize),

		deleteCallback: deleteCallback,

		startChan:   make(chan struct{}, 1),
		updateChan:  make(chan struct{}, 1),
		pauseChan:   make(chan struct{}, 1),
		closingChan: make(chan struct{}, 1),
		closedChan:  make(chan struct{}, 1),
	}

	if strings.HasSuffix(name, "#temp") {
		topic.isTemporary = true
	}

	nodeID, _ := binary.Varint([]byte(topic.name))
	topic.guidFactory = NewGUIDFactory(nodeID)

	go topic.messagePump()

	return topic
}

// Start 启动
func (topic *Topic) Start() {
	select {
	case topic.startChan <- struct{}{}:
	default:
	}
}

// Pause 暂停
func (topic *Topic) Pause() error {
	return topic.doPause(true)
}

// UnPause 恢复
func (topic *Topic) UnPause() error {
	return topic.doPause(false)
}

// 执行暂停/恢复
func (topic *Topic) doPause(pause bool) error {
	if pause {
		topic.isPausing.Store(true)
	} else {
		topic.isPausing.Store(false)
	}

	select {
	case topic.pauseChan <- struct{}{}:
	case <-topic.closingChan:
		// 正在退出
	}

	return nil
}

// Close 关闭
func (topic *Topic) Close() error {
	return topic.exit(false)
}

// Delete 关闭并删除topic
func (topic *Topic) Delete() error {
	return topic.exit(true)
}

func (topic *Topic) exit(deleted bool) error {
	if !topic.isExiting.CompareAndSwap(false, true) {
		// 已经关闭
		return e.ErrTopicIsExiting
	}

	close(topic.closingChan)

	if deleted {
		// 如果删除这个topic
		topic.channelsLock.Lock()
		for _, channel := range topic.channels {
			delete(topic.channels, channel.GetName())
			channel.Delete()
		}
		topic.channelsLock.Unlock()

		// 清空队列
		return topic.Empty()
	}

	// 如果只是关闭这个topic
	topic.channelsLock.RLock()
	for _, channel := range topic.channels {
		channel.Close()
	}
	topic.channelsLock.RUnlock()

	// TODO: 在内存队列中的数据要进行持久化操作，否则会丢失
	return nil
}

// Empty 清空所有数据
func (topic *Topic) Empty() error {
	for {
		select {
		case <-topic.memoryMsgChan:
		default:
			goto finish
		}
	}

finish:
	return nil
}

// IsPausing 返回是否处于暂停状态
func (topic *Topic) IsPausing() bool {
	return topic.isPausing.Load()
}

// GenerateGUID 生成一个message ID
func (topic *Topic) GenerateGUID() iface.MessageID {
	return topic.guidFactory.NewMessageID()
}

// GetName 获取topic的名称
func (topic *Topic) GetName() string {
	return topic.name
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

	deleteCallback := func(channel iface.IChannel) {
		_ = topic.DeleteExistingChannel(channel.GetName())
	}
	c := channel.NewChannel(topic.name, name, deleteCallback)
	topic.channels[name] = c
	topic.updateChan <- struct{}{}

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
	// 检查channel是否存在
	topic.channelsLock.RLock()
	channel, exist := topic.channels[name]
	if !exist {
		return e.ErrChannelNotFound
	}
	topic.channelsLock.RUnlock()

	// 存在就删除这个channel
	channel.Delete()

	topic.channelsLock.Lock()
	delete(topic.channels, name)
	numChannels := len(topic.channels)
	topic.channelsLock.Unlock()

	// 更新messagePump状态
	select {
	case topic.updateChan <- struct{}{}:
	case <-topic.closingChan:
	}

	// 临时topic若channel为空，则立即删除这个topic
	if numChannels == 0 && topic.isTemporary {
		go topic.deleter.Do(func() {
			topic.deleteCallback(topic)
		})
	}

	return nil
}

func (topic *Topic) PutMessage(message iface.IMessage) error {
	topic.channelsLock.RLock()
	defer topic.channelsLock.RUnlock()
	if topic.isExiting.Load() {
		return e.ErrTopicIsExiting
	}

	select {
	case topic.memoryMsgChan <- message:
	default:
		// TODO:超出的消息先暂时丢弃
		return errors.New("message is discarded")
	}

	topic.messageCount.Add(1)
	topic.messageBytes.Add(uint64(len(message.GetData())))

	return nil
}

func (topic *Topic) messagePump() {
	var memoryMsgChan chan iface.IMessage
	var msg iface.IMessage
	var channels []iface.IChannel

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
	logger.Infof("topic [%] is running", topic.name)

	topic.channelsLock.RLock()
	for _, channel := range topic.channels {
		channels = append(channels, channel)
	}
	topic.channelsLock.RUnlock()
	if len(channels) > 0 && !topic.isPausing.Load() {
		memoryMsgChan = topic.memoryMsgChan
	}

	for {
		select {
		case msg = <-memoryMsgChan: // 获取msg
		case <-topic.updateChan: // 更新channels
			channels = channels[:0]
			topic.channelsLock.RLock()
			for _, channel := range topic.channels {
				channels = append(channels, channel)
			}
			topic.channelsLock.RUnlock()
			if len(channels) == 0 || topic.isPausing.Load() {
				memoryMsgChan = nil
			} else {
				memoryMsgChan = topic.memoryMsgChan
			}

			continue
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
		if topic.isPausing.Load() {
			continue
		}
		for i, channel := range channels {
			var chanMsg iface.IMessage

			if i > 0 {
				chanMsg = message.NewMessage(msg.GetID(), msg.GetData())
			} else {
				chanMsg = msg
			}
			_ = channel.PutMessage(chanMsg)
		}
	}

Exit:
	topic.closedChan <- struct{}{}
	logger.Infof("topic [%s] is exited", topic.name)
}
