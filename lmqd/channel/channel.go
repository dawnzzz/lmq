package channel

import (
	"errors"
	"github.com/dawnzzz/lmq/iface"
	"github.com/dawnzzz/lmq/pkg/e"
	"strings"
	"sync"
	"sync/atomic"
)

type Channel struct {
	sync.RWMutex

	topicName   string       // topic名称
	name        string       // channel名称
	isTemporary bool         // 标记是否是临时的topic
	isExiting   atomic.Bool  // 是否退出
	exitLock    sync.RWMutex // 发送消息与退出的互斥
	isPausing   atomic.Bool  // 是否已经暂停

	memoryMsgChan chan iface.IMessage // 内存chan

	deleteCallback func(topic iface.IChannel)
	deleter        sync.Once

	clients map[int64]Consumer

	inFlightMessages         map[iface.MessageID]iface.IMessage // 在给客户端发送过程中的message
	inFlightMessagesPriQueue *inFlightPriQueue                  // 在给客户端发送过程中的message，优先队列
	inFlightMessagesLock     sync.Mutex

	messageCount atomic.Uint64 // 消息数量
	requeueCount atomic.Uint64 // 重新入队的消息数量
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
	return channel.doPause(true)
}

func (channel *Channel) UnPause() error {
	return channel.doPause(false)
}

func (channel *Channel) doPause(pause bool) error {
	if pause {
		channel.isPausing.Store(true)
	} else {
		channel.isPausing.Store(false)
	}

	channel.RLock()
	for _, c := range channel.clients {
		if pause {
			c.Pause()
		} else {
			c.UnPause()
		}
	}
	channel.Unlock()

	return nil
}

func (channel *Channel) Empty() error {
	channel.Lock()
	defer channel.Unlock()

	channel.initPQ() // 清空优先队列
	for _, c := range channel.clients {
		c.Empty()
	}

	// 清空内存队列中的数据
	for {
		select {
		case <-channel.memoryMsgChan:
		default:
			goto finish
		}
	}

finish:
	return nil
}

func (channel *Channel) Close() error {
	return channel.exit(false)
}

func (channel *Channel) Delete() error {
	return channel.exit(true)
}

func (channel *Channel) exit(deleted bool) error {
	channel.exitLock.Lock()
	defer channel.exitLock.Unlock()

	// channel已经在退出了
	if !channel.isExiting.CompareAndSwap(false, true) {
		return e.ErrChannelIsExiting
	}

	// 关闭客户端
	channel.Lock()
	for _, c := range channel.clients {
		_ = c.Close()
	}
	channel.Unlock()

	if deleted {
		// 如果删除channel，则关闭之前先清空channel
		return channel.Empty()
	}

	return nil
}

func (channel *Channel) IsPausing() bool {
	return channel.isPausing.Load()
}

func (channel *Channel) IsExiting() bool {
	return channel.isExiting.Load()
}

func (channel *Channel) GetName() string {
	return channel.name
}

// PutMessage 投递一个消息
func (channel *Channel) PutMessage(message iface.IMessage) error {
	channel.exitLock.RLock()
	defer channel.exitLock.RUnlock()

	// 检查是否退出
	if channel.isExiting.Load() {
		return e.ErrChannelIsExiting
	}

	// 消息发送到管道中
	err := channel.put(message)
	if err != nil {
		return err
	}

	// 没有错误
	channel.messageCount.Add(1)
	return nil
}

func (channel *Channel) put(message iface.IMessage) error {
	select {
	case channel.memoryMsgChan <- message:
	default:
		// TODO:超出的消息先暂时丢弃
		return errors.New("message is discarded")
	}

	return nil
}

// FinishMessage 结束消息的投递
func (channel *Channel) FinishMessage(clientID int64, messageID iface.MessageID) error {
	// 将消息从inflight字典中删除
	message, err := channel.popInFlightMessage(clientID, messageID)
	if err != nil {
		return err
	}

	// 将消息从inflight优先队列中删除
	channel.removeFromInFlightPriQueue(message)

	return nil
}

// RequeueMessage 将message重新入队发送
func (channel *Channel) RequeueMessage(clientID int64, messageID iface.MessageID) error {
	// 首先从in-flight中移除
	message, err := channel.popInFlightMessage(clientID, messageID)
	if err != nil {
		return err
	}

	channel.removeFromInFlightPriQueue(message)
	channel.requeueCount.Add(1)

	// 重新送入队列
	channel.exitLock.RLock()
	if channel.isExiting.Load() {
		channel.exitLock.RUnlock()
		return e.ErrChannelIsExiting
	}

	err = channel.put(message)
	channel.exitLock.RUnlock()
	return err
}

// popInFlightMessage 将一个消息从in flight字典中取出
func (channel *Channel) popInFlightMessage(clientID int64, messageID iface.MessageID) (iface.IMessage, error) {
	channel.inFlightMessagesLock.Lock()
	msg, ok := channel.inFlightMessages[messageID]
	if !ok {
		channel.inFlightMessagesLock.Unlock()
		return nil, e.ErrMessageIDIsNotInFlight
	}

	if msg.GetClientID() != clientID {
		return nil, e.ErrClientNotOwnTheMessage
	}

	delete(channel.inFlightMessages, messageID)
	channel.inFlightMessagesLock.Unlock()

	return msg, nil
}

func (channel *Channel) removeFromInFlightPriQueue(message iface.IMessage) {
	channel.inFlightMessagesLock.Lock()
	if message.GetIndex() == -1 {
		// 这个消息已经移除了
		channel.inFlightMessagesLock.Unlock()
		return
	}

	channel.inFlightMessagesPriQueue.Remove(message.GetIndex())
	channel.inFlightMessagesLock.Unlock()
}
