package channel

import (
	"errors"
	"github.com/dawnzzz/lmq/config"
	"github.com/dawnzzz/lmq/iface"
	"github.com/dawnzzz/lmq/logger"
	"github.com/dawnzzz/lmq/pkg/e"
	"strings"
	"sync"
	"sync/atomic"
	"time"
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

	clients map[uint64]iface.IConsumer

	inFlightMessages         map[iface.MessageID]iface.IMessage // 在给客户端发送过程中的message
	inFlightMessagesPriQueue *inFlightPriQueue                  // 在给客户端发送过程中的message，优先队列
	inFlightMessagesLock     sync.Mutex

	messageCount atomic.Uint64 // 消息数量
	requeueCount atomic.Uint64 // 重新入队的消息数量
	timeoutCount atomic.Uint64 // 超时消息的数量
}

func NewChannel(topicName, name string, deleteCallback func(topic iface.IChannel)) iface.IChannel {
	channel := &Channel{
		topicName: topicName,
		name:      name,

		memoryMsgChan: make(chan iface.IMessage, config.GlobalLmqdConfig.MemQueueSize),

		clients: map[uint64]iface.IConsumer{},

		deleteCallback: deleteCallback,
	}
	channel.isTemporary = strings.HasSuffix(channel.name, "#temp")

	// 初始化优先队列
	channel.initPQ()

	go channel.queueScanWorker()

	return channel
}

func (channel *Channel) initPQ() {
	priQueueSize := config.GlobalLmqdConfig.MemQueueSize / 10

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

// GetMemoryMsgChan 获取memoryMsgChan
func (channel *Channel) GetMemoryMsgChan() chan iface.IMessage {
	return channel.memoryMsgChan
}

// AddClient 为通道添加一个订阅的用户
func (channel *Channel) AddClient(clientID uint64, client iface.IConsumer) error {
	channel.exitLock.RLock()
	defer channel.exitLock.RUnlock()

	if channel.isExiting.Load() {
		return e.ErrChannelIsExiting
	}

	channel.RLock()
	_, ok := channel.clients[clientID]
	channel.RUnlock()
	if ok {
		return nil
	}

	channel.Lock()
	channel.clients[clientID] = client
	channel.Unlock()

	return nil
}

// RemoveClient 为channel移除一个用户
func (channel *Channel) RemoveClient(clientID uint64) {
	channel.exitLock.RLock()
	defer channel.exitLock.RUnlock()

	if channel.isExiting.Load() {
		return
	}

	channel.RLock()
	_, ok := channel.clients[clientID]
	channel.RUnlock()
	if !ok {
		return
	}

	channel.Lock()
	delete(channel.clients, clientID)
	channel.Unlock()

	if len(channel.clients) == 0 && channel.isTemporary {
		go channel.deleter.Do(func() { channel.deleteCallback(channel) })
	}
}

// FinishMessage 结束消息的投递
func (channel *Channel) FinishMessage(clientID uint64, messageID iface.MessageID) error {
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
func (channel *Channel) RequeueMessage(clientID uint64, messageID iface.MessageID) error {
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

func (channel *Channel) StartInFlightTimeout(message iface.IMessage, clientID uint64, timeout time.Duration) error {
	now := time.Now()
	message.SetClientID(clientID)
	message.SetPriority(now.Add(timeout).UnixNano())
	err := channel.pushInFlightMessage(message)
	if err != nil {
		return err
	}
	channel.addToInFlightPQ(message)
	return nil
}

func (channel *Channel) pushInFlightMessage(message iface.IMessage) error {
	channel.inFlightMessagesLock.Lock()
	_, ok := channel.inFlightMessages[message.GetID()]
	if ok {
		channel.inFlightMessagesLock.Unlock()
		return errors.New("ID already in flight")
	}
	channel.inFlightMessages[message.GetID()] = message
	channel.inFlightMessagesLock.Unlock()
	return nil
}

func (channel *Channel) addToInFlightPQ(message iface.IMessage) {
	channel.inFlightMessagesLock.Lock()
	channel.inFlightMessagesPriQueue.Push(message)
	channel.inFlightMessagesLock.Unlock()
}

// popInFlightMessage 将一个消息从in flight字典中取出
func (channel *Channel) popInFlightMessage(clientID uint64, messageID iface.MessageID) (iface.IMessage, error) {
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

// 扫描队列，处理超时消息
func (channel *Channel) queueScanWorker() {
	ticker := time.NewTicker(config.GlobalLmqdConfig.ScanQueueInterval)
	for {
		select {
		case <-ticker.C:
			// 处理 in-flight 的超时消息
			go channel.processInFlightQueue()
		}

		if channel.isExiting.Load() {
			ticker.Stop()
			return
		}
	}
}

func (channel *Channel) processInFlightQueue() {
	channel.exitLock.RLock()
	channel.exitLock.RUnlock()

	if channel.isExiting.Load() {
		return
	}

	now := time.Now().UnixNano()
	for {
		channel.inFlightMessagesLock.Lock()
		msg := channel.inFlightMessagesPriQueue.PeekAndShift(now)
		channel.inFlightMessagesLock.Unlock()

		if msg == nil {
			return
		}

		_, err := channel.popInFlightMessage(msg.GetClientID(), msg.GetID())
		if err != nil {
			return
		}

		channel.timeoutCount.Add(1)

		channel.RLock()
		client, ok := channel.clients[msg.GetClientID()]
		if ok {
			client.TimeoutMessage()
		}
		channel.RUnlock()

		logger.Infof("message id = %v timeout, now requeue", msg.GetID())
		_ = channel.put(msg)
	}
}
