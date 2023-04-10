package lmqd

import (
	"github.com/dawnzzz/lmq/iface"
	"github.com/dawnzzz/lmq/lmqd/basestructure"
	"github.com/dawnzzz/lmq/logger"
	"github.com/dawnzzz/lmq/pkg/e"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
)

const (
	starting = uint32(iota)
	running
	closing
	closed
)

// LmqDaemon lmqd
type LmqDaemon struct {
	status     atomic.Uint32           // 当前运行状态：starting、running、closing
	topics     map[string]iface.ITopic // 保存所有的topic字典
	topicsLock sync.RWMutex            // 控制对topic字典的互斥访问

	startChan   chan struct{}
	closingChan chan struct{}
	closedChan  chan struct{}
}

func NewLmqDaemon() iface.ILmqDaemon {
	lmqd := &LmqDaemon{
		topics: map[string]iface.ITopic{},

		startChan:   make(chan struct{}),
		closingChan: make(chan struct{}),
		closedChan:  make(chan struct{}, 1),
	}
	lmqd.status.Store(starting)

	go lmqd.messagePump()

	return lmqd
}

func (lmqd *LmqDaemon) Main() {
	lmqd.Start()

	signalChan := make(chan os.Signal)
	signal.Notify(signalChan)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	// 开启一个协程监听退出信号
	go func() {
		select {
		case <-signalChan:
			// 收到退出信号，关闭lmqd
			lmqd.Close()
			return
		}
	}()

	// 阻塞，直到lmq完全退出
	<-lmqd.closedChan
}

// Start 开启lmqd
func (lmqd *LmqDaemon) Start() {
	select {
	case lmqd.startChan <- struct{}{}:
	default:
	}
}

// Close 关闭lmqd
func (lmqd *LmqDaemon) Close() {
	// 检查是否处于正在关闭的状态，如果是则直接返回
	if lmqd.status.Load() == closing {
		return
	}

	select {
	case lmqd.closingChan <- struct{}{}:
		lmqd.status.Store(closing) // 转为关闭状态
	default:
	}
}

// GetTopic 根据名字获取一个topic，如果不存在则新建一个topic
func (lmqd *LmqDaemon) GetTopic(name string) iface.ITopic {
	// 查询topic是否已经存在
	lmqd.topicsLock.RLock()
	if t, exist := lmqd.topics[name]; exist {
		// topic已经存在，直接返回
		lmqd.topicsLock.RUnlock()
		return t
	}
	lmqd.topicsLock.RUnlock()

	// 不存在则新建一个topic
	// 换一个更细粒度的锁
	lmqd.topicsLock.Lock()
	defer lmqd.topicsLock.Unlock()
	if t, exist := lmqd.topics[name]; exist {
		// topic已经存在，直接返回
		return t
	}

	t := basestructure.NewTopic(name)
	t.Start()
	lmqd.topics[name] = t

	return t
}

// GetExistingTopic 获取一个已经存在的topic
func (lmqd *LmqDaemon) GetExistingTopic(topicName string) (iface.ITopic, error) {
	lmqd.topicsLock.RLock()
	defer lmqd.topicsLock.RUnlock()

	t, exist := lmqd.topics[topicName]
	if !exist {
		return nil, e.ErrTopicNotFound
	}

	return t, nil
}

// DeleteExistingTopic 删除一个已经存在的topic
func (lmqd *LmqDaemon) DeleteExistingTopic(topicName string) error {
	//TODO implement me
	panic("implement me")
}

// Publish 发布消息
func (lmqd *LmqDaemon) Publish(topic iface.ITopic, message iface.IMessage) error {
	if lmqd.status.Load() != running {
		return e.ErrLMQDIsNotRunning
	}

	err := topic.Publish(message)
	return err
}

func (lmqd *LmqDaemon) messagePump() {
	// 开启循环，知道收到start信号或者close信号
	for {
		select {
		case <-lmqd.closingChan: // 收到close信号，直接退出
			goto Exit
		case <-lmqd.startChan: // 收到start信号，退出循环
		}
		break
	}

	// 改为运行中状态
	lmqd.status.Store(running)
	logger.Info("lmqd is running")

	for {
		select {
		case <-lmqd.closingChan:
			goto Exit
		}
	}

Exit:
	for _, t := range lmqd.topics {
		t.Close()
	}

	// 已经完全退出
	lmqd.status.Store(closed)
	lmqd.closedChan <- struct{}{}
	logger.Info("lmqd is exited")
}
