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
	exited
)

// LmqDaemon lmqd
type LmqDaemon struct {
	status     atomic.Uint32           // 当前运行状态：starting、running、closing
	topics     map[string]iface.ITopic // 保存所有的topic字典
	topicsLock sync.RWMutex            // 控制对topic字典的互斥访问

	exitChan chan struct{}
}

func NewLmqDaemon() iface.ILmqDaemon {
	lmqd := &LmqDaemon{
		topics: map[string]iface.ITopic{},

		exitChan: make(chan struct{}, 1),
	}
	lmqd.status.Store(starting)

	return lmqd
}

func (lmqd *LmqDaemon) Main() {
	lmqd.status.Store(running)
	logger.Info("lmqd is running")

	signalChan := make(chan os.Signal)
	signal.Notify(signalChan)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	// 开启一个协程监听退出信号
	go func() {
		select {
		case <-signalChan:
			// 收到退出信号，关闭lmqd
			lmqd.Exit()
			return
		}
	}()

	// 阻塞，直到lmq完全退出
	<-lmqd.exitChan
	logger.Info("lmqd is exited")
}

// Exit 退出lmqd
func (lmqd *LmqDaemon) Exit() {
	// 检查是否处于正在关闭的状态，如果是则直接返回
	if lmqd.status.Load() == exited {
		return
	}

	select {
	case lmqd.exitChan <- struct{}{}:
		lmqd.status.Store(exited) // 转为关闭状态
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
	lmqd.topicsLock.Lock()
	defer lmqd.topicsLock.Unlock()

	t, exist := lmqd.topics[topicName]
	if !exist {
		// 不存在，返回错误
		return e.ErrTopicNotFound
	}

	// 删除topic
	t.Delete()
	delete(lmqd.topics, t.GetName())

	return nil
}

// Publish 发布消息
func (lmqd *LmqDaemon) Publish(topic iface.ITopic, message iface.IMessage) error {
	if lmqd.status.Load() != running {
		return e.ErrLMQDIsNotRunning
	}

	err := topic.Publish(message)
	return err
}
