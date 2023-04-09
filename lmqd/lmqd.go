package lmqd

import (
	"github.com/dawnzzz/lmq/iface"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
)

const (
	starting = int32(iota)
	running
	closing
	closed
)

// LmqDaemon lmqd
type LmqDaemon struct {
	status atomic.Int32            // 当前运行状态：starting、running、closing
	topics map[string]iface.ITopic // 保存所有的topic字典

	startChan   chan struct{}
	closingChan chan struct{}
	closedChan  chan struct{}
}

func NewLmqDaemon() iface.ILmqDaemon {
	lmqd := &LmqDaemon{
		startChan:   make(chan struct{}),
		closingChan: make(chan struct{}),
		closedChan:  make(chan struct{}, 1),
	}

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
		lmqd.status.Store(running)
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

// AddTopic 添加topic
func (lmqd *LmqDaemon) AddTopic(name string) error {
	//TODO implement me
	panic("implement me")
}

// GetTopic 获取topic
func (lmqd *LmqDaemon) GetTopic(name string) (iface.ITopic, bool) {
	//TODO implement me
	panic("implement me")
}

// Publish 发布消息
func (lmqd *LmqDaemon) Publish(topic iface.ITopic, message iface.IMessage) {
	//TODO implement me
	panic("implement me")
}

// CloseTopic 关闭一个topic
func (lmqd *LmqDaemon) CloseTopic(topic iface.ITopic) {
	//TODO implement me
	panic("implement me")
}

// DeleteTopic 删除一个topic
func (lmqd *LmqDaemon) DeleteTopic(topic iface.ITopic) {
	//TODO implement me
	panic("implement me")
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

	for {
		select {
		case <-lmqd.closingChan:
			goto Exit
		}
	}

Exit:
	// 已经完全退出
	lmqd.status.Store(closed)
	lmqd.closedChan <- struct{}{}
}
