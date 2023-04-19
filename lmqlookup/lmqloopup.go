package lmqlookup

import (
	"github.com/dawnzzz/lmq/iface"
	"github.com/dawnzzz/lmq/lmqlookup/tcp"
	"github.com/dawnzzz/lmq/lmqlookup/topology"
	"github.com/dawnzzz/lmq/logger"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
)

type LmqLookup struct {
	sync.RWMutex

	tcpServer *tcp.TcpServer

	registrationDB iface.IRegistrationDB // 用于存储拓扑结构

	isClosing atomic.Bool
	exitChan  chan struct{}
}

func NewLmqLookUp() iface.ILmqLookup {
	lmqLookup := &LmqLookup{
		registrationDB: topology.NewRegistrationDB(),

		exitChan: make(chan struct{}),
	}

	lmqLookup.tcpServer = tcp.NewTcpServer(lmqLookup.registrationDB)

	return lmqLookup
}

func (lmqLookup *LmqLookup) Stop() {
	if !lmqLookup.isClosing.CompareAndSwap(false, true) {
		// 已经关闭，直接退出
		return
	}

	close(lmqLookup.exitChan)
}

func (lmqLookup *LmqLookup) Main() {
	go lmqLookup.tcpServer.Start()
	logger.Info("lmq lookup is running")

	signalChan := make(chan os.Signal)
	signal.Notify(signalChan)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	// 开启一个协程监听退出信号
	go func() {
		select {
		case <-signalChan:
			// 收到退出信号，关闭lmqd
			lmqLookup.Stop()
			return
		}
	}()

	// 阻塞，直到lmq完全退出
	<-lmqLookup.exitChan
	logger.Info("lmq lookup is exited")
}
