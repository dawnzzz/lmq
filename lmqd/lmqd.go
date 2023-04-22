package lmqd

import (
	serveriface "github.com/dawnzzz/hamble-tcp-server/iface"
	"github.com/dawnzzz/lmq/config"
	"github.com/dawnzzz/lmq/iface"
	"github.com/dawnzzz/lmq/internel/utils"
	"github.com/dawnzzz/lmq/lmqd/lookup"
	"github.com/dawnzzz/lmq/lmqd/tcp"
	"github.com/dawnzzz/lmq/lmqd/topic"
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
	clientIDMap      map[serveriface.IConnection]uint64 // 记录连接与client的映射关系
	clientIDSequence uint64                             // 客户端ID
	clientIDLock     sync.RWMutex
	isLoading        atomic.Bool

	lookupManager iface.ILookupManager

	status     atomic.Uint32           // 当前运行状态：starting、running、closing
	topics     map[string]iface.ITopic // 保存所有的topic字典
	topicsLock sync.RWMutex            // 控制对topic字典的互斥访问

	tcpServer *tcp.TcpServer

	waitGroup utils.WaitGroupWrapper

	exitChan chan struct{}
}

func NewLmqDaemon() iface.ILmqDaemon {
	lmqd := &LmqDaemon{
		clientIDMap: make(map[serveriface.IConnection]uint64),

		topics: map[string]iface.ITopic{},

		exitChan: make(chan struct{}, 1),
	}
	lmqd.tcpServer = tcp.NewTcpServer(lmqd)
	lmqd.lookupManager = lookup.NewManager(config.GlobalLmqdConfig.LookupAddresses)
	lmqd.status.Store(starting)

	return lmqd
}

func (lmqd *LmqDaemon) Main() {
	go lmqd.tcpServer.Start()  // 开启TCP服务器
	lmqd.lookupManager.Start() // 开启lookup manager
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

	// 关闭tcp服务器
	lmqd.tcpServer.Stop()

	// 关闭lookup manager
	lmqd.lookupManager.Close()

	// 关闭所有得topic
	for _, t := range lmqd.topics {
		_ = t.Close()
	}

	// 持久化元数据信息
	_ = lmqd.PersistMetaData()

	lmqd.waitGroup.Wait()

	select {
	case lmqd.exitChan <- struct{}{}:
		lmqd.status.Store(exited) // 转为关闭状态
	default:
	}

	close(lmqd.exitChan)
}

// GetTopic 根据名字获取一个topic，如果不存在则新建一个topic
func (lmqd *LmqDaemon) GetTopic(name string) (iface.ITopic, error) {
	// 检查名字是否合法
	if !utils.TopicOrChannelNameIsValid(name) {
		return nil, e.ErrTopicNameInValid
	}

	// 查询topic是否已经存在
	lmqd.topicsLock.RLock()
	if t, exist := lmqd.topics[name]; exist {
		// topic已经存在，直接返回
		lmqd.topicsLock.RUnlock()
		return t, nil
	}
	lmqd.topicsLock.RUnlock()

	// 不存在则新建一个topic
	// 换一个更细粒度的锁
	lmqd.topicsLock.Lock()
	defer lmqd.topicsLock.Unlock()
	if t, exist := lmqd.topics[name]; exist {
		lmqd.topicsLock.Unlock()
		// topic已经存在，直接返回
		return t, nil
	}

	deleteCallback := func(t iface.ITopic) {
		_ = lmqd.DeleteExistingTopic(t.GetName())
	}
	t := topic.NewTopic(lmqd, name, deleteCallback)
	lmqd.topics[name] = t

	if lmqd.isLoading.Load() {
		return t, nil
	}

	t.Start()

	return t, nil
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
	_ = t.Delete()
	delete(lmqd.topics, t.GetName())

	return nil
}

func (lmqd *LmqDaemon) GenerateClientID(conn serveriface.IConnection) uint64 {
	lmqd.clientIDLock.RLock()
	if clientID, ok := lmqd.clientIDMap[conn]; ok {
		lmqd.clientIDLock.RUnlock()
		return clientID
	}
	lmqd.clientIDLock.RUnlock()

	lmqd.clientIDLock.Lock()
	defer lmqd.clientIDLock.Unlock()

	if clientID, ok := lmqd.clientIDMap[conn]; ok {
		return clientID
	}

	lmqd.clientIDSequence++
	lmqd.clientIDMap[conn] = lmqd.clientIDSequence

	return lmqd.clientIDSequence
}

// Notify 通知lmqd进行持久化，通知lookup
func (lmqd *LmqDaemon) Notify(v interface{}, persist bool) {
	isLoading := lmqd.isLoading.Load()

	lmqd.waitGroup.Wrap(func() {
		select {
		case <-lmqd.exitChan:
		case lmqd.lookupManager.GetNotifyChan() <- v:
			if !persist && isLoading {
				return
			}

			err := lmqd.PersistMetaData()
			if err != nil {
				logger.Errorf("lmqd PersistMetaData failed in Notify, err: %s", err.Error())
			}
		}

	})
}
