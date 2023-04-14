package tcp

import (
	"encoding/json"
	serveriface "github.com/dawnzzz/hamble-tcp-server/iface"
	"github.com/dawnzzz/lmq/iface"
	"sync"
	"sync/atomic"
	"time"
)

const (
	statusInit = uint32(iota)
	statusSubscribed
	statusPublishing
	statusClosing
)

type TcpClient struct {
	ID         uint64 // Client对象的唯一标识
	connection serveriface.IConnection

	Status    atomic.Uint32 // 客户端当前状态
	IsPausing atomic.Bool   // 标记是否暂停

	channel iface.IChannel // 订阅的通道

	ReadyCount    atomic.Int64 // 准备好接收的message数量
	InFlightCount atomic.Int64 // in-flight消息数量
	RequeueCount  atomic.Int64 // requeue消息数量
	MessageCount  atomic.Int64 // 发布消息的数量

	closingChan chan struct{}
}

// 对象池
var clientPool = sync.Pool{
	New: func() interface{} {
		return &TcpClient{}
	}}

func NewTcpClient(id uint64, conn serveriface.IConnection) *TcpClient {
	client := clientPool.Get().(*TcpClient) // 从对象池中取出一个对象
	client.ID = id
	client.connection = conn
	client.Status.Store(statusInit)
	client.closingChan = make(chan struct{})

	go client.messagePump()

	return client
}

func DestroyTcpClient(client *TcpClient) {
	// 恢复client的状态
	client.ID = 0
	client.connection = nil
	client.Status.Store(statusClosing)
	client.IsPausing.Store(false)

	client.channel = nil

	client.ReadyCount.Store(0)
	client.InFlightCount.Store(0)
	client.RequeueCount.Store(0)
	client.MessageCount.Store(0)

	client.closingChan = nil

	clientPool.Put(client)
}

func (tcpClient *TcpClient) Pause() {
	tcpClient.IsPausing.Store(true)
}

func (tcpClient *TcpClient) UnPause() {
	tcpClient.IsPausing.Store(false)
}

func (tcpClient *TcpClient) Close() error {
	select {
	case tcpClient.closingChan <- struct{}{}:
	default:
	}

	tcpClient.Status.Store(statusClosing)

	return nil
}

func (tcpClient *TcpClient) Empty() {
	//TODO implement me
	panic("implement me")
}

// IsReadyRecv 客户端是否已经可以接收消息
func (tcpClient *TcpClient) IsReadyRecv() bool {
	if tcpClient.channel == nil {
		return false
	}

	if tcpClient.Status.Load() == statusClosing {
		return false
	}

	if tcpClient.IsPausing.Load() || tcpClient.Status.Load() != statusSubscribed {
		// 客户端正在暂停或者客户端不处于sub状态，说明一定不能接收消息
		return false
	}

	if tcpClient.ReadyCount.Load() == 0 || tcpClient.InFlightCount.Load() >= tcpClient.RequeueCount.Load() {
		return false
	}

	return true
}

// IsReadyPub 客户端是否已经可以发布消息
func (tcpClient *TcpClient) IsReadyPub() bool {
	if tcpClient.Status.Load() == statusClosing {
		return false
	}

	if tcpClient.Status.Load() != statusInit || tcpClient.Status.Load() != statusPublishing {
		return false
	}

	return true
}

// IsReadySub 客户端是否已经可以订阅消息
func (tcpClient *TcpClient) IsReadySub() bool {
	if tcpClient.Status.Load() == statusClosing {
		return false
	}

	if tcpClient.Status.Load() != statusInit {
		return false
	}

	return true
}

// SendMessage 向客户端发送消息
func (tcpClient *TcpClient) sendMessage(message iface.IMessage) error {
	tcpClient.InFlightCount.Add(1)

	data, _ := json.Marshal(message)
	err := tcpClient.connection.SendBufMsg(SendMsgID, data)
	if err != nil {
		return err
	}

	tcpClient.ReadyCount.Add(-1)
	return nil
}

func (tcpClient *TcpClient) messagePump() {
	var memoryMsgChan chan iface.IMessage
	var msg iface.IMessage
	var subChannel iface.IChannel

	for {
		if !tcpClient.IsReadyRecv() {
			memoryMsgChan = nil
		} else {
			memoryMsgChan = tcpClient.channel.GetMemoryMsgChan()
			subChannel = tcpClient.channel
		}

		select {
		case <-tcpClient.closingChan:
			goto Exit
		case msg = <-memoryMsgChan:
			// 向客户端发送消息
			msg.AddAttempts(1)

			_ = subChannel.StartInFlightTimeout(msg, tcpClient.ID, time.Second*3) // TODO:timeout时间可定义
			err := tcpClient.sendMessage(msg)
			if err != nil {
				goto Exit
			}
		}
	}

Exit:
	tcpClient.Close()
	tcpClient.connection.Stop()
	return
}