package channel

import (
	"github.com/dawnzzz/lmq/iface"
	"sync/atomic"
)

type IConsumer interface {
	Pause()
	UnPause()
	Close() error
	Empty()
}

const (
	statusInit = uint32(iota)
	statusSub
	statusClosed
)

type TcpClient struct {
	Status    atomic.Uint32 // 客户端当前状态
	IsPausing atomic.Bool   // 标记是否暂停

	ReadyCount    atomic.Int64 // 准备好接收的message数量
	InFlightCount atomic.Int64 // in-flight消息数量
	RequeueCount  atomic.Int64 // requeue消息数量
}

func (tcpClient *TcpClient) Pause() {
	tcpClient.IsPausing.Store(true)
}

func (tcpClient *TcpClient) UnPause() {
	tcpClient.IsPausing.Store(false)
}

func (tcpClient *TcpClient) Close() error {
	//TODO implement me
	panic("implement me")
}

func (tcpClient *TcpClient) Empty() {
	//TODO implement me
	panic("implement me")
}

// IsReady 客户端是否已经可以接收消息
func (tcpClient *TcpClient) IsReady() bool {
	if tcpClient.IsPausing.Load() || tcpClient.Status.Load() != statusSub {
		// 客户端正在暂停或者客户端不处于sub状态，说明一定不能接收消息
		return false
	}

	if tcpClient.ReadyCount.Load() == 0 || tcpClient.InFlightCount.Load() >= tcpClient.RequeueCount.Load() {
		return false
	}

	return true
}

// SendMessage 向客户端发送消息
func (tcpClient *TcpClient) SendMessage(message iface.IMessage) error {
	//TODO implement me
	panic("implement me")
}
