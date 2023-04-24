package lookup

import (
	"github.com/dawnzzz/lmq/iface"
	"github.com/dawnzzz/lmq/internel/utils"
	"sync/atomic"
)

type Manager struct {
	lmqd        iface.ILmqDaemon
	lookupPeers []*lookupPeer
	notifyChan  chan interface{} // 用于通知lookup本节点的topic或者channel更新了
	isExiting   atomic.Bool
	exitChan    chan struct{}

	utils.WaitGroupWrapper
}

func NewManager(lmqd iface.ILmqDaemon, lookupAddresses []string) iface.ILookupManager {
	lookupPeers := make([]*lookupPeer, len(lookupAddresses))
	for i, address := range lookupAddresses {
		lookupPeers[i], _ = newLookupPeer(lmqd, address)
	}

	m := &Manager{
		lmqd:        lmqd,
		lookupPeers: lookupPeers,
		notifyChan:  make(chan interface{}),
		exitChan:    make(chan struct{}),
	}

	return m
}

func (m *Manager) Start() {
	for _, peer := range m.lookupPeers {
		if peer != nil {
			peer.start()
		}
	}
	go m.lookupLoop()
}

func (m *Manager) Close() {
	if !m.isExiting.CompareAndSwap(false, true) {
		return
	}

	close(m.exitChan)
	// 等待所有请求发送完成
	m.Wait()
	// 关闭所有lookup peer
	for _, peer := range m.lookupPeers {
		if peer != nil {
			peer.close()
		}
	}
}

func (m *Manager) GetNotifyChan() chan interface{} {
	return m.notifyChan
}

func (m *Manager) lookupLoop() {
	for {
		if m.isExiting.Load() {
			goto exit
		}

		var unRegister bool
		var topicName, channelName string
		select {
		case <-m.exitChan:
			goto exit
		case v := <-m.notifyChan:
			switch v.(type) { // 检查v的类型
			case iface.ITopic: // 如果是topic
				t := v.(iface.ITopic)
				topicName = t.GetName()
				if t.IsExiting() {
					// 已经退出了，unregister
					unRegister = true
				} else {
					// 没有退出，register
					unRegister = false
				}

			case iface.IChannel: // 如果是channel
				c := v.(iface.IChannel)
				topicName = c.GetTopicName()
				channelName = c.GetName()
				if c.IsExiting() {
					// 已经退出了，unregister
					unRegister = true
				} else {
					// 没有退出，register
					unRegister = false
				}
			}

			for _, peer := range m.lookupPeers {
				m.Wrap(func() {
					if peer != nil {
						_ = peer.sendRegistration(unRegister, topicName, channelName)
					}
				})
			}
		}
	}

exit:
}

func (m *Manager) GetLookupTopicChannels(topicName string) []string {
	channels := make([]string, 0, 10)
	for _, peer := range m.lookupPeers {
		if peer != nil {
			c := peer.getTopicChannels(topicName)
			channels = append(channels, c...)
		}
	}

	// 去重
	channels = utils.Uniq(channels)
	return channels
}
