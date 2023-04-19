package topology

import (
	"fmt"
	"github.com/dawnzzz/lmq/iface"
	"sync/atomic"
	"time"
)

// LmqdInfo 与lmq lookup连接的lmqd信息
type LmqdInfo struct {
	id            string
	lastUpdate    atomic.Int64
	RemoteAddress string
	Hostname      string
	TcpPort       int
}

func (info *LmqdInfo) GetID() string {
	return info.id
}

func (info *LmqdInfo) GetLastUpdate() time.Time {
	timestamp := info.lastUpdate.Load()
	return time.Unix(0, timestamp)
}

func (info *LmqdInfo) SetLastUpdate(updateTime time.Time) {
	info.lastUpdate.Store(updateTime.Unix())
}

func (info *LmqdInfo) GetRemoteAddress() string {
	return info.RemoteAddress
}

func (info *LmqdInfo) SetRemoteAddress(remoteAddress string) {
	info.RemoteAddress = remoteAddress
}

func (info *LmqdInfo) GetHostName() string {
	return info.Hostname
}

func (info *LmqdInfo) SetHostName(hostname string) {
	info.Hostname = hostname
}

func (info *LmqdInfo) GetTcpPort() int {
	return info.TcpPort
}

func (info *LmqdInfo) SetTcpPort(tcpPort int) {
	info.TcpPort = tcpPort
}

type LmqdProducer struct {
	peerInfo     iface.ILmqdInfo
	tombstoned   bool
	tombstonedAt time.Time
}

func (p *LmqdProducer) GetLmqdInfo() iface.ILmqdInfo {
	return p.peerInfo
}

func (p *LmqdProducer) SetLmqdInfo(peerInfo iface.ILmqdInfo) {
	p.peerInfo = peerInfo
}

func (p *LmqdProducer) String() string {
	return fmt.Sprintf("%s [%s %d]", p.peerInfo.GetRemoteAddress(), p.peerInfo.GetHostName(), p.peerInfo.GetTcpPort())
}

func (p *LmqdProducer) Tombstone() {
	p.tombstoned = true
	p.tombstonedAt = time.Now()
}

func (p *LmqdProducer) IsTombstoned(lifetime time.Duration) bool {
	return p.tombstoned && time.Since(p.tombstonedAt) < lifetime
}

func ProducerMap2Slice(producerMap iface.ProducerMap) iface.IProducers {
	producers := &LmqdProducers{}

	for _, producer := range producerMap {
		*producers = append(*producers, producer)
	}

	return producers
}

type LmqdProducers []iface.ILmqdProducer

func (producers LmqdProducers) FilterByActive(inactivityTimeout time.Duration, tombstoneLifetime time.Duration) iface.IProducers {
	now := time.Now()
	results := LmqdProducers{}
	for _, p := range producers {
		cur := p.GetLmqdInfo().GetLastUpdate()
		if now.Sub(cur) > inactivityTimeout || p.IsTombstoned(tombstoneLifetime) {
			continue
		}
		results = append(results, p)
	}
	return results
}

func (producers LmqdProducers) LmqdInfo() []iface.ILmqdInfo {
	var results []iface.ILmqdInfo
	for i := 0; i < len(producers); i++ {
		p := producers[i]
		results = append(results, p.GetLmqdInfo())
	}
	return results
}
