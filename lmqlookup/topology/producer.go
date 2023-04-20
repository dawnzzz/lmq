package topology

import (
	"fmt"
	"github.com/dawnzzz/lmq/iface"
	"time"
)

type LmqdProducer struct {
	peerInfo     iface.ILmqdInfo
	tombstoned   bool
	tombstonedAt time.Time
}

func NewLmqProducer(peerInfo iface.ILmqdInfo) iface.ILmqdProducer {
	p := &LmqdProducer{
		peerInfo: peerInfo,
	}

	return p
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

func (producers LmqdProducers) Len() int {
	return len(producers)
}

func (producers LmqdProducers) GetItem(index int) iface.ILmqdProducer {
	if index < 0 || index >= producers.Len() {
		return nil
	}

	return producers[index]
}
