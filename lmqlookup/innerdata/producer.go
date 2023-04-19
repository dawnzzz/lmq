package innerdata

import (
	"fmt"
	"time"
)

type PeerInfo struct {
	id            string
	RemoteAddress string
	Hostname      string
	TcpPort       int
}

type Producer struct {
	peerInfo     *PeerInfo
	tombstoned   bool
	tombstonedAt time.Time
}

type Producers []*Producer
type ProducerMap map[string]*Producer

func (p *Producer) GetPeerInfo() *PeerInfo {
	return p.peerInfo
}

func (p *Producer) SetPeerInfo(peerInfo *PeerInfo) {
	p.peerInfo = peerInfo
}

func (p *Producer) String() string {
	return fmt.Sprintf("[%s %d]", p.peerInfo.Hostname, p.peerInfo.TcpPort)
}

func (p *Producer) Tombstone() {
	p.tombstoned = true
	p.tombstonedAt = time.Now()
}

func (p *Producer) IsTombstoned(lifetime time.Duration) bool {
	return p.tombstoned && time.Since(p.tombstonedAt) < lifetime
}

func ProducerMap2Slice(producerMap ProducerMap) Producers {
	producers := Producers{}

	for _, producer := range producerMap {
		producers = append(producers, producer)
	}

	return producers
}
