package tcp

import (
	serveriface "github.com/dawnzzz/hamble-tcp-server/iface"
	"github.com/dawnzzz/lmq/config"
	"github.com/dawnzzz/lmq/iface"
)

type Node struct {
	RemoteAddress string   `json:"remote_address"`
	Hostname      string   `json:"hostname"`
	TCPPort       int      `json:"tcp_port"`
	Tombstones    []bool   `json:"tombstones"`
	Topics        []string `json:"topics"`
}

type NodesHandler struct {
	*BaseHandler
}

func (h *NodesHandler) Handle(request serveriface.IRequest) {
	// 筛选出存活的节点（不过滤tombstone的节点）
	producers := h.registrationDB.FindProducers(iface.LmqdCategory, "", "").FilterByActive(config.GlobalLmqLookupConfig.InactiveProducerTimeout, 0)
	nodes := make([]*Node, producers.Len())
	topicProducersMap := make(map[string]iface.IProducers)
	for i := 0; i < producers.Len(); i++ {
		p := producers.GetItem(i)

		topics := h.registrationDB.LookupRegistrations(p.GetLmqdInfo().GetID()).Filter(iface.TopicCategory, "*", "").Keys()

		tombstones := make([]bool, len(topics))
		for j, topic := range topics {
			if _, exist := topicProducersMap[topic]; !exist {
				topicProducersMap[topic] = h.registrationDB.FindProducers(iface.TopicCategory, topic, "")
			}

			topicProducers := topicProducersMap[topic]
			for k := 0; k < topicProducers.Len(); k++ {
				tp := topicProducers.GetItem(k)
				if tp.GetLmqdInfo().Equals(p.GetLmqdInfo()) {
					tombstones[j] = tp.IsTombstoned(config.GlobalLmqLookupConfig.TombstoneLifetime)
					break
				}
			}
		}

		nodes[i] = &Node{
			RemoteAddress: p.GetLmqdInfo().GetRemoteAddress(),
			Hostname:      p.GetLmqdInfo().GetHostName(),
			TCPPort:       p.GetLmqdInfo().GetTcpPort(),
			Tombstones:    tombstones,
			Topics:        topics,
		}
	}

	_ = h.SendDataResponse(request, nodes)
}
