package topic

import (
	"github.com/bwmarrin/snowflake"
	"github.com/dawnzzz/lmq/iface"
)

// GUIDFactory message id 生成器
type GUIDFactory struct {
	node *snowflake.Node
}

func NewGUIDFactory(nodeID int64) *GUIDFactory {
	node, _ := snowflake.NewNode(nodeID % 1024)

	return &GUIDFactory{
		node: node,
	}
}

func (factory *GUIDFactory) NewMessageID() iface.MessageID {
	return factory.node.Generate().IntBytes()
}
