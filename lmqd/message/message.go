package message

import (
	"github.com/dawnzzz/lmq/iface"
	"time"
)

type Message struct {
	id        iface.MessageID
	data      []byte
	timestamp int64
	attempts  uint16

	// 优先队列中使用到的数据结构
	clientID int64
	pri      int64
	index    int
}

func NewMessage(id iface.MessageID, data []byte) iface.IMessage {
	msg := &Message{
		id:        id,
		data:      data,
		timestamp: time.Now().UnixNano(),
	}

	return msg
}

func (msg *Message) GetID() iface.MessageID {
	return msg.id
}

func (msg *Message) GetData() []byte {
	return msg.data
}

func (msg *Message) GetTimestamp() int64 {
	return msg.timestamp
}

func (msg *Message) GetAttempts() uint16 {
	return msg.attempts
}

func (msg *Message) AddAttempts(delta uint16) {
	msg.attempts += delta
}

func (msg *Message) GetPriority() int64 {
	return msg.pri
}

func (msg *Message) SetPriority(pri int64) {
	msg.pri = pri
}

func (msg *Message) GetClientID() int64 {
	return msg.clientID
}

func (msg *Message) SetClientID(clientID int64) {
	msg.clientID = clientID
}

func (msg *Message) GetIndex() int {
	return msg.index
}

func (msg *Message) SetIndex(index int) {
	msg.index = index
}
