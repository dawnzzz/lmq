package message

import (
	"github.com/dawnzzz/lmq/iface"
	"time"
)

type Message struct {
	ID        iface.MessageID `json:"ID"`
	Data      []byte          `json:"Data"`
	Timestamp int64           `json:"Timestamp"`
	Attempts  uint16          `json:"Attempts"`

	// 优先队列中使用到的数据结构
	clientID int64
	pri      int64
	index    int
}

func NewMessage(id iface.MessageID, data []byte) iface.IMessage {
	msg := &Message{
		ID:        id,
		Data:      data,
		Timestamp: time.Now().UnixNano(),
	}

	return msg
}

func (msg *Message) GetID() iface.MessageID {
	return msg.ID
}

func (msg *Message) GetData() []byte {
	return msg.Data
}

func (msg *Message) GetTimestamp() int64 {
	return msg.Timestamp
}

func (msg *Message) GetAttempts() uint16 {
	return msg.Attempts
}

func (msg *Message) AddAttempts(delta uint16) {
	msg.Attempts += delta
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
