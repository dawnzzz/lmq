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
	clientID uint64
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

func (msg *Message) GetDataLength() int32 {
	return int32(len(msg.Data))
}

func (msg *Message) GetLength() int32 {
	idLen := iface.MsgIDLength
	dataLen := len(msg.Data)
	timestampLen := 8
	attemptsLen := 2

	return int32(idLen + dataLen + timestampLen + attemptsLen)
}

func (msg *Message) GetTimestamp() int64 {
	return msg.Timestamp
}

func (msg *Message) SetTimestamp(timestamp int64) {
	msg.Timestamp = timestamp
}

func (msg *Message) GetAttempts() uint16 {
	return msg.Attempts
}

func (msg *Message) SetAttempts(attempts uint16) {
	msg.Attempts = attempts
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

func (msg *Message) GetClientID() uint64 {
	return msg.clientID
}

func (msg *Message) SetClientID(clientID uint64) {
	msg.clientID = clientID
}

func (msg *Message) GetIndex() int {
	return msg.index
}

func (msg *Message) SetIndex(index int) {
	msg.index = index
}
