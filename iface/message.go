package iface

const (
	MsgIDLength = 16
)

type MessageID [MsgIDLength]byte

type IMessage interface {
	GetID() MessageID    // 获取message id
	GetData() []byte     // 获取消息的内容
	GetTimestamp() int64 // 获取消息时间戳
	GetAttempts() uint16 // 获取尝试次数
	GetPriority() int64  // 优先级
	GetClientID() int64  // 获取客户端ID
	GetIndex() int
	SetIndex(index int)
}
