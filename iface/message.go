package iface

const (
	MsgIDLength = 8
)

type MessageID [MsgIDLength]byte

func (msgID MessageID) Bytes() []byte {
	return msgID[:]
}

type IMessage interface {
	GetID() MessageID             // 获取message id
	GetData() []byte              // 获取消息的内容
	GetDataLength() int32         // 获取消息数据部分的长度
	GetLength() int32             // 获取消息持久化总长度（包括ID 时间戳等）
	GetTimestamp() int64          // 获取消息时间戳
	SetTimestamp(timestamp int64) // 设置时间戳
	GetAttempts() uint16          // 获取尝试次数
	SetAttempts(attempts uint16)  // 设置尝试次数
	AddAttempts(delta uint16)     //	增加尝试次数
	GetPriority() int64           // 优先级
	SetPriority(pri int64)        // 设置优先级
	GetClientID() uint64          // 获取客户端ID
	SetClientID(clientID uint64)  // 设置客户端ID
	GetIndex() int                // index为在优先队列中的位置
	SetIndex(index int)
}
