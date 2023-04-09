package iface

type IMessage interface {
	GetID() uint32   // 获取message id
	GetData() []byte // 获取消息的内容
}
