package iface

type IChannel interface {
	Pause() error    // 暂停channel
	UnPause() error  // 恢复channel
	Empty() error    // 清空channel
	Close() error    // 关闭channel
	Delete() error   // 关闭并删除channel
	IsPausing() bool // 返回是否处于暂停状态
	IsExiting() bool // 是否退出

	GetName() string                   // 获取一个channel的name
	PutMessage(message IMessage) error // 向channel发布一个消息
	FinishMessage(clientID int64, messageID MessageID) error
	RequeueMessage(clientID int64, messageID MessageID) error
}
