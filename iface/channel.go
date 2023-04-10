package iface

type IChannel interface {
	Start()            // 开启channel
	Pause()            // 暂停channel
	UnPause()          // 恢复channel
	Empty()            // 清空channel
	Close()            // 关闭channel
	Delete()           // 关闭并删除channel
	GetStatus() uint32 // 获取状态
	IsPausing() bool   // 返回是否处于暂停状态

	GetName() string                // 获取一个channel的name
	Publish(message IMessage) error // 向channel发布一个消息
}
