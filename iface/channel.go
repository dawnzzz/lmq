package iface

type IChannel interface {
	Start()          // 开启channel
	Pause()          // 暂停channel
	UnPause()        // 恢复channel
	Close()          // 关闭channel
	Delete()         // 关闭并删除channel
	IsPausing() bool // 返回是否处于暂停状态

	Publish(message IMessage) error // 向channel发布一个消息
}
