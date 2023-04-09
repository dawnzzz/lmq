package iface

type ITopic interface {
	Start()          // 开启topic
	Pause()          // 暂停topic
	UnPause()        // 恢复topic
	Close()          // 关闭topic
	Delete()         // 关闭并删除topic
	IsPausing() bool // 返回是否处于暂停状态

	GetName() string                         // 获取一个topic的name
	GetChannel(name string) (IChannel, bool) // 根据名字获取一个channel
	CloseChannel(channel IChannel)           // 关闭一个channel
	DeleteChannel(channel IChannel)          // 删除一个channel
	Publish(message IMessage) error          // 向topic发布一个消息
}
