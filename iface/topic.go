package iface

type ITopic interface {
	Start()            // 开启topic
	Pause()            // 暂停topic
	UnPause()          // 恢复topic
	Empty()            // 清空topic
	Close()            // 关闭topic
	Delete()           // 关闭并删除topic
	GetStatus() uint32 // 获取状态
	IsPausing() bool   // 返回是否处于暂停状态

	GetName() string                                  // 获取一个topic的name
	GetChannel(name string) IChannel                  // 获取一个channel，如果没有就新建一个
	GetExistingChannel(name string) (IChannel, error) // 根据名字获取一个已存在的channel
	DeleteExistingChannel(name string) error          // 删除一个存在的channel
	Publish(message IMessage) error                   // 向topic发布一个消息
}
