package iface

type ITopic interface {
	Start()          // 开启topic
	Pause() error    // 暂停topic
	UnPause() error  // 恢复topic
	Empty() error    // 清空topic
	Close() error    // 关闭topic
	Delete() error   // 关闭并删除topic
	IsPausing() bool // 返回是否处于暂停状态

	GenerateGUID() MessageID // 生成一个messageID

	GetName() string                                  // 获取一个topic的name
	GetChannel(name string) (IChannel, error)         // 获取一个channel，如果没有就新建一个
	GetExistingChannel(name string) (IChannel, error) // 根据名字获取一个已存在的channel
	DeleteExistingChannel(name string) error          // 删除一个存在的channel
	PutMessage(message IMessage) error                // 向topic发布一个消息
}
