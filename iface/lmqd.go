package iface

type ILmqDaemon interface {
	Start() // 开启lmqd
	Close() // 关闭lmqd
	Main()

	AddTopic(name string) error             // 增加一个topic
	GetTopic(name string) (ITopic, bool)    // 根据名字获取一个topic
	CloseTopic(topic ITopic)                // 关闭一个topic
	DeleteTopic(topic ITopic)               // 删除一个topic
	Publish(topic ITopic, message IMessage) // 向topic内发布一个消息
}
