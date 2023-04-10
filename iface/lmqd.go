package iface

type ILmqDaemon interface {
	Start() // 开启lmqd
	Close() // 关闭lmqd
	Main()

	GetTopic(name string) ITopic                       // 根据名字获取一个topic，如果没有就新增一个
	GetExistingTopic(topicName string) (ITopic, error) // 根据名字获取一个存在的topic
	DeleteExistingTopic(topicName string) error        // 删除一个存在的topic
	Publish(topic ITopic, message IMessage) error      // 向topic内发布一个消息
}
