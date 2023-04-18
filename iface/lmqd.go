package iface

import serveriface "github.com/dawnzzz/hamble-tcp-server/iface"

type ILmqDaemon interface {
	Exit() // 退出lmqd
	Main()

	GetTopic(name string) (ITopic, error)              // 根据名字获取一个topic，如果没有就新增一个
	GetExistingTopic(topicName string) (ITopic, error) // 根据名字获取一个存在的topic
	DeleteExistingTopic(topicName string) error        // 删除一个存在的topic

	GenerateClientID(conn serveriface.IConnection) uint64 // 生成一个clientID

	Notify(persist bool)    // 通知lmqd进行持久化
	LoadMetaData() error    // 加载元数据信息
	PersistMetaData() error // 持久化元数据信息
}
