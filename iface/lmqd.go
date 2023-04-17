package iface

import serveriface "github.com/dawnzzz/hamble-tcp-server/iface"

type ILmqDaemon interface {
	Exit() // 退出lmqd
	Main()

	GetTopic(name string) (ITopic, error)              // 根据名字获取一个topic，如果没有就新增一个
	GetExistingTopic(topicName string) (ITopic, error) // 根据名字获取一个存在的topic
	DeleteExistingTopic(topicName string) error        // 删除一个存在的topic

	GenerateClientID(conn serveriface.IConnection) uint64 // 生成一个clientID
}
