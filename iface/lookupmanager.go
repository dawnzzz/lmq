package iface

// ILookupManager 每个lmqd都会维护一个
type ILookupManager interface {
	Start()
	Close()
	GetNotifyChan() chan interface{}
	GetLookupTopicChannels(topicName string) []string
}
