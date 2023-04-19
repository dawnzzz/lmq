package config

import "time"

type LmqLookupConfig struct {
	TcpHost string
	TcpPort int

	TcpServerWorkerPoolSize   int // TCP服务器Worker数量
	TcpServerMaxWorkerTaskLen int // TCP服务器 Worker任务队列长度
	TcpServerMaxMsgChanLen    int // 连接发送队列的缓冲区长度
	TcpServerMaxConn          int // TCP服务器最大连接数

	InactiveProducerTimeout time.Duration
	TombstoneLifetime       time.Duration
}

var GlobalLmqLookupConfig *LmqLookupConfig

func init() {
	GlobalLmqLookupConfig = &LmqLookupConfig{
		TcpHost: "0.0.0.0",
		TcpPort: 6300,

		TcpServerWorkerPoolSize:   20,
		TcpServerMaxWorkerTaskLen: 4096,
		TcpServerMaxMsgChanLen:    4096,
		TcpServerMaxConn:          15000,

		InactiveProducerTimeout: 300 * time.Second,
		TombstoneLifetime:       45 * time.Second,
	}
}
