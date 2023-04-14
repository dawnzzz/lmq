package config

import "time"

var GlobalLmqdConfig *LmqdConfig

type LmqdConfig struct {
	TcpHost string
	TcpPort int

	MaxMessageSize uint64 // 消息的最大长度

	MemQueueSize              int // 内存topic/channel的消息数
	TcpServerWorkerPoolSize   int // TCP服务器Worker数量
	TcpServerMaxWorkerTaskLen int // TCP服务器 Worker任务队列长度
	TcpServerMaxMsgChanLen    int // 连接发送队列的缓冲区长度
	TcpServerMaxConn          int // TCP服务器最大连接数

	TLSHost     string
	TLSPort     int
	TLSCertFile string // TLS证书文件
	TLSKeyFile  string // TLS密钥文件

	MessageTimeout    time.Duration
	ScanQueueInterval time.Duration
}

func init() {
	GlobalLmqdConfig = &LmqdConfig{
		TcpHost:                   "0.0.0.0",
		TcpPort:                   6200,
		MaxMessageSize:            1024768,
		MemQueueSize:              10000,
		TcpServerWorkerPoolSize:   10,
		TcpServerMaxWorkerTaskLen: 2048,
		TcpServerMaxMsgChanLen:    2048,
		TcpServerMaxConn:          12000,
		TLSHost:                   "0.0.0.0",
		TLSPort:                   6201,
		TLSCertFile:               "",
		TLSKeyFile:                "",

		MessageTimeout:    5 * time.Second,
		ScanQueueInterval: 100 * time.Millisecond,
	}
}
