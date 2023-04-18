package config

import "time"

var GlobalLmqdConfig *LmqdConfig

type LmqdConfig struct {
	TcpHost string
	TcpPort int

	MinMessageSize int32 // 消息的最小长度
	MaxMessageSize int32 // 消息的最大长度

	DataRootPath string        // 用于保存持久化数据得根目录
	SyncEvery    int64         // 磁盘队列进行多少次读写操作时进行一次同步
	SyncTimeout  time.Duration // 队列文件最长多长时间进行一次同步

	MaxBytesPerFile           int64 // 磁盘队列文件每一个文件的最大长度
	MemQueueSize              int   // 内存topic/channel的消息数
	TcpServerWorkerPoolSize   int   // TCP服务器Worker数量
	TcpServerMaxWorkerTaskLen int   // TCP服务器 Worker任务队列长度
	TcpServerMaxMsgChanLen    int   // 连接发送队列的缓冲区长度
	TcpServerMaxConn          int   // TCP服务器最大连接数

	TLSHost     string
	TLSPort     int
	TLSCertFile string // TLS证书文件
	TLSKeyFile  string // TLS密钥文件

	MessageTimeout    time.Duration
	ScanQueueInterval time.Duration
}

func init() {
	GlobalLmqdConfig = &LmqdConfig{
		TcpHost:        "0.0.0.0",
		TcpPort:        6200,
		MinMessageSize: 0,
		MaxMessageSize: 1024768,

		DataRootPath: "data",
		SyncEvery:    10,
		SyncTimeout:  10 * time.Second,

		MaxBytesPerFile:           1024 * 1024 * 64,
		MemQueueSize:              100,
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
