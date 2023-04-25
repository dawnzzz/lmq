package config

import "time"

var GlobalLmqdConfig *LmqdConfig

type LmqdConfig struct {
	TcpHost string `mapstructure:"tcp_host"`
	TcpPort int    `mapstructure:"tcp_port"`

	MinMessageSize int32 `mapstructure:"min_message_size"` // 消息的最小长度
	MaxMessageSize int32 `mapstructure:"max_message_size"` // 消息的最大长度

	DataRootPath string        `mapstructure:"data_root_path"` // 用于保存持久化数据得根目录
	SyncEvery    int64         `mapstructure:"sync_every"`     // 磁盘队列进行多少次读写操作时进行一次同步
	SyncTimeout  time.Duration `mapstructure:"sync_timeout"`   // 队列文件最长多长时间进行一次同步

	MaxBytesPerFile           int64 `mapstructure:"max_bytes_per_file"`             // 磁盘队列文件每一个文件的最大长度
	MemQueueSize              int   `mapstructure:"mem_queue_size"`                 // 内存topic/channel的消息数
	TcpServerWorkerPoolSize   int   `mapstructure:"tcp_server_worker_pool_size"`    // TCP服务器Worker数量
	TcpServerMaxWorkerTaskLen int   `mapstructure:"tcp_server_max_worker_task_len"` // TCP服务器 Worker任务队列长度
	TcpServerMaxMsgChanLen    int   `mapstructure:"tcp_server_max_msg_chan_len"`    // 连接发送队列的缓冲区长度
	TcpServerMaxConn          int   `mapstructure:"tcp_server_max_conn"`            // TCP服务器最大连接数

	//TLSHost     string `mapstructure:"tls_host"`
	//TLSPort     int    `mapstructure:"tls_port"`
	//TLSCertFile string `mapstructure:"tls_cert_file"` // TLS证书文件
	//TLSKeyFile  string `mapstructure:"tls_key_file"`  // TLS密钥文件

	MessageTimeout    time.Duration `mapstructure:"message_timeout"`
	ScanQueueInterval time.Duration `mapstructure:"scan_queue_interval"`

	HeartBeatInterval time.Duration `mapstructure:"heart_beat_interval"` // 向lmq lookup发送心跳的时间间隔
	LookupAddresses   []string      `mapstructure:"lookup_addresses"`    // lmq lookup的地址，可配置多个lmq lookup
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
		//TLSHost:                   "0.0.0.0",
		//TLSPort:                   6201,
		//TLSCertFile:               "",
		//TLSKeyFile:                "",

		MessageTimeout:    5 * time.Second,
		ScanQueueInterval: 100 * time.Millisecond,

		HeartBeatInterval: 60 * time.Second,
		LookupAddresses:   []string{},
	}
}
