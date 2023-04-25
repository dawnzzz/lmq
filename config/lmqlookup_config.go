package config

import "time"

type LmqLookupConfig struct {
	TcpHost string `mapstructure:"tcp_host"`
	TcpPort int    `mapstructure:"tcp_port"`

	TcpServerWorkerPoolSize   int `mapstructure:"tcp_server_worker_pool_size"`    // TCP服务器Worker数量
	TcpServerMaxWorkerTaskLen int `mapstructure:"tcp_server_max_worker_task_len"` // TCP服务器 Worker任务队列长度
	TcpServerMaxMsgChanLen    int `mapstructure:"tcp_server_max_msg_chan_len"`    // 连接发送队列的缓冲区长度
	TcpServerMaxConn          int `mapstructure:"tcp_server_max_conn"`            // TCP服务器最大连接数

	InactiveProducerTimeout time.Duration `mapstructure:"inactive_producer_timeout"`
	TombstoneLifetime       time.Duration `mapstructure:"tombstone_lifetime"`
}

var GlobalLmqLookupConfig *LmqLookupConfig

func init() {
	GlobalLmqLookupConfig = &LmqLookupConfig{
		TcpHost: "0.0.0.0",
		TcpPort: 6300,

		TcpServerWorkerPoolSize:   10,
		TcpServerMaxWorkerTaskLen: 2048,
		TcpServerMaxMsgChanLen:    2048,
		TcpServerMaxConn:          10000,

		InactiveProducerTimeout: 300 * time.Second,
		TombstoneLifetime:       45 * time.Second,
	}
}
