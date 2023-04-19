package iface

import "time"

// ILmqdInfo 与lmq lookup相连的lmqd信息
type ILmqdInfo interface {
	GetID() string
	GetLastUpdate() time.Time
	SetLastUpdate(updateTime time.Time)
	GetRemoteAddress() string
	SetRemoteAddress(remoteAddress string)
	GetHostName() string
	SetHostName(hostname string)
	GetTcpPort() int
	SetTcpPort(tcpPort int)
}

// ILmqdProducer 与lmq lookup相连的lmqd
type ILmqdProducer interface {
	GetLmqdInfo() ILmqdInfo
	SetLmqdInfo(peerInfo ILmqdInfo)
	String() string
	Tombstone()
	IsTombstoned(lifetime time.Duration) bool
}

type IProducers interface {
	FilterByActive(inactivityTimeout time.Duration, tombstoneLifetime time.Duration) IProducers
	LmqdInfo() []ILmqdInfo
}

type ProducerMap map[string]ILmqdProducer
