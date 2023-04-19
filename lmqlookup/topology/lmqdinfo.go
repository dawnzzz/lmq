package topology

import (
	"github.com/dawnzzz/lmq/iface"
	"sync/atomic"
	"time"
)

// LmqdInfo 与lmq lookup连接的lmqd信息
type LmqdInfo struct {
	id            string
	lastUpdate    atomic.Int64
	RemoteAddress string
	Hostname      string
	TcpPort       int
}

func NewLmqdInfo(id string, remoteAddress string, hostname string, tcpPort int) iface.ILmqdInfo {
	info := &LmqdInfo{
		id:            id,
		RemoteAddress: remoteAddress,
		Hostname:      hostname,
		TcpPort:       tcpPort,
	}
	info.lastUpdate.Store(time.Now().Unix())

	return info
}

func (info *LmqdInfo) GetID() string {
	return info.id
}

func (info *LmqdInfo) SetID(id string) {
	info.id = id
}

func (info *LmqdInfo) GetLastUpdate() time.Time {
	timestamp := info.lastUpdate.Load()
	return time.Unix(0, timestamp)
}

func (info *LmqdInfo) SetLastUpdate(updateTime time.Time) {
	info.lastUpdate.Store(updateTime.Unix())
}

func (info *LmqdInfo) GetRemoteAddress() string {
	return info.RemoteAddress
}

func (info *LmqdInfo) SetRemoteAddress(remoteAddress string) {
	info.RemoteAddress = remoteAddress
}

func (info *LmqdInfo) GetHostName() string {
	return info.Hostname
}

func (info *LmqdInfo) SetHostName(hostname string) {
	info.Hostname = hostname
}

func (info *LmqdInfo) GetTcpPort() int {
	return info.TcpPort
}

func (info *LmqdInfo) SetTcpPort(tcpPort int) {
	info.TcpPort = tcpPort
}
