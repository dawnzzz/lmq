package iface

type ILmqLookup interface {
	Stop() // 退出lmq lookup
	Main()
}
