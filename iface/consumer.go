package iface

type IConsumer interface {
	Pause()
	UnPause()
	Close() error
	Empty()
}
