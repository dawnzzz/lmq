package channel

type Consumer interface {
	Pause()
	UnPause()
	Close() error
	Empty()
}
