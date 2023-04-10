package basestructure

import "sync/atomic"

const (
	starting = uint32(iota)
	running
	pausing
	closing
	Closed
)

// base topic和channel都继承这个结构体
type base struct {
	status atomic.Uint32

	startChan   chan struct{}
	updateChan  chan struct{}
	pauseChan   chan struct{}
	closingChan chan struct{}
	closedChan  chan struct{}
}

func newBase() base {
	return base{
		startChan:   make(chan struct{}),
		updateChan:  make(chan struct{}),
		pauseChan:   make(chan struct{}),
		closingChan: make(chan struct{}),
		closedChan:  make(chan struct{}, 1),
	}
}

// Start 启动
func (b *base) Start() {
	select {
	case b.startChan <- struct{}{}:
	default:
	}
}

// Pause 暂停
func (b *base) Pause() {
	if b.status.CompareAndSwap(running, pausing) {
		return
	}

	select {
	case b.pauseChan <- struct{}{}:
		b.status.CompareAndSwap(running, pausing)
	}
}

// UnPause 恢复
func (b *base) UnPause() {
	if b.status.CompareAndSwap(pausing, running) {
		return
	}

	select {
	case b.pauseChan <- struct{}{}:
		b.status.CompareAndSwap(pausing, running)
	}
}

// Close 关闭
func (b *base) Close() {
	// 检查是否处于关闭状态
	if b.status.Load() == closing {
		return
	}

	select {
	case b.closingChan <- struct{}{}:
		b.status.Store(closing)
	default:
	}
}

// GetStatus 获取状态
func (b *base) GetStatus() uint32 {
	return b.status.Load()
}

// IsPausing 返回是否处于暂停状态
func (b *base) IsPausing() bool {
	return b.status.Load() == pausing
}
