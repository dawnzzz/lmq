package channel

import (
	"container/heap"
	"github.com/dawnzzz/lmq/iface"
)

type inFlightPriQueue struct {
	internal internalInFlightPriQueue
}

func newInFlightPriQueue(capacity int) *inFlightPriQueue {
	internal := make(internalInFlightPriQueue, 0, capacity)

	return &inFlightPriQueue{
		internal: internal,
	}
}

func (inflight *inFlightPriQueue) Push(msg iface.IMessage) {
	n := inflight.internal.Len()
	msg.SetIndex(n)

	heap.Push(&inflight.internal, msg)
}

func (inflight *inFlightPriQueue) Pop() iface.IMessage {
	raw := heap.Pop(&inflight.internal)

	msg, _ := raw.(iface.IMessage)
	msg.SetIndex(-1)

	return msg
}

func (inflight *inFlightPriQueue) Remove(i int) iface.IMessage {
	raw := heap.Remove(&inflight.internal, i)

	msg, _ := raw.(iface.IMessage)
	msg.SetIndex(-1)

	return msg
}

/*
internalInFlightPriQueue 实现了heap.Interface接口
*/
type internalInFlightPriQueue []iface.IMessage

func (queue internalInFlightPriQueue) Len() int {
	return len(queue)
}

func (queue internalInFlightPriQueue) Less(i, j int) bool {
	return queue[i].GetPriority() >= queue[j].GetPriority()
}

func (queue internalInFlightPriQueue) Swap(i, j int) {
	queue[i], queue[j] = queue[j], queue[i]
}

func (queue *internalInFlightPriQueue) Push(x any) {
	*queue = append(*queue, x.(iface.IMessage))
}

func (queue *internalInFlightPriQueue) Pop() any {
	old := *queue
	n := len(old)
	x := old[n-1]
	*queue = old[:n-1]

	return x
}
