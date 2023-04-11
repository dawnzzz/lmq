package channel

import "github.com/dawnzzz/lmq/iface"

type inFlightPriQueue []iface.IMessage

func (queue inFlightPriQueue) Len() int {
	return len(queue)
}

func (queue inFlightPriQueue) Less(i, j int) bool {
	return queue[i].GetPriority() >= queue[j].GetPriority()
}

func (queue inFlightPriQueue) Swap(i, j int) {
	queue[i], queue[j] = queue[j], queue[i]
}

func (queue *inFlightPriQueue) Push(x any) {
	*queue = append(*queue, x.(iface.IMessage))
}

func (queue *inFlightPriQueue) Pop() any {
	old := *queue
	n := len(old)
	x := old[n-1]
	*queue = old[:n-1]

	return x
}
