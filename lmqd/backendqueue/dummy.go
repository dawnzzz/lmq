package backendqueue

type DummyBackendQueue struct {
	readChan chan []byte
}

func NewDummyBackendQueue() BackendQueue {
	return &DummyBackendQueue{readChan: make(chan []byte)}
}

func (queue *DummyBackendQueue) Put(bytes []byte) error {
	return nil
}

func (queue *DummyBackendQueue) ReadChan() <-chan []byte {
	return queue.readChan
}

func (queue *DummyBackendQueue) Close() error {
	return nil
}

func (queue *DummyBackendQueue) Delete() error {
	return nil
}

func (queue *DummyBackendQueue) Empty() error {
	return nil
}
