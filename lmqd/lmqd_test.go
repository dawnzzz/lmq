package lmqd

import (
	"errors"
	"testing"
)

func TestLmqd(t *testing.T) {
	lmqd := NewLmqDaemon()
	lmqd.Start()

	err := lmqd.AddTopic("test")
	if err != nil {
		t.Error("add topic err")
		return
	}

	err = lmqd.AddTopic("test")
	if err == nil || !errors.Is(err, ErrTopicDuplicated) {
		t.Error("add topic dup test err")
		return
	}

	topic, exist := lmqd.GetTopic("test")
	if !exist {
		t.Error("get topic err")
		return
	}

	lmqd.CloseTopic(topic)

	lmqd.Close()
}
