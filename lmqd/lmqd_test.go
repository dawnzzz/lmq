package lmqd

import (
	"errors"
	"github.com/dawnzzz/lmq/pkg/e"
	"testing"
)

func TestLmqd(t *testing.T) {
	lmqd := NewLmqDaemon()
	lmqd.Start()

	topic := lmqd.GetTopic("test")
	if topic == nil {
		t.Error("add topic err")
		return
	}

	_, err := lmqd.GetExistingTopic("test1")
	if err == nil || !errors.Is(err, e.ErrTopicNotFound) {
		t.Error("add topic dup test err")
		return
	}

	topic.Close()

	lmqd.Close()
}
