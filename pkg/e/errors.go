package e

import "errors"

var (
	ErrLMQDIsNotRunning = errors.New("lmqd is not running")
	ErrTopicNotFound    = errors.New("topic is not found")
	ErrChannelNotFound  = errors.New("channel is not found")
)
