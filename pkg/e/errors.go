package e

import "errors"

var (
	ErrLMQDIsNotRunning  = errors.New("lmqd is not running")
	ErrTopicDuplicated   = errors.New("topic name is duplicated")
	ErrTopicNotFound     = errors.New("topic is not found")
	ErrChannelDuplicated = errors.New("channel name is duplicated")
	ErrChannelNotFound   = errors.New("channel is not found")
)
