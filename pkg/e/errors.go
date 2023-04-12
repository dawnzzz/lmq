package e

import "errors"

var (
	ErrLMQDIsNotRunning       = errors.New("lmqd is not running")
	ErrTopicNotFound          = errors.New("topic is not found")
	ErrTopicIsExiting         = errors.New("topic is exiting")
	ErrChannelNotFound        = errors.New("channel is not found")
	ErrChannelIsExiting       = errors.New("channel is exiting")
	ErrMessageIDIsNotInFlight = errors.New("message ID is not in flight")
	ErrClientNotOwnTheMessage = errors.New("this client not own the message")
)
