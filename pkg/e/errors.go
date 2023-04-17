package e

import (
	"errors"
	"fmt"
	"github.com/dawnzzz/lmq/config"
)

var (
	ErrTopicNameInValid = errors.New("topic name is invalid")
	ErrTopicNotFound    = errors.New("topic is not found")
	ErrTopicIsExiting   = errors.New("topic is exiting")

	ErrChannelNameInValid = errors.New("channel name is invalid")
	ErrChannelNotFound    = errors.New("channel is not found")
	ErrChannelIsExiting   = errors.New("channel is exiting")

	ErrMessageIDIsNotInFlight = errors.New("message ID is not in flight")
	ErrClientNotOwnTheMessage = errors.New("this client not own the message")
	ErrMessageLengthInvalid   = fmt.Errorf("message length is in valid, length is limited (%v, %v)", config.GlobalLmqdConfig.MinMessageSize, config.GlobalLmqdConfig.MaxMessageSize)
)
