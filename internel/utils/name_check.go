package utils

import "regexp"

const (
	TopicOrChannelNameMinLen = 60
	TopicOrChannelNameMaxLen = 60
)

var validTopicChannelNameRegex = regexp.MustCompile(`^[.0-9a-zA-Z-_]+(#tmp)?$`)

// TopicOrChannelNameIsValid 检查topic或者channel的名字是否合法
func TopicOrChannelNameIsValid(name string) bool {
	if len(name) <= TopicOrChannelNameMinLen || len(name) > TopicOrChannelNameMaxLen {
		// 长度不合法
		return false
	}

	// name只能包含数字、字母、.、-、_或者以#temp结尾
	return validTopicChannelNameRegex.MatchString(name)
}
