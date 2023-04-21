package protocol

// task ID
const (
	PubID = uint32(iota)
	SubID
	SendMsgID
	CreateTopicID
	DeleteTopicID
	EmptyTopicID
	PauseTopicID
	UnPauseTopicID
	CreateChannelID
	DeleteChannelID
	EmptyChannelID
	PauseChannelID
	UnPauseChannelID
	RydID
	FinID
	ReqID

	PingID
	IdentityID
	RegisterID
	UnRegisterID
	LookupID
	TopicsID
	ChannelsID
	TombstoneTopicID
)
