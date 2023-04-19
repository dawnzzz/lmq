package iface

type Category uint8

const (
	TopicCategory   = Category(iota)
	ChannelCategory = Category(iota)
)

type IRegistration interface {
	GetCategory() Category
	SetCategory(category Category)
	GetKey() string
	SetKey(key string)
	GetCSubKey() string
	SetCSubKey(subKey string)
	IsMatch(category Category, key string, subKey string) bool
}

type IRegistrations interface {
	Filter(category Category, key string, subKey string) IRegistrations
	Keys() []string
	SubKeys() []string
}

type IRegistrationDB interface {
	AddRegistration(key IRegistration)
	AddProducer(key IRegistration, p ILmqdProducer) bool
	RemoveProducer(key IRegistration, id string) (bool, int)
	RemoveRegistration(key IRegistration)
	FindRegistrations(category Category, key string, subKey string) IRegistrations
	FindProducers(category Category, key string, subKey string) IProducers
	LookupRegistrations(id string) IRegistrations
}
