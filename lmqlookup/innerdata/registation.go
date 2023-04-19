package innerdata

const (
	TopicCategory   = uint8(iota)
	ChannelCategory = uint8(iota)
)

type Registration struct {
	Category uint8 // 类别：topic channel
	Key      string
	SubKey   string
}

func (registration *Registration) IsMatch(category uint8, key string, subKey string) bool {
	if registration.Category != category {
		return false
	}

	if key != "*" && registration.Key != key {
		return false
	}

	if subKey != "*" && registration.SubKey != key {
		return false
	}

	return true
}

type Registrations []Registration

func (registrations Registrations) Filter(category uint8, key string, subKey string) Registrations {
	results := Registrations{}

	for _, registration := range registrations {
		if registration.IsMatch(category, key, subKey) {
			results = append(results, registration)
		}
	}

	return results
}

func (registrations Registrations) Keys() []string {
	keys := make([]string, len(registrations))

	for i, registration := range registrations {
		keys[i] = registration.Key
	}

	return keys
}

func (registrations Registrations) SubKeys() []string {
	subKeys := make([]string, len(registrations))

	for i, registration := range registrations {
		subKeys[i] = registration.SubKey
	}

	return subKeys
}
