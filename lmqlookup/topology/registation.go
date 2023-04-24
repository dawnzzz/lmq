package topology

import "github.com/dawnzzz/lmq/iface"

type Registration struct {
	Category iface.Category // 类别：topic channel
	Key      string
	SubKey   string
}

func MakeRegistration(category iface.Category, key string, subKey string) Registration {
	return Registration{
		Category: category,
		Key:      key,
		SubKey:   subKey,
	}
}

func (registration Registration) GetCategory() iface.Category {
	return registration.Category
}

func (registration Registration) GetKey() string {
	return registration.Key
}

func (registration Registration) GetCSubKey() string {
	return registration.SubKey
}

func (registration Registration) SetCategory(category iface.Category) {
	registration.Category = category
}

func (registration Registration) SetKey(key string) {
	registration.Key = key
}

func (registration Registration) SetCSubKey(subKey string) {
	registration.SubKey = subKey
}

func (registration Registration) IsMatch(category iface.Category, key string, subKey string) bool {
	if registration.Category != category {
		return false
	}

	if key != "*" && registration.Key != key {
		return false
	}

	if subKey != "*" && registration.SubKey != subKey {
		return false
	}

	return true
}

type Registrations []iface.IRegistration

func (registrations Registrations) Filter(category iface.Category, key string, subKey string) iface.IRegistrations {
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
		keys[i] = registration.GetKey()
	}

	return keys
}

func (registrations Registrations) SubKeys() []string {
	subKeys := make([]string, len(registrations))

	for i, registration := range registrations {
		subKeys[i] = registration.GetCSubKey()
	}

	return subKeys
}

func (registrations Registrations) Len() int {
	return len(registrations)
}

func (registrations Registrations) GetItem(index int) iface.IRegistration {
	if index < 0 || index >= registrations.Len() {
		return nil
	}

	return registrations[index]
}
