package topology

import (
	"github.com/dawnzzz/lmq/iface"
	"sync"
)

type RegistrationDB struct {
	sync.RWMutex
	registrationMap map[iface.IRegistration]iface.ProducerMap
}

func NewRegistrationDB() *RegistrationDB {
	return &RegistrationDB{
		registrationMap: make(map[iface.IRegistration]iface.ProducerMap),
	}
}

func (r *RegistrationDB) AddRegistration(key iface.IRegistration) {
	r.Lock()
	defer r.Unlock()

	_, ok := r.registrationMap[key]
	if !ok {
		r.registrationMap[key] = make(map[string]iface.ILmqdProducer)
	}
}

func (r *RegistrationDB) AddProducer(key iface.IRegistration, p iface.ILmqdProducer) bool {
	r.Lock()
	defer r.Unlock()

	_, ok := r.registrationMap[key]
	if !ok {
		r.registrationMap[key] = make(map[string]iface.ILmqdProducer)
	}

	producerMap := r.registrationMap[key]
	_, found := producerMap[p.GetLmqdInfo().GetID()]
	if !found {
		producerMap[p.GetLmqdInfo().GetID()] = p
	}

	return !found
}

// RemoveProducer 返回是否有数据删除、剩余producer的数量
func (r *RegistrationDB) RemoveProducer(key iface.IRegistration, id string) (bool, int) {
	r.Lock()
	defer r.Unlock()

	producerMap, ok := r.registrationMap[key]
	if !ok {
		return false, 0
	}

	var removed bool
	if _, exists := producerMap[id]; exists {
		removed = true
	}

	delete(producerMap, id)
	return removed, len(producerMap)
}

func (r *RegistrationDB) RemoveRegistration(key iface.IRegistration) {
	r.Lock()
	defer r.Unlock()
	delete(r.registrationMap, key)
}

func (r *RegistrationDB) needFilter(key string, subKey string) bool {
	return key == "*" || subKey == "*"
}

func (r *RegistrationDB) FindRegistrations(category iface.Category, key string, subKey string) iface.IRegistrations {
	r.RLock()
	defer r.RUnlock()

	if !r.needFilter(key, subKey) {
		k := MakeRegistration(category, key, subKey)

		if _, ok := r.registrationMap[k]; ok {
			return Registrations{k}
		}

		return Registrations{}
	}

	results := Registrations{}
	for k := range r.registrationMap {
		if !k.IsMatch(category, key, subKey) {
			continue
		}

		results = append(results, k)
	}

	return results
}

func (r *RegistrationDB) FindProducers(category iface.Category, key string, subKey string) iface.IProducers {
	r.RLock()
	defer r.RUnlock()

	if !r.needFilter(key, subKey) {
		k := MakeRegistration(category, key, subKey)

		return ProducerMap2Slice(r.registrationMap[k])
	}

	producers := LmqdProducers{}
	idSet := map[string]struct{}{}
	for registration, producerMap := range r.registrationMap {
		if !registration.IsMatch(category, key, subKey) {
			continue
		}

		for id, producer := range producerMap {
			if _, exist := idSet[id]; !exist {
				producers = append(producers, producer)
				idSet[id] = struct{}{}
			}
		}
	}

	return producers
}

func (r *RegistrationDB) LookupRegistrations(id string) iface.IRegistrations {
	r.RLock()
	defer r.RUnlock()

	results := Registrations{}
	for registration, producerMap := range r.registrationMap {
		if _, exists := producerMap[id]; exists {
			results = append(results, registration)
		}
	}

	return results
}

func (r *RegistrationDB) RemoveProducerFromAllRegistrations(id string) {
	r.Lock()
	defer r.Unlock()

	for registration, producerMap := range r.registrationMap {
		if _, exists := producerMap[id]; exists {
			_, ok := r.registrationMap[registration]
			if !ok {
				continue
			}

			delete(producerMap, id)
		}
	}
}
