package innerdata

import "sync"

type RegistrationDB struct {
	sync.RWMutex
	registrationMap map[Registration]ProducerMap
}

func NewRegistrationDB() *RegistrationDB {
	return &RegistrationDB{
		registrationMap: make(map[Registration]ProducerMap),
	}
}

func (r *RegistrationDB) AddRegistration(key Registration) {
	r.Lock()
	defer r.Unlock()

	_, ok := r.registrationMap[key]
	if !ok {
		r.registrationMap[key] = make(map[string]*Producer)
	}
}

func (r *RegistrationDB) AddProducer(key Registration, p *Producer) bool {
	r.Lock()
	defer r.Unlock()

	_, ok := r.registrationMap[key]
	if !ok {
		r.registrationMap[key] = make(map[string]*Producer)
	}

	producerMap := r.registrationMap[key]
	_, found := producerMap[p.peerInfo.id]
	if !found {
		producerMap[p.peerInfo.id] = p
	}

	return !found
}

// RemoveProducer 返回是否有数据删除、剩余producer的数量
func (r *RegistrationDB) RemoveProducer(key Registration, id string) (bool, int) {
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

func (r *RegistrationDB) RemoveRegistration(key Registration) {
	r.Lock()
	defer r.Unlock()
	delete(r.registrationMap, key)
}

func (r *RegistrationDB) needFilter(key string, subKey string) bool {
	return key == "*" || subKey == "*"
}

func (r *RegistrationDB) FindRegistrations(category uint8, key string, subKey string) Registrations {
	r.RLock()
	defer r.RUnlock()

	if !r.needFilter(key, subKey) {
		k := Registration{
			Category: category,
			Key:      key,
			SubKey:   subKey,
		}

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

func (r *RegistrationDB) FindProducers(category uint8, key string, subKey string) Producers {
	r.RLock()
	defer r.RUnlock()

	if !r.needFilter(key, subKey) {
		k := Registration{
			Category: category,
			Key:      key,
			SubKey:   subKey,
		}

		return ProducerMap2Slice(r.registrationMap[k])
	}

	producers := Producers{}
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

func (r *RegistrationDB) LookupRegistrations(id string) Registrations {
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
