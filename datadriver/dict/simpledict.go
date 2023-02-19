package dict

type SimpleDict struct {
	dict map[string]any
}

func (d *SimpleDict) RandomKeys(limit int) []string {
	keys := make([]string, limit)
	for i := 0; i < limit; i++ {
		for k := range d.dict {
			keys[i] = k
			break
		}
	}
	return keys
}

func (d *SimpleDict) RandomDistinctKeys(limit int) []string {
	size := limit
	if size > len(d.dict) {
		size = len(d.dict)
	}
	s := NewSimpleDict(size)
	count := 0
	for k := range d.dict {
		if count == size {
			break
		}
		count += s.PutIfAbsent(k, nil)
	}
	keys := make([]string, 0)
	for key := range s.dict {
		keys = append(keys, key)
	}
	return keys
}

func (d *SimpleDict) Clear() {
	d.dict = make(map[string]any, len(d.dict))
}

func NewSimpleDict(size int) *SimpleDict {
	return &SimpleDict{
		dict: make(map[string]any, size),
	}
}
func (d *SimpleDict) Get(key string) (val any, exists bool) {
	val, ok := d.dict[key]
	return val, ok
}
func (d *SimpleDict) Len() int {
	return len(d.dict)
}
func (d *SimpleDict) Put(key string, val any) (result int) {
	_, existed := d.dict[key]
	d.dict[key] = val
	if existed {
		return 0
	}
	return 1
}
func (d *SimpleDict) PutIfAbsent(key string, val any) (result int) {
	if _, existed := d.dict[key]; existed {
		return 0
	}
	d.dict[key] = val
	return 1
}
func (d *SimpleDict) PutIfExists(key string, val any) (result int) {
	if _, existed := d.dict[key]; existed {
		d.dict[key] = val
		return 1
	}
	return 0
}
func (d *SimpleDict) Remove(key string) (val any, result int) {
	val, existed := d.dict[key]
	delete(d.dict, key)
	if existed {
		return val, 1
	}
	return nil, 0
}
func (d *SimpleDict) ForEach(consumer Consumer) {
	for key, val := range d.dict {
		if !consumer(key, val) {
			break
		}
	}
}
func (d *SimpleDict) Keys() []string {
	keys := make([]string, 0, len(d.dict))
	for key := range d.dict {
		keys = append(keys, key)
	}
	return keys
}
