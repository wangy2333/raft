package storage

import "sync"

type Storage interface {
	Set(key string, value []byte)
	Get(key string) ([]byte, bool)
	HasDate() bool
}
type MapStorage struct {
	mut sync.Mutex
	m   map[string][]byte
}

func NewMapStorage() *MapStorage {
	return &MapStorage{
		m: make(map[string][]byte),
	}
}

//最后需要持久化还是需要刷到硬盘中成为文件。 但是当前这种也是可以使用的
func (ms *MapStorage) Set(key string, value []byte) {
	ms.mut.Lock()
	defer ms.mut.Unlock()
	ms.m[key] = value
}
func (ms *MapStorage) Get(key string) ([]byte, bool) {
	ms.mut.Lock()
	defer ms.mut.Unlock()
	bytes, found := ms.m[key]
	return bytes, found
}
func (ms *MapStorage) HasDate() bool {
	ms.mut.Lock()
	defer ms.mut.Unlock()
	if len(ms.m) > 0 {
		return true
	} else {
		return false
	}
}
