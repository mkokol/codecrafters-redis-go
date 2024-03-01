package domain

import (
	"sync"
	"time"
)

type smartDict struct {
	Data map[string]string
	mu   sync.Mutex
}

func (sd *smartDict) Add(key string, val string, ttlMS int) {
	sd.mu.Lock()
	defer sd.mu.Unlock()

	sd.Data[key] = val

	if ttlMS == -1 {
		return
	}

	go func(dict map[string]string, key string, ttlMS int64) {
		time.Sleep(time.Duration(ttlMS) * time.Millisecond)

		sd.Remove(key)
	}(sd.Data, key, int64(ttlMS))
}

func (sd *smartDict) Get(key string) (string, bool) {
	sd.mu.Lock()
	defer sd.mu.Unlock()

	val, ok := sd.Data[key]

	return val, ok
}

func (sd *smartDict) Remove(key string) {
	sd.mu.Lock()
	defer sd.mu.Unlock()

	delete(sd.Data, key)
}

var Dict = smartDict{
	Data: map[string]string{},
}
