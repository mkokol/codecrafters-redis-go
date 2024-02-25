package domain

import (
	"sync"
	"time"
)

type SmartDict struct {
	Data map[string]string
	Mu   sync.Mutex
}

func (sd *SmartDict) Add(key string, val string, ttlMS int) {
	sd.Mu.Lock()
	sd.Data[key] = val
	sd.Mu.Unlock()

	if ttlMS == -1 {
		return
	}

	go func(dict map[string]string, key string, ttlMS int64) {
		time.Sleep(time.Duration(ttlMS) * time.Millisecond)

		sd.Remove(key)
	}(sd.Data, key, int64(ttlMS))
}

func (sd *SmartDict) Get(key string) (string, bool) {
	sd.Mu.Lock()
	val, ok := sd.Data[key]
	sd.Mu.Unlock()

	return val, ok
}

func (sd *SmartDict) Remove(key string) {
	sd.Mu.Lock()
	delete(sd.Data, key)
	sd.Mu.Unlock()
}
