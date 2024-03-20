package domain

import (
	"sync"
	"time"
)

type smartDict struct {
	Data   map[string]string
	Stream map[string]StreamRecord
	mu     sync.Mutex
}

type StreamRecord struct {
	RecordId string
	Data     map[string]string
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

func (sd *smartDict) Size() int {
	sd.mu.Lock()
	defer sd.mu.Unlock()

	return len(sd.Data)
}

func (sd *smartDict) XAdd(key string, val StreamRecord) {
	sd.mu.Lock()
	defer sd.mu.Unlock()

	sd.Stream[key] = val
}

func (sd *smartDict) XGet(key string) (StreamRecord, bool) {
	sd.mu.Lock()
	defer sd.mu.Unlock()

	val, ok := sd.Stream[key]

	return val, ok
}

var Dict = smartDict{
	Data:   map[string]string{},
	Stream: map[string]StreamRecord{},
}
