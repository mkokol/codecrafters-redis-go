package domain

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
)

type smartStream struct {
	Data  map[string][]StreamRecord
	Order []string
	mu    sync.Mutex
}

type StreamRecord struct {
	RecordId string
	Data     map[string]string
}

func (ss *smartStream) Add(key string, val StreamRecord) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	ss.Data[key] = append(ss.Data[key], val)
	ss.Order = append(ss.Order, val.RecordId)
}

func (ss *smartStream) Get(key string) ([]StreamRecord, bool) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	val, ok := ss.Data[key]

	return val, ok
}

func (ss *smartStream) BuildStreamId(currentStreamId string) string {
	if !strings.Contains(currentStreamId, "*") {
		return currentStreamId
	}

	if currentStreamId == "*" {
		return "TODO"
	}

	lastStreamId := "0-0"

	if len(ss.Order) > 0 {
		lastStreamId = ss.Order[len(ss.Order)-1]
	}

	lastStreamIdParts := strings.Split(lastStreamId, "-")
	currentStreamIddParts := strings.Split(currentStreamId, "-")

	if lastStreamIdParts[0] != currentStreamIddParts[0] {
		return currentStreamIddParts[0] + "-0"
	}

	lastID, err := strconv.Atoi(lastStreamIdParts[1])

	if err != nil {
		fmt.Println(err.Error())

		return ""
	}

	return currentStreamIddParts[0] + "-" + strconv.Itoa(lastID+1)
}

func (ss *smartStream) ValidateStreamId(currentStreamId string) bool {
	if currentStreamId == "*" {
		return true
	}

	lastStreamId := "0-0"

	if len(ss.Order) > 0 {
		lastStreamId = ss.Order[len(ss.Order)-1]
	}

	lastStreamIdParts := strings.Split(lastStreamId, "-")
	currentStreamIddParts := strings.Split(currentStreamId, "-")

	lastTS, err := strconv.Atoi(lastStreamIdParts[0])

	if err != nil {
		fmt.Println(err.Error())

		return false
	}

	currentTS, err := strconv.Atoi(currentStreamIddParts[0])

	if err != nil {
		fmt.Println(err.Error())

		return false
	}

	if currentTS < lastTS {
		return false
	}

	if currentStreamIddParts[1] == "*" {
		return true
	}

	lastID, err := strconv.Atoi(lastStreamIdParts[1])

	if err != nil {
		fmt.Println(err.Error())

		return false
	}

	currentID, err := strconv.Atoi(currentStreamIddParts[1])

	if err != nil {
		fmt.Println(err.Error())

		return false
	}

	if currentTS == lastTS && currentID <= lastID {
		return false
	}

	return true
}

var Stream = smartStream{
	Data: map[string][]StreamRecord{},
}
