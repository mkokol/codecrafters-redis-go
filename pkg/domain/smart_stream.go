package domain

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

type StreamRecord struct {
	RecordId string
	Data     map[string]string
}

type streamOrder struct {
	IdsOrder  []int64
	IdsStruct map[int64][]int
}

func (so *streamOrder) Append(recordId string) {
	streamTS, streamID := ParsStreamId(recordId)

	if len(so.IdsOrder) == 0 || so.IdsOrder[len(so.IdsOrder)-1] != streamTS {
		so.IdsOrder = append(so.IdsOrder, streamTS)
	}

	so.IdsStruct[streamTS] = append(so.IdsStruct[streamTS], streamID)
}

type StreamDataSet struct {
	Data        map[string]StreamRecord
	StreamOrder streamOrder
}

func (sds *StreamDataSet) Add(val StreamRecord) {
	sds.Data[val.RecordId] = val
	sds.StreamOrder.Append(val.RecordId)
}

type smartStream struct {
	DataSet map[string]*StreamDataSet
	mu      sync.Mutex
}

func (ss *smartStream) Add(key string, val StreamRecord) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	if _, ok := ss.DataSet[key]; !ok {
		ss.DataSet[key] = &StreamDataSet{
			Data: map[string]StreamRecord{},
			StreamOrder: streamOrder{
				IdsOrder:  []int64{0},
				IdsStruct: map[int64][]int{0: {0}},
			},
		}
	}

	ss.DataSet[key].Add(val)
}

func (ss *smartStream) Get(key string) (*StreamDataSet, bool) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	val, ok := ss.DataSet[key]

	return val, ok
}

func (ss *smartStream) BuildStreamId(streamKey string, currentStreamId string) string {
	if !strings.Contains(currentStreamId, "*") {
		return currentStreamId
	}

	if currentStreamId == "*" {
		return fmt.Sprintf("%d-0", time.Now().UnixMilli())
	}

	lastTS := int64(0)
	lastID := 0
	ds, ok := ss.Get(streamKey)

	if ok {
		idsOrder := ds.StreamOrder.IdsOrder
		lastTS = idsOrder[len(idsOrder)-1]

		idsStruct := ds.StreamOrder.IdsStruct
		lastID = idsStruct[lastTS][len(idsStruct[lastTS])-1]
	}

	currentTS, _ := ParsStreamId(currentStreamId)

	if lastTS != currentTS {
		return fmt.Sprintf("%d-0", currentTS)
	}

	return fmt.Sprintf("%d-%d", currentTS, lastID+1)
}

func (ss *smartStream) ValidateStreamId(streamKey string, currentStreamId string) bool {
	if currentStreamId == "*" {
		return true
	}

	ds, ok := ss.Get(streamKey)

	if !ok {
		return true
	}

	idsOrder := ds.StreamOrder.IdsOrder
	lastTS := idsOrder[len(idsOrder)-1]

	idsStruct := ds.StreamOrder.IdsStruct
	lastID := idsStruct[lastTS][len(idsStruct[lastTS])-1]

	currentTS, currentID := ParsStreamId(currentStreamId)

	if currentTS < lastTS {
		return false
	}

	if currentID == -1 {
		return true
	}

	if currentTS == lastTS && currentID <= lastID {
		return false
	}

	return true
}

func ParsStreamId(streamId string) (int64, int) {
	streamIdParts := strings.Split(streamId, "-")
	ts, err := strconv.ParseInt(streamIdParts[0], 10, 64)

	if err != nil {
		return -1, -1
	}

	id, err := strconv.Atoi(streamIdParts[1])

	if err != nil {
		return ts, -1
	}

	return ts, id
}

var Stream = smartStream{
	DataSet: map[string]*StreamDataSet{},
}
