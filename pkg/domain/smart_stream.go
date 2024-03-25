package domain

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"
)

type StreamSearchRange struct {
	StartAtId string
	EndAtId   string
}

type StreamRecord struct {
	RecordId string
	Data     map[string]string
}

type streamOrder struct {
	IdsOrder  []int64
	IdsStruct map[int64][]int
}

func (so *streamOrder) append(recordId string) {
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
	sds.StreamOrder.append(val.RecordId)
}

type smartStream struct {
	DataSet map[string]*StreamDataSet
	Ch      chan string
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

func (ss *smartStream) GetStreamsRecords(streams map[string]StreamSearchRange) map[string][]StreamRecord {
	outData := map[string][]StreamRecord{}

	for key, searchRange := range streams {
		ds, ok := Stream.Get(key)

		if !ok {
			continue
		}

		startTS, startID := int64(0), 0
		if searchRange.StartAtId != "-" {
			startTS, startID = ParsStreamId(searchRange.StartAtId)
		}

		endTS, endID := int64(math.MaxInt64), math.MaxInt32
		if searchRange.EndAtId != "+" {
			endTS, endID = ParsStreamId(searchRange.EndAtId)
		}

		var streamData []StreamRecord

		for _, streamTS := range ds.StreamOrder.IdsOrder {
			if streamTS < startTS || streamTS > endTS {
				continue
			}

			for _, streamID := range ds.StreamOrder.IdsStruct[streamTS] {
				if streamTS == 0 && streamID == 0 {
					continue
				}

				if streamTS == startTS && streamID < startID {
					continue
				}

				if streamTS == endTS && streamID > endID {
					continue
				}

				streamId := fmt.Sprintf("%d-%d", streamTS, streamID)
				data := map[string]string{}

				for k, v := range ds.Data[streamId].Data {
					data[k] = v
				}

				record := StreamRecord{
					RecordId: streamId,
					Data:     data,
				}

				streamData = append(streamData, record)
			}
		}

		if len(streamData) > 0 {
			outData[key] = streamData
		}
	}

	return outData
}

var Stream = smartStream{
	DataSet: map[string]*StreamDataSet{},
	Ch:      make(chan string, 256),
}
