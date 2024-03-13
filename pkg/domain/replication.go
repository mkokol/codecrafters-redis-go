package domain

import (
	"fmt"
	"sync"
)

type ReplSync struct {
	MsgType string
	ReplId  string
	Offset  int
}

type replication struct {
	Connections  map[string]*Connection
	InSyncOffset int
	Ch           chan ReplSync
	AckStat      map[string]int
	mu           sync.Mutex
}

func (r *replication) Add(connection *Connection) {
	r.mu.Lock()
	defer r.mu.Unlock()

	replicaId := (*connection.Net).RemoteAddr().String()

	if _, ok := r.Connections[replicaId]; !ok {
		r.Connections[replicaId] = connection
	}
}

func (r *replication) Remove(replicaId string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.Connections[replicaId]; !ok {
		delete(r.Connections, replicaId)
	}
}

func (r *replication) NotifyAllReplicas(message string) {
	r.InSyncOffset += len(message)

	for _, connection := range r.Connections {
		_, err := (*connection.Net).Write([]byte(message))

		if err != nil {
			fmt.Println(err.Error())
		}
	}
}

func (r *replication) InSyncReplicas() int {
	inSyncCount := 0

	for _, replOffset := range r.AckStat {
		if replOffset >= r.InSyncOffset {
			inSyncCount += 1
		}
	}

	return inSyncCount
}

var Replications = replication{
	Connections: map[string]*Connection{},
	Ch:          make(chan ReplSync, 256),
	AckStat:     map[string]int{},
}
