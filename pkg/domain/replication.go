package domain

import (
	"fmt"
	"sync"
)

type replication struct {
	Connections map[string]*Connection
	mu          sync.Mutex
}

func (repl *replication) Add(connection *Connection) {
	repl.mu.Lock()
	defer repl.mu.Unlock()

	replicaId := (*connection.Net).RemoteAddr().String()

	if _, ok := repl.Connections[replicaId]; !ok {
		repl.Connections[replicaId] = connection
	}
}

func (repl *replication) Remove(replicaId string) {
	repl.mu.Lock()
	defer repl.mu.Unlock()

	if _, ok := repl.Connections[replicaId]; !ok {
		delete(repl.Connections, replicaId)
	}
}

func (repl *replication) NotifyAllReplicas(command Command) {
	for replicaId, connection := range repl.Connections {
		if replicaId == (*command.Conn.Net).RemoteAddr().String() {
			continue
		}

		_, err := (*connection.Net).Write([]byte(command.Raw))

		if err != nil {
			fmt.Println(err.Error())
		}
	}
}

func (repl *replication) GetNumOfReplicas() int {
	return len(repl.Connections)
}

var Replications = replication{
	Connections: map[string]*Connection{},
}
