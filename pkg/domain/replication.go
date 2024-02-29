package domain

import (
	"fmt"
	"net"
	"sync"
)

type Connection struct {
	Conn      *net.Conn
	ParsedLen int
	Type      string
}

type Replication struct {
	Connections map[string]*Connection
	mu          sync.Mutex
}

func (repl *Replication) Add(connection *Connection) {
	repl.mu.Lock()
	defer repl.mu.Unlock()

	replicaId := (*connection.Conn).RemoteAddr().String()

	if _, ok := repl.Connections[replicaId]; !ok {
		repl.Connections[replicaId] = connection
	}
}

func (repl *Replication) Remove(replicaId string) {
	repl.mu.Lock()
	defer repl.mu.Unlock()

	if _, ok := repl.Connections[replicaId]; !ok {
		delete(repl.Connections, replicaId)
	}
}

func (repl *Replication) NotifyAllReplicas(currentConnection Connection, command Command) {
	for replicaId, connection := range repl.Connections {
		if replicaId == (*currentConnection.Conn).RemoteAddr().String() {
			continue
		}

		_, err := (*connection.Conn).Write([]byte(command.Raw))

		if err != nil {
			fmt.Println(err.Error())
		}
	}
}
