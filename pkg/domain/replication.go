package domain

import (
	"fmt"
	"net"
	"sync"
)

type Replication struct {
	Replicas map[string]net.Conn
	Mu       sync.Mutex
}

func (repl *Replication) Add(conn net.Conn) {
	replicaId := conn.RemoteAddr().String()

	if _, ok := repl.Replicas[replicaId]; !ok {
		repl.Mu.Lock()
		repl.Replicas[replicaId] = conn
		repl.Mu.Unlock()
	}
}

func (repl *Replication) Remove(conn net.Conn) {
	replicaId := conn.RemoteAddr().String()

	if _, ok := repl.Replicas[replicaId]; !ok {
		repl.Mu.Lock()
		delete(repl.Replicas, replicaId)
		repl.Mu.Unlock()
	}
}

func (repl *Replication) NotifyAllReplicas(currentConn net.Conn, message string) {
	for replicaId, replica := range repl.Replicas {
		if replicaId == currentConn.RemoteAddr().String() {
			continue
		}

		_, err := replica.Write([]byte(message))

		if err != nil {
			fmt.Println(err.Error())
		}
	}
}
