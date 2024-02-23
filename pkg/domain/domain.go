package domain

import "net"

type Conf struct {
	OpenPort   int
	MasterHost string
	MasterPort int
}

var Dict = map[string]string{}

var Replicas = map[string]net.Conn{}
