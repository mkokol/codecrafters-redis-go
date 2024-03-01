package domain

type Conf struct {
	OpenPort   int
	MasterHost string
	MasterPort int
}

var Config Conf
