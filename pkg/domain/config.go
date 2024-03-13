package domain

type Conf struct {
	OpenPort    int
	MasterHost  string
	MasterPort  int
	RdbDir      string
	RdbFileName string
}

var Config Conf
