package domain

import (
	"sync"
)

type Conf struct {
	mu         sync.Mutex
	Processed  int
	OpenPort   int
	MasterHost string
	MasterPort int
}

func (c *Conf) IncrementOffset(inc int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.Processed += inc
}

func (c *Conf) GetOffset() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.Processed
}

var Config Conf
