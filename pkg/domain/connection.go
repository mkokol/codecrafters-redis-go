package domain

import (
	"fmt"
	"net"
)

type Connection struct {
	Net    *net.Conn
	Type   string
	offset int
}

func (c *Connection) HandleWrite(respMessage string) {

	_, errWrite := (*c.Net).Write([]byte(respMessage))

	if errWrite != nil {
		fmt.Println("Writing Error", errWrite.Error())

		Replications.Remove(
			(*c.Net).RemoteAddr().String(),
		)
	}
}

func (c *Connection) IncreaseOffsetFor(delta int) {
	c.offset += delta
}

func (c *Connection) GetOffset() int {
	return c.offset
}
