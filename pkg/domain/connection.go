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

func (c *Connection) Write(respMessage string) {
	_, errWrite := (*c.Net).Write([]byte(respMessage))

	if errWrite != nil {
		fmt.Println("Writing Error", errWrite.Error())

		Replications.Remove(
			(*c.Net).RemoteAddr().String(),
		)
	}
}

func (c *Connection) Read() (string, error) {
	buf := make([]byte, 1024)
	n, err := (*c.Net).Read(buf)

	if err != nil {
		fmt.Println("Reading Error", err.Error())

		return "", err
	}

	return string(buf[:n]), nil
}

func (c *Connection) GetReplId() string {
	return (*c.Net).RemoteAddr().String()
}

func (c *Connection) IncreaseOffsetFor(delta int) {
	c.offset += delta
}

func (c *Connection) GetOffset() int {
	return c.offset
}
