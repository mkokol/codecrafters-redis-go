package main

import (
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/pkg/core"
	"net"
	"os"
	"strconv"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.

	config := core.ParseCliParams()
	listener, err := net.Listen("tcp", "0.0.0.0:"+strconv.Itoa(config.OpenPort))
	go core.HandleHandShake(config)

	if err != nil {
		fmt.Println("Failed to bind to port:", config.OpenPort)

		os.Exit(1)
	}

	fmt.Println("")

	dict := map[string]string{}

	for {
		conn, err := listener.Accept()

		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())

			continue
		}

		go core.HandleClient(conn, dict, config)
	}
}
