package main

import (
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/pkg/core"
	"net"
	"os"
	"strconv"
)

func main() {
	config := core.ParseCliParams()
	listenOn := "0.0.0.0:" + strconv.Itoa(config.OpenPort)
	listener, err := net.Listen("tcp", listenOn)
	core.SendHandShake(config)

	if err != nil {
		fmt.Println("Failed to bind to port:", config.OpenPort)

		os.Exit(1)
	}

	fmt.Println("Listen on: ", listenOn)

	for {
		conn, err := listener.Accept()

		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())

			continue
		}

		go core.HandleClient(conn, config)
	}
}
