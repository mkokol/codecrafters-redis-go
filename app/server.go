package main

import (
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/pkg/core"
	"github.com/codecrafters-io/redis-starter-go/pkg/domain"
	"net"
	"os"
	"strconv"
)

func main() {
	domain.Config = core.ParseCliParams()
	listenOn := "0.0.0.0:" + strconv.Itoa(domain.Config.OpenPort)
	listener, err := net.Listen("tcp", listenOn)

	core.SendHandShake()

	if err != nil {
		fmt.Println("Failed to bind to port:", domain.Config.OpenPort)

		os.Exit(1)
	}

	fmt.Println("Listen on: ", listenOn)

	for {
		conn, err := listener.Accept()

		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())

			continue
		}

		go core.HandleClient(&domain.Connection{
			Net:  &conn,
			Type: "User",
		})
	}
}
