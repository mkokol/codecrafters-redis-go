package core

import (
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/pkg/domain"
	"net"
	"os"
	"strconv"
)

func SendHandShake() {
	if domain.Config.MasterHost == "" {
		fmt.Println("There is no master conf")

		return
	}

	masterAddr := fmt.Sprintf("%s:%d", domain.Config.MasterHost, domain.Config.MasterPort)
	conn, err := net.Dial("tcp", masterAddr)

	if err != nil {
		fmt.Println("Dial failed:", err.Error())

		os.Exit(1)
	}

	connection := domain.Connection{
		Net:  &conn,
		Type: "Master",
	}
	domain.Replications.Add(&connection)

	commands := []string{
		domain.RedisStringArray([]string{"PING"}),
		domain.RedisStringArray([]string{"REPLCONF", "listening-port", strconv.Itoa(domain.Config.OpenPort)}),
		domain.RedisStringArray([]string{"REPLCONF", "capa", "psync2"}),
		domain.RedisStringArray([]string{"PSYNC", "?", "-1"}),
	}

	go HandleClient(&connection)

	for _, commandMessage := range commands {
		connection.HandleWrite(commandMessage)
	}
}
