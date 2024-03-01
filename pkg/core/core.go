package core

import (
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/pkg/domain"
	"net"
	"os"
	"strconv"
)

func ParseCliParams() domain.Conf {
	cmdParams := os.Args[1:]
	config := domain.Conf{
		OpenPort: 6379,
	}

	for i, val := range cmdParams {
		switch val {
		case "--port", "-port", "--p", "-p":
			if i+1 >= len(cmdParams) {
				fmt.Println("Failed to parse parameter:", val)

				os.Exit(1)
			}

			openPort, err := strconv.Atoi(cmdParams[i+1])

			if err != nil {
				fmt.Println("Failed to parse parameter:", val)

				os.Exit(1)
			}

			config.OpenPort = openPort
		case "--replicaof", "-replicaof", "--r", "-r":
			if i+2 >= len(cmdParams) {
				fmt.Println("Failed to parse parameter:", val)

				os.Exit(1)
			}

			masterHost := cmdParams[i+1]
			masterPort, err := strconv.Atoi(cmdParams[i+2])

			if err != nil {
				fmt.Println("Failed to parse parameter:", val)

				os.Exit(1)
			}

			config.MasterHost = masterHost
			config.MasterPort = masterPort
		}
	}

	return config
}

func HandleClient(
	connection *domain.Connection,
) {
	defer func(conn net.Conn) {
		err := conn.Close()
		if err != nil {
			fmt.Println("An error occurs during closing connection.")
		}
	}(*connection.Net)

	for {
		buf := make([]byte, 1024)
		n, errRead := (*connection.Net).Read(buf)

		if errRead != nil {
			fmt.Println("Reading Error", errRead.Error())

			break
		}

		for _, command := range domain.ParsCommands(buf[:n], connection) {
			HandleCommand(&command)
		}
	}
}

func HandleCommand(command *domain.Command) {
	switch command.Cmd {
	case "ping":
		command.HandlePingCommand()
	case "echo":
		command.HandleEchoCommand()
	case "set":
		command.HandleSetCommand()
	case "get":
		command.HandleGetCommand()
	case "info":
		command.HandleInfoCommand()
	case "replconf":
		command.HandleReplConfCommand()
	case "psync":
		command.HandlePSyncCommand()
	default:
		command.Conn.HandleWrite("*0\r\n")
	}

	command.Conn.IncreaseOffsetFor(len(command.Raw))
}
