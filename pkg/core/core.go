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

		case "--dir", "-dir":
			if i+1 >= len(cmdParams) {
				fmt.Println("Failed to parse parameter:", val)

				os.Exit(1)
			}

			config.RdbDir = cmdParams[i+1]

		case "--dbfilename", "-dbfilename":
			if i+1 >= len(cmdParams) {
				fmt.Println("Failed to parse parameter:", val)

				os.Exit(1)
			}

			config.RdbFileName = cmdParams[i+1]
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
		message, err := (*connection).Read()

		if err != nil {
			break
		}

		for _, command := range domain.ParsCommands(message, connection) {
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
	case "keys":
		command.HandleKeysCommand()
	case "info":
		command.HandleInfoCommand()
	case "replconf":
		command.HandleReplConfCommand()
	case "psync":
		command.HandlePSyncCommand()
	case "wait":
		command.HandleWaitCommand()
	case "config":
		command.HandleConfigCommand()
	default:
		fmt.Println("Handle Command:", command.Cmd)

		command.Conn.Write("*0\r\n")
	}

	command.Conn.IncreaseOffsetFor(len(command.Raw))
}
