package core

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/pkg/domain"
	"strconv"
	"strings"
)

func HandleCommand(connection *domain.Connection, command domain.Command) {
	var respMessage string

	switch command.Cmd {
	case "ping":
		respMessage = "+PONG\r\n"
	case "echo":
		respMessage = "+" + command.Args[0] + "\r\n"
	case "set":
		respMessage = HandleSetCommand(command)
		Replications.NotifyAllReplicas(*connection, command)
	case "get":
		respMessage = HandleGetCommand(command)
	case "info":
		respMessage = HandleInfoCommand()
	case "replconf":
		respMessage = HandleReplConfCommand(command)
		connection.Type = "Replica"
		Replications.Add(connection)
	case "psync":
		respMessage = HandlePSyncCommand(command)
	default:
		respMessage = "*0\r\n"
	}

	_, errWrite := (*connection.Conn).Write([]byte(respMessage))

	if errWrite != nil {
		fmt.Println("Writing Error", errWrite.Error())

		Replications.Remove(
			(*connection.Conn).RemoteAddr().String(),
		)
	}

	if command.Cmd == "psync" {
		SendRDBFile(*connection)
	}

	if command.Cmd == "replconf" {
		fmt.Println("----------------------")
		fmt.Println((*connection.Conn).RemoteAddr().String())
		fmt.Println(strings.Replace(command.Raw, "\r\n", "__", -1))
		fmt.Println(strings.Replace(respMessage, "\r\n", "__", -1))
		fmt.Println("----------------------")
	}

	domain.Config.IncrementOffset(len(command.Raw))

	(*connection).ParsedLen += len(command.Raw)
}

func HandleSetCommand(command domain.Command) string {
	var key = command.Args[0]
	var val = command.Args[1]
	var ttlMS = -1
	var respMessage = "+OK\r\n"

	if len(command.Args) == 4 && strings.ToLower(command.Args[2]) == "px" {
		var err error
		ttlMS, err = strconv.Atoi(command.Args[3])

		if err != nil {
			fmt.Println("Error to parse time to leave for ")

			respMessage = "$-1\r\n"
		}
	}

	Dict.Add(key, val, ttlMS)

	return respMessage
}

func HandleGetCommand(command domain.Command) string {
	val, ok := Dict.Get(command.Args[0])

	if ok {
		return "+" + val + "\r\n"
	}

	return "$-1\r\n"
}

func HandleInfoCommand() string {
	params := map[string]string{}

	//role: Value is "master" if the instance is replica of no one, or "slave" if the instance is a replica of some master instance. Note that a replica can be master of another replica (chained replication).
	params["role"] = "master"

	if domain.Config.MasterHost != "" {
		params["role"] = "slave"
	}

	//master_replid: The replication ID of the Redis server.
	params["master_replid"] = RandStringBytes(40)

	//master_repl_offset: The server's current replication offset
	params["master_repl_offset"] = fmt.Sprintf("%d", domain.Config.GetOffset())

	paramsStrBuffer := new(bytes.Buffer)

	for key, value := range params {
		_, err := fmt.Fprintf(paramsStrBuffer, "%s:%s\r\n", key, value)

		if err != nil {
			fmt.Println("can not join params in to string")
		}
	}

	respMessage := paramsStrBuffer.String()

	return fmt.Sprintf("$%d\r\n%s\r\n", len(respMessage), respMessage)
}

func HandleReplConfCommand(command domain.Command) string {
	respMessage := "+OK\r\n"

	if strings.ToLower(command.Args[0]) == "getack" {
		respMessage = domain.RedisStringArray([]string{"REPLCONF", "ACK", strconv.Itoa(domain.Config.GetOffset())})
	}

	return respMessage
}

func HandlePSyncCommand(command domain.Command) string {
	respMessage := "+OK\r\n"

	if command.Args[0] == "?" {
		replicationId := RandStringBytes(40)
		respMessage = fmt.Sprintf("+FULLRESYNC %s 0\r\n", replicationId)
	}

	return respMessage
}

func SendRDBFile(connection domain.Connection) {
	data, err := base64.StdEncoding.DecodeString("UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==")

	if err != nil {
		fmt.Println("Can't parse RDB file base64 content: ", err.Error())
	}

	_, errWrite := (*connection.Conn).Write([]byte(fmt.Sprintf("$%d\r\n%s", len(data), data)))

	if errWrite != nil {
		fmt.Println("Writing Error", errWrite.Error())
	}
}
