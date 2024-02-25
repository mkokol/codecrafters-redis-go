package core

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/pkg/domain"
	"net"
	"strconv"
	"strings"
)

var Dict = domain.SmartDict{
	Data: map[string]string{},
}

var Replications = domain.Replication{
	Replicas: map[string]net.Conn{},
}

func HandleClient(
	conn net.Conn,
	config domain.Conf,
) {
	defer func(conn net.Conn) {
		err := conn.Close()
		if err != nil {
			fmt.Println("An error occurs during closing connection.")
		}
	}(conn)

	for {
		buf := make([]byte, 1024)
		n, errRead := conn.Read(buf)

		if errRead != nil {
			fmt.Println("Reading Error", errRead.Error())

			break
		}
		HandleCommands(config, conn, buf[:n])
	}
}

func HandleCommands(config domain.Conf, conn net.Conn, data []byte) {
	for _, command := range splitCommands(data) {
		HandleCommand(config, conn, command)
	}
}

func HandleCommand(config domain.Conf, conn net.Conn, rawMessage string) {
	if rawMessage[0] == '+' {
		return
	}

	message := strings.Split(rawMessage, "\r\n")

	if len(message) < 3 {
		return
	}

	command := strings.ToLower(message[2])

	var respMessage string

	switch command {
	case "ping":
		respMessage = "+PONG\r\n"
	case "echo":
		respMessage = "+" + message[4] + "\r\n"
	case "set":
		respMessage = HandleSetCommand(message)
		Replications.NotifyAllReplicas(conn, rawMessage)
	case "get":
		respMessage = HandleGetCommand(message)
	case "info":
		respMessage = HandleInfoCommand(config)
	case "replconf":
		respMessage = HandleReplConfCommand(message)
		Replications.Add(conn)
	case "psync":
		respMessage = HandlePSyncCommand(message)
	default:
		respMessage = "*0"
	}

	_, errWrite := conn.Write([]byte(respMessage))

	if errWrite != nil {
		fmt.Println("Writing Error", errWrite.Error())

		Replications.Remove(conn)
	}

	if command == "psync" {
		go SendRDBFile(conn)
	}
}

func splitCommands(data []byte) []string {
	var newCommand []string
	var commands []string

	for _, letter := range string(data) {

		if len(newCommand) > 0 && (letter == '+' || letter == '*') {
			commands = append(commands, strings.Join(newCommand, ""))

			newCommand = []string{}
		}

		newCommand = append(newCommand, string(letter))
	}

	commands = append(commands, strings.Join(newCommand, ""))

	return commands
}

func HandleSetCommand(message []string) string {
	var key = message[4]
	var val = message[6]
	var ttlMS = -1
	var respMessage = "+OK"

	if len(message) == 12 && strings.ToLower(message[8]) == "px" {
		var err error
		ttlMS, err = strconv.Atoi(message[10])

		if err != nil {
			fmt.Println("Error to parse time to leave for ")

			respMessage = "$-1"
		}
	}

	Dict.Add(key, val, ttlMS)

	return respMessage + "\r\n"
}

func HandleGetCommand(message []string) string {
	val, ok := Dict.Get(message[4])

	if ok {
		return "+" + val + "\r\n"
	}

	return "$-1\r\n"
}

func HandleInfoCommand(config domain.Conf) string {
	params := map[string]string{}

	//role: Value is "master" if the instance is replica of no one, or "slave" if the instance is a replica of some master instance. Note that a replica can be master of another replica (chained replication).
	params["role"] = "master"

	if config.MasterHost != "" {
		params["role"] = "slave"
	}

	//master_replid: The replication ID of the Redis server.
	params["master_replid"] = RandStringBytes(40)

	//master_repl_offset: The server's current replication offset
	params["master_repl_offset"] = "0"

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

func HandleReplConfCommand(message []string) string {
	respMessage := "+OK\r\n"

	if strings.ToLower(message[4]) == "getack" {
		respMessage = "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n0\r\n"
	}

	return respMessage
}

func HandlePSyncCommand(message []string) string {
	respMessage := "+OK\r\n"

	if message[4] == "?" {
		replicationId := RandStringBytes(40)
		respMessage = fmt.Sprintf("+FULLRESYNC %s 0\r\n", replicationId)
	}

	return respMessage
}

func SendRDBFile(conn net.Conn) {
	data, err := base64.StdEncoding.DecodeString("UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==")

	if err != nil {
		fmt.Println("Can't parse RDB file base64 content: ", err.Error())
	}

	_, errWrite := conn.Write([]byte(fmt.Sprintf("$%d\r\n%s", len(data), data)))

	if errWrite != nil {
		fmt.Println("Writing Error", errWrite.Error())
	}
}
