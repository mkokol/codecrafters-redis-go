package core

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/pkg/domain"
	"net"
	"strconv"
	"strings"
	"time"
)

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
		buf := make([]byte, 256)
		n, errRead := conn.Read(buf)

		if errRead != nil {
			fmt.Println("Reading Error", errRead.Error())

			break
		}
		rawMessage := string(buf[:n])
		message := strings.Split(rawMessage, "\r\n")
		command := strings.ToLower(message[2])

		var respMessage string

		switch command {
		case "ping":
			respMessage = "+PONG"
		case "echo":
			respMessage = "+" + message[4]
		case "set":
			respMessage = HandleSetCommand(message, domain.Dict)

			for _, replica := range domain.Replicas {
				_, err := replica.Write([]byte(rawMessage))

				if err != nil {
					fmt.Println(err.Error())
				}
			}
		case "get":
			respMessage = HandleGetCommand(message, domain.Dict)
		case "info":
			respMessage = HandleInfoCommand(config)
		case "replconf":
			respMessage = "+OK"

			domain.Replicas[len(domain.Replicas)] = conn
		case "psync":

			respMessage = HandlePSyncCommand(message)
		default:
			respMessage = "*0"
		}

		_, errWrite := conn.Write([]byte(fmt.Sprintf("%v\r\n", respMessage)))

		if errWrite != nil {
			fmt.Println("Writing Error", errWrite.Error())
		}

		if command == "psync" {
			SendRDBFile(conn)
		}
	}
}

func HandleSetCommand(message []string, dict map[string]string) string {
	key := message[4]
	dict[message[4]] = message[6]
	var respMessage = "+OK"

	if len(message) == 12 && strings.ToLower(message[8]) == "px" {
		ttlMS, err := strconv.Atoi(message[10])

		if err != nil {
			fmt.Println("Error to parse time to leave for ")

			delete(dict, key)
			respMessage = "$-1"
		} else {
			go func(dict map[string]string, key string, ttlMS int64) {
				time.Sleep(time.Duration(ttlMS) * time.Millisecond)

				delete(dict, key)
			}(dict, key, int64(ttlMS))
		}
	}

	return respMessage
}

func HandleGetCommand(message []string, dict map[string]string) string {
	var respMessage = "$-1"
	val, ok := dict[message[4]]

	if ok {
		respMessage = "+" + val
	}

	return respMessage
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

	return fmt.Sprintf("$%d\r\n%s", len(respMessage), respMessage)
}

func HandlePSyncCommand(message []string) string {
	respMessage := "+OK"

	if message[4] == "?" {
		replicationId := RandStringBytes(40)
		respMessage = fmt.Sprintf("+FULLRESYNC %s 0", replicationId)
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
