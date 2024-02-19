package main

import (
	"bytes"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type Conf struct {
	openPort  int
	replicaOf string
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	openPortParam := flag.Int("port", 6379, "Port on which application would be lunched.")
	replicaOfParam := flag.String("replicaof", "", "Host and Port of the master server.")
	flag.Parse()

	c := Conf{
		openPort:  *openPortParam,
		replicaOf: *replicaOfParam,
	}

	listener, err := net.Listen("tcp", "0.0.0.0:"+strconv.Itoa(c.openPort))

	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	dict := map[string]string{}

	for {
		conn, err := listener.Accept()

		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())

			continue
		}

		go handleClient(conn, dict, c)
	}
}

func handleClient(conn net.Conn, dict map[string]string, c Conf) {
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

		message := strings.Split(string(buf[:n]), "\r\n")
		command := strings.ToLower(message[2])

		var respMessage string

		switch command {
		case "ping":
			respMessage = "+PONG"
		case "echo":
			respMessage = "+" + message[4]
		case "set":
			respMessage = HandleSetCommand(message, dict)
		case "get":
			respMessage = HandleGetCommand(message, dict)
		case "info":
			respMessage = HandleInfoCommand(c)
		default:
			respMessage = "*0"
		}

		_, errWrite := conn.Write([]byte(fmt.Sprintf("%v\r\n", respMessage)))

		if errWrite != nil {
			fmt.Println("Writing Error", errWrite.Error())
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

func HandleInfoCommand(c Conf) string {
	params := map[string]string{}

	//role: Value is "master" if the instance is replica of no one, or "slave" if the instance is a replica of some master instance. Note that a replica can be master of another replica (chained replication).
	params["role"] = "master"

	if c.replicaOf != "" {
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

const letterBytes = "0123456789abcdefghijklmnopqrstuvwxyz"

func RandStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}
