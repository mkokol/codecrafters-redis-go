package main

import (
	"bytes"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type Conf struct {
	openPort   int
	masterHost string
	masterPort int
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.

	config := parseCliParams()
	listener, err := net.Listen("tcp", "0.0.0.0:"+strconv.Itoa(config.openPort))
	go handleHandShake(config)

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

		go handleClient(conn, dict, config)
	}
}

func parseCliParams() Conf {
	cmdParams := os.Args[1:]
	config := Conf{
		openPort: 6379,
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

			config.openPort = openPort
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

			config.masterHost = masterHost
			config.masterPort = masterPort
		}
	}

	return config
}

func handleHandShake(config Conf) {
	if config.masterHost == "" {
		return
	}

	masterAddr := fmt.Sprintf("%s:%d", config.masterHost, config.masterPort)
	connClient, err := net.Dial("tcp", masterAddr)

	if err != nil {
		fmt.Println("Dial failed:", err.Error())

		os.Exit(1)
	}

	_, err = connClient.Write([]byte("*1\r\n$4\r\nping\r\n"))

	if err != nil {
		fmt.Println("Write to server failed:", err.Error())

		os.Exit(1)
	}

	reply := make([]byte, 1024)

	_, err = connClient.Read(reply)

	if err != nil {
		fmt.Println("Write to server failed:", err.Error())

		os.Exit(1)
	}

	fmt.Println("reply from server=", string(reply))

	err = connClient.Close()

	if err != nil {
		fmt.Println("Can't close the connection:", err.Error())

		os.Exit(1)
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

func HandleInfoCommand(config Conf) string {
	params := map[string]string{}

	//role: Value is "master" if the instance is replica of no one, or "slave" if the instance is a replica of some master instance. Note that a replica can be master of another replica (chained replication).
	params["role"] = "master"

	if config.masterHost != "" {
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
