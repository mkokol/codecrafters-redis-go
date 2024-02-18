package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	openPort := flag.Int("port", 6379, "Port on which application would be lunched.")
	flag.Parse()

	listener, err := net.Listen("tcp", "0.0.0.0:"+strconv.Itoa(*openPort))

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

		go handleClient(conn, dict)
	}
}

func handleClient(conn net.Conn, dict map[string]string) {
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
