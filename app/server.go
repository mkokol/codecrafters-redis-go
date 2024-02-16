package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	listener, err := net.Listen("tcp", "0.0.0.0:6379")

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
		buf := make([]byte, 1024)
		n, errRead := conn.Read(buf)

		if errRead != nil {
			fmt.Println("Reading Error", errRead.Error())

			break
		}

		message := strings.Split(string(buf[:n]), "\r\n")
		command := strings.ToLower(message[2])

		fmt.Println("---")
		fmt.Println(message)
		fmt.Println(len(message))
		fmt.Println("---")

		var respMessage string

		switch command {
		case "ping":
			respMessage = "+PONG"
		case "echo":
			respMessage = "+" + message[4]
		case "set":
			key := message[4]
			dict[message[4]] = message[6]
			respMessage = "+OK"

			if len(message) == 12 && strings.ToLower(message[8]) == "px" {
				ttlMS, err := strconv.Atoi(message[10])

				if err != nil {
					fmt.Println("Error to parse time to leave for ")

					delete(dict, key)
					respMessage = "$-1"
				} else {
					go waitThenDelete(dict, key, int64(ttlMS))
				}
			}
		case "get":
			respMessage = "$-1"
			val, ok := dict[message[4]]

			if ok {
				respMessage = "+" + val
			}
		default:
			respMessage = "*0"
		}

		_, errWrite := conn.Write([]byte(fmt.Sprintf("%v\r\n", respMessage)))

		if errWrite != nil {
			fmt.Println("Writing Error", errWrite.Error())
		}
	}
}

func waitThenDelete(dict map[string]string, key string, ttlMS int64) {
	time.Sleep(time.Duration(ttlMS) * time.Millisecond)

	delete(dict, key)
}
