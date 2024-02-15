package main

import (
	"fmt"
	"net"
	"os"
	"strings"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	listener, err := net.Listen("tcp", "0.0.0.0:6379")

	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	for {
		conn, err := listener.Accept()

		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())

			continue
		}

		go handleClient(conn)
	}
}

func handleClient(conn net.Conn) {
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

		var message = string(buf[:n])
		var respMessage []byte

		if strings.Contains(strings.ToLower(message), "ping") {
			respMessage = []byte("+PONG\r\n")
		}

		if strings.Contains(strings.ToLower(message), "echo") {
			messageParts := strings.Split(message, "\r\n")
			text := messageParts[len(messageParts)-2]
			respMessage = []byte("+" + text + "\r\n")

		}

		if len(respMessage) > 0 {
			_, errWrite := conn.Write(respMessage)

			if errWrite != nil {
				fmt.Println("Writing Error", errWrite.Error())
			}
		}
	}
}
