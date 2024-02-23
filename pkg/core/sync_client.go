package core

import (
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/pkg/domain"
	"net"
	"os"
	"strings"
)

var connClientObj *net.Conn

func getClient(config domain.Conf) net.Conn {
	if connClientObj != nil {
		return *connClientObj
	}

	masterAddr := fmt.Sprintf("%s:%d", config.MasterHost, config.MasterPort)
	connClient, err := net.Dial("tcp", masterAddr)

	if err != nil {
		fmt.Println("Dial failed:", err.Error())

		os.Exit(1)
	}

	connClientObj = &connClient

	return *connClientObj
}

func sendCommand(connClient net.Conn, command string) {
	_, err := connClient.Write(
		[]byte(command),
	)

	if err != nil {
		fmt.Println("Write to server failed:", err.Error())

		os.Exit(1)
	}
}

func SendHandShake(config domain.Conf) {
	if config.MasterHost == "" {
		fmt.Println("There is no master conf")

		return
	}

	connClient := getClient(config)

	commands := []string{
		"*1\r\n$4\r\nping\r\n",
		fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n%d\r\n", config.OpenPort),
		"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n",
		"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n",
	}

	for _, command := range commands {
		sendCommand(connClient, command)
	}

	replicaId := connClient.RemoteAddr().String()

	if _, ok := domain.Replicas[replicaId]; !ok {
		domain.Replicas[replicaId] = connClient
	}

	for {
		buf := make([]byte, 256)
		n, errRead := connClient.Read(buf)

		if errRead != nil {
			fmt.Println("Error for getting data from master:", errRead.Error())

			break
		}

		rawMessage := string(buf[:n])
		message := strings.Split(rawMessage, "\r\n")
		resp := strings.ToLower(message[0])

		if resp != "+pong" && resp != "+ok" {
			HandleCommands(config, connClient, buf[:n])
		}
	}
}
