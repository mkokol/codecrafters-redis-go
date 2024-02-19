package core

import (
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/pkg/domain"
	"math/rand"
	"os"
	"strconv"
)

const letterBytes = "0123456789abcdefghijklmnopqrstuvwxyz"

func RandStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

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
		}
	}

	return config
}
