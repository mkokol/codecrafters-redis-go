package domain

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
)

var digits = []byte{'1', '2', '3', '4', '5', '6', '7', '8', '9', '0'}

const letterBytes = "0123456789abcdefghijklmnopqrstuvwxyz"

func RandStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func RedisStringArray(s []string) string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("*%d\r\n", len(s)))

	for _, v := range s {
		b.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(v), v))
	}

	return b.String()
}

func parseNumber(i *int, message string) int {
	var commandLen []byte

	for contains(digits, message[*i]) {
		commandLen = append(commandLen, message[*i])
		// jump to next character
		*i++
	}

	numOfCommands, err := strconv.Atoi(string(commandLen))

	if err != nil {
		fmt.Println("Can not parse number in command", err.Error())
	}

	// skip \r\n in the end of the line
	*i += 2

	return numOfCommands
}

func contains(list []byte, character byte) bool {
	for _, val := range list {
		if val == character {
			return true
		}
	}

	return false
}
