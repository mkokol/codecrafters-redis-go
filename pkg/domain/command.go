package domain

import (
	"fmt"
	"strconv"
	"strings"
)

type Command struct {
	Cmd  string
	Args []string
	Raw  string
}

var digits = []byte{'1', '2', '3', '4', '5', '6', '7', '8', '9', '0'}

func ParsCommands(data []byte) []Command {
	message := string(data)
	var commands []Command
	i := 0

	for i < len(message) {
		var params []string
		start := i

		if message[i] == '*' {
			i++
			numOfCommands := parseNumber(&i, message)

			for numOfCommands > 0 {
				if message[i] == '$' {
					// skip data param character
					i++
					// read amount of data in param record
					numOfBytes := parseNumber(&i, message)
					params = append(params, message[i:i+numOfBytes])
					// jump over data that was pared
					i += numOfBytes
					// skip \r\n in the end of the line
					i += 2
				}
				numOfCommands--
			}
		} else if message[i] == '+' {
			for message[i] != '\n' {
				i += 1
			}
			i += 1
		} else if message[i] == '$' {
			// skip data param character
			i++
			// jump over data that was pared
			i += parseNumber(&i, message)
		}

		tmp := message[start:i]

		fmt.Println(">>> ||||||||||||||||||")
		fmt.Println(tmp)
		fmt.Println("<<< ||||||||||||||||||")

		if len(params) == 0 {
			continue
		}

		commands = append(
			commands,
			Command{
				Cmd:  strings.ToLower(params[0]),
				Args: params[1:],
				Raw:  message[start:i],
			},
		)
	}

	return commands
}

func RedisStringArray(s []string) string {
	var b strings.Builder

	b.WriteByte('*')
	b.WriteString(strconv.Itoa(len(s)))
	b.WriteString("\r\n")

	for _, v := range s {
		b.WriteByte('$')
		b.WriteString(strconv.Itoa(len(v)))
		b.WriteString("\r\n")
		b.WriteString(v)
		b.WriteString("\r\n")
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
