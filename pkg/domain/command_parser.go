package domain

import (
	"fmt"
	"strings"
)

func ParsCommands(message string, conn *Connection) []Command {
	var commands []Command
	i := 0

	for i < len(message) {
		var params []string
		start := i

		switch message[i] {
		case '*':
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
		case '+':
			for message[i] != '\n' {
				i += 1
			}
			i += 1
		case '$':
			// skip data param character
			i++
			// jump over data that was pared
			i += parseNumber(&i, message)
		default:
			fmt.Println(">>> I have missed something:", message[i])
		}

		if len(params) == 0 {
			continue
		}

		commands = append(
			commands,
			Command{
				Cmd:  strings.ToLower(params[0]),
				Args: params[1:],
				Raw:  message[start:i],
				Conn: conn,
			},
		)
	}

	return commands
}
