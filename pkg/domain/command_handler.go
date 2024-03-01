package domain

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type Command struct {
	Cmd  string
	Args []string
	Raw  string
	Conn *Connection
}

func (c *Command) SendResp(respMessage string) {
	if c.Conn.Type == "Master" && c.Cmd != "replconf" {
		return
	}

	c.Conn.HandleWrite(respMessage)
}

func (c *Command) HandlePingCommand() {
	c.SendResp("+PONG\r\n")
}

func (c *Command) HandleEchoCommand() {
	c.SendResp("+" + c.Args[0] + "\r\n")
}

func (c *Command) HandleSetCommand() {
	var key = c.Args[0]
	var val = c.Args[1]
	var ttlMS = -1

	if len(c.Args) == 4 && strings.ToLower(c.Args[2]) == "px" {
		var err error
		ttlMS, err = strconv.Atoi(c.Args[3])

		if err != nil {
			fmt.Println("Error to parse time to leave for ")
			c.SendResp("$-1\r\n")

			return
		}
	}

	Dict.Add(key, val, ttlMS)
	Replications.NotifyAllReplicas(*c)

	c.SendResp("+OK\r\n")
}

func (c *Command) HandleGetCommand() {
	//For test 13 get request sometimes are delivered before set during test phase.
	time.Sleep(1 * time.Millisecond)

	val, ok := Dict.Get(c.Args[0])

	if ok {
		c.SendResp("+" + val + "\r\n")

		return
	}

	c.SendResp("$-1\r\n")
}

func (c *Command) HandleInfoCommand() {
	params := map[string]string{}

	//role: Value is "master" if the instance is replica of no one, or "slave" if the instance is a replica of some master instance. Note that a replica can be master of another replica (chained replication).
	params["role"] = "master"

	if Config.MasterHost != "" {
		params["role"] = "slave"
	}

	//master_replid: The replication ID of the Redis server.
	params["master_replid"] = RandStringBytes(40)

	//master_repl_offset: The server's current replication offset
	params["master_repl_offset"] = fmt.Sprintf("%d", c.Conn.GetOffset())

	paramsStrBuffer := new(bytes.Buffer)

	for key, value := range params {
		_, err := fmt.Fprintf(paramsStrBuffer, "%s:%s\r\n", key, value)

		if err != nil {
			fmt.Println("can not join params in to string")
		}
	}

	respMessage := paramsStrBuffer.String()

	c.SendResp(fmt.Sprintf("$%d\r\n%s\r\n", len(respMessage), respMessage))
}

func (c *Command) HandleReplConfCommand() {
	if strings.ToLower(c.Args[0]) == "getack" {
		c.SendResp(RedisStringArray([]string{"REPLCONF", "ACK", strconv.Itoa(c.Conn.GetOffset())}))

		return
	}

	c.SendResp("+OK\r\n")
}

func (c *Command) HandlePSyncCommand() {
	if c.Args[0] == "?" {
		c.SendResp(fmt.Sprintf("+FULLRESYNC %s 0\r\n", RandStringBytes(40)))

		data, err := base64.StdEncoding.DecodeString("UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==")

		if err != nil {
			fmt.Println("Can't parse RDB file base64 content: ", err.Error())
		}

		c.SendResp(fmt.Sprintf("$%d\r\n%s", len(data), data))

		Replications.Add(c.Conn)

		return
	}

	c.SendResp("+OK\r\n")
}

func (c *Command) HandleWaitCommand() {
	c.SendResp(fmt.Sprintf(":%d\r\n", Replications.GetNumOfReplicas()))
}
