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

	c.Conn.Write(respMessage)
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
	Replications.NotifyAllReplicas(c.Raw)

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

func (c *Command) HandleKeysCommand() {
	keys := make([]string, Dict.Size())
	i := 0

	for key, _ := range Dict.Data {
		keys[i] = key
		i++
	}

	c.SendResp(RedisStringArray(keys))
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
	action := strings.ToLower(c.Args[0])
	if action == "getack" {
		c.SendResp(RedisStringArray([]string{"REPLCONF", "ACK", strconv.Itoa(c.Conn.GetOffset())}))

		return
	} else if action == "ack" {
		offset, err := strconv.Atoi(c.Args[1])

		if err != nil {
			fmt.Println(err.Error())
		}

		Replications.Ch <- ReplSync{
			MsgType: action,
			ReplId:  (*c.Conn).GetReplId(),
			Offset:  offset,
		}

		return
	}

	c.SendResp("+OK\r\n")
}

func (c *Command) HandlePSyncCommand() {
	c.SendResp(fmt.Sprintf("+FULLRESYNC %s 0\r\n", RandStringBytes(40)))

	data, err := base64.StdEncoding.DecodeString("UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==")

	if err != nil {
		fmt.Println("Can't parse RDB file base64 content: ", err.Error())
	}

	c.SendResp(fmt.Sprintf("$%d\r\n%s", len(data), data))

	Replications.Add(c.Conn)
}

func (c *Command) HandleWaitCommand() {
	replCount, errCount := strconv.Atoi(c.Args[0])

	if errCount != nil {
		fmt.Println(errCount.Error())

		return
	}

	replTimeOut, errTimeOut := strconv.Atoi(c.Args[1])

	if errTimeOut != nil {
		fmt.Println(errTimeOut.Error())

		return
	}

	if Replications.InSyncOffset == 0 {
		c.SendResp(fmt.Sprintf(":%d\r\n", len(Replications.Connections)))

		return
	}

	Replications.NotifyAllReplicas(
		RedisStringArray([]string{"REPLCONF", "GETACK", "*"}),
	)

	timeout := time.NewTimer(time.Duration(replTimeOut) * time.Millisecond)
	defer timeout.Stop()

	go func() {
		_ = <-timeout.C

		Replications.Ch <- ReplSync{
			MsgType: "time_out",
			Offset:  replTimeOut,
		}
	}()

	for {
		replSync := <-Replications.Ch

		if replSync.MsgType == "ack" {
			Replications.AckStat[replSync.ReplId] = replSync.Offset
		}

		if replSync.MsgType == "time_out" || Replications.InSyncReplicas() >= replCount {
			break
		}
	}

	c.SendResp(fmt.Sprintf(":%d\r\n", Replications.InSyncReplicas()))
}

func (c *Command) HandleConfigCommand() {
	action := strings.ToLower(c.Args[1])

	switch action {
	case "dir":
		c.SendResp(RedisStringArray([]string{"dir", Config.RdbDir}))
	case "dbfilename":
		c.SendResp(RedisStringArray([]string{"dbfilename", Config.RdbFileName}))
	}
}

func (c *Command) HandleTypeCommand() {
	valType := "none"

	if _, ok := Dict.Get(c.Args[0]); ok {
		valType = "string"
	}

	if _, ok := Stream.Get(c.Args[0]); ok {
		valType = "stream"
	}

	c.SendResp(fmt.Sprintf("+%s\r\n", valType))
}

func (c *Command) HandleXAddCommand() {
	key := c.Args[0]
	streamId := c.Args[1]

	if !Stream.ValidateStreamId(key, streamId) {
		errorMessage := "ERR The ID specified in XADD is equal or smaller than the target stream top item"

		if streamId == "0-0" {
			errorMessage = "ERR The ID specified in XADD must be greater than 0-0"
		}

		c.SendResp(fmt.Sprintf("-%s\r\n", errorMessage))

		return
	}

	streamId = Stream.BuildStreamId(key, streamId)

	data := map[string]string{}
	i := 2

	for i < len(c.Args) {
		data[c.Args[i]] = c.Args[i+1]
		i += 2
	}

	record := StreamRecord{
		RecordId: streamId,
		Data:     data,
	}

	Stream.Add(key, record)

	c.SendResp(fmt.Sprintf("$%d\r\n%s\r\n", len(streamId), streamId))
}

func (c *Command) HandleXRangeCommand() {
	key := c.Args[0]
	ds, ok := Stream.Get(key)
	startTS, startID := ParsStreamId(c.Args[1])
	endTS, endID := ParsStreamId(c.Args[2])

	if !ok {
		return
	}

	out := []string{}

	for _, streamTS := range ds.StreamOrder.IdsOrder {
		if streamTS < startTS || streamTS > endTS {
			continue
		}

		for _, streamID := range ds.StreamOrder.IdsStruct[streamTS] {
			if (streamTS == 0 && streamID == 0) || streamID < startID || streamID > endID {
				continue
			}

			streamId := fmt.Sprintf("%d-%d", streamTS, streamID)
			data := []string{}

			for k, v := range ds.Data[streamId].Data {
				data = append(data, k, v)
			}

			out = append(
				out,
				fmt.Sprintf(
					"*2\r\n%s\r\n%s",
					fmt.Sprintf("$%d\r\n%s", len(streamId), streamId),
					RedisStringArray(data),
				),
			)
		}
	}

	c.SendResp(
		fmt.Sprintf(
			"*%d\r\n%s",
			len(out),
			strings.Join(out, ""),
		),
	)
}
