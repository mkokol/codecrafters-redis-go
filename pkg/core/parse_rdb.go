package core

import (
	"encoding/binary"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/pkg/domain"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	RESIZE_DB = 0xFB
	MS_EXPIRY = 0xFC
)

func ParseRdbFile() {
	conf := domain.Config

	if conf.RdbDir == "" || conf.RdbFileName == "" {
		return
	}

	fileData, err := readFile(conf.RdbDir, conf.RdbFileName)

	if err != nil {
		fmt.Println("Unable to read file:", err)

		return
	}

	resideDbPosition := strings.IndexByte(fileData, RESIZE_DB)

	size := int(fileData[resideDbPosition+1])
	// 4 is the number of bytes between the fb op and the len of the first key
	start := resideDbPosition + 4

	for i := 0; i < size; i++ {
		var ttl int64 = -1

		if fileData[start-1] == MS_EXPIRY {
			ttlData := []byte(fileData[start : start+8])
			expiryMs := binary.LittleEndian.Uint64(ttlData)
			expireTime := time.UnixMilli(int64(expiryMs)).UnixMilli()
			ttl = expireTime - time.Now().UnixMilli()
			start += 9
		}

		keyLen := int(fileData[start])

		key := fileData[start+1 : start+keyLen+1]

		start += keyLen + 1
		valLen := int(fileData[start])
		val := fileData[start+1 : start+valLen+1]
		start += valLen + 2

		if ttl < -1 {
			continue
		}

		domain.Dict.Add(key, val, int(ttl))
	}
}

func readFile(rdbDir string, rdbFileName string) (string, error) {
	fPointer, err := os.Open(
		filepath.Join(rdbDir, rdbFileName),
	)

	if err != nil {
		return "", err
	}

	defer fPointer.Close()

	var b strings.Builder
	buf := make([]byte, 1024)

	for {
		n, err := fPointer.Read(buf)

		if err == io.EOF {
			break
		}

		if err != nil {
			fmt.Println("Error to read file:", err.Error())

			continue
		}

		if n > 0 {
			b.Write(buf[:n])
		}
	}

	return b.String(), nil
}
