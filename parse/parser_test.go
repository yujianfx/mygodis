package parse

import (
	"bufio"
	"bytes"
	"fmt"
	"testing"
	"time"
)

func Test_parseBulkString(t *testing.T) {
	t.Run("test parseBulkString", func(t *testing.T) {
		ping := ([]byte)("*1\r\n$4\r\nPING\r\n")
		reader := bytes.NewReader(ping)
		bufioReader := bufio.NewReader(reader)
		ch := make(chan *Payload)
		go func() {
			for {
				fmt.Printf("data:%v", <-ch)
			}
		}()
		for {
			readBytes, _ := bufioReader.ReadBytes('\n')
			if len(readBytes) == 0 {
				break
			}
			bs := bytes.TrimSuffix(readBytes, []byte{'\r', '\n'})
			parseArray(bs, bufioReader, ch)
		}
		select {
		case <-time.After(time.Second * 2):
		}
	})
}
