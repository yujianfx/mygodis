package parse

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	logger "mygodis/log"
	"mygodis/resp"
	"runtime/debug"
	"strconv"
)

type Payload struct {
	Data resp.Reply
	Err  error
}

func Parse(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parse0(reader, ch)
	return ch
}
func ParseBytes(data []byte) ([]resp.Reply, error) {
	ch := make(chan *Payload)
	reader := bytes.NewReader(data)
	go parse0(reader, ch)
	var results []resp.Reply
	for payload := range ch {
		if payload == nil {
			return nil, errors.New("no protocol")
		}
		if payload.Err != nil {
			if payload.Err == io.EOF {
				break
			}
			return nil, payload.Err
		}
		results = append(results, payload.Data)
	}
	return results, nil
}
func ParseOne(data []byte) (resp.Reply, error) {
	ch := make(chan *Payload)
	reader := bytes.NewReader(data)
	go parse0(reader, ch)
	payload := <-ch
	if payload == nil {
		return nil, errors.New("no protocol")
	}
	return payload.Data, payload.Err
}
func parse0(reader io.Reader, ch chan<- *Payload) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn("server error: ", err)
			logger.Errorf("%v", debug.Stack())
			ch <- &Payload{
				Err: errors.New("server error"),
			}
		}
	}()
	bufioReader := bufio.NewReader(reader)
	for {
		line, err := bufioReader.ReadBytes('\n')
		if err != nil {
			ch <- &Payload{
				Err: err,
			}
			close(ch)
			return
		}
		length := len(line)
		if length == 0 || line[length-1] != '\n' {
			continue
		}
		line = bytes.TrimSuffix(line, []byte{'\r', '\n'})
		switch line[0] {
		case '+':
			s := string(line[1:])
			ch <- &Payload{
				Data: resp.MakeSimpleStringReply(s),
			}
		case '-':
			ch <- &Payload{
				Data: resp.MakeErrReply(string(line[1:])),
			}
		case ':':
			val, err := strconv.ParseInt(string(line[1:]), 10, 64)
			if err != nil {
				ch <- &Payload{
					Err: err,
					Data: &resp.ProtocolErrReply{
						Msg: fmt.Sprintf("illegal number %v", err),
					},
				}
				continue
			}
			ch <- &Payload{
				Data: resp.MakeIntReply(val),
			}
		case '$':
			err := parseBulkString(line, bufioReader, ch)
			if err != nil {
				ch <- &Payload{
					Err: err,
				}
				close(ch)
				return
			}
		case '*':
			err = parseArray(line, bufioReader, ch)
			if err != nil {
				ch <- &Payload{
					Err: err,
				}
				close(ch)
				return
			}
		default:
			ch <- &Payload{
				Data: resp.MakeMultiBulkReply(bytes.Split(line, []byte{' '})),
			}
		}
	}

}
func parseArray(line []byte, reader *bufio.Reader, ch chan<- *Payload) error {
	if len(line) == 0 || line[0] != '*' {
		return nil
	}
	length, err := strconv.ParseInt(string(line[1:]), 10, 64)
	if err != nil {
		ch <- &Payload{
			Data: &resp.ProtocolErrReply{
				Msg: fmt.Sprintf("illegal number %v", err),
			},
		}
		return nil
	}
	if length <= 0 {
		ch <- &Payload{
			Data: resp.MakeEmptyMultiBulkReply(),
		}
		return nil
	}
	results := make([][]byte, 0)
	for i := 0; i < int(length); i++ {
		bufBytes, err := reader.ReadBytes('\n')
		if err != nil {
			return err
		}
		if len(bufBytes) == 0 || bufBytes[0] != '$' || bufBytes[len(bufBytes)-1] != '\n' {
			continue
		}
		if len(bufBytes) == -1 {
			results = append(results, []byte{})
		}
		bufBytes = bytes.TrimSuffix(bufBytes, []byte{'\r', '\n'})
		strLen, pErr := strconv.ParseInt(string(bufBytes[1:]), 10, 64)
		if pErr != nil {
			return pErr
		}
		payload := make([]byte, strLen+2)
		read, rErr := reader.Read(payload)
		if rErr != nil || read != int(strLen+2) {
			return rErr
		}
		results = append(results, payload[:strLen])
	}
	ch <- &Payload{
		Data: resp.MakeMultiBulkReply(results),
	}
	return nil

}
func parseBulkString(line []byte, reader io.Reader, ch chan<- *Payload) error {
	length, err := strconv.ParseInt(string(line[1:]), 10, 64)
	if err != nil {
		ch <- &Payload{
			Data: &resp.ProtocolErrReply{
				Msg: fmt.Sprintf("illegal number %v", err),
			},
		}
		return nil
	}
	if length <= 0 {
		ch <- &Payload{
			Data: resp.MakeNullBulkReply(),
		}
		return nil
	}
	buf := make([]byte, length+2)
	_, err = io.ReadFull(reader, buf)
	if err != nil {
		return err
	}
	ch <- &Payload{
		Data: resp.MakeBulkReply(buf[:len(buf)-2]),
	}
	return nil
}
func parseRDBBulkString(reader *bufio.Reader, ch chan<- *Payload) error {
	head, err := reader.ReadBytes('\n')
	if err != nil {
		return err
	}
	head = bytes.TrimSuffix(head, []byte{'\r', '\n'})
	if len(head) == 0 {
		return errors.New("invalid head")
	}
	length, pErr := strconv.ParseInt(string(head[1:]), 10, 64)
	if pErr != nil {
		return errors.New("invalid head")
	}
	body := make([]byte, length)
	_, rerr := io.ReadFull(reader, body)
	if rerr != nil {
		return rerr
	}
	ch <- &Payload{
		Data: resp.MakeBulkReply(body),
	}

	return nil
}
