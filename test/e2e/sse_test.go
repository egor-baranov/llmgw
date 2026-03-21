package e2e_test

import (
	"bufio"
	"bytes"
	"io"
	"strings"
)

type sseFrame struct {
	Event string
	Data  []byte
}

func readSSEFrames(r io.Reader, fn func(sseFrame) error) error {
	reader := bufio.NewReader(r)
	var event string
	var data bytes.Buffer
	flush := func() error {
		if data.Len() == 0 && event == "" {
			return nil
		}
		payload := bytes.TrimSuffix(data.Bytes(), []byte{'\n'})
		frame := sseFrame{Event: event, Data: append([]byte(nil), payload...)}
		event = ""
		data.Reset()
		return fn(frame)
	}
	for {
		line, err := reader.ReadString('\n')
		if err != nil && err != io.EOF {
			return err
		}
		line = strings.TrimRight(line, "\r\n")
		if strings.HasPrefix(line, "event:") {
			event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		} else if strings.HasPrefix(line, "data:") {
			data.WriteString(strings.TrimSpace(strings.TrimPrefix(line, "data:")))
			data.WriteByte('\n')
		} else if line == "" {
			if flushErr := flush(); flushErr != nil {
				return flushErr
			}
		}
		if err == io.EOF {
			return flush()
		}
	}
}
