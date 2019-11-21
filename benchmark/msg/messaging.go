package msg

import (
	"compress/flate"
	"encoding/gob"
	"fmt"
	"io"
)

type (
	MessageType byte

	SyncConfigMessage struct {
		Config *WorkerConfig
	}
)

const (
	MESSAGE_UNKNOWN MessageType = iota
	MESSAGE_SYNC_CONFIG
)

func Send(w io.Writer, body interface{}) error {
	var typ MessageType

	switch body.(type) {
	case *SyncConfigMessage:
		typ = MESSAGE_SYNC_CONFIG
	default:
		return fmt.Errorf("unexpected type: %#v", body)
	}

	if err := sendType(w, typ); err != nil {
		return fmt.Errorf("sendType failed: %w", err)
	}
	if err := sendBody(w, body); err != nil {
		return fmt.Errorf("sendBody failed: %w", err)
	}
	return nil
}
func Receive(r io.Reader) (interface{}, error) {
	typ, err := receiveType(r)
	if err != nil {
		return nil, fmt.Errorf("receiveType failed: %w", err)
	}

	var body interface{}
	switch typ {
	case MESSAGE_SYNC_CONFIG:
		body = &SyncConfigMessage{}
	default:
		return nil, fmt.Errorf("unexpected type: %#v", typ)
	}

	if err := receiveBody(r, body); err != nil {
		return nil, fmt.Errorf("receiveBody failed: %w", err)
	}
	return body, nil
}

func sendType(w io.Writer, typ MessageType) error {
	if _, err := w.Write([]byte{byte(typ)}); err != nil {
		return fmt.Errorf("writer.Write failed: %w", err)
	}
	return nil
}
func receiveType(r io.Reader) (MessageType, error) {
	b := []byte{0}
	if _, err := r.Read(b); err != nil {
		return MESSAGE_UNKNOWN, fmt.Errorf("reader.Read failed: %w", err)
	}
	return MessageType(b[0]), nil
}

func sendBody(w io.Writer, data interface{}) error {
	inflator, err := flate.NewWriter(w, flate.DefaultCompression)
	if err != nil {
		return fmt.Errorf("flate.NewWriter failed: %w", err)
	}

	if err := gob.NewEncoder(inflator).Encode(data); err != nil {
		return fmt.Errorf("gob.NewEncoder.Encode failed: %w", err)
	}

	if err := inflator.Flush(); err != nil {
		return fmt.Errorf("inflator.Flush failed: %w", err)
	}

	return nil
}
func receiveBody(r io.Reader, data interface{}) error {
	if err := gob.NewDecoder(flate.NewReader(r)).Decode(data); err != nil {
		return fmt.Errorf("gob.NewDecoder.Decode failed: %w", err)
	}

	return nil
}
