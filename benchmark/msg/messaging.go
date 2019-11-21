package msg

import (
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

func WriteMessageType(w io.Writer, typ MessageType) error {
	if _, err := w.Write([]byte{byte(typ)}); err != nil {
		return fmt.Errorf("writer.Write failed: %w", err)
	}
	return nil
}
func ReadMessageType(r io.Reader) (MessageType, error) {
	b := []byte{0}
	if _, err := r.Read(b); err != nil {
		return MESSAGE_UNKNOWN, fmt.Errorf("reader.Read failed: %w", err)
	}
	return MessageType(b[0]), nil
}
