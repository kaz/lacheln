package msg

import (
	"fmt"
	"io"

	"github.com/pierrec/lz4"
	"github.com/vmihailenco/msgpack"
)

type (
	MessageType byte

	AcknowledgedMessage struct {
		Status string
		Detail string
	}
	PutQueryMessage struct {
		Query []*Query
	}
	BenchmarkJobMessage struct {
		Mode   string
		Config *BenchmarkConfig
	}
	MetricsRequestMessage struct {
	}
	MetricsResponseMessage struct {
		Metric *Metric
	}
)

const (
	typeUnknown MessageType = iota
	typeAcknowledged
	typePutQuery
	typeBenchmarkJob
	typeMetricsRequest
	typeMetricsResponse
)

func Send(w io.Writer, body interface{}) error {
	var typ MessageType

	switch body.(type) {
	case *AcknowledgedMessage:
		typ = typeAcknowledged
	case *PutQueryMessage:
		typ = typePutQuery
	case *BenchmarkJobMessage:
		typ = typeBenchmarkJob
	case *MetricsRequestMessage:
		typ = typeMetricsRequest
	case *MetricsResponseMessage:
		typ = typeMetricsResponse
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
	case typeAcknowledged:
		body = &AcknowledgedMessage{}
	case typePutQuery:
		body = &PutQueryMessage{}
	case typeBenchmarkJob:
		body = &BenchmarkJobMessage{}
	case typeMetricsRequest:
		body = &MetricsRequestMessage{}
	case typeMetricsResponse:
		body = &MetricsResponseMessage{}
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
		return typeUnknown, fmt.Errorf("reader.Read failed: %w", err)
	}
	return MessageType(b[0]), nil
}

func sendBody(w io.Writer, data interface{}) error {
	compressor := lz4.NewWriter(w)
	if err := msgpack.NewEncoder(compressor).Encode(data); err != nil {
		return fmt.Errorf("msgpack.NewEncoder.Encode failed: %w", err)
	}

	if err := compressor.Flush(); err != nil {
		return fmt.Errorf("compressor.Flush failed: %w", err)
	}

	return nil
}
func receiveBody(r io.Reader, data interface{}) error {
	if err := msgpack.NewDecoder(lz4.NewReader(r)).Decode(data); err != nil {
		return fmt.Errorf("msgpack.NewDecoder.Decode failed: %w", err)
	}

	return nil
}
