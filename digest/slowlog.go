package digest

import (
	"fmt"
	"os"

	"github.com/percona/go-mysql/log"
	"github.com/percona/go-mysql/log/slow"
)

type (
	SlowLogQuerySource struct {
		file *os.File
	}
)

func NewSlowLogQuerySource(path string) (QuerySource, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("os.Open failed: %w", err)
	}

	return &SlowLogQuerySource{file}, nil
}

func (qs *SlowLogQuerySource) Query() chan string {
	parser := slow.NewSlowLogParser(qs.file, log.Options{})
	go parser.Start()

	ch := make(chan string)
	go func() {
		for event := range parser.EventChan() {
			ch <- event.Query
		}
		close(ch)
	}()

	return ch
}

func (qs *SlowLogQuerySource) Close() error {
	return qs.file.Close()
}
