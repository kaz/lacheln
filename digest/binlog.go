package digest

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/siddontang/go-mysql/replication"
)

type (
	BinlogQuerySource struct {
		file *os.File
	}
)

func NewBinlogQuerySource(path string) (QuerySource, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("os.Open failed: %w", err)
	}

	// skip binlog file header
	if _, err := file.Seek(4, os.SEEK_SET); err != nil {
		return nil, fmt.Errorf("Seek failed: %w", err)
	}

	return &BinlogQuerySource{file}, nil
}

func (qs *BinlogQuerySource) Query() chan string {
	ch := make(chan string)
	go func() {
		err := replication.NewBinlogParser().ParseReader(qs.file, func(event *replication.BinlogEvent) error {
			if queryEvent, ok := event.Event.(*replication.QueryEvent); ok {
				query := string(queryEvent.Query)
				// ignore pseudo gtid
				if !strings.HasPrefix(query, "drop view if exists `_pseudo_gtid_`") {
					ch <- query
				}
			}
			return nil
		})
		if err != nil {
			log.Printf("NewBinlogParser.ParseReader failed: %v\n", err)
		}
		close(ch)
	}()

	return ch
}

func (qs *BinlogQuerySource) Close() error {
	return qs.file.Close()
}
