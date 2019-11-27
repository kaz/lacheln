package digest

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/percona/go-mysql/query"
	"github.com/urfave/cli/v2"
	"gopkg.in/yaml.v2"
)

type (
	QuerySource interface {
		Query() chan string
		Close() error
	}

	Entry struct {
		ID          string
		Fingerprint string

		ReadOnly bool
		Query    string
		Count    int32
		Ratio    float32
	}
)

func Action(context *cli.Context) error {
	var qs QuerySource

	if context.String("slowlog") != "" {
		slowlog, err := NewSlowLogQuerySource(context.String("slowlog"))
		if err != nil {
			return fmt.Errorf("NewSlowLogQuerySource failed: %w", err)
		}
		defer slowlog.Close()

		qs = slowlog
	} else if context.String("pcap") != "" {
		pcap, err := NewPcapQuerySource(context.String("pcap"))
		if err != nil {
			return fmt.Errorf("NewPcapQuerySource failed: %w", err)
		}
		defer pcap.Close()

		qs = pcap
	} else if context.String("binlog") != "" {
		binlog, err := NewBinlogQuerySource(context.String("binlog"))
		if err != nil {
			return fmt.Errorf("NewBinlogQuerySource failed: %w", err)
		}
		defer binlog.Close()

		qs = binlog
	} else {
		return fmt.Errorf("no query source was specified")
	}

	var out io.Writer = os.Stdout

	outFilePath := context.String("output")
	if outFilePath != "" {
		outFile, err := os.Create(outFilePath)
		if err != nil {
			return fmt.Errorf("os.Create failed: %w", err)
		}
		defer outFile.Close()

		out = outFile
	}

	if err := yaml.NewEncoder(out).Encode(digest(qs.Query())); err != nil {
		return fmt.Errorf("yaml.Encoder.Encode failed: %w", err)
	}
	return nil
}

func digest(ch chan string) []*Entry {
	wg := &sync.WaitGroup{}
	data := &sync.Map{}

	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			for sql := range ch {
				fp := query.Fingerprint(sql)

				newEnt := &Entry{
					query.Id(fp),
					fp + "\n",
					strings.HasPrefix(fp, "select"),
					sql + "\n",
					1,
					1.0,
				}

				if ent, loaded := data.LoadOrStore(newEnt.ID, newEnt); loaded {
					atomic.AddInt32(&ent.(*Entry).Count, 1)
				}
			}
			wg.Done()
		}()
	}

	wg.Wait()

	entries := []*Entry{}
	data.Range(func(k, v interface{}) bool {
		entries = append(entries, v.(*Entry))
		return true
	})

	sort.Slice(entries, func(i, j int) bool { return entries[i].Count > entries[j].Count })
	return entries
}
