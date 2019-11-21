package digest

import (
	"fmt"
	"io"
	"os"
	"sort"

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
		Count int
		ID    string

		Query       string
		Fingerprint string
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
	} else {
		return fmt.Errorf("no query source was specified")
	}

	entries := []*Entry{}
	entMap := map[string]*Entry{}

	for q := range qs.Query() {
		fp := query.Fingerprint(q)
		id := query.Id(fp)

		if _, ok := entMap[fp]; !ok {
			entMap[fp] = &Entry{0, id, q + "\n", fp + "\n"}
			entries = append(entries, entMap[fp])
		}

		entMap[fp].Count += 1
	}

	sort.Slice(entries, func(i, j int) bool { return entries[i].Count > entries[j].Count })

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

	if err := yaml.NewEncoder(out).Encode(entries); err != nil {
		return fmt.Errorf("yaml.Encoder.Encode failed: %w", err)
	}
	return nil
}
