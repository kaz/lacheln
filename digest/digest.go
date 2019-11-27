package digest

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

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
		Count    int
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

	id := context.String("export")
	if id != "" {
		return export(out, qs, id)
	}
	return digest(out, qs)
}

func digest(out io.Writer, qs QuerySource) error {
	data := map[string]*Entry{}

	for sql := range qs.Query() {
		fp := query.Fingerprint(sql)
		id := query.Id(fp)

		if ent, ok := data[id]; ok {
			ent.Count += 1
		} else {
			data[id] = &Entry{id, fp + "\n", strings.HasPrefix(fp, "select"), sql + "\n", 1, 1.0}
		}
	}

	entries := []*Entry{}
	for _, ent := range data {
		entries = append(entries, ent)
	}

	sort.Slice(entries, func(i, j int) bool { return entries[i].Count > entries[j].Count })

	if err := yaml.NewEncoder(out).Encode(entries); err != nil {
		return fmt.Errorf("yaml.Encoder.Encode failed: %w", err)
	}
	return nil
}
func export(out io.Writer, qs QuerySource, id string) error {
	for sql := range qs.Query() {
		if id == query.Id(query.Fingerprint(sql)) {
			fmt.Fprintf(out, "%s;\n", sql)
		}
	}
	return nil
}
