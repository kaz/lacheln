package digest

import (
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/percona/go-mysql/log"
	"github.com/percona/go-mysql/log/slow"
	"github.com/percona/go-mysql/query"
	"github.com/urfave/cli"
	"gopkg.in/yaml.v2"
)

type (
	Entry struct {
		Count uint
		Query string
	}
)

func Action(context *cli.Context) error {
	slowlogFile, err := os.Open(context.String("slowlog"))
	if err != nil {
		return fmt.Errorf("os.Open failed: %w", err)
	}
	defer slowlogFile.Close()

	entries := []*Entry{}
	entMap := map[string]*Entry{}

	parser := slow.NewSlowLogParser(slowlogFile, log.Options{})
	go parser.Start()

	for event := range parser.EventChan() {
		fp := query.Fingerprint(event.Query)

		if _, ok := entMap[fp]; !ok {
			entMap[fp] = &Entry{0, event.Query}
			entries = append(entries, entMap[fp])
		}

		entMap[fp].Count += 1
	}

	sort.Slice(entries, func(i, j int) bool { return entries[i].Count > entries[j].Count })

	data, err := yaml.Marshal(entries)
	if err != nil {
		return fmt.Errorf("yaml.Marshal failed: %w", err)
	}

	var out io.Writer = os.Stdout
	outFilePath := context.String("out")
	if outFilePath != "" {
		outFile, err := os.Create(outFilePath)
		if err != nil {
			return fmt.Errorf("os.Create failed: %w", err)
		}
		defer outFile.Close()

		out = outFile
	}

	if _, err := out.Write(data); err != nil {
		return fmt.Errorf("out.Write failed: %w", err)
	}

	return nil
}
