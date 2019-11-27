package duplicate

import (
	"fmt"
	"io"
	"math/rand"
	"os"

	"github.com/kaz/lacheln/benchmark/msg"
	"github.com/urfave/cli/v2"
	"gopkg.in/yaml.v2"
)

type (
	Entry struct {
		ReadOnly bool
		Query    string
		Count    int
		Ratio    float32

		Replace []*Replace `yaml:",omitempty"`
	}
	Replace struct {
		Key  string
		Args []string `yaml:",omitempty"`
	}
)

func Action(context *cli.Context) error {
	input, err := os.Open(context.String("input"))
	if err != nil {
		return fmt.Errorf("os.Open failed: %w", err)
	}
	defer input.Close()

	entries := []*Entry{}
	if err := yaml.NewDecoder(input).Decode(&entries); err != nil {
		return fmt.Errorf("yaml.Decoder.Decode failed: %w", err)
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

	if context.Bool("dry-run") {
		return dryrun(out, entries)
	}
	return execute(out, entries)
}

func dryrun(out io.Writer, entries []*Entry) error {
	for _, ent := range entries {
		ent.Count = 1
		ent.Ratio = 1.0
	}

	for _, que := range duplicate(entries) {
		fmt.Fprintf(out, "%s;\n", que.SQL)
	}
	return nil
}
func execute(out io.Writer, entries []*Entry) error {
	queries := duplicate(entries)
	rand.Shuffle(len(queries), func(i, j int) { queries[i], queries[j] = queries[j], queries[i] })

	if err := msg.Send(out, &msg.PutQueryMessage{Query: queries}); err != nil {
		return fmt.Errorf("msg.Send failed: %w", err)
	}
	return nil
}
