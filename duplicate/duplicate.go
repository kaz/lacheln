package duplicate

import (
	"fmt"
	"io"
	"os"

	"github.com/kaz/lacheln/duplicate/codec"
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
		for _, ent := range entries {
			ent.Count = 1
			ent.Ratio = 1.0
		}

		strategy := duplicate(entries)
		for _, frag := range strategy.Fragments {
			tmp := strategy.Templates[frag.Reference]
			fmt.Fprintf(out, tmp.SQL+";\n", frag.Arguments...)
		}
	} else {
		if err := codec.Serialize(out, duplicate(entries)); err != nil {
			return fmt.Errorf("msg.Send failed: %w", err)
		}
	}
	return nil
}
