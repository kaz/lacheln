package duplicate

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"strings"

	"github.com/urfave/cli"
	"gopkg.in/yaml.v2"
)

type (
	Entry struct {
		Count   int
		Query   string
		Replace []Replace `yaml:",omitempty"`
	}
	Replace struct {
		Key  string   `yaml:",omitempty"`
		Args []string `yaml:",omitempty"`
	}
)

func Action(context *cli.Context) error {
	input, err := os.Open(context.String("yaml"))
	if err != nil {
		return fmt.Errorf("os.Open failed: %w", err)
	}
	defer input.Close()

	entries := []*Entry{}
	if err := yaml.NewDecoder(input).Decode(&entries); err != nil {
		return fmt.Errorf("yaml.Decoder.Decode failed: %w", err)
	}

	initGenerators()

	sqls := []string{}
	for _, ent := range entries {
		for i := 0; i < ent.Count; i++ {
			vals := []interface{}{}
			if ent.Replace != nil {
				for _, rep := range ent.Replace {
					args := []string{}
					if rep.Args != nil {
						args = rep.Args
					}

					dummy, err := getDummy(rep.Key, args...)
					if err != nil {
						return fmt.Errorf("getDummy failed: %w", err)
					}

					vals = append(vals, dummy)
				}
			}
			sqls = append(sqls, fmt.Sprintf(ent.Query, vals...))
		}
	}

	rand.Shuffle(len(sqls), func(i, j int) { sqls[i], sqls[j] = sqls[j], sqls[i] })

	var out io.Writer = os.Stdout

	outFilePath := context.String("sql")
	if outFilePath != "" {
		outFile, err := os.Create(outFilePath)
		if err != nil {
			return fmt.Errorf("os.Create failed: %w", err)
		}
		defer outFile.Close()

		out = outFile
	}

	for _, sql := range sqls {
		if _, err := fmt.Fprintf(out, "%s;\n", strings.TrimSpace(sql)); err != nil {
			return fmt.Errorf("fmt.Fprintf failed: %w", err)
		}
	}

	return nil
}
