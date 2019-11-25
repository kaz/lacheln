package hq

import (
	"fmt"
	"os"

	"github.com/kaz/sql-replay/benchmark/msg"
	"github.com/urfave/cli/v2"
)

func ActionPut(context *cli.Context) error {
	input, err := os.Open(context.String("input"))
	if err != nil {
		return fmt.Errorf("os.Open failed: %w", err)
	}
	defer input.Close()

	raw, err := msg.Receive(input)
	if err != nil {
		return fmt.Errorf("msg.Receive failed: %w", err)
	}

	query, ok := raw.(*msg.PutQueryMessage)
	if !ok {
		return fmt.Errorf("invalid data format")
	}

	conf, err := readConfig(context.String("config"))
	if err != nil {
		return fmt.Errorf("readConfig failed: %w", err)
	}

	size := len(query.Query) / len(conf.Workers)

	broadcast(conf.Workers, func(i int, worker string) error {
		last := len(query.Query)
		if (i+1)*size < last {
			last = (i + 1) * size
		}
		return communicate(worker, &msg.PutQueryMessage{Query: query.Query[i*size : last]})
	})
	return nil
}
