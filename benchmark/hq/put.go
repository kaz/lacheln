package hq

import (
	"fmt"
	"os"

	"github.com/kaz/lacheln/benchmark/msg"
	"github.com/urfave/cli/v2"
)

func ActionPut(context *cli.Context) error {
	conf, err := readConfig(context.String("config"))
	if err != nil {
		return fmt.Errorf("readConfig failed: %w", err)
	}

	input, err := os.Open(context.String("data"))
	if err != nil {
		return fmt.Errorf("os.Open failed: %w", err)
	}
	defer input.Close()

	raw, err := msg.Receive(input)
	if err != nil {
		return fmt.Errorf("msg.Receive failed: %w", err)
	}

	data, ok := raw.(*msg.PutStrategyMessage)
	if !ok {
		return fmt.Errorf("invalid data format")
	}

	size := len(data.Strategy.Fragments) / len(conf.Workers)

	broadcast(conf.Workers, func(i int, worker string) error {
		last := (i + 1) * size
		if i+1 == len(conf.Workers) {
			last = len(data.Strategy.Fragments)
		}
		return communicate(worker, &msg.PutStrategyMessage{
			Mode: context.String("mode"),
			Strategy: &msg.Strategy{
				Templates: data.Strategy.Templates,
				Fragments: data.Strategy.Fragments[i*size : last],
			},
		})
	})
	return nil
}
