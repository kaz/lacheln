package hq

import (
	"fmt"
	"os"

	"github.com/kaz/lacheln/benchmark/msg"
	"github.com/kaz/lacheln/duplicate/codec"
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

	if context.Bool("reset") {
		broadcast(conf.Workers, func(i int, worker string) error {
			return communicate(worker, &msg.PutStrategyMessage{Reset: true})
		})
	}

	i := 0
	for rawMsg := range codec.Deserialize(input) {
		if err := communicate(conf.Workers[i%len(conf.Workers)], rawMsg); err != nil {
			return fmt.Errorf("communicate failed: %w", err)
		}
		i++
	}

	return nil
}
