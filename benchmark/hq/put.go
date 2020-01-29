package hq

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"

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
		err := broadcast(conf.Workers, func(i int, worker string) error {
			return communicate(worker, &msg.PutStrategyMessage{Reset: true})
		})
		if err != nil {
			return fmt.Errorf("broadcast failed: %w", err)
		}
	}

	wg := &sync.WaitGroup{}
	chErr := make(chan error)
	chMsg := codec.Deserialize(input)

	var i int64 = -1
	for j := 0; j < len(conf.Workers); j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for rawMsg := range chMsg {
				if err := communicate(conf.Workers[int(atomic.AddInt64(&i, 1))%len(conf.Workers)], rawMsg); err != nil {
					chErr <- fmt.Errorf("communicate failed: %w", err)
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		chErr <- nil
	}()

	return <-chErr
}
