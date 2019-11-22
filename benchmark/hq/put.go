package hq

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"sync"

	"github.com/kaz/sql-replay/benchmark/msg"
	"github.com/urfave/cli/v2"
	"gopkg.in/yaml.v2"
)

func ActionPut(context *cli.Context) error {
	rawConfig, err := ioutil.ReadFile(context.String("config"))
	if err != nil {
		return fmt.Errorf("ioutil.ReadFile failed: %w", err)
	}

	conf := &config{}
	if err := yaml.Unmarshal(rawConfig, conf); err != nil {
		return fmt.Errorf("yaml.Unmarshal failed: %w", err)
	}

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

	size := len(query.Query) / len(conf.Workers)

	wg := &sync.WaitGroup{}
	for i, worker := range conf.Workers {
		wg.Add(1)
		go func(i int, worker string) {
			last := len(query.Query)
			if (i+1)*size < last {
				last = (i + 1) * size
			}
			if err := put(worker, query.Query[i*size:last]); err != nil {
				fmt.Fprintf(os.Stderr, "sync failed: %v\n", err)
			}
			wg.Done()
		}(i, worker)
	}

	wg.Wait()
	return nil
}

func put(worker string, queries []*msg.Query) error {
	conn, err := net.Dial("tcp4", worker)
	if err != nil {
		return fmt.Errorf("new.Dial failed: %w", err)
	}
	defer conn.Close()

	if err := msg.Send(conn, &msg.PutQueryMessage{Query: queries}); err != nil {
		return fmt.Errorf("msg.Send failed: %w", err)
	}

	raw, err := msg.Receive(conn)
	if err != nil {
		return fmt.Errorf("msg.Receive failed: %w", err)
	}

	ack, ok := raw.(*msg.AcknowledgedMessage)
	if !ok {
		return fmt.Errorf("unexpected message: %v", raw)
	}

	fmt.Printf("[%v] worker %v -> %v\n", ack.Status, worker, ack.Detail)
	return nil
}
