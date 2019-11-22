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

func ActionMetrics(context *cli.Context) error {
	rawConfig, err := ioutil.ReadFile(context.String("config"))
	if err != nil {
		return fmt.Errorf("ioutil.ReadFile failed: %w", err)
	}

	conf := &config{}
	if err := yaml.Unmarshal(rawConfig, conf); err != nil {
		return fmt.Errorf("yaml.Unmarshal failed: %w", err)
	}

	wg := &sync.WaitGroup{}
	for _, worker := range conf.Workers {
		wg.Add(1)
		go func(worker string) {
			if err := fetchMetrics(worker); err != nil {
				fmt.Fprintf(os.Stderr, "fetch metrics failed: %v\n", err)
			}
			wg.Done()
		}(worker)
	}

	wg.Wait()
	return nil
}

func fetchMetrics(worker string) error {
	conn, err := net.Dial("tcp4", worker)
	if err != nil {
		return fmt.Errorf("new.Dial failed: %w", err)
	}
	defer conn.Close()

	if err := msg.Send(conn, &msg.MetricsRequestMessage{}); err != nil {
		return fmt.Errorf("msg.Send failed: %w", err)
	}

	raw, err := msg.Receive(conn)
	if err != nil {
		return fmt.Errorf("msg.Receive failed: %w", err)
	}

	resp, ok := raw.(*msg.MetricsResponseMessage)
	if !ok {
		return fmt.Errorf("unexpected message: %v", raw)
	}

	fmt.Printf("worker %v -> %v%%\n", worker, float64(resp.Spec.Current)/float64(resp.Spec.Total)*100.0)
	return nil
}
