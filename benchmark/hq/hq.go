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

type (
	hqConfig struct {
		Workers []string
	}
)

func ActionSync(context *cli.Context) error {
	rawConfig, err := ioutil.ReadFile(context.String("config"))
	if err != nil {
		return fmt.Errorf("ioutil.ReadFile failed: %w", err)
	}

	workerConf := &msg.WorkerConfig{}
	if err := yaml.Unmarshal(rawConfig, workerConf); err != nil {
		return fmt.Errorf("yaml.Unmarshal failed: %w", err)
	}

	hqConf := &hqConfig{}
	if err := yaml.Unmarshal(rawConfig, hqConf); err != nil {
		return fmt.Errorf("yaml.Unmarshal failed: %w", err)
	}

	wg := &sync.WaitGroup{}
	for _, worker := range hqConf.Workers {
		wg.Add(1)
		go func(worker string) {
			if err := doSync(worker, workerConf); err != nil {
				fmt.Fprintf(os.Stderr, "sync failed: %v\n", err)
			}
			wg.Done()
		}(worker)
	}

	wg.Wait()
	return nil
}

func doSync(worker string, conf *msg.WorkerConfig) error {
	conn, err := net.Dial("tcp4", worker)
	if err != nil {
		return fmt.Errorf("new.Dial failed: %w", err)
	}
	defer conn.Close()

	if err := msg.Send(conn, &msg.SyncConfigMessage{Config: conf}); err != nil {
		return fmt.Errorf("msg.Send failed: %w", err)
	}

	return nil
}
