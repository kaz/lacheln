package hq

import (
	"fmt"
	"log"
	"net"
	"os"
	"sync"

	"github.com/kaz/sql-replay/benchmark/msg"
	"gopkg.in/yaml.v2"
)

type (
	config struct {
		Workers []string
	}
)

func readConfig(configPath string) (*config, error) {
	configFile, err := os.Open(configPath)
	if err != nil {
		return nil, fmt.Errorf("os.Open failed: %w", err)
	}
	defer configFile.Close()

	conf := &config{}
	if err := yaml.NewDecoder(configFile).Decode(conf); err != nil {
		return nil, fmt.Errorf("yaml.NewDecoder.Decode failed: %w", err)
	}

	return conf, nil
}

func broadcast(workers []string, action func(int, string) error) {
	wg := &sync.WaitGroup{}
	for i, worker := range workers {
		wg.Add(1)
		go func(worker string) {
			if err := action(i, worker); err != nil {
				log.Printf("action failed: %v\n", err)
			}
			wg.Done()
		}(worker)
	}
	wg.Wait()
}

func communicate(worker string, data interface{}) error {
	conn, err := net.Dial("tcp4", worker)
	if err != nil {
		return fmt.Errorf("new.Dial failed: %w", err)
	}
	defer conn.Close()

	if err := msg.Send(conn, data); err != nil {
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

	log.Printf("[%v] worker %v -> %v\n", ack.Status, worker, ack.Detail)
	return nil
}
