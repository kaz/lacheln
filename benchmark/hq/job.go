package hq

import (
	"fmt"
	"os"
	"time"

	"github.com/kaz/lacheln/benchmark/msg"
	"github.com/urfave/cli/v2"
	"gopkg.in/yaml.v2"
)

func ActionJob(context *cli.Context) error {
	configFile, err := os.Open(context.String("config"))
	if err != nil {
		return fmt.Errorf("os.Open failed: %w", err)
	}
	defer configFile.Close()

	benchConf := &msg.BenchmarkConfig{}
	if err := yaml.NewDecoder(configFile).Decode(benchConf); err != nil {
		return fmt.Errorf("yaml.NewDecoder.Decode failed: %w", err)
	}

	switch context.String("mode") {
	case "start":
	case "cancel":
	default:
		return fmt.Errorf("no job mode specified")
	}

	conf, err := readConfig(context.String("config"))
	if err != nil {
		return fmt.Errorf("readConfig failed: %w", err)
	}

	broadcast(conf.Workers, func(i int, worker string) error {
		return communicate(worker, &msg.BenchmarkJobMessage{
			Mode:    context.String("mode"),
			Config:  benchConf,
			StartAt: time.Now().Add(-10 * time.Second),
		})
	})
	return nil
}
