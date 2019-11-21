package main

import (
	"fmt"
	"os"

	"github.com/kaz/sql-replay/benchmark/worker"
	"github.com/kaz/sql-replay/digest"
	"github.com/kaz/sql-replay/duplicate"
	"github.com/urfave/cli/v2"
)

var (
	Version = "dev"
)

func main() {
	app := &cli.App{

		Name:    "sql-replay",
		Usage:   "benchmarking tool for db",
		Version: Version,

		Commands: []*cli.Command{
			{
				Name:   "digest",
				Action: digest.Action,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name: "slowlog",
					},
					&cli.StringFlag{
						Name: "pcap",
					},
					&cli.StringFlag{
						Name: "output",
					},
				},
			},
			{
				Name:   "duplicate",
				Action: duplicate.Action,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name: "yaml",
					},
					&cli.StringFlag{
						Name: "sql",
					},
				},
			},
			{
				Name:   "worker",
				Action: worker.Action,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name: "port",
					},
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(-1)
	}
}
