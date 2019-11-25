package main

import (
	"fmt"
	"os"

	"github.com/kaz/sql-replay/benchmark/hq"
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
						Name: "binlog",
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
						Name:     "input",
						Required: true,
					},
					&cli.StringFlag{
						Name: "output",
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
			{
				Name: "hq",
				Subcommands: []*cli.Command{
					{
						Name:   "put",
						Action: hq.ActionPut,
						Flags: []cli.Flag{
							&cli.StringFlag{
								Name:     "config",
								Required: true,
							},
							&cli.StringFlag{
								Name:     "input",
								Required: true,
							},
						},
					},
					{
						Name:   "job",
						Action: hq.ActionJob,
						Flags: []cli.Flag{
							&cli.StringFlag{
								Name:     "config",
								Required: true,
							},
							&cli.StringFlag{
								Name:     "mode",
								Required: true,
							},
						},
					},
					{
						Name:   "metrics",
						Action: hq.ActionMetrics,
						Flags: []cli.Flag{
							&cli.StringFlag{
								Name:     "config",
								Required: true,
							},
							&cli.BoolFlag{
								Name: "progress",
							},
						},
					},
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(-1)
	}
}
