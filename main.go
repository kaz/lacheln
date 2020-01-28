package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/kaz/lacheln/benchmark/hq"
	"github.com/kaz/lacheln/benchmark/worker"
	"github.com/kaz/lacheln/digest"
	"github.com/kaz/lacheln/duplicate"
	"github.com/urfave/cli/v2"
)

var (
	Version = "dev"
)

func init() {
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	go func() { os.Exit(128 + int((<-ch).(syscall.Signal))) }()
}

func main() {
	app := &cli.App{

		Name:    "lacheln",
		Usage:   "Database benchmarking toolchain written in Go",
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
					&cli.StringFlag{
						Name: "export",
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
					&cli.BoolFlag{
						Name: "dry-run",
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
							&cli.StringFlag{
								Name:     "mode",
								Required: true,
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
