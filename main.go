package main

import (
	"fmt"
	"os"

	"github.com/kaz/sql-replay/digest"
	"github.com/kaz/sql-replay/duplicate"
	"github.com/urfave/cli"
)

var (
	Version = "dev"
)

func main() {
	app := cli.NewApp()

	app.Name = "sql-replay"
	app.Usage = "benchmarking tool for db"
	app.Version = Version

	app.Commands = []cli.Command{
		{
			Name:   "digest",
			Action: digest.Action,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name: "slowlog",
				},
				cli.StringFlag{
					Name: "pcap",
				},
				cli.StringFlag{
					Name: "output",
				},
			},
		},
		{
			Name:   "duplicate",
			Action: duplicate.Action,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name: "yaml",
				},
				cli.StringFlag{
					Name: "sql",
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(-1)
	}
}
