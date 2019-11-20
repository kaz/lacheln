package cli

import (
	"os"

	"github.com/kaz/sql-replay/digest"
	"github.com/kaz/sql-replay/duplicate"
	"github.com/urfave/cli"
)

var (
	Version = "dev"
)

func Start() error {
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
					Name: "yaml",
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

	return app.Run(os.Args)
}
