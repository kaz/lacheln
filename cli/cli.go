package cli

import (
	"os"

	"github.com/kaz/sql-replay/cli/digest"
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
	}

	return app.Run(os.Args)
}
