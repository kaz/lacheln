package main

import (
	"fmt"
	"os"

	"github.com/kaz/sql-replay/cli"
)

func main() {
	if err := cli.Start(); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(-1)
	}
}
