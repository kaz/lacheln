package msg

import (
	"time"
)

type (
	BenchmarkConfig struct {
		Threads   int
		RWServers []*Server `yaml:"rw_servers"`
		ROServers []*Server `yaml:"ro_servers"`
	}

	Server struct {
		DSN         string
		Connections int
	}

	Query struct {
		RO  bool
		SQL string
	}

	Spec struct {
		Total   int
		Current int
	}
	Metric struct {
		Start  time.Time
		Finish time.Time

		Fail    int
		Success int
	}
)
