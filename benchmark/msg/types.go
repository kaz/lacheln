package msg

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

	Metric struct {
		Total   int
		Current int
		QPS     map[int64]int32
	}
)
