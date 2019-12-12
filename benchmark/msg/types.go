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

	Spec struct {
		Total   int
		Current int
	}
	Metric struct {
		Processed map[int64]int64
	}
)
