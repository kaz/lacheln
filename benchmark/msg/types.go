package msg

type (
	BenchmarkConfig struct {
		Threads   int
		RWServers []string `yaml:"rw_servers"`
		ROServers []string `yaml:"ro_servers"`
	}

	Query struct {
		RO  bool
		SQL string
	}

	Metric struct {
		Total     int64
		Timestamp [][][2]uint16
	}
)
