package msg

type (
	BenchmarkConfig struct {
		Threads   int
		RWServers []string `yaml:"rw_servers"`
		ROServers []string `yaml:"ro_servers"`
	}

	Strategy struct {
		Templates []*Template
		Fragments []*Fragment
	}
	Template struct {
		RO  bool
		SQL string
	}
	Fragment struct {
		Reference int
		Arguments []interface{}
	}

	Metric struct {
		Total     int64
		Timestamp [][][2]uint16
	}
)
