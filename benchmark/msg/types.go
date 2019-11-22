package msg

type (
	WorkerConfig struct {
		Connections int

		RWServers []string `yaml:"rw_servers"`
		ROServers []string `yaml:"ro_servers"`
	}

	Query struct {
		RO  bool
		SQL string
	}
)
