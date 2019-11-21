package benchmark

type (
	Config struct {
		Connection int

		RWServers []string
		ROServers []string
	}

	SyncConfigMessage struct {
		Config *Config
	}
)
