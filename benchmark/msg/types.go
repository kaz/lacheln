package msg

import "time"

type (
	BenchmarkConfig struct {
		Threads int
		Servers []*Server
	}
	Server struct {
		RO              bool
		DSN             string
		MaxOpenConns    int           `yaml:"max_open_conns"`
		MaxIdleConns    int           `yaml:"max_idle_conns"`
		ConnMaxLifetime time.Duration `yaml:"conn_max_lifetime"`
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
