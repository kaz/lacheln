package worker

import (
	"github.com/kaz/sql-replay/benchmark/msg"
)

type (
	benchmarker struct {
		config  *msg.BenchmarkConfig
		queries []*msg.Query
	}
)

func (b *benchmarker) benchmark() {

}
