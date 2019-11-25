package worker

import (
	"database/sql"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/kaz/sql-replay/benchmark/msg"
)

type (
	benchmarker struct {
		config  *msg.BenchmarkConfig
		queries []*msg.Query

		now int32

		rwConn []*sql.DB
		roConn []*sql.DB

		metrics []*msg.Metric
	}
)

func (b *benchmarker) startBenchmark() error {
	b.now = 0

	b.rwConn = []*sql.DB{}
	for _, h := range b.config.RWServers {
		conn, err := sql.Open("mysql", h.DSN)
		if err != nil {
			return fmt.Errorf("sql.Open failed: %w", err)
		}
		conn.SetMaxOpenConns(h.Connections)
		b.rwConn = append(b.rwConn, conn)
	}

	b.roConn = []*sql.DB{}
	for _, h := range b.config.ROServers {
		conn, err := sql.Open("mysql", h.DSN)
		if err != nil {
			return fmt.Errorf("sql.Open failed: %w", err)
		}
		conn.SetMaxOpenConns(h.Connections)
		b.roConn = append(b.roConn, conn)
	}

	b.metrics = []*msg.Metric{}
	for i := 0; i < b.config.Threads; i++ {
		metric := &msg.Metric{}
		b.metrics = append(b.metrics, metric)

		go b.benchmark(metric)
	}

	return nil
}

func (b *benchmarker) benchmark(metric *msg.Metric) {
	metric.Start = time.Now()

	for {
		i := int(atomic.AddInt32(&b.now, 1))
		if i >= len(b.queries) {
			break
		}

		query := b.queries[i]
		db := b.roConn[i%len(b.roConn)]
		if !query.RO {
			db = b.rwConn[i%len(b.rwConn)]
		}

		rows, err := db.Query(query.SQL)
		if err != nil {
			fmt.Fprintf(os.Stderr, "db.Query failed: %v\n", err)
			metric.Fail += 1
		} else {
			metric.Success += 1
			rows.Close()
		}
	}

	metric.Finish = time.Now()
}
