package worker

import (
	"database/sql"
	"fmt"
	"os"
	"sync/atomic"

	_ "github.com/go-sql-driver/mysql"
	"github.com/kaz/sql-replay/benchmark/msg"
)

type (
	benchmarker struct {
		config  *msg.BenchmarkConfig
		queries []*msg.Query

		now  int32
		fail int32

		rwConn []*sql.DB
		roConn []*sql.DB
	}
)

func (b *benchmarker) startBenchmark() error {
	b.now = 0
	b.fail = 0

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

	for i := 0; i < b.config.Threads; i++ {
		go b.benchmark()
	}
	return nil
}

func (b *benchmarker) benchmark() {
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
			atomic.AddInt32(&b.fail, 1)
		}

		rows.Close()
	}
	fmt.Println("benchmark thread successfully terminated")
}
