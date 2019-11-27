package worker

import (
	"database/sql"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/kaz/lacheln/benchmark/msg"
)

type (
	benchmarker struct {
		config  *msg.BenchmarkConfig
		queries []*msg.Query

		wg  *sync.WaitGroup
		now int32

		rwConn []*sql.DB
		roConn []*sql.DB

		metrics []*msg.Metric
	}
)

func (b *benchmarker) startBenchmark() error {
	if len(b.queries) < 1 {
		return fmt.Errorf("No query")
	}
	if b.wg != nil {
		return fmt.Errorf("Job is already working")
	}

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

	b.now = -1
	b.wg = &sync.WaitGroup{}

	b.metrics = []*msg.Metric{}
	for i := 0; i < b.config.Threads; i++ {
		metric := &msg.Metric{}
		b.metrics = append(b.metrics, metric)

		b.wg.Add(1)
		go b.benchmark(metric)
	}

	go func() {
		b.wg.Wait()
		for _, db := range append(b.rwConn, b.roConn...) {
			db.Close()
		}

		b.now = int32(len(b.queries))
		b.wg = nil
	}()

	return nil
}
func (b *benchmarker) cancelBenchmark() error {
	if b.wg == nil {
		return fmt.Errorf("No job is working")
	}

	b.now = int32(len(b.queries))
	b.wg.Wait()

	return nil
}

func (b *benchmarker) benchmark(metric *msg.Metric) {
	metric.Start = time.Now()
	log.Println("benchmark thread was spawned")

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
			log.Printf("db.Query failed: %v\n", err)
			metric.Fail += 1
		} else {
			metric.Success += 1
			rows.Close()
		}
	}

	metric.Finish = time.Now()
	log.Println("benchmark thread was terminated")
	b.wg.Done()
}
