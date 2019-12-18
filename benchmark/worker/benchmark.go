package worker

import (
	"database/sql"
	"fmt"
	"log"
	"sync"

	_ "github.com/go-sql-driver/mysql"
	"github.com/kaz/lacheln/benchmark/msg"
)

type (
	benchmarker struct {
		wg        *sync.WaitGroup
		cancelled bool
	}
)

func (b *benchmarker) Start(config *msg.BenchmarkConfig, queries []*msg.Query) error {
	if len(queries) < 1 {
		return fmt.Errorf("No query")
	}
	if b.wg != nil {
		return fmt.Errorf("Job is already working")
	}

	b.cancelled = false
	b.wg = &sync.WaitGroup{}

	size := len(queries) / config.Threads

	for i := 0; i < config.Threads; i++ {
		rwConn := []*sql.DB{}
		for _, h := range config.RWServers {
			conn, err := sql.Open("mysql", h.DSN)
			if err != nil {
				return fmt.Errorf("sql.Open failed: %w", err)
			}
			rwConn = append(rwConn, conn)
		}

		roConn := []*sql.DB{}
		for _, h := range config.ROServers {
			conn, err := sql.Open("mysql", h.DSN)
			if err != nil {
				return fmt.Errorf("sql.Open failed: %w", err)
			}
			roConn = append(roConn, conn)
		}

		last := (i + 1) * size
		if i+1 == config.Threads {
			last = len(queries)
		}

		b.wg.Add(1)
		go b.run(rwConn, roConn, queries[i*size:last])
	}

	go func() {
		b.wg.Wait()
		b.wg = nil
	}()

	return nil
}
func (b *benchmarker) Cancel() error {
	if b.wg == nil {
		return fmt.Errorf("No job is working")
	}

	b.cancelled = true
	b.wg.Wait()

	return nil
}

func (b *benchmarker) run(rwConn, roConn []*sql.DB, queries []*msg.Query) {
	log.Println("benchmark thread was spawned")

	for i, query := range queries {
		db := roConn[i%len(roConn)]
		if !query.RO {
			db = rwConn[i%len(rwConn)]
		}

		rows, err := db.Query(query.SQL)
		if err != nil {
			log.Printf("db.Query failed: %v\n", err)
			continue
		}
		rows.Close()

		if b.cancelled {
			break
		}
	}

	for _, db := range append(rwConn, roConn...) {
		db.Close()
	}

	log.Println("benchmark thread was terminated")
	b.wg.Done()
}
