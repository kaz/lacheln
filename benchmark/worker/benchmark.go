package worker

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/kaz/lacheln/benchmark/msg"
)

type (
	benchmarker struct {
		cancelled bool
		wg        *sync.WaitGroup

		Metric *msg.Metric
	}
)

func getConnections(servers []string) ([]*sql.DB, error) {
	connections := make([]*sql.DB, len(servers))
	for i, dsn := range servers {
		conn, err := sql.Open("mysql", dsn)
		if err != nil {
			return nil, fmt.Errorf("sql.Open failed: %w", err)
		}
		connections[i] = conn
	}

	rand.Shuffle(len(connections), func(i, j int) { connections[i], connections[j] = connections[j], connections[i] })
	return connections, nil
}

func (b *benchmarker) Start(config *msg.BenchmarkConfig, queries []*msg.Query) error {
	if len(queries) < 1 {
		return fmt.Errorf("No query")
	}
	if b.wg != nil {
		return fmt.Errorf("Job is already working")
	}

	b.cancelled = false
	b.wg = &sync.WaitGroup{}

	b.Metric = &msg.Metric{
		Total: int64(len(queries)),
		TS:    [][]int64{},
	}

	size := len(queries) / config.Threads

	for i := 0; i < config.Threads; i++ {
		roConn, err := getConnections(config.ROServers)
		if err != nil {
			return fmt.Errorf("getConnections failed: %w", err)
		}

		rwConn, err := getConnections(config.RWServers)
		if err != nil {
			return fmt.Errorf("getConnections failed: %w", err)
		}

		last := (i + 1) * size
		if i+1 == config.Threads {
			last = len(queries)
		}
		chunk := queries[i*size : last]

		ts := make([]int64, len(chunk))
		b.Metric.TS = append(b.Metric.TS, ts)

		b.wg.Add(1)
		go b.run(rwConn, roConn, chunk, ts)
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

func (b *benchmarker) run(rwConn, roConn []*sql.DB, queries []*msg.Query, ts []int64) {
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

		ts[i] = time.Now().Unix()

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
