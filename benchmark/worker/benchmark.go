package worker

import (
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/kaz/lacheln/benchmark/msg"
)

type (
	benchmarker struct {
		cancelled bool
		wg        *sync.WaitGroup
		mu        *sync.Mutex

		startAt time.Time
		conns   map[bool][]*sql.DB

		Strategy *msg.Strategy
		Metric   *msg.Metric
	}

	benchmarkThread struct {
		parent *benchmarker

		fragments []*msg.Fragment
		stamps    [][2]uint16
	}
)

func NewBenchmarker() *benchmarker {
	return &benchmarker{
		cancelled: false,
		wg:        &sync.WaitGroup{},
		mu:        &sync.Mutex{},
	}
}

func (b *benchmarker) PutStrategy(strategy *msg.Strategy, reset bool) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if reset || b.Strategy == nil {
		b.Strategy = strategy
		return nil
	}

	if len(strategy.Templates) != len(b.Strategy.Templates) {
		return fmt.Errorf("template count is not match")
	}

	b.Strategy.Fragments = append(b.Strategy.Fragments, strategy.Fragments...)
	return nil
}

func (b *benchmarker) Start(config *msg.BenchmarkConfig, startAt time.Time) error {
	if b.Strategy.Fragments == nil {
		return fmt.Errorf("No strategy")
	}
	if b.wg != nil {
		return fmt.Errorf("Job is already working")
	}

	b.startAt = startAt
	b.conns = map[bool][]*sql.DB{
		true:  []*sql.DB{},
		false: []*sql.DB{},
	}
	for _, server := range config.Servers {
		conn, err := sql.Open("mysql", server.DSN)
		if err != nil {
			return fmt.Errorf("sql.Open failed: %w", err)
		}

		conn.SetMaxOpenConns(server.MaxOpenConns)
		conn.SetMaxIdleConns(server.MaxIdleConns)
		conn.SetConnMaxLifetime(server.ConnMaxLifetime * time.Millisecond)
		b.conns[server.RO] = append(b.conns[server.RO], conn)
	}

	b.Metric = &msg.Metric{
		Total:     int64(len(b.Strategy.Fragments)),
		Timestamp: [][][2]uint16{},
	}

	size := len(b.Strategy.Fragments) / config.Threads

	for i := 0; i < config.Threads; i++ {
		last := (i + 1) * size
		if i+1 == config.Threads {
			last = len(b.Strategy.Fragments)
		}
		fragments := b.Strategy.Fragments[i*size : last]

		stamps := make([][2]uint16, len(fragments))
		b.Metric.Timestamp = append(b.Metric.Timestamp, stamps)

		bt := &benchmarkThread{
			parent:    b,
			fragments: fragments,
			stamps:    stamps,
		}

		b.wg.Add(1)
		go bt.Run()
	}

	go func() {
		b.wg.Wait()
		b.wg = nil

		for _, pool := range b.conns {
			for _, db := range pool {
				db.Close()
			}
		}
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

func (bt *benchmarkThread) Run() {
	log.Println("benchmark thread was spawned")

	for i, frag := range bt.fragments {
		template := bt.parent.Strategy.Templates[frag.Reference]
		query := fmt.Sprintf(template.SQL, frag.Arguments...)
		pool := bt.parent.conns[template.RO]

		queryStartAt := time.Now()

		rows, err := pool[i%len(pool)].Query(query)
		if err != nil {
			log.Printf("db.Query failed: %v\n", err)
			continue
		}
		for rows.Next() {
		}

		bt.stamps[i] = [2]uint16{
			uint16(queryStartAt.Sub(bt.parent.startAt).Seconds()),
			uint16(time.Now().Sub(queryStartAt).Milliseconds()),
		}

		if bt.parent.cancelled {
			break
		}
	}

	log.Println("benchmark thread was terminated")
	bt.parent.wg.Done()
}
