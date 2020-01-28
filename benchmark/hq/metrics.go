package hq

import (
	"fmt"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/cheggaaa/pb/v3"
	"github.com/kaz/lacheln/benchmark/msg"
	"github.com/urfave/cli/v2"
)

type (
	collector struct {
		workers []string

		mu *sync.Mutex

		total   int64
		current int64
		qps     map[uint16]int64
		latency []uint16
	}
)

func ActionMetrics(context *cli.Context) error {
	conf, err := readConfig(context.String("config"))
	if err != nil {
		return fmt.Errorf("readConfig failed: %w", err)
	}

	c := &collector{
		workers: conf.Workers,
		mu:      &sync.Mutex{},
	}

	switch context.String("mode") {
	case "progress":
		c.Progress()
	case "result":
		c.Result()
	case "graph":
		c.Graph()
	default:
		return fmt.Errorf("no such mode: %v", context.String("mode"))
	}

	return nil
}

func (c *collector) Progress() {
	progress := pb.Full.New(0).Start()

	for {
		c.fetch()

		progress.SetTotal(c.total).SetCurrent(c.current)
		if c.current >= c.total {
			progress.Write()
			break
		}

		time.Sleep(1 * time.Second)
	}
}
func (c *collector) Result() {
	c.fetch()

	var qpsSum uint64
	for _, value := range c.qps {
		qpsSum += uint64(value)
	}

	fmt.Printf("%9.2f %% (%d/%d)\n", 100*float64(c.current)/float64(c.total), c.current, c.total)
	fmt.Printf("%9.0f q/s\n\n", float64(qpsSum)/float64(len(c.qps)))

	sort.Slice(c.latency, func(i, j int) bool { return c.latency[i] < c.latency[j] })

	var latencySum uint64
	for _, value := range c.latency {
		latencySum += uint64(value)
	}

	fmt.Printf("average latency: %6.0f ms\n", float64(latencySum)/float64(len(c.latency)))
	fmt.Printf("50perc. latency: %6.0d ms\n", c.latency[int(0.50*float64(len(c.latency)))])
	fmt.Printf("75perc. latency: %6.0d ms\n", c.latency[int(0.75*float64(len(c.latency)))])
	fmt.Printf("90perc. latency: %6.0d ms\n", c.latency[int(0.90*float64(len(c.latency)))])
	fmt.Printf("95perc. latency: %6.0d ms\n", c.latency[int(0.95*float64(len(c.latency)))])
	fmt.Printf("99perc. latency: %6.0d ms\n", c.latency[int(0.99*float64(len(c.latency)))])
}
func (c *collector) Graph() {
	c.fetch()

	data := make([][2]int64, 0, len(c.qps))
	for key, value := range c.qps {
		data = append(data, [2]int64{int64(key), value})
	}

	sort.Slice(data, func(i, j int) bool { return data[i][0] < data[j][0] })

	for _, kv := range data {
		fmt.Printf("%d\t%d\n", kv[0], kv[1])
	}
}

func (c *collector) fetch() {
	c.mu = &sync.Mutex{}
	c.total = 0
	c.current = 0
	c.qps = make(map[uint16]int64)

	broadcast(c.workers, c.collect)
}

func (c *collector) collect(i int, worker string) error {
	conn, err := net.Dial("tcp4", worker)
	if err != nil {
		return fmt.Errorf("new.Dial failed: %w", err)
	}
	defer conn.Close()

	if err := msg.Send(conn, &msg.MetricsRequestMessage{}); err != nil {
		return fmt.Errorf("msg.Send failed: %w", err)
	}

	raw, err := msg.Receive(conn)
	if err != nil {
		return fmt.Errorf("msg.Receive failed: %w", err)
	}

	resp, ok := raw.(*msg.MetricsResponseMessage)
	if !ok {
		return fmt.Errorf("unexpected message: %v", raw)
	}

	c.merge(resp.Metric)
	return nil
}

func (c *collector) merge(metric *msg.Metric) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.total += metric.Total

	for _, tss := range metric.Timestamp {
		for _, ts := range tss {
			if ts[0] != 0 {
				c.current += 1
				c.qps[ts[0]] += 1
				c.latency = append(c.latency, ts[1])
			}
		}
	}
}
