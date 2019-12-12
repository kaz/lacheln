package hq

import (
	"fmt"
	"math"
	"net"
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

		spec   *msg.Spec
		metric *msg.Metric
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

	if context.Bool("progress") {
		c.Progress()
	} else {
		c.Oneshot()
	}

	return nil
}

func (c *collector) Progress() {
	progress := pb.Full.New(0).Start()

	for {
		c.fetch()

		progress.SetTotal(int64(c.spec.Total)).SetCurrent(int64(c.spec.Current))
		if c.spec.Current >= c.spec.Total {
			progress.Write()
			break
		}

		time.Sleep(1 * time.Second)
	}
}
func (c *collector) Oneshot() {
	c.fetch()

	var min int64 = math.MaxInt64
	var max int64 = math.MinInt64

	for key, _ := range c.metric.Processed {
		if key < min {
			min = key
		}
		if key > max {
			max = key
		}
	}

	fmt.Printf("%9.2f %% (%d/%d)\n", 100*float64(c.spec.Current)/float64(c.spec.Total), c.spec.Current, c.spec.Total)
	fmt.Printf("%9.0f q/s\n", float64(c.spec.Current)/float64(max-min))
}

func (c *collector) fetch() {
	c.spec = &msg.Spec{}
	c.metric = &msg.Metric{Processed: make(map[int64]int64)}
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

	c.merge(resp.Spec, resp.Metrics)
	return nil
}

func (c *collector) merge(spec *msg.Spec, metrics []*msg.Metric) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.spec.Total += spec.Total
	c.spec.Current += spec.Current

	for _, metric := range metrics {
		for key, value := range metric.Processed {
			if _, ok := c.metric.Processed[key]; !ok {
				c.metric.Processed[key] = 0
			}
			c.metric.Processed[key] += value
		}
	}
}
