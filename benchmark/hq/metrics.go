package hq

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/cheggaaa/pb/v3"
	"github.com/kaz/sql-replay/benchmark/msg"
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
	go func() {
		ch := make(chan os.Signal)
		signal.Notify(ch, os.Interrupt)
		<-ch
		os.Exit(0)
	}()

	progress := pb.Full.New(0).Start()

	for {
		c.fetch()
		progress.SetTotal(int64(c.spec.Total)).SetCurrent(int64(c.spec.Current))
		time.Sleep(1 * time.Second)
	}
}
func (c *collector) Oneshot() {
	c.fetch()

	fmt.Printf("Progress  : %7.2f %% (%d/%d)\n", 100*float64(c.spec.Current)/float64(c.spec.Total), c.spec.Current, c.spec.Total)
	fmt.Printf("Failed    : %9d\n", c.metric.Fail)
	fmt.Printf("Succeeded : %9d\n", c.metric.Success)

	finish := c.metric.Finish
	if finish.IsZero() {
		finish = time.Now()
	}
	fmt.Printf("QPS       : %9.0f\n", float64(c.metric.Success)/finish.Sub(c.metric.Start).Seconds())
}

func (c *collector) fetch() {
	c.spec = &msg.Spec{}
	c.metric = &msg.Metric{}
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
		c.metric.Fail += metric.Fail
		c.metric.Success += metric.Success

		if c.metric.Start.IsZero() || metric.Start.Before(c.metric.Start) {
			c.metric.Start = metric.Start
		}
		if c.metric.Finish.IsZero() || metric.Finish.After(c.metric.Finish) {
			c.metric.Finish = metric.Finish
		}
	}
}
