package hq

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"time"

	"github.com/cheggaaa/pb/v3"
	"github.com/kaz/sql-replay/benchmark/msg"
	"github.com/urfave/cli/v2"
)

type (
	collector struct {
		specCh       chan *msg.Spec
		mergedSpec   *msg.Spec
		metricCh     chan *msg.Metric
		mergedMetric *msg.Metric
	}
)

func ActionMetrics(context *cli.Context) error {
	conf, err := readConfig(context.String("config"))
	if err != nil {
		return fmt.Errorf("readConfig failed: %w", err)
	}

	c := &collector{make(chan *msg.Spec), &msg.Spec{}, make(chan *msg.Metric), &msg.Metric{}}
	go c.startReceiver()

	if context.Bool("progress") {
		c.Progress(conf)
	} else {
		c.Oneshot(conf)
	}

	return nil
}

func (c *collector) Progress(conf *config) {
	go func() {
		ch := make(chan os.Signal)
		signal.Notify(ch, os.Interrupt)
		<-ch
		os.Exit(0)
	}()

	progress := pb.New(0).Start()

	for {
		c.mergedSpec = &msg.Spec{}
		c.mergedMetric = &msg.Metric{}
		broadcast(conf.Workers, c.collect)

		progress.SetTotal(int64(c.mergedSpec.Total)).SetCurrent(int64(c.mergedSpec.Current))
		time.Sleep(1 * time.Second)
	}
}
func (c *collector) Oneshot(conf *config) {
	broadcast(conf.Workers, c.collect)

	fmt.Printf("Progress  : %7.2f %% (%d/%d)\n", 100*float64(c.mergedSpec.Current)/float64(c.mergedSpec.Total), c.mergedSpec.Current, c.mergedSpec.Total)
	fmt.Printf("Failed    : %9d\n", c.mergedMetric.Fail)
	fmt.Printf("Succeeded : %9d\n", c.mergedMetric.Success)

	finish := c.mergedMetric.Finish
	if finish.IsZero() {
		finish = time.Now()
	}
	fmt.Printf("QPS       : %9.0f\n", float64(c.mergedMetric.Success)/finish.Sub(c.mergedMetric.Start).Seconds())
}

func (c *collector) startReceiver() {
	for {
		select {
		case spec := <-c.specCh:
			c.mergedSpec.Total += spec.Total
			c.mergedSpec.Current += spec.Current
		case metric := <-c.metricCh:
			c.mergedMetric.Fail += metric.Fail
			c.mergedMetric.Success += metric.Success

			if c.mergedMetric.Start.IsZero() || metric.Start.Before(c.mergedMetric.Start) {
				c.mergedMetric.Start = metric.Start
			}
			if c.mergedMetric.Finish.IsZero() || metric.Finish.After(c.mergedMetric.Finish) {
				c.mergedMetric.Finish = metric.Finish
			}
		}
	}
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

	c.specCh <- resp.Spec
	for _, m := range resp.Metrics {
		c.metricCh <- m
	}

	return nil
}
