package worker

import (
	"fmt"
	"log"
	"net"

	"github.com/kaz/lacheln/benchmark/msg"
	"github.com/urfave/cli/v2"
)

type (
	worker struct {
		listener    net.Listener
		benchmarker *benchmarker
	}
)

func Action(context *cli.Context) error {
	port := context.String("port")
	if port == "" {
		port = "6060"
	}

	listener, err := net.Listen("tcp4", ":"+port)
	if err != nil {
		return fmt.Errorf("net.Listen failed: %w", err)
	}

	fmt.Println("listening on " + port)

	w := &worker{
		listener,
		NewBenchmarker(),
	}
	return w.Start()
}

func (w *worker) Start() error {
	for {
		conn, err := w.listener.Accept()
		if err != nil {
			return fmt.Errorf("listener.Accept failed: %w", err)
		}
		go w.handle(conn)
	}
}

func (w *worker) handle(c net.Conn) {
	defer c.Close()
	defer func() {
		if err := recover(); err != nil {
			detail := fmt.Sprintf("handling error: %v", err)
			log.Println(detail)

			if err := msg.Send(c, &msg.AcknowledgedMessage{Status: "NG", Detail: detail}); err != nil {
				log.Printf("sending error message failed: msg.Send failed: %v\n", err)
			}
		}
	}()

	rawBody, err := msg.Receive(c)
	if err != nil {
		panic(fmt.Errorf("msg.Receive failed: %w", err))
	}

	var resp interface{}

	if body, ok := rawBody.(*msg.PutStrategyMessage); ok {
		if err := w.benchmarker.PutStrategy(body.Strategy, body.Reset); err != nil {
			panic(fmt.Errorf("benchmarker.PutStrategy failed: %w", err))
		}
		resp = &msg.AcknowledgedMessage{Status: "OK", Detail: fmt.Sprintf("received strategy: %v templates, %v fragments", len(w.benchmarker.Strategy.Templates), len(w.benchmarker.Strategy.Fragments))}
	} else if body, ok := rawBody.(*msg.BenchmarkJobMessage); ok {
		switch body.Mode {
		case "start":
			if err := w.benchmarker.Start(body.Config, body.StartAt); err != nil {
				panic(fmt.Errorf("benchmarker.Start failed: %w", err))
			}
			resp = &msg.AcknowledgedMessage{Status: "OK", Detail: fmt.Sprintf("benchmark was started with config: %+v", body.Config)}
		case "cancel":
			if err := w.benchmarker.Cancel(); err != nil {
				panic(fmt.Errorf("benchmarker.Cancel failed: %w", err))
			}
			resp = &msg.AcknowledgedMessage{Status: "OK", Detail: "benchmark was cancelled"}
		default:
			panic(fmt.Errorf("unexpected mode: %v", body.Mode))
		}
	} else if _, ok := rawBody.(*msg.MetricsRequestMessage); ok {
		resp = &msg.MetricsResponseMessage{
			Metric: w.benchmarker.Metric,
		}
	} else {
		panic(fmt.Errorf("unexpected message type: %v", rawBody))
	}

	if err := msg.Send(c, resp); err != nil {
		panic(fmt.Errorf("msg.Send failed: %w", err))
	}

	if ack, ok := resp.(*msg.AcknowledgedMessage); ok {
		log.Println(ack.Detail)
	}
}
