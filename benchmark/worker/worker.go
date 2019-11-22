package worker

import (
	"fmt"
	"net"
	"os"
	"os/signal"

	"github.com/kaz/sql-replay/benchmark/msg"
	"github.com/urfave/cli/v2"
)

type (
	worker struct {
		listener net.Listener
		benchmarker
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

	w := &worker{listener: listener}
	return w.Start()
}

func (w *worker) Start() error {
	go func() {
		ch := make(chan os.Signal)
		signal.Notify(ch, os.Interrupt)
		<-ch
		os.Exit(0)
	}()

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
			fmt.Fprintln(os.Stderr, detail)

			if err := msg.Send(c, &msg.AcknowledgedMessage{Status: "NG", Detail: detail}); err != nil {
				fmt.Fprintf(os.Stderr, "sending error message failed: msg.Send failed: %v", err)
			}
		}
	}()

	rawBody, err := msg.Receive(c)
	if err != nil {
		panic(fmt.Errorf("msg.Receive failed: %w", err))
	}

	var resp interface{}

	if body, ok := rawBody.(*msg.PutQueryMessage); ok {
		w.queries = body.Query

		detail := fmt.Sprintf("received %v queries", len(w.queries))
		resp = &msg.AcknowledgedMessage{Status: "OK", Detail: detail}

		fmt.Println(detail)
	} else if body, ok := rawBody.(*msg.BenchmarkStartMessage); ok {
		w.config = body.Config
		if err := w.startBenchmark(); err != nil {
			panic(fmt.Errorf("starting benchmark failed: %w", err))
		}

		detail := fmt.Sprintf("benchmark was started with config: %+v", w.config)
		resp = &msg.AcknowledgedMessage{Status: "OK", Detail: detail}

		fmt.Println(detail)
	} else {
		panic(fmt.Errorf("unexpected message type: %v", rawBody))
	}

	if err := msg.Send(c, resp); err != nil {
		panic(fmt.Errorf("msg.Send failed: %w", err))
	}
}
