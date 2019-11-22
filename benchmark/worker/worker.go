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

		config  *msg.WorkerConfig
		queries []*msg.Query
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
		go func() {
			if err := w.handle(conn); err != nil {
				fmt.Fprintf(os.Stderr, "handle exits with error: %v\n", err)
			}
		}()
	}
}

func (w *worker) handle(c net.Conn) error {
	defer c.Close()

	rawBody, err := msg.Receive(c)
	if err != nil {
		return fmt.Errorf("msg.Receive failed: %w", err)
	}

	var resp interface{}

	if body, ok := rawBody.(*msg.SyncConfigMessage); ok {
		w.config = body.Config

		detail := fmt.Sprintf("received config: %+v", w.config)
		resp = &msg.AcknowledgedMessage{Status: "OK", Detail: detail}

		fmt.Println(detail)
	} else if body, ok := rawBody.(*msg.PutQueryMessage); ok {
		w.queries = body.Query

		detail := fmt.Sprintf("received %v queries", len(w.queries))
		resp = &msg.AcknowledgedMessage{Status: "OK", Detail: detail}

		fmt.Println(detail)
	} else {
		return fmt.Errorf("unexpected message type: %v", rawBody)
	}

	if err := msg.Send(c, resp); err != nil {
		return fmt.Errorf("msg.Send failed: %w", err)
	}

	return nil
}
