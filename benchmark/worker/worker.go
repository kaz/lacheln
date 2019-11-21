package worker

import (
	"encoding/gob"
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

		config *msg.WorkerConfig
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

	msgType, err := msg.ReadMessageType(c)
	if err != nil {
		return fmt.Errorf("msg.ReadMessageType failed: %w", err)
	}

	switch msgType {
	case msg.MESSAGE_SYNC_CONFIG:
		message := &msg.SyncConfigMessage{}
		if err := gob.NewDecoder(c).Decode(message); err != nil {
			return fmt.Errorf("gob.NewDecoder.Decode failed: %w", err)
		}

		w.config = message.Config
		fmt.Printf("received config: %v\n", w.config)

	default:
		return fmt.Errorf("unexpected message type: %v", msgType)
	}

	return nil
}