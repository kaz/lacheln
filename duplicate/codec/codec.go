package codec

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"sync"

	"github.com/kaz/lacheln/benchmark/msg"
)

const (
	CHUNKS = 720
)

func Serialize(w io.Writer, strategy *msg.Strategy) error {
	wg := &sync.WaitGroup{}
	chErr := make(chan error)
	chData := make(chan []byte)
	chReturn := make(chan bool)

	size := len(strategy.Fragments) / CHUNKS

	for i := 0; i < CHUNKS; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			last := (i + 1) * size
			if i+1 == CHUNKS {
				last = len(strategy.Fragments)
			}

			buf := bytes.NewBuffer(nil)
			err := msg.Send(buf, &msg.PutStrategyMessage{
				Strategy: &msg.Strategy{
					Templates: strategy.Templates,
					Fragments: strategy.Fragments[i*size : last],
				},
			})
			if err != nil {
				chErr <- fmt.Errorf("msg.Send failed: %w", err)
				return
			}

			chData <- buf.Bytes()
		}(i)
	}

	go func() {
		wg.Wait()
		chReturn <- true
	}()

	for {
		select {
		case err := <-chErr:
			return err
		case <-chReturn:
			return nil
		case data := <-chData:
			if err := binary.Write(w, binary.BigEndian, int64(len(data))); err != nil {
				return fmt.Errorf("binary.Write failed: %w", err)
			}
			if _, err := w.Write(data); err != nil {
				return fmt.Errorf("w.Write failed: %w", err)
			}
		}
	}
}

func Deserialize(r io.Reader) chan msg.TypedRawMessage {
	ch := make(chan msg.TypedRawMessage)

	go func() {
		for i := 0; i < CHUNKS; i++ {
			var size int64
			if err := binary.Read(r, binary.BigEndian, &size); err != nil {
				log.Printf("binary.Read failed: %v\n", err)
				return
			}

			raw := make(msg.TypedRawMessage, size)
			if _, err := r.Read(raw); err != nil {
				log.Printf("r.Read failed: %v\n", err)
				return
			}

			ch <- raw
		}
		close(ch)
	}()

	return ch
}
