package duplicate

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"

	"github.com/cheggaaa/pb/v3"
	"github.com/kaz/sql-replay/benchmark/msg"
)

type (
	duplicator struct {
		entries []*Entry
		queries []*msg.Query

		ch chan *Entry
		mu *sync.Mutex
		wg *sync.WaitGroup
		pb *pb.ProgressBar
	}
)

func newDuplicator(entries []*Entry) *duplicator {
	return &duplicator{entries: entries}
}

func (d *duplicator) handleInterruptSignal() {
	ch := make(chan os.Signal)
	signal.Notify(ch, os.Interrupt)
	<-ch
	os.Exit(0)
}

func (d *duplicator) duplicate() {
	go d.handleInterruptSignal()

	total := 0
	for _, ent := range d.entries {
		total += ent.Count
	}

	d.queries = make([]*msg.Query, 0, total)

	d.ch = make(chan *Entry)
	d.mu = &sync.Mutex{}
	d.wg = &sync.WaitGroup{}
	d.pb = pb.Full.Start(total)

	for i := 0; i < 2048; i++ {
		d.wg.Add(1)
		go d.process()
	}

	for _, ent := range d.entries {
		for i := 0; i < ent.Count; i++ {
			d.ch <- ent
		}
	}

	close(d.ch)
	d.wg.Wait()
	d.pb.Write().Finish()
}

func (d *duplicator) process() {
	for ent := range d.ch {
		vals := []interface{}{}
		if ent.Replace != nil {
			for _, rep := range ent.Replace {
				args := []string{}
				if rep.Args != nil {
					args = rep.Args
				}

				dummy, err := getDummy(rep.Key, args...)
				if err != nil {
					log.Printf("getDummy failed: %v\n", err)
					continue
				}

				vals = append(vals, dummy)
			}
		}

		d.mu.Lock()
		d.queries = append(d.queries, &msg.Query{RO: ent.ReadOnly, SQL: fmt.Sprintf(ent.Query, vals...)})
		d.mu.Unlock()

		d.pb.Increment()
	}
	d.wg.Done()
}
