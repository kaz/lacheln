package duplicate

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/cheggaaa/pb/v3"
	"github.com/kaz/sql-replay/benchmark/msg"
)

type (
	duplicator struct {
		entries []*Entry
		queries []*msg.Query

		chEnt chan *Entry
		chQue chan *msg.Query

		pb *pb.ProgressBar
	}
)

func newDuplicator(entries []*Entry) *duplicator {
	return &duplicator{entries, []*msg.Query{}, make(chan *Entry), make(chan *msg.Query), nil}
}

func (d *duplicator) duplicate() {
	go d.signalHandle()
	go d.progress()
	go d.receive()
	for i := 0; i < 512; i++ {
		go d.process()
	}
	d.dispatch()
}
func (d *duplicator) signalHandle() {
	ch := make(chan os.Signal)
	signal.Notify(ch, os.Interrupt)
	<-ch
	os.Exit(0)
}
func (d *duplicator) progress() {
	var total int64
	for _, ent := range d.entries {
		total += int64(ent.Count)
	}

	d.pb = pb.Full.Start64(total)

	for d.pb.Current() < total {
		time.Sleep(1 * time.Second)
	}

	d.pb.Write().Finish()
	close(d.chEnt)
	close(d.chQue)
}
func (d *duplicator) dispatch() {
	for _, ent := range d.entries {
		for i := 0; i < ent.Count; i++ {
			d.chEnt <- ent
		}
	}
}
func (d *duplicator) process() {
	for ent := range d.chEnt {
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
					d.chEnt <- ent
					return
				}

				vals = append(vals, dummy)
			}
		}
		d.chQue <- &msg.Query{RO: ent.ReadOnly, SQL: fmt.Sprintf(ent.Query, vals...)}
	}
}
func (d *duplicator) receive() {
	for que := range d.chQue {
		d.queries = append(d.queries, que)
		d.pb.Increment()
	}
}
