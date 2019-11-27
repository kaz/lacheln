package duplicate

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"github.com/cheggaaa/pb/v3"
	"github.com/kaz/sql-replay/benchmark/msg"
)

type (
	duplicator struct {
		entries []*Entry
		queries []*msg.Query

		ptr int32
		wg  *sync.WaitGroup
		pb  *pb.ProgressBar
	}
)

func newDuplicator(entries []*Entry) *duplicator {
	flat := []*Entry{}
	for _, ent := range entries {
		count := int(float32(ent.Count) * ent.Ratio)
		for i := 0; i < count; i++ {
			flat = append(flat, ent)
		}
	}

	return &duplicator{entries: flat, queries: make([]*msg.Query, len(flat))}
}

func (d *duplicator) duplicate() {
	d.ptr = -1
	d.wg = &sync.WaitGroup{}
	d.pb = pb.Full.Start(len(d.entries))

	for i := 0; i < 2048; i++ {
		d.wg.Add(1)
		go d.process()
	}

	d.wg.Wait()
	d.pb.Write().Finish()
}

func (d *duplicator) process() {
	for {
		i := atomic.AddInt32(&d.ptr, 1)
		if int(i) >= len(d.entries) {
			break
		}

		ent := d.entries[i]

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

		d.queries[i] = &msg.Query{RO: ent.ReadOnly, SQL: fmt.Sprintf(ent.Query, vals...)}
		d.pb.Increment()
	}
	d.wg.Done()
}
