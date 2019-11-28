package duplicate

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"

	"github.com/cheggaaa/pb/v3"
	"github.com/kaz/lacheln/benchmark/msg"
	"github.com/kaz/lacheln/duplicate/dummy"
)

const (
	PROCESSOR_NUM = 2048
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

func duplicate(entries []*Entry) []*msg.Query {
	for _, ent := range entries[:] {
		count := int(float32(ent.Count-1) * ent.Ratio)
		for i := 0; i < count; i++ {
			entries = append(entries, ent)
		}
	}

	total := len(entries)
	rand.Shuffle(total, func(i, j int) { entries[i], entries[j] = entries[j], entries[i] })

	d := &duplicator{
		entries: entries,
		queries: make([]*msg.Query, total),
		ptr:     -1,
		wg:      &sync.WaitGroup{},
		pb:      pb.Full.Start(total),
	}

	d.wg.Add(PROCESSOR_NUM)
	for i := 0; i < PROCESSOR_NUM; i++ {
		go d.process()
	}

	d.wg.Wait()
	d.pb.Write().Finish()

	return d.queries
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

				val, err := dummy.Get(rep.Key, args...)
				if err != nil {
					log.Printf("getDummy failed: %v\n", err)
					continue
				}

				vals = append(vals, val)
			}
		}

		d.queries[i] = &msg.Query{RO: ent.ReadOnly, SQL: fmt.Sprintf(ent.Query, vals...)}
		d.pb.Increment()
	}
	d.wg.Done()
}
