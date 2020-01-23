package duplicate

import (
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
		ptr int64
		wg  *sync.WaitGroup
		pb  *pb.ProgressBar

		ent []*Entry
		st  *msg.Strategy
	}
)

func duplicate(entries []*Entry) *msg.Strategy {
	templates := make([]*msg.Template, len(entries))
	fragments := []*msg.Fragment{}

	for ref, ent := range entries {
		templates[ref] = &msg.Template{RO: ent.ReadOnly, SQL: ent.Query}

		count := int(float32(ent.Count) * ent.Ratio)
		for i := 0; i < count; i++ {
			fragments = append(fragments, &msg.Fragment{Reference: ref})
		}
	}

	total := len(fragments)
	rand.Shuffle(total, func(i, j int) { fragments[i], fragments[j] = fragments[j], fragments[i] })

	d := &duplicator{
		ptr: -1,
		wg:  &sync.WaitGroup{},
		pb:  pb.Full.Start(total),
		ent: entries,
		st:  &msg.Strategy{Templates: templates, Fragments: fragments},
	}

	d.wg.Add(PROCESSOR_NUM)
	for i := 0; i < PROCESSOR_NUM; i++ {
		go d.process()
	}

	d.wg.Wait()
	d.pb.Write().Finish()

	return d.st
}

func (d *duplicator) process() {
	for {
		i := atomic.AddInt64(&d.ptr, 1)
		if int(i) >= len(d.st.Fragments) {
			break
		}

		frag := d.st.Fragments[i]
		ent := d.ent[frag.Reference]

		frag.Arguments = make([]interface{}, len(ent.Replace))
		for ia, rep := range ent.Replace {
			args := []string{}
			if rep.Args != nil {
				args = rep.Args
			}

			var err error
			frag.Arguments[ia], err = dummy.Get(rep.Key, args...)
			if err != nil {
				log.Printf("getDummy failed: %v\n", err)
				continue
			}
		}

		d.pb.Increment()
	}
	d.wg.Done()
}
