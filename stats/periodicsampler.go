package stats

import (
	"log"
	"time"
)

var _ = log.Println

type Sample interface {
	SubtractSample(Sample)
	CopySample() Sample
	TableHeader() string
	TableRow() string
}

type PeriodicSampler struct {
	curr        int
	dt          time.Duration
	samples     []Sample
	measureFunc func() Sample

	stop bool
}

func NewPeriodicSampler(period time.Duration, samples int, measure func() Sample) *PeriodicSampler {
	me := &PeriodicSampler{
		samples:     make([]Sample, samples),
		measureFunc: measure,
		dt:          period,
	}
	go me.sample()
	return me
}

func (me *PeriodicSampler) sample() {
	for !me.stop {
		m := me.measureFunc()
		if m == nil {
			continue
		}

		me.curr = (me.curr + 1) % len(me.samples)
		me.samples[me.curr] = m
		time.Sleep(me.dt)
	}
}

func (me *PeriodicSampler) Stop() {
	// TODO - should use synchronization to be really correct.
	me.stop = true
}

func (me *PeriodicSampler) Values() []Sample {
	curr := me.curr
	l := len(me.samples)

	v := make([]Sample, 0, l)
	for i := (curr + 1) % l; i != curr; i = (i + 1) % l {
		if me.samples[i] != nil {
			v = append(v, me.samples[i])
		}
	}
	return v
}

func (me *PeriodicSampler) Diffs() (out []Sample) {
	vals := me.Values()
	var last Sample
	for _, v := range vals {
		c := v.CopySample()
		if last != nil {
			c.SubtractSample(last)
			out = append(out, c)
		}
		last = v
	}
	return out
}
