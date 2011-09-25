package termite

import (
	"log"
	"time"
)

var _ = log.Println

type PeriodicSampler struct {
	curr        int
	dtNs        int64
	samples     []interface{}
	measureFunc func() interface{}

	stop bool
}

func NewPeriodicSampler(period float64, samples int, measure func() interface{}) *PeriodicSampler {
	me := &PeriodicSampler{
		samples:     make([]interface{}, samples),
		measureFunc: measure,
		dtNs:        int64(period * 1e9),
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
		time.Sleep(me.dtNs)
	}
}

func (me *PeriodicSampler) Stop() {
	me.stop = true
}

func (me *PeriodicSampler) Values() []interface{} {
	curr := me.curr
	l := len(me.samples)

	v := make([]interface{}, 0, l)
	for i := (curr + 1) % l; i != curr; i = (i + 1) % l {
		if me.samples[i] != nil {
			v = append(v, me.samples[i])
		}
	}
	return v
}
