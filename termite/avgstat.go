package termite
import (
	"time"
)

type AvgStat struct {
	curr    int
	dtNs    int64
	samples []float64
	measureFunc  func() float64

	stop    bool
}

func NewAvgStat(period float64, samples int, measure func() float64) *AvgStat {
	me := &AvgStat{
		samples: make([]float64, samples),
		measureFunc: measure,
		dtNs: int64(period * 1e9),
	}
	go me.sample()
	return me
}

func (me *AvgStat) sample() {
	for !me.stop {
		time.Sleep(me.dtNs)
		m := me.measureFunc()
		me.curr = (me.curr + 1) % len(me.samples)
		me.samples[me.curr] = m
	}
}

func (me *AvgStat) Stop() {
	me.stop = true
}
func (me *AvgStat) Values(delta bool) []float64 {
	curr := me.curr
	l := len(me.samples)

	v := make([]float64, l)
	j := 0
	for i := (curr + 1) % l; i != curr; i = (i + 1) % l {
		v[j] = me.samples[i]
		j++
	}

	if delta {
		for j := len(v) - 1; j > 0; j-- {
			v[j] -= v[j-1]
		}
		v = v[1:]
	}
	return v
}

