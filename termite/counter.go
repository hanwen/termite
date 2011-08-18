// TODO - move into a separate package.
package termite

import (
	"log"
	"sync"
)

var _ = log.Println

type MultiResolutionCounter struct {
	buckets [][]int
	timestamp int
	interval int64
	mutex sync.Mutex
}

func NewMultiResolutionCounter(interval int64, now int64, resolutions []int) *MultiResolutionCounter {
	me := &MultiResolutionCounter{}
	me.timestamp = int(now/interval)
	me.interval = interval
	
	for i := 0; i < len(resolutions); i++ {
		l := resolutions[i]
		if l <= 0 {
			panic("resolution must be positive")
		}
		me.buckets = append(me.buckets, make([]int, l))
	}

	return me
}

func (me *MultiResolutionCounter) move(bucketIdx int, count int) int {
	acc := 0
	bucket := me.buckets[bucketIdx]
	for i, _ := range bucket {
		j := len(bucket) - i - 1
		v := bucket[j]
		bucket[j] = 0
		next := j + count
		if next < len(bucket) {
			bucket[next] += v
		} else {
			acc += v
		}
	}
	return acc
}

func (me *MultiResolutionCounter) update(shift int) {
	acc := 0
	for i, b := range me.buckets {
		if shift == 0 {
			b[0] += acc
			break
		}
		acc += me.move(i, int(shift))
		shift /= len(b)
	}		
}

func (me *MultiResolutionCounter) Add(ns int64, events int) {
	me.mutex.Lock()
	defer me.mutex.Unlock()

	ts := int(ns/me.interval)

	dt := ts - me.timestamp
	if dt > 0 {
		me.update(dt)
		me.timestamp = ts
	}
	me.buckets[0][0] += events
}

func sum(b []int) int {
	r := 0
	for _, v := range b {
		r += v
	}
	return r
}

func (me *MultiResolutionCounter) Read() (r []int) {
	me.mutex.Lock()
	defer me.mutex.Unlock()

	r = []int{me.buckets[0][0]}

	acc := 0
	for _, b := range me.buckets {
		acc += sum(b)
		r = append(r, acc)
	}
	return
}
