package splice
import (
	"sync"
)

var splicePool *pairPool

type pairPool struct {
	sync.Mutex
	unused map[*Pair]bool
	usedCount   int
}

func ClearSplicePool() {
	splicePool.clear()
}

func Get() (*Pair, error) {
	return splicePool.get()
}

func Used() int {
	return splicePool.used()
}

func Done(p *Pair) {
	splicePool.done(p)
}

func newSplicePairPool() *pairPool {
	return &pairPool{
		unused: make(map[*Pair]bool),
	}
}

func (me *pairPool) clear() {
	me.Lock()
	defer me.Unlock()
	for p := range me.unused {
		p.Close()
	}
	me.unused = make(map[*Pair]bool)
}

func (me *pairPool) used() int {
	me.Lock()
	defer me.Unlock()
	return me.usedCount
}


func (me *pairPool) get() (p *Pair, err error) {
	me.Lock()
	defer me.Unlock()

	me.usedCount++
	for s := range me.unused {
		delete(me.unused, s)
		return s, nil
	}
	return newSplicePair()
}

func (me *pairPool) done(p *Pair) {
	me.Lock()
	defer me.Unlock()

	me.usedCount--
	me.unused[p] = true
}



func init() {
	splicePool = newSplicePairPool()
}
