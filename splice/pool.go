package splice

func ClearSplicePool() {
	splicePool.clear()
}

func Get() (*Pair, error) {
	return splicePool.get()
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

func (me *pairPool) get() (p *Pair, err error) {
	me.Lock()
	defer me.Unlock()

	for s := range me.unused {
		delete(me.unused, s)
		return s, nil
	}

	return newSplicePair()
}

func (me *pairPool) done(p *Pair) {
	me.Lock()
	defer me.Unlock()

	me.unused[p] = true
}

var splicePool *pairPool
var pipeMaxSize *int

// From manpage on ubuntu Lucid:
//
// Since Linux 2.6.11, the pipe capacity is 65536 bytes.
const DefaultPipeSize = 16 * 4096

func init() {
	splicePool = newSplicePairPool()
}
