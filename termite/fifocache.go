package termite
import (
	"log"
)

var _ = log.Println

type cacheEntry struct {
	key string
	val interface {}
	index  int
}

// A fixed entry count cache with FIFO eviction policy.
//
// Should be protected by a Mutex (not RWMutex) for all methods.
type FifoCache struct {
	size int

	// Circular buffer.
	lastUsedKeys []*cacheEntry
	nextEvict    int

	// the key => contents map.  TODO - should use string rather
	// than []byte?
	contents     map[string]*cacheEntry
}

func NewFifoCache(size int) (me *FifoCache) {
	me = &FifoCache{
		size: size,
		lastUsedKeys: make([]*cacheEntry, size),
		contents: map[string]*cacheEntry{},
	}

	return me
}

func (me *FifoCache) Add(key string, val interface{}) {
	evict := me.lastUsedKeys[me.nextEvict]
	if evict != nil {
		me.contents[evict.key] = nil, false
	}

	e := &cacheEntry{
		key: key,
		val: val,
		index: me.nextEvict,
	}

	me.contents[key] = e
	me.lastUsedKeys[me.nextEvict] = e
	me.nextEvict = (me.nextEvict + 1)%me.size
}

func (me *FifoCache) swap(i, j int) {
	me.lastUsedKeys[i], me.lastUsedKeys[j] = me.lastUsedKeys[j], me.lastUsedKeys[i]

	if me.lastUsedKeys[i] != nil {
		me.lastUsedKeys[i].index = i
	}
	if me.lastUsedKeys[j] != nil {
		me.lastUsedKeys[j].index = j
	}
}

func (me *FifoCache) Has(key string) bool {
	_, ok := me.contents[key]
	return ok
}

func (me *FifoCache) Size() int {
	return me.size
}

func (me *FifoCache) Get(key string) (val interface{}) {
	v, ok := me.contents[key]
	if !ok {
		return nil
	}

	me.swap(me.nextEvict, v.index)
	me.nextEvict = (me.nextEvict + 1) % me.size

	return v.val
}
