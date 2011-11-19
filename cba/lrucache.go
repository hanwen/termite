package cba

import (
	"log"
)

var _ = log.Println

type cacheEntry struct {
	key   string
	val   interface{}
	index int
}

// A fixed entry count cache with FIFO eviction policy.
//
// Should be protected by a Mutex (not RWMutex) for all methods.
type LruCache struct {
	size int

	// Circular buffer.
	lastUsedKeys []*cacheEntry
	nextEvict    int

	// the key => contents map.
	contents map[string]*cacheEntry
}

func NewLruCache(size int) (me *LruCache) {
	me = &LruCache{
		size:         size,
		lastUsedKeys: make([]*cacheEntry, size),
		contents:     map[string]*cacheEntry{},
	}

	return me
}

func (me *LruCache) Add(key string, val interface{}) {
	evict := me.lastUsedKeys[me.nextEvict]
	if evict != nil {
		delete(me.contents, evict.key)
	}

	e := &cacheEntry{
		key:   key,
		val:   val,
		index: me.nextEvict,
	}

	me.contents[key] = e
	me.lastUsedKeys[me.nextEvict] = e
	me.nextEvict = (me.nextEvict + 1) % me.size
}

func (me *LruCache) swap(i, j int) {
	me.lastUsedKeys[i], me.lastUsedKeys[j] = me.lastUsedKeys[j], me.lastUsedKeys[i]

	if me.lastUsedKeys[i] != nil {
		me.lastUsedKeys[i].index = i
	}
	if me.lastUsedKeys[j] != nil {
		me.lastUsedKeys[j].index = j
	}
}

func (me *LruCache) Has(key string) bool {
	_, ok := me.contents[key]
	return ok
}

func (me *LruCache) Size() int {
	return len(me.contents)
}

func (me *LruCache) Get(key string) (val interface{}) {
	v, ok := me.contents[key]
	if !ok {
		return nil
	}

	me.swap(me.nextEvict, v.index)
	me.nextEvict = (me.nextEvict + 1) % me.size

	return v.val
}
