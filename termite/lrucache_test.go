package termite

import (
	"testing"
)

func TestLruCache(t *testing.T) {
	c := NewLruCache(2)

	v1 := interface{}(1)
	v2 := interface{}(2)
	v3 := interface{}(3)

	c.Add("1", v1)
	c.Add("2", v2)

	v := c.Get("0")
	if v != nil {
		t.Errorf("got %v for nonexistent key", v)
	}

	v = c.Get("1")
	if v != v1 {
		t.Errorf("mismatch for key 1: %v != %v", v, v1)
	}

	c.Add("3", v3)
	v = c.Get("2")
	if v != nil {
		t.Errorf("key 2 should have been evicted: %v", v)
	}
}
