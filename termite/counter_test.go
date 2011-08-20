package termite

import (
	"testing"
	"log"
)

var _ = log.Println

func TestMultiResolutionCounter(t *testing.T) {
	c := NewMultiResolutionCounter(1, 0, []int{2, 2})
	r := c.Read()
	if len(r) != 3 {
		t.Fatalf("Read() length should be 3 %v", r)
	}

	c.Add(0, 1)
	r = c.Read()
	if r[0] != 1 || r[1] != 1 || r[2] != 1 {
		t.Errorf("val error %v", r)
	}

	c.Add(1, 0)
	r = c.Read()
	if r[0] != 0 || r[1] != 1 || r[2] != 1 {
		t.Errorf("val error %v", r)
	}

	c.Add(2, 0)
	r = c.Read()
	if r[0] != 0 || r[1] != 0 || r[2] != 1 {
		t.Errorf("val error ts==2 %v, %v", r, c.buckets)
	}
}
