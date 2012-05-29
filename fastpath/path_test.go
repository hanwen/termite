package fastpath

import (
	"testing"
)

func TestFastJoin(t *testing.T) {
	cases :=[]string{"a///", "b", "a/b",
		"a", "b", "a/b",
		"a", "//b", "a/b",
		"", "b", "b",
	}
	
	for i := 0; i < len(cases);  {
		got := Join(cases[i],cases[i+1])
		if got != cases[i+2] {
			t.Errorf("Join: case %d gave %q", i/3, got)
		}
		i += 3
	}
}
