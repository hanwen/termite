package termite

import (
	"testing"
)

func checkGetopt(t *testing.T, got GetoptResult, want GetoptResult) {
	if len(got.Args) != len(want.Args) {
		t.Errorf("args length mismatch. Got %v (%d) want %v (%d)", got.Args, len(got.Args), want.Args, len(want.Args))
	} else {
		for i, a := range got.Args {
			if w := want.Args[i]; w != a {
				t.Errorf("Arg %d mismatch. Got %q want %q", a, w)
			}
		}
	}
	if len(got.Long) != len(want.Long) {
		t.Errorf("long options length mismatch. Got %v want %v", got.Long, want.Long)
	}
	for k, wantv := range want.Long {
		gotv, ok := got.Long[k]
		if ok {
			if wantv != gotv {
				t.Errorf("Long option %s mismatch. Got %q, want %q", k, gotv, wantv)
			}
		} else {
			t.Errorf("Missing long option %s", k)
		}
	}
	for k, wantv := range want.Short {
		gotv, ok := got.Short[k]
		if ok {
			if wantv != gotv {
				t.Errorf("Short option %c mismatch. Got %q, want %q", k, gotv, wantv)
			}
		} else {
			t.Errorf("Missing short option %c", k)
		}
	}
}

func TestGetopt(t *testing.T) {
	t.Log("Case: single arg")
	g := Getopt([]string{"q"}, nil, nil, true)
	w := GetoptResult{
		Args: []string{"q"},
	}
	checkGetopt(t, g, w)

	t.Log("Case: no options")
	g = Getopt([]string{"a", "b"}, nil, nil, false)
	w = GetoptResult{
		Args: []string{"a", "b"},
	}
	checkGetopt(t, g, w)

	t.Log("Case: simple short option")
	a := []string{"-o", "b"}
	g = Getopt(a, nil, nil, false)
	w = GetoptResult{
		Args:  []string{"b"},
		Short: map[byte]string{'o': ""},
	}
	checkGetopt(t, g, w)

	t.Log("Case: short option with arg")
	g = Getopt(a, nil, []byte{'o'}, false)
	w = GetoptResult{
		Short: map[byte]string{'o': "b"},
	}
	checkGetopt(t, g, w)

	t.Log("Case: long option")
	a = []string{"--long", "b"}
	g = Getopt(a, nil, nil, false)
	w = GetoptResult{
		Args: []string{"b"},
		Long: map[string]string{"long": ""},
	}
	checkGetopt(t, g, w)

	t.Log("Case: long option, with =")
	a = []string{"--long=val", "b"}
	g = Getopt(a, []string{"long"}, nil, false)
	w = GetoptResult{
		Args: []string{"b"},
		Long: map[string]string{"long": "val"},
	}
	checkGetopt(t, g, w)

	t.Log("Case: long option, with =, empty")
	a = []string{"--long=", "b"}
	g = Getopt(a, []string{"long"}, nil, false)
	w = GetoptResult{
		Args: []string{"b"},
		Long: map[string]string{"long": ""},
	}
	checkGetopt(t, g, w)

	t.Log("Case: long option, without =")
	a = []string{"--long", "b"}
	g = Getopt(a, []string{"long"}, nil, false)
	w = GetoptResult{
		Long: map[string]string{"long": "b"},
	}
	checkGetopt(t, g, w)

	t.Log("Case: reorder")
	a = []string{"--", "--long", "b"}
	g = Getopt(a, []string{"long"}, nil, false)
	w = GetoptResult{
		Args: []string{"--long", "b"},
	}
	checkGetopt(t, g, w)

}
