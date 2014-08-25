package termite

import (
	"testing"
)

func checkGetopt(name string, t *testing.T, got GetoptResult, want GetoptResult) {
	if len(got.Args) != len(want.Args) {
		t.Errorf("%s: args length mismatch. Got %v (%d) want %v (%d)", name, got.Args, len(got.Args), want.Args, len(want.Args))
	} else {
		for i, a := range got.Args {
			if w := want.Args[i]; w != a {
				t.Errorf("%s: arg %d mismatch. Got %q want %q", name, a, w)
			}
		}
	}
	if len(got.Long) != len(want.Long) {
		t.Errorf("%s: long options length mismatch. Got %v want %v", name, got.Long, want.Long)
	}
	for k, wantv := range want.Long {
		gotv, ok := got.Long[k]
		if ok {
			if wantv != gotv {
				t.Errorf("%s: long option %s mismatch. Got %q, want %q", name, k, gotv, wantv)
			}
		} else {
			t.Errorf("%s: missing long option %s", name, k)
		}
	}
	for k, wantv := range want.Short {
		gotv, ok := got.Short[k]
		if ok {
			if wantv != gotv {
				t.Errorf("%s: short option %c mismatch. Got %q, want %q", name, k, gotv, wantv)
			}
		} else {
			t.Errorf("%s: missing short option %c", name, k)
		}
	}
}

func TestGetopt(t *testing.T) {
	type testcase struct {
		name    string
		args    []string
		long    []string
		short   []byte
		reorder bool
		want    GetoptResult
	}

	cases := []testcase{
		{
			"Case: single arg",
			[]string{"q"}, nil, nil, true,
			GetoptResult{
				Args: []string{"q"},
			},
		},
		{
			"Case: no options",
			[]string{"a", "b"}, nil, nil, false,
			GetoptResult{
				Args: []string{"a", "b"},
			},
		},
		{
			("Case: simple short option"),
			[]string{"-o", "b"}, nil, nil, false,
			GetoptResult{
				Args:  []string{"b"},
				Short: map[byte]string{'o': ""},
			},
		},
		{
			"Case: short option with arg",
			[]string{"-o", "b"},
			nil, []byte{'o'}, false,
			GetoptResult{
				Short: map[byte]string{'o': "b"},
			},
		},
		{
			"Case: long option",
			[]string{"--long", "b"},
			nil, nil, false,
			GetoptResult{
				Args: []string{"b"},
				Long: map[string]string{"long": ""},
			},
		},
		{
			"Case: long option, with =",
			[]string{"--long=val", "b"},
			[]string{"long"}, nil, false,
			GetoptResult{
				Args: []string{"b"},
				Long: map[string]string{"long": "val"},
			},
		},
		{
			"Case: long option, with =, empty",
			[]string{"--long=", "b"},
			[]string{"long"}, nil, false,
			GetoptResult{
				Args: []string{"b"},
				Long: map[string]string{"long": ""},
			},
		},
		{
			"Case: long option, without =",
			[]string{"--long", "b"},
			[]string{"long"}, nil, false,
			GetoptResult{
				Long: map[string]string{"long": "b"},
			},
		},
		{
			"Case: reorder",
			[]string{"--", "--long", "b"},
			[]string{"long"}, nil, false,
			GetoptResult{
				Args: []string{"--long", "b"},
			},
		},
	}

	for _, c := range cases {
		g := Getopt(c.args, c.long, c.short, c.reorder)
		checkGetopt(c.name, t, g, c.want)
	}
}
