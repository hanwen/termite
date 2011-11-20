package stats

import (
	"strings"
	"testing"
)

func TestRpcTimingString(t *testing.T) {
	timing := RpcTiming{
		N: 1, Ns: 500e6,
	}

	s := timing.String()
	want := "500 ms"
	if !strings.Contains(s, want) {
		t.Errorf("%q missing: %q", want, s)
	}
}
