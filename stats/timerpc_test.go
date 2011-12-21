package stats

import (
	"strings"
	"testing"
	"time"
)

func TestRpcTimingString(t *testing.T) {
	timing := RpcTiming{
		N: 1, Duration: 500 * time.Millisecond,
	}

	s := timing.String()
	want := "500 ms"
	if !strings.Contains(s, want) {
		t.Errorf("%q missing: %q", want, s)
	}
}
