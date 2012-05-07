package stats

import (
	"testing"
)

func TestDiskStat(t *testing.T) {
	d, err := TotalDiskStats()

	if err != nil {
		t.Fatalf("DiskStat failed: %v", err)
	}

	t.Log(&d)
}
