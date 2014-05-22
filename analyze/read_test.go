package analyze

import (
	"testing"
)

func TestParseDepFile(t *testing.T) {
	targets, deps := ParseDepFile([]byte("foo bar : dep1 dep2 \\\n dep3"))
	if len(targets) != 2 || len(deps) != 3 {
		t.Fatalf("got %v, %v want 2 targets, 3 deps", targets, deps)
	}
	if targets[0] != "foo" || targets[1] != "bar" {
		t.Fatalf("got %v want foo, bar", targets)
	}
	if deps[0] != "dep1" || deps[1] != "dep2" || deps[2] != "dep3" {
		t.Fatalf("got %v want dep1, dep2", deps)
	}
}
