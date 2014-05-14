package analyze

import (
	"time"
)

type Annotation struct {
	Deps     []string
	Dir      string
	Target   string
	Read     []string
	Written  []string
	Deleted  []string
	Time     time.Time
	Duration time.Duration
	Command  string
	Filename string
}

type Action struct {
	Commands    []string
	Reads       map[string]struct{}
	Deps        map[string]struct{}
	Writes      map[string]struct{}
	Targets     map[string]struct{}
	Duration    time.Duration
	Time        time.Time
	Annotations []string
}
