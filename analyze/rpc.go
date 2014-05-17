package analyze

import (
	"path/filepath"
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

	action *Action
}

func (a *Annotation) ID() string {
	_, base := filepath.Split(a.Filename)
	return base
}
