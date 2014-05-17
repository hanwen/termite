package analyze

import (
	"path/filepath"
	"time"
)

type Command struct {
	Deps      []string
	Dir       string
	Target    string
	Reads     []string
	Writes    []string
	Deletions []string
	Time      time.Time
	Duration  time.Duration
	Command   string
	Filename  string

	target *Target
}

func (a *Command) ID() string {
	_, base := filepath.Split(a.Filename)
	return base
}
