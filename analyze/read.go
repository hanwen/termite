package analyze

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type Graph struct {
	TargetByWrite  map[string]*Target
	TargetByName   map[string]*Target
	CommandByID    map[string]*Command
	CommandByWrite map[string]*Command
	Errors         []Error
}

type Target struct {
	Name     string
	Reads    map[string]struct{}
	Deps     map[string]struct{}
	Writes   map[string]struct{}
	Duration time.Duration
	Commands []*Command
}

func (a *Target) Start() time.Time {
	return a.Commands[0].Time
}

type Error interface {
	HTML(g *Graph) string
}

type dupWrite struct {
	write    string
	commands []*Command
}

func (d *dupWrite) HTML(g *Graph) string {
	s := ""
	for _, a := range d.commands {
		s += fmt.Sprintf("<li>%s\n", commandRef(a))
	}
	return fmt.Sprintf("file %q written by <ul>%s</ul>", d.write, s)
}

func ReadDir(dir string) ([]*Command, error) {
	dir = filepath.Clean(dir)
	result := []*Command{}
	f, err := os.Open(dir)
	if err != nil {
		return nil, err
	}

	names, err := f.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	base := filepath.Dir(dir)
	for _, nm := range names {
		fn := filepath.Join(dir, nm)
		contents, err := ioutil.ReadFile(fn)
		if err != nil {
			return nil, fmt.Errorf("read(%q): %v", fn, err)
		}

		var a Command
		if err := json.Unmarshal(contents, &a); err != nil {
			return nil, fmt.Errorf("Unmarshal(%q): %v", fn, err)
		}

		a.Target, err = filepath.Rel(base, filepath.Join(a.Dir, a.Target))
		if err != nil {
			return nil, fmt.Errorf("rel: %v", a)
		}
		a.Target = filepath.Clean(a.Target)
		a.Filename = nm
		var clean []string
		for _, p := range a.Deps {
			p, err = filepath.Rel(base, filepath.Join(a.Dir, p))
			if err != nil {
				return nil, fmt.Errorf("rel %q %v", p, err)
			}

			clean = append(clean, p)
		}
		a.Deps = clean

		result = append(result, &a)
	}

	return result, nil
}

type annSlice []*Command

func (s annSlice) Len() int {
	return len(s)
}

func (s annSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s annSlice) Less(i, j int) bool {
	return s[i].Time.UnixNano() < s[j].Time.UnixNano()
}

func (g *Graph) computeTarget(target *Target) {
	target.Writes = map[string]struct{}{}
	target.Reads = map[string]struct{}{}
	target.Deps = map[string]struct{}{}
	sort.Sort(annSlice(target.Commands))

	yes := struct{}{}
	for _, a := range target.Commands {
		a.target = target
		target.Duration += a.Duration
		for _, f := range a.Deps {
			if f == "" {
				continue
			}
			target.Deps[f] = yes
		}
		for _, f := range a.Reads {
			if f == "" {
				continue
			}
			if strings.HasPrefix(f, "/") {
				continue
			}
			target.Reads[f] = yes
		}
		for _, f := range a.Deletions {
			if f == "" {
				continue
			}
			delete(target.Writes, f)
			delete(target.Reads, f)
		}
		for _, f := range a.Writes {
			if f == "" {
				continue
			}
			target.Writes[f] = yes
		}
		if a.Target != "" {
			target.Name = a.Target
		}

	}
}

func (g *Graph) addError(e Error) {
	g.Errors = append(g.Errors, e)
}

func (g *Graph) addCommand(ann *Command) {
	g.CommandByID[ann.ID()] = ann
	for _, w := range ann.Writes {
		if exist, ok := g.CommandByWrite[w]; ok {
			g.addError(&dupWrite{w, []*Command{exist, ann}})
		} else {
			g.CommandByWrite[w] = ann
		}
	}

	target := g.TargetByName[ann.Target]
	if target == nil {
		target = &Target{}
		g.TargetByName[ann.Target] = target
	}

	target.Commands = append(target.Commands, ann)
}

func NewGraph(anns []*Command) *Graph {
	g := Graph{
		TargetByName:   map[string]*Target{},
		CommandByWrite: map[string]*Command{},
		CommandByID:    map[string]*Command{},
	}

	for _, ann := range anns {
		g.addCommand(ann)
	}

	for _, a := range g.TargetByName {
		g.computeTarget(a)
	}
	return &g
}
