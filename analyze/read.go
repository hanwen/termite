package analyze

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

type edge struct {
	target *Target
	dep    *Target
}

type Graph struct {
	strings        map[string]*String
	TargetByWrite  map[*String]*Target
	TargetByName   map[*String]*Target
	CommandByID    map[*String]*Command
	CommandByWrite map[string]*Command
	Errors         []Error
	UsedEdges      map[edge]struct{}
}

type Target struct {
	Name     *String
	Reads    map[*String]struct{}
	Deps     map[*String]struct{}
	Writes   map[*String]struct{}
	Duration time.Duration
	Commands []*Command
	Errors   []Error
}

func (g *Graph) Lookup(s string) *String {
	return g.strings[s]
}

func (g *Graph) Intern(s string) *String {
	interned := g.strings[s]
	if interned == nil {
		interned = &String{s}
		g.strings[s] = interned
	}
	return interned
}

// String is an interned string.
type String struct {
	string
}

func (s *String) String() string {
	return s.string
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

/* parse a "target: dep" file. */
func ParseDepFile(content []byte) ([]string, []string) {
	content = bytes.Replace(content, []byte("\\\n"), nil, -1)
	components := bytes.Split(content, []byte(":"))
	if len(components) != 2 {
		return nil, nil
	}

	targetStrs := bytes.Split(components[0], []byte(" "))
	depStrs := bytes.Split(components[1], []byte(" "))

	var targets, deps []string
	for _, t := range targetStrs {
		if len(t) > 0 {
			targets = append(targets, string(t))
		}
	}
	for _, d := range depStrs {
		if len(d) > 0 {
			deps = append(deps, string(d))
		}
	}

	return targets, deps
}

func ReadDir(dir string, depRe *regexp.Regexp) ([]*Command, error) {
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

		// add contents of .dep file to the command.
		for _, w := range a.Writes {
			if depRe == nil || !depRe.MatchString(w) {
				continue
			}
			c, err := ioutil.ReadFile(filepath.Join(base, w))
			if err != nil {
				continue
			}

			found := false
			targets, deps := ParseDepFile(c)
			if len(targets) == 0 {
				continue
			}
			for _, t := range targets {
				t = filepath.Clean(filepath.Join(a.Dir, t))
				if t == filepath.Join(base, a.Target) {
					found = true
				}
			}
			if !found {
				continue
			}

			for _, d := range deps {
				a.Deps = append(a.Deps, d)
			}
		}

		var clean []string
		for _, p := range a.Deps {
			if !filepath.IsAbs(p) {
				p = filepath.Join(a.Dir, p)
			}
			p, err = filepath.Rel(base, p)
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
	target.Writes = map[*String]struct{}{}
	target.Reads = map[*String]struct{}{}
	target.Deps = map[*String]struct{}{}
	sort.Sort(annSlice(target.Commands))

	yes := struct{}{}
	for _, a := range target.Commands {
		a.target = target
		target.Duration += a.Duration
		for _, f := range a.Deps {
			if f == "" {
				continue
			}
			target.Deps[g.Intern(f)] = yes
		}
		for _, f := range a.Reads {
			if f == "" {
				continue
			}
			if strings.HasPrefix(f, "/") {
				continue
			}
			target.Reads[g.Intern(f)] = yes
		}
		for _, f := range a.Deletions {
			if f == "" {
				continue
			}
			delete(target.Writes, g.Intern(f))
			delete(target.Reads, g.Intern(f))
		}
		for _, f := range a.Writes {
			if f == "" {
				continue
			}
			target.Writes[g.Intern(f)] = yes
		}
		if a.Target != "" {
			target.Name = g.Intern(a.Target)
		}

	}
}

func (g *Graph) addError(e Error) {
	g.Errors = append(g.Errors, e)
}

func (g *Graph) addCommand(ann *Command) {
	g.CommandByID[g.Intern(ann.ID())] = ann
	for _, w := range ann.Writes {
		if exist, ok := g.CommandByWrite[w]; ok {
			g.addError(&dupWrite{w, []*Command{exist, ann}})
		} else {
			g.CommandByWrite[w] = ann
		}
	}

	target := g.TargetByName[g.Intern(ann.Target)]
	if target == nil {
		target = &Target{}
		g.TargetByName[g.Intern(ann.Target)] = target
	}

	target.Commands = append(target.Commands, ann)
	for _, w := range ann.Writes {
		g.TargetByWrite[g.Intern(w)] = target
	}
}

func NewGraph(anns []*Command) *Graph {
	g := Graph{
		strings:        map[string]*String{},
		TargetByName:   map[*String]*Target{},
		TargetByWrite:  map[*String]*Target{},
		CommandByWrite: map[string]*Command{},
		CommandByID:    map[*String]*Command{},
		UsedEdges:      map[edge]struct{}{},
	}

	for _, ann := range anns {
		g.addCommand(ann)
	}

	for _, a := range g.TargetByName {
		g.computeTarget(a)
	}
	g.checkTargets()
	return &g
}
