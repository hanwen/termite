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
	ActionByWrite     map[string]*Action
	ActionByTarget    map[string]*Action
	AnnotationByID    map[string]*Annotation
	AnnotationByWrite map[string]*Annotation
	Errors            []Error
}

type Action struct {
	Target      string
	Reads       map[string]struct{}
	Deps        map[string]struct{}
	Writes      map[string]struct{}
	Duration    time.Duration
	Annotations []*Annotation
}

func (a *Action) Start() time.Time {
	return a.Annotations[0].Time
}

type Error interface {
	HTML(g *Graph) string
}

type dupWrite struct {
	write       string
	annotations []*Annotation
}

func (d *dupWrite) HTML(g *Graph) string {
	s := ""
	for _, a := range d.annotations {
		s += fmt.Sprintf("<li>%s\n", annotationRef(a))
	}
	return fmt.Sprintf("file %q written by <ul>%s</ul>", d.write, s)
}

func ReadDir(dir string) ([]*Annotation, error) {
	dir = filepath.Clean(dir)
	result := []*Annotation{}
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

		var a Annotation
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

type annSlice []*Annotation

func (s annSlice) Len() int {
	return len(s)
}

func (s annSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s annSlice) Less(i, j int) bool {
	return s[i].Time.UnixNano() < s[j].Time.UnixNano()
}

func (g *Graph) computeAction(action *Action) {
	action.Writes = map[string]struct{}{}
	action.Reads = map[string]struct{}{}
	action.Deps = map[string]struct{}{}
	sort.Sort(annSlice(action.Annotations))

	yes := struct{}{}
	for _, a := range action.Annotations {
		a.action = action
		action.Duration += a.Duration
		for _, f := range a.Deps {
			if f == "" {
				continue
			}
			action.Deps[f] = yes
		}
		for _, f := range a.Read {
			if f == "" {
				continue
			}
			if strings.HasPrefix(f, "/") {
				continue
			}
			action.Reads[f] = yes
		}
		for _, f := range a.Deleted {
			if f == "" {
				continue
			}
			delete(action.Writes, f)
			delete(action.Reads, f)
		}
		for _, f := range a.Written {
			if f == "" {
				continue
			}
			action.Writes[f] = yes
		}
		if a.Target != "" {
			action.Target = a.Target
		}

	}
}

func (g *Graph) addError(e Error) {
	g.Errors = append(g.Errors, e)
}

func (g *Graph) addAnnotation(ann *Annotation) {
	g.AnnotationByID[ann.ID()] = ann
	for _, w := range ann.Written {
		if exist, ok := g.AnnotationByWrite[w]; ok {
			g.addError(&dupWrite{w, []*Annotation{exist, ann}})
		} else {
			g.AnnotationByWrite[w] = ann
		}
	}

	action := g.ActionByTarget[ann.Target]
	if action == nil {
		action = &Action{}
		g.ActionByTarget[ann.Target] = action
	}

	action.Annotations = append(action.Annotations, ann)
}

func NewGraph(anns []*Annotation) *Graph {
	g := Graph{
		ActionByTarget:    map[string]*Action{},
		AnnotationByWrite: map[string]*Annotation{},
		AnnotationByID:    map[string]*Annotation{},
	}

	for _, ann := range anns {
		g.addAnnotation(ann)
	}

	for _, a := range g.ActionByTarget {
		g.computeAction(a)
	}
	return &g
}
