package analyze

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

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

func CombineCommands(annotations []*Annotation) *Action {
	sort.Sort(annSlice(annotations))

	action := Action{
		Writes:  map[string]struct{}{},
		Reads:   map[string]struct{}{},
		Deps:    map[string]struct{}{},
		Targets: map[string]struct{}{},
	}
	yes := struct{}{}
	for _, a := range annotations {
		action.Commands = append(action.Commands, a.Command)
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
			action.Targets[a.Target] = yes
		}

		action.Annotations = append(action.Annotations, a.Filename)
	}

	action.Time = annotations[0].Time
	return &action
}
