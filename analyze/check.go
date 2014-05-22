package analyze

import (
	"fmt"
	"log"
)

type targetSet map[*Target]struct{}

var yes = struct{}{}

type undeclaredDep struct {
	Target string
	Read   string
}

func (u *undeclaredDep) HTML(g *Graph) string {
	return fmt.Sprintf("target %s reads undeclared dependency %s",
		g.targetURL(u.Target), u.Read)
}

type unusedDep struct {
	Target string
	Dep    string
}

func (u *unusedDep) HTML(g *Graph) string {
	return fmt.Sprintf("target %s does not use dependency %s",
		g.targetURL(u.Target), u.Dep)
}

func (g *Graph) checkTarget(target *Target) {
	realDeps := map[*Target]string{}
	for r := range target.Reads {
		log.Println("R", r)
		t := g.TargetByWrite[r]
		if t != nil {
			realDeps[t] = r
		}
	}

	usedDeps := targetSet{}
	for dep, name := range realDeps {
		if _, ok := target.Deps[name]; ok {
			usedDeps[dep] = yes
		}
		if _, ok := target.Deps[dep.Name]; ok {
			usedDeps[dep] = yes
		}
	}
	for d := range usedDeps {
		delete(realDeps, d)
	}

	for d := range realDeps {
		t := g.findPath(target, d)
		if t != nil {
			usedDeps[t] = yes
		} else {
			e := &undeclaredDep{target.Name, realDeps[d]}
			g.Errors = append(g.Errors, e)
			target.Errors = append(target.Errors, e)
		}
	}

	// TODO - check declared edges for use.
}

func (g *Graph) findPath(target *Target, needle *Target) *Target {
	if target == needle {
		return target
	}
	for d := range target.Deps {
		dep := g.TargetByName[d]
		if dep != nil && g.findPath(dep, needle) != nil {
			return target
		}
	}
	return nil
}

func (g *Graph) checkTargets() {
	for _, target := range g.TargetByName {
		g.checkTarget(target)
	}
}
