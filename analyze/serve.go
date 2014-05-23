package analyze

import (
	"fmt"
	"html"
	"net/http"
	"sort"
	"strings"
)

type internedSlice []*String

func (s internedSlice) Len() int {
	return len(s)
}
func (s internedSlice) Swap(i, j int) {
	s[j], s[i] = s[i], s[j]
}

func (s internedSlice) Less(i, j int) bool {
	return s[i].String() < s[j].String()
}

func internKeys(m map[*String]struct{}) []*String {
	var ks internedSlice
	for k := range m {
		ks = append(ks, k)
	}
	sort.Sort(ks)
	return ks
}

func keys(m map[string]struct{}) []string {
	var ks []string
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

func (g *Graph) targetURL(a *String) string {
	if _, ok := g.TargetByName[a]; ok {
		return fmt.Sprintf("<a href=\"/target?t=%s\">%s</a>\n", a.String(), html.EscapeString(a.String()))
	} else {
		return html.EscapeString(a.String())
	}
}

func commandRef(ann *Command) string {
	return fmt.Sprintf("<a href=\"/command?id=%s\">%s</a>", ann.ID(), ann.ID())
}

func (g *Graph) writeNode(w http.ResponseWriter, a *Target) {
	fmt.Fprintf(w, "<html><body>\n")
	fmt.Fprintf(w, "<p>name: %s</p>\n", a.Name)
	fmt.Fprintf(w, "<p>written files</p>\n")
	fmt.Fprintf(w, "<ul>\n")
	for _, k := range internKeys(a.Writes) {
		fmt.Fprintf(w, "<li>%s\n", k)
	}
	fmt.Fprintf(w, "</ul>\n")

	fmt.Fprintf(w, "<p>declared deps</p>\n")
	fmt.Fprintf(w, "<ul>\n")
	for _, k := range internKeys(a.Deps) {
		fmt.Fprintf(w, "<li>%s\n", g.targetURL(k))
	}
	fmt.Fprintf(w, "</ul>\n")

	fmt.Fprintf(w, "<p>read files</p>\n")
	fmt.Fprintf(w, "<ul>\n")
	for _, k := range internKeys(a.Reads) {
		fmt.Fprintf(w, "<li>%s\n", g.targetURL(k))
	}
	fmt.Fprintf(w, "</ul>\n")

	fmt.Fprintf(w, "<p>commands</p>\n")
	for _, k := range a.Commands {
		fmt.Fprintf(w, "<li>%s : <pre>\n", commandRef(k))
		w.Write([]byte(html.EscapeString(k.Command) + "\n"))
		fmt.Fprintf(w, "</pre></li>\n")
	}
	fmt.Fprintf(w, "<p>errors</p><ul>\n")
	for _, e := range a.Errors {
		fmt.Fprintf(w, "<li>%s</li>\n", e.HTML(g))
	}
	fmt.Fprintf(w, "</ul>\n")

	fmt.Fprintf(w, "<p>timing: %s</p>\n", a.Duration)
	fmt.Fprintf(w, "</body></html>\n")
}

func (g *Graph) writeCommand(w http.ResponseWriter, a *Command) {
	fmt.Fprintf(w, "<html><body>\n")
	fmt.Fprintf(w, "<p>start %s, duration %s</p>\n", a.Time, a.Duration)

	fmt.Fprintf(w, "<p>target: %s</p>\n", a.Target)
	fmt.Fprintf(w, "<p>written files</p>\n")
	fmt.Fprintf(w, "<ul>\n")
	for _, k := range a.Writes {
		fmt.Fprintf(w, "<li>%s\n", k)
	}
	fmt.Fprintf(w, "</ul>\n")

	fmt.Fprintf(w, "<p>read files</p>\n")
	fmt.Fprintf(w, "<ul>\n")
	for _, k := range a.Reads {
		fmt.Fprintf(w, "<li>%s\n", g.targetURL(g.Intern(k)))
	}
	fmt.Fprintf(w, "</ul>\n")

	fmt.Fprintf(w, "<p>declared deps</p>\n")
	fmt.Fprintf(w, "<ul>\n")
	for _, k := range a.Deps {
		if strings.HasPrefix(k, "/") {
			continue
		}
		fmt.Fprintf(w, "<li>%s\n", g.targetURL(g.Intern(k)))
	}
	fmt.Fprintf(w, "</ul>\n")

	fmt.Fprintf(w, "<p>command</p>\n")
	fmt.Fprintf(w, "<pre>\n")
	w.Write([]byte(html.EscapeString(a.Command) + "\n"))
	fmt.Fprintf(w, "</pre>")
	fmt.Fprintf(w, "</body></html>\n")

}

func (g *Graph) ServeTarget(w http.ResponseWriter, req *http.Request) {
	values := req.URL.Query()
	names, ok := values["t"]
	if !ok {
		http.Error(w, "404 target param not found", http.StatusNotFound)
		return
	}

	a, ok := g.TargetByName[g.Intern(names[0])]
	if !ok {
		http.Error(w, fmt.Sprintf("404 target %s not found", names[0]), http.StatusNotFound)
		return
	}

	g.writeNode(w, a)
}

func (g *Graph) ServeErrors(w http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(w, "<html><body>\n")
	fmt.Fprintf(w, "<p>errors</p><ul>\n")
	for _, e := range g.Errors {
		fmt.Fprintf(w, "<li>%s</li>\n", e.HTML(g))
	}
	fmt.Fprintf(w, "</ul>\n")
	fmt.Fprintf(w, "</body></html>\n")
}

func (g *Graph) ServeCommand(w http.ResponseWriter, req *http.Request) {
	values := req.URL.Query()
	names, ok := values["id"]
	if !ok {
		http.Error(w, "404 command param not found", http.StatusNotFound)
		return
	}

	a, ok := g.CommandByID[g.Lookup(names[0])]
	if !ok {
		http.Error(w, fmt.Sprintf("404 command %s not found", names[0]), http.StatusNotFound)
		return
	}

	g.writeCommand(w, a)
}

func (g *Graph) ServeRoot(w http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(w, "<html><body>\n")
	fmt.Fprintf(w, "<ul>\n")
	fmt.Fprintf(w, "<li><a href=\"/targets\">targets</a>")
	fmt.Fprintf(w, "<li><a href=\"/errors\">errors</a>")
	fmt.Fprintf(w, "</ul></body></html>\n")

}

func (g *Graph) ServeTargets(w http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(w, "<html><body>\n")
	fmt.Fprintf(w, "<p>targets</p><ul>\n")

	var keys internedSlice
	for k := range g.TargetByName {
		keys = append(keys, k)
	}
	sort.Sort(keys)

	for _, k := range keys {
		fmt.Fprintf(w, "<li>%s", g.targetURL(k))
	}
	fmt.Fprintf(w, "</ul></body></html>\n")
}

func (g *Graph) Serve(addr string) error {
	http.HandleFunc("/targets", g.ServeTargets)
	http.HandleFunc("/target", g.ServeTarget)
	http.HandleFunc("/errors", g.ServeErrors)
	http.HandleFunc("/command", g.ServeCommand)
	http.HandleFunc("/", g.ServeRoot)
	return http.ListenAndServe(addr, nil)
}
