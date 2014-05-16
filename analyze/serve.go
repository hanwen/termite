package analyze

import (
	"fmt"
	"html"
	"net/http"
	"sort"
	"strings"
)

type Graph struct {
	// Write result => Action
	Actions map[string]*Action
	Errors  []error
}

func keys(m map[string]struct{}) []string {
	var ks []string
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

func (g *Graph) actionURL(a string) string {
	if _, ok := g.Actions[a]; ok {
		return fmt.Sprintf("<a href=\"/target?t=%s\">%s</a>\n", a, html.EscapeString(a))
	} else {
		return html.EscapeString(a)
	}
}

func (g *Graph) writeNode(w http.ResponseWriter, a *Action) {
	fmt.Fprintf(w, "<html><body>\n")
	fmt.Fprintf(w, "<p>targets</p>\n")
	fmt.Fprintf(w, "<ul>\n")
	for _, k := range keys(a.Targets) {
		fmt.Fprintf(w, "<li>%s\n", k)
	}
	fmt.Fprintf(w, "</ul>\n")
	fmt.Fprintf(w, "<p>written files</p>\n")
	fmt.Fprintf(w, "<ul>\n")
	for _, k := range keys(a.Writes) {
		fmt.Fprintf(w, "<li>%s\n", k)
	}
	fmt.Fprintf(w, "</ul>\n")

	fmt.Fprintf(w, "<p>declared deps</p>\n")
	fmt.Fprintf(w, "<ul>\n")
	for _, k := range keys(a.Deps) {
		if strings.HasPrefix(k, "/") {
			continue
		}
		fmt.Fprintf(w, "<li>%s\n", g.actionURL(k))
	}
	fmt.Fprintf(w, "</ul>\n")

	fmt.Fprintf(w, "<p>actual deps</p>\n")
	fmt.Fprintf(w, "<ul>\n")
	for _, k := range keys(a.Reads) {
		fmt.Fprintf(w, "<li>%s\n", g.actionURL(k))
	}
	fmt.Fprintf(w, "</ul>\n")

	fmt.Fprintf(w, "<p>commands</p>\n")
	fmt.Fprintf(w, "<pre>\n")
	for _, k := range a.Commands {
		w.Write([]byte(html.EscapeString(k) + "\n"))
	}
	fmt.Fprintf(w, "</pre>\n")

	fmt.Fprintf(w, "<p>timing: %s</p>\n", a.Duration)
	fmt.Fprintf(w, "</body></html>\n")
}

func (g *Graph) ServeAction(w http.ResponseWriter, req *http.Request) {
	values := req.URL.Query()
	names, ok := values["t"]
	if !ok {
		http.Error(w, "404 action param not found", http.StatusNotFound)
		return
	}

	a, ok := g.Actions[names[0]]
	if !ok {
		http.Error(w, fmt.Sprintf("404 action %s not found", names[0]), http.StatusNotFound)
		return
	}

	g.writeNode(w, a)
}

func (g *Graph) ServeErrors(w http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(w, "<html><body>\n")
	fmt.Fprintf(w, "<p>errors</p><ul>\n")
	for _, e := range g.Errors {
		fmt.Fprintf(w, "<li>%s</li>\n", e)

	}
	fmt.Fprintf(w, "</ul>\n")
	fmt.Fprintf(w, "</body></html>\n")
}

func (g *Graph) ServeActions(w http.ResponseWriter, req *http.Request) {
	var ks []string
	for k := range g.Actions {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	fmt.Fprintf(w, "<html><body>\n")
	fmt.Fprintf(w, "<ul>\n")
	for _, k := range ks {
		fmt.Fprintf(w, "<li>%s", g.actionURL(k))
	}
	fmt.Fprintf(w, "</ul></body></html>\n")
}

func (g *Graph) Serve(addr string) error {
	http.HandleFunc("/target", g.ServeAction)
	http.HandleFunc("/errors", g.ServeErrors)
	http.HandleFunc("/", g.ServeActions)
	return http.ListenAndServe(addr, nil)
}
