package analyze

import (
	"fmt"
	"html"
	"net/http"
	"sort"
	"strings"
)

func keys(m map[string]struct{}) []string {
	var ks []string
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

func (g *Graph) actionURL(a string) string {
	if _, ok := g.ActionByTarget[a]; ok {
		return fmt.Sprintf("<a href=\"/target?t=%s\">%s</a>\n", a, html.EscapeString(a))
	} else {
		return html.EscapeString(a)
	}
}

func annotationRef(ann *Annotation) string {
	return fmt.Sprintf("<a href=\"/annotation?id=%s\">%s</a>", ann.ID(), ann.ID())
}

func (g *Graph) writeNode(w http.ResponseWriter, a *Action) {
	fmt.Fprintf(w, "<html><body>\n")
	fmt.Fprintf(w, "<p>targets: %s</p>\n", a.Target)
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

	fmt.Fprintf(w, "<p>read files</p>\n")
	fmt.Fprintf(w, "<ul>\n")
	for _, k := range keys(a.Reads) {
		fmt.Fprintf(w, "<li>%s\n", g.actionURL(k))
	}
	fmt.Fprintf(w, "</ul>\n")

	fmt.Fprintf(w, "<p>commands</p>\n")
	for _, k := range a.Annotations {
		fmt.Fprintf(w, "<li>%s : <pre>\n", annotationRef(k))
		w.Write([]byte(html.EscapeString(k.Command) + "\n"))
		fmt.Fprintf(w, "</pre></li>\n")
	}

	fmt.Fprintf(w, "<p>timing: %s</p>\n", a.Duration)
	fmt.Fprintf(w, "</body></html>\n")
}

func (g *Graph) writeAnnotation(w http.ResponseWriter, a *Annotation) {
	fmt.Fprintf(w, "<html><body>\n")
	fmt.Fprintf(w, "<p>start %s, duration %s</p>\n", a.Time, a.Duration)

	fmt.Fprintf(w, "<p>target: %s</p>\n", a.Target)
	fmt.Fprintf(w, "<p>written files</p>\n")
	fmt.Fprintf(w, "<ul>\n")
	for _, k := range a.Written {
		fmt.Fprintf(w, "<li>%s\n", k)
	}
	fmt.Fprintf(w, "</ul>\n")

	fmt.Fprintf(w, "<p>read files</p>\n")
	fmt.Fprintf(w, "<ul>\n")
	for _, k := range a.Read {
		fmt.Fprintf(w, "<li>%s\n", g.actionURL(k))
	}
	fmt.Fprintf(w, "</ul>\n")

	fmt.Fprintf(w, "<p>declared deps</p>\n")
	fmt.Fprintf(w, "<ul>\n")
	for _, k := range a.Deps {
		if strings.HasPrefix(k, "/") {
			continue
		}
		fmt.Fprintf(w, "<li>%s\n", g.actionURL(k))
	}
	fmt.Fprintf(w, "</ul>\n")

	fmt.Fprintf(w, "<p>command</p>\n")
	fmt.Fprintf(w, "<pre>\n")
	w.Write([]byte(html.EscapeString(a.Command) + "\n"))
	fmt.Fprintf(w, "</pre>")
	fmt.Fprintf(w, "</body></html>\n")

}

func (g *Graph) ServeAction(w http.ResponseWriter, req *http.Request) {
	values := req.URL.Query()
	names, ok := values["t"]
	if !ok {
		http.Error(w, "404 action param not found", http.StatusNotFound)
		return
	}

	a, ok := g.ActionByTarget[names[0]]
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
		fmt.Fprintf(w, "<li>%s</li>\n", e.HTML(g))
	}
	fmt.Fprintf(w, "</ul>\n")
	fmt.Fprintf(w, "</body></html>\n")
}

func (g *Graph) ServeAnnotation(w http.ResponseWriter, req *http.Request) {
	values := req.URL.Query()
	names, ok := values["id"]
	if !ok {
		http.Error(w, "404 annotation param not found", http.StatusNotFound)
		return
	}

	a, ok := g.AnnotationByID[names[0]]
	if !ok {
		http.Error(w, fmt.Sprintf("404 annotation %s not found", names[0]), http.StatusNotFound)
		return
	}

	g.writeAnnotation(w, a)
}

func (g *Graph) ServeRoot(w http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(w, "<html><body>\n")
	fmt.Fprintf(w, "<ul>\n")
	fmt.Fprintf(w, "<li><a href=\"/targets\">targets</a>")
	fmt.Fprintf(w, "<li><a href=\"/errors\">errors</a>")
	fmt.Fprintf(w, "</ul></body></html>\n")

}

func (g *Graph) ServeActions(w http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(w, "<html><body>\n")
	fmt.Fprintf(w, "<p>targets</p><ul>\n")

	keys := []string{}
	for k := range g.ActionByTarget {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		fmt.Fprintf(w, "<li>%s", g.actionURL(k))
	}
	fmt.Fprintf(w, "</ul></body></html>\n")
}

func (g *Graph) Serve(addr string) error {
	http.HandleFunc("/targets", g.ServeActions)
	http.HandleFunc("/target", g.ServeAction)
	http.HandleFunc("/errors", g.ServeErrors)
	http.HandleFunc("/annotation", g.ServeAnnotation)
	http.HandleFunc("/", g.ServeRoot)
	return http.ListenAndServe(addr, nil)
}
