package termite

import (
	"fmt"
	"github.com/hanwen/termite/attr"
	"log"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

var _ = log.Println

func (me *Master) MaybeRunInMaster(req *WorkRequest, rep *WorkResponse) bool {
	binary := req.Binary
	_, binary = filepath.Split(binary)

	switch binary {
	case "mkdir":
		return mkdirMaybeMasterRun(me, req, rep)
	case "rm":
		return rmMaybeMasterRun(me, req, rep)
		// TODO - implement mv ?
	}
	return false
}

func recurseNames(master *Master, name string) (names []string) {
	a := master.fileServer.attributes.GetDir(name)

	for n, m := range a.NameModeMap {
		if m.IsDirectory() {
			names = append(names, recurseNames(master, filepath.Join(name, n))...)
		} else {
			names = append(names, filepath.Join(name, n))
		}
	}
	if !a.Deletion() {
		names = append(names, name)
	}
	return
}

func rmMaybeMasterRun(master *Master, req *WorkRequest, rep *WorkResponse) bool {
	g := Getopt(req.Argv[1:], nil, nil, true)

	force := g.HasLong("force") || g.HasShort('f')
	delete(g.Long, "force")
	delete(g.Short, 'f')

	recursive := g.HasLong("recursive") || g.HasShort('r') || g.HasShort('R')
	delete(g.Long, "recursive")
	delete(g.Short, 'R')
	delete(g.Short, 'r')

	if g.HasOptions() {
		return false
	}

	log.Println("Running in master:", req.Summary())
	todo := []string{}
	for _, a := range g.Args {
		if a[0] != '/' {
			a = filepath.Join(req.Dir, a)
		}
		a = strings.TrimLeft(filepath.Clean(a), "/")
		todo = append(todo, a)
	}

	fs := attr.FileSet{}
	msgs := []string{}
	status := 0
	if recursive {
		for _, t := range todo {
			for _, n := range recurseNames(master, t) {
				fs.Files = append(fs.Files, &attr.FileAttr{
					Path: n,
				})
			}
		}
	} else {
		for _, p := range todo {
			a := master.fileServer.attributes.GetDir(p)
			switch {
			case a.Deletion():
				if !force {
					msgs = append(msgs, fmt.Sprintf("rm: no such file or directory: %s", p))
					status = 1
				}
			case a.IsDirectory() && !recursive:
				msgs = append(msgs, fmt.Sprintf("rm: is a directory: %s", p))
				status = 1
			default:
				fs.Files = append(fs.Files, &attr.FileAttr{Path: p})
			}
		}
	}
	fs.Sort()
	master.replay(fs)

	rep.Stderr = strings.Join(msgs, "\n")
	rep.Exit.WaitStatus = syscall.WaitStatus(status << 8)
	return true
}

func mkdirMaybeMasterRun(master *Master, req *WorkRequest, rep *WorkResponse) bool {
	g := Getopt(req.Argv[1:], nil, nil, true)

	hasParent := g.HasLong("parent") || g.HasShort('p')

	delete(g.Long, "parent")
	delete(g.Short, 'p')

	if len(g.Short) > 0 || len(g.Long) > 0 {
		return false
	}
	for _, a := range g.Args {
		// mkdir -p a/../b should create both a and b.
		if strings.Contains(a, "..") {
			return false
		}
	}

	log.Println("Running in master:", req.Summary())
	for _, a := range g.Args {
		if a[0] != '/' {
			a = filepath.Join(req.Dir, a)
		}
		a = filepath.Clean(a)
		if hasParent {
			mkdirParentMasterRun(master, a, rep)
		} else {
			mkdirNormalMasterRun(master, a, rep)
		}
	}
	return true
}

// Should receive full path.
func mkdirParentMasterRun(master *Master, arg string, rep *WorkResponse) {
	rootless := strings.TrimLeft(arg, "/")
	components := strings.Split(rootless, "/")

	msgs := []string{}
	parent := master.attributes.Get("")
	for i := range components {
		p := strings.Join(components[:i+1], "/")

		dirAttr := master.fileServer.attributes.Get(p)
		if dirAttr.Deletion() {
			entry := mkdirEntry(p)
			parent.Ctime_ns = entry.Ctime_ns
			parent.Mtime_ns = entry.Mtime_ns
			fs := attr.FileSet{
				Files: []*attr.FileAttr{parent, entry},
			}
			master.replay(fs)

			parent = entry
		} else if dirAttr.IsDirectory() {
			parent = dirAttr
		} else {
			msgs = append(msgs, fmt.Sprintf("Not a directory: /%s", p))
			break
		}
	}

	if len(msgs) > 0 {
		rep.Stderr = strings.Join(msgs, "\n")
		rep.Exit.WaitStatus = 1 << 8
	}
}

func mkdirEntry(rootless string) *attr.FileAttr {
	now := time.Nanoseconds()

	return &attr.FileAttr{
		Path: rootless,
		FileInfo: &os.FileInfo{
			Mode:     syscall.S_IFDIR | 0755,
			Atime_ns: now,
			Ctime_ns: now,
			Mtime_ns: now,
		},
		NameModeMap: map[string]attr.FileMode{},
	}
}

func mkdirNormalMasterRun(master *Master, arg string, rep *WorkResponse) {
	rootless := strings.TrimLeft(arg, "/")
	dir, _ := SplitPath(rootless)
	dirAttr := master.fileServer.attributes.Get(dir)
	if dirAttr.Deletion() {
		rep.Stderr = fmt.Sprintf("File not found: /%s", dir)
		rep.Exit = os.Waitmsg{
			WaitStatus: (1 << 8),
		}
		return
	}

	if !dirAttr.IsDirectory() {
		rep.Stderr = fmt.Sprintf("Is not a directory: /%s", dir)
		rep.Exit = os.Waitmsg{
			WaitStatus: (1 << 8),
		}
		return
	}

	chAttr := master.fileServer.attributes.Get(rootless)
	if !chAttr.Deletion() {
		rep.Stderr = fmt.Sprintf("File exists: /%s", rootless)
		rep.Exit = os.Waitmsg{
			WaitStatus: (1 << 8),
		}
		return
	}
	chAttr = mkdirEntry(rootless)

	fs := attr.FileSet{}
	dirAttr.Ctime_ns = chAttr.Ctime_ns
	dirAttr.Mtime_ns = chAttr.Mtime_ns
	fs.Files = append(fs.Files, dirAttr, chAttr)
	master.replay(fs)
}
