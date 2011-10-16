package termite

import (
	"fmt"
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
	}
	return false
}

func recurseNames(master *Master, name string) (names []string) {
	a := master.fileServer.attr.GetDir(name)

	for n, m := range a.NameModeMap {
		if m.IsDirectory() {
			names = append(names, recurseNames(master, filepath.Join(name, n))...)
		} else {
			names = append(names, filepath.Join(name, n))
		}
	}
	names = append(names, name)
	return
}

func rmMaybeMasterRun(master *Master, req *WorkRequest, rep *WorkResponse) bool {
	g := Getopt(req.Argv[1:], nil, nil, true)

	force := g.HasLong("force") || g.HasShort('f')
	g.Long["force"] = "", false
	g.Short['f'] = "", false

	recursive := g.HasLong("recursive") || g.HasShort('r') || g.HasShort('R')
	g.Long["recursive"] = "", false
	g.Short['R'] = "", false
	g.Short['r'] = "", false

	if g.HasOptions() {
		return false
	}

	todo := []string{}
	for _, a := range g.Args {
		if a[0] != '/' {
			a = filepath.Join(req.Dir, a)
		}
		a = strings.TrimLeft(filepath.Clean(a), "/")
		todo = append(todo, a)
	}

	fs := FileSet{}
	msgs := []string{}
	status := 0
	if recursive {
		for _, t := range todo {
			for _, n := range recurseNames(master, t) {
				fs.Files = append(fs.Files, &FileAttr{
					Path: n,
				})
			}
		}
	} else {
		for _, p := range todo {
			attr := master.fileServer.attr.GetDir(p)
			switch {
			case attr.Deletion():
				if !force {
					msgs = append(msgs, fmt.Sprintf("rm: no such file or directory: %s", p))
					status = 1
				}
			case attr.IsDirectory() && !recursive:
				msgs = append(msgs, fmt.Sprintf("rm: is a directory: %s", p))
				status = 1
			default:
				fs.Files = append(fs.Files, &FileAttr{Path: p})
			}
		}
	}
	fs.Sort()
	master.replay(fs)
	master.mirrors.queueFiles(nil, fs)

	rep.Stderr = strings.Join(msgs, "\n")
	rep.Exit.WaitStatus = syscall.WaitStatus(status << 8)
	return true
}

func mkdirMaybeMasterRun(master *Master, req *WorkRequest, rep *WorkResponse) bool {
	g := Getopt(req.Argv[1:], nil, nil, true)

	hasParent := g.HasLong("parent") || g.HasShort('p')

	g.Long["parent"] = "", false
	g.Short['p'] = "", false

	if len(g.Short) > 0 || len(g.Long) > 0 {
		return false
	}
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

func mkdirParentMasterRun(master *Master, arg string, rep *WorkResponse) {
	rootless := strings.TrimLeft(arg, "/")
	components := strings.Split(rootless, "/")
	fs := FileSet{}
	msgs := []string{}
	for i := range components {
		p := strings.Join(components[:i+1], "/")

		dirAttr := master.fileServer.attr.Get(p)
		if dirAttr.Deletion() {
			fs.Files = append(fs.Files, mkdirEntry(p))
		} else if dirAttr.IsDirectory() {
			// ok.
		} else {
			msgs = append(msgs, fmt.Sprintf("Not a directory: /%s", p))
		}
	}

	master.replay(fs)
	master.mirrors.queueFiles(nil, fs)
	if len(msgs) > 0 {
		rep.Stderr = strings.Join(msgs, "\n")
		rep.Exit.WaitStatus = 1 << 8
	}
}

func mkdirEntry(rootless string) *FileAttr {
	now := time.Nanoseconds()
	// TODO - could do without these.
	uid := os.Getuid()
	gid := os.Getgid()

	return &FileAttr{
		Path: rootless,
		FileInfo: &os.FileInfo{
			Mode:     syscall.S_IFDIR | 0755,
			Atime_ns: now,
			Ctime_ns: now,
			Mtime_ns: now,
			Uid:      uid,
			Gid:      gid,
		},
	}
}

func mkdirNormalMasterRun(master *Master, arg string, rep *WorkResponse) {
	rootless := strings.TrimLeft(arg, "/")
	dir, _ := SplitPath(rootless)
	dirAttr := master.fileServer.attr.Get(dir)
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

	chAttr := mkdirEntry(rootless)

	fs := FileSet{}
	fs.Files = append(fs.Files, chAttr)
	master.replay(fs)
	master.mirrors.queueFiles(nil, fs)
}
