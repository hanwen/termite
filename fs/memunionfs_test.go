package fs

import (
	"fmt"
	"github.com/hanwen/go-fuse/fuse"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"
)

var _ = fmt.Print
var _ = log.Print

const entryTtl = 100 * time.Millisecond

var CheckSuccess = fuse.CheckSuccess

// TODO - use ioutil.WriteFile directly.
func writeToFile(path string, contents string) {
	err := ioutil.WriteFile(path, []byte(contents), 0644)
	CheckSuccess(err)
}

func readFromFile(path string) string {
	b, err := ioutil.ReadFile(path)
	CheckSuccess(err)
	return string(b)
}

func dirNames(path string) map[string]bool {
	f, err := os.Open(path)
	fuse.CheckSuccess(err)

	result := make(map[string]bool)
	names, err := f.Readdirnames(-1)
	fuse.CheckSuccess(err)
	err = f.Close()
	CheckSuccess(err)

	for _, nm := range names {
		result[nm] = true
	}
	return result
}

func setupMemUfs(t *testing.T) (workdir string, ufs *MemUnionFs, cleanup func()) {
	// Make sure system setting does not affect test.
	syscall.Umask(0)

	wd, _ := ioutil.TempDir("", "")
	err := os.Mkdir(wd+"/mnt", 0700)
	fuse.CheckSuccess(err)

	err = os.Mkdir(wd+"/backing", 0700)
	fuse.CheckSuccess(err)

	os.Mkdir(wd+"/ro", 0700)
	fuse.CheckSuccess(err)

	roFs := fuse.NewLoopbackFileSystem(wd + "/ro")
	memFs, err := NewMemUnionFs(wd+"/backing", roFs)
	if err != nil {
		t.Fatal(err)
	}

	// We configure timeouts are smaller, so we can check for
	// UnionFs's cache consistency.
	opts := &fuse.FileSystemOptions{
		EntryTimeout:    entryTtl/2,
		AttrTimeout:     entryTtl/2,
		NegativeTimeout: entryTtl/2,
		PortableInodes:  true,
	}

	state, conn, err := fuse.MountNodeFileSystem(wd+"/mnt", memFs, opts)
	CheckSuccess(err)
	conn.Debug = fuse.VerboseTest()
	state.Debug = fuse.VerboseTest()
	go state.Loop()

	return wd, memFs, func() {
		state.Unmount()
		os.RemoveAll(wd)
	}
}

func TestMemUnionFsSymlink(t *testing.T) {
	wd, ufs, clean := setupMemUfs(t)
	defer clean()

	err := os.Symlink("/foobar", wd+"/mnt/link")
	CheckSuccess(err)

	val, err := os.Readlink(wd + "/mnt/link")
	CheckSuccess(err)

	if val != "/foobar" {
		t.Errorf("symlink mismatch: %v", val)
	}

	r := ufs.Reap()
	if len(r) != 2 || r["link"] == nil || r["link"].Link != "/foobar" {
		t.Errorf("expect 1 symlink reap result: %v", r)
	}
}

func TestMemUnionFsSymlinkPromote(t *testing.T) {
	wd, ufs, clean := setupMemUfs(t)
	defer clean()

	err := os.Mkdir(wd+"/ro/subdir", 0755)
	CheckSuccess(err)

	err = os.Symlink("/foobar", wd+"/mnt/subdir/link")
	CheckSuccess(err)

	r := ufs.Reap()
	if len(r) != 2 || r["subdir"] == nil || r["subdir/link"] == nil || r["subdir/link"].Link != "/foobar" {
		t.Errorf("expect 1 symlink reap result: %v", r)
	}
}

func TestMemUnionFsChtimes(t *testing.T) {
	wd, ufs, clean := setupMemUfs(t)
	defer clean()

	writeToFile(wd+"/ro/file", "a")
	err := os.Chtimes(wd+"/ro/file", time.Unix(42, 0), time.Unix(43, 0))
	CheckSuccess(err)

	err = os.Chtimes(wd+"/mnt/file", time.Unix(82, 0), time.Unix(83, 0))
	CheckSuccess(err)

	st := syscall.Stat_t{}
	err = syscall.Lstat(wd + "/mnt/file", &st)
	if st.Atim.Nano() != 82e9 || st.Mtim.Nano() != 83e9 {
		t.Error("Incorrect timestamp", st)
	}

	r := ufs.Reap()
	if r["file"] == nil || r["file"].Original == "" {
		t.Errorf("expect 1 file reap result: %v", r)
	}
}

func TestMemUnionFsChmod(t *testing.T) {
	wd, ufs, clean := setupMemUfs(t)
	defer clean()

	ro_fn := wd + "/ro/file"
	m_fn := wd + "/mnt/file"
	writeToFile(ro_fn, "a")
	err := os.Chmod(m_fn, 070)
	CheckSuccess(err)

	st := syscall.Stat_t{}
	err = syscall.Lstat(m_fn, &st)
	CheckSuccess(err)
	if st.Mode&07777 != 070 {
		t.Errorf("Unexpected mode found: %o", st.Mode)
	}

	r := ufs.Reap()
	if r["file"] == nil || r["file"].Original == "" {
		t.Errorf("expect 1 file reap result: %v", r)
	}
}

func TestMemUnionFsChown(t *testing.T) {
	wd, _, clean := setupMemUfs(t)
	defer clean()

	ro_fn := wd + "/ro/file"
	m_fn := wd + "/mnt/file"
	writeToFile(ro_fn, "a")

	err := os.Chown(m_fn, 0, 0)
	code := fuse.ToStatus(err)
	if code != fuse.EPERM {
		t.Error("Unexpected error code", code, err)
	}
}

func TestMemUnionFsDelete(t *testing.T) {
	wd, ufs, clean := setupMemUfs(t)
	defer clean()

	writeToFile(wd+"/ro/file", "a")
	_, err := os.Lstat(wd + "/mnt/file")
	CheckSuccess(err)

	err = os.Remove(wd + "/mnt/file")
	CheckSuccess(err)

	_, err = os.Lstat(wd + "/mnt/file")
	if err == nil {
		t.Fatal("should have disappeared.")
	}

	r := ufs.Reap()
	if r["file"] == nil || r["file"].Attr != nil {
		t.Errorf("expect 1 deletion reap result: %v", r)
	}
}

func TestMemUnionFsBasic(t *testing.T) {
	wd, _, clean := setupMemUfs(t)
	defer clean()

	writeToFile(wd+"/mnt/rw", "a")
	writeToFile(wd+"/ro/ro1", "a")
	writeToFile(wd+"/ro/ro2", "b")
	names := dirNames(wd + "/mnt")

	expected := map[string]bool{
		"rw": true, "ro1": true, "ro2": true,
	}
	if !reflect.DeepEqual(expected, names) {
		t.Fatal("dir contents mismatch", expected, names)
	}

	writeToFile(wd+"/mnt/new", "new contents")

	contents := readFromFile(wd + "/mnt/new")
	if contents != "new contents" {
		t.Errorf("read mismatch: '%v'", contents)
	}
	writeToFile(wd+"/mnt/ro1", "promote me")

	os.Remove(wd + "/mnt/new")
	names = dirNames(wd + "/mnt")
	want := map[string]bool{"rw": true, "ro1": true, "ro2": true}
	if !reflect.DeepEqual(names, want) {
		t.Fatalf("mismatch got %v want %v", names, want)
	}

	os.Remove(wd + "/mnt/ro1")
	names = dirNames(wd + "/mnt")
	want = map[string]bool{
		"rw": true, "ro2": true,
	}
	if !reflect.DeepEqual(want, names) {
		t.Fatalf("got %v want %v", names, want)
	}
}

func TestMemUnionFsPromote(t *testing.T) {
	wd, ufs, clean := setupMemUfs(t)
	defer clean()

	err := os.Mkdir(wd+"/ro/subdir", 0755)
	CheckSuccess(err)
	writeToFile(wd+"/ro/subdir/file", "content")
	writeToFile(wd+"/mnt/subdir/file", "other-content")

	r := ufs.Reap()
	if r["subdir/file"] == nil || r["subdir/file"].Backing == "" {
		t.Errorf("expect 1 file reap result: %v", r)
	}
}

func TestMemUnionFsSubdirCreate(t *testing.T) {
	wd, ufs, clean := setupMemUfs(t)
	defer clean()

	err := os.MkdirAll(wd+"/ro/subdir/sub2", 0755)
	CheckSuccess(err)
	writeToFile(wd+"/mnt/subdir/sub2/file", "other-content")
	_, err = os.Lstat(wd + "/mnt/subdir/sub2/file")
	CheckSuccess(err)

	r := ufs.Reap()
	if r["subdir/sub2/file"] == nil || r["subdir/sub2/file"].Backing == "" {
		t.Errorf("expect 1 file reap result: %v", r)
	}
}

func TestMemUnionFsCreate(t *testing.T) {
	wd, ufs, clean := setupMemUfs(t)
	defer clean()

	writeToFile(wd+"/mnt/file.txt", "hello")
	_, err := os.Lstat(wd + "/mnt/file.txt")
	CheckSuccess(err)

	r := ufs.Reap()
	if r["file.txt"] == nil || r["file.txt"].Backing == "" {
		t.Errorf("expect 1 file reap result: %v", r)
	}
}

func TestMemUnionFsOpenUndeletes(t *testing.T) {
	wd, _, clean := setupMemUfs(t)
	defer clean()

	writeToFile(wd+"/ro/file", "X")
	err := os.Remove(wd + "/mnt/file")
	CheckSuccess(err)
	writeToFile(wd+"/mnt/file", "X")
	_, err = os.Lstat(wd + "/mnt/file")
}

func TestMemUnionFsMkdir(t *testing.T) {
	wd, ufs, clean := setupMemUfs(t)
	defer clean()

	dirname := wd + "/mnt/subdir"
	err := os.Mkdir(dirname, 0755)
	CheckSuccess(err)

	err = os.Remove(dirname)
	CheckSuccess(err)

	r := ufs.Reap()
	if len(r) > 2 || r[""] == nil || r["subdir"] != nil {
		t.Errorf("expect 1 file reap result: %v", r)
	}
}

func TestMemUnionFsMkdirPromote(t *testing.T) {
	wd, ufs, clean := setupMemUfs(t)
	defer clean()

	dirname := wd + "/ro/subdir/subdir2"
	err := os.MkdirAll(dirname, 0755)
	CheckSuccess(err)

	err = os.Mkdir(wd+"/mnt/subdir/subdir2/dir3", 0755)
	CheckSuccess(err)

	r := ufs.Reap()
	if r["subdir/subdir2/dir3"] == nil || r["subdir/subdir2/dir3"].Attr.Mode&fuse.S_IFDIR == 0 {
		t.Errorf("expect 1 file reap result: %v", r)
	}
}

func TestMemUnionFsRmdirMkdir(t *testing.T) {
	wd, _, clean := setupMemUfs(t)
	defer clean()

	err := os.Mkdir(wd+"/ro/subdir", 0755)
	CheckSuccess(err)

	dirname := wd + "/mnt/subdir"
	err = os.Remove(dirname)
	CheckSuccess(err)

	err = os.Mkdir(dirname, 0755)
	CheckSuccess(err)
}

func TestMemUnionFsLink(t *testing.T) {
	wd, _, clean := setupMemUfs(t)
	defer clean()

	content := "blabla"
	fn := wd + "/ro/file"
	err := ioutil.WriteFile(fn, []byte(content), 0666)
	CheckSuccess(err)

	err = os.Link(wd+"/mnt/file", wd+"/mnt/linked")
	CheckSuccess(err)

	var st2 syscall.Stat_t
	err = syscall.Lstat(wd + "/mnt/linked", &st2)
	CheckSuccess(err)

	var st1 syscall.Stat_t
	err = syscall.Lstat(wd + "/mnt/file", &st1)
	CheckSuccess(err)

	if st1.Ino != st2.Ino {
		t.Errorf("inode numbers should be equal for linked files %v, %v", st1.Ino, st2.Ino)
	}
	c, err := ioutil.ReadFile(wd + "/mnt/linked")
	if string(c) != content {
		t.Errorf("content mismatch got %q want %q", string(c), content)
	}
}

func TestMemUnionFsCreateLink(t *testing.T) {
	wd, _, clean := setupMemUfs(t)
	defer clean()

	content := "blabla"
	fn := wd + "/mnt/file"
	err := ioutil.WriteFile(fn, []byte(content), 0666)
	CheckSuccess(err)

	err = os.Link(wd+"/mnt/file", wd+"/mnt/linked")
	CheckSuccess(err)
}

func TestMemUnionFsTruncate(t *testing.T) {
	wd, _, clean := setupMemUfs(t)
	defer clean()

	writeToFile(wd+"/ro/file", "hello")
	os.Truncate(wd+"/mnt/file", 2)
	content := readFromFile(wd + "/mnt/file")
	if content != "he" {
		t.Errorf("unexpected content %v", content)
	}
}

func TestMemUnionFsCopyChmod(t *testing.T) {
	wd, _, clean := setupMemUfs(t)
	defer clean()

	contents := "hello"
	fn := wd + "/mnt/y"
	err := ioutil.WriteFile(fn, []byte(contents), 0644)
	CheckSuccess(err)

	err = os.Chmod(fn, 0755)
	CheckSuccess(err)

	st := syscall.Stat_t{}
	err = syscall.Lstat(fn, &st)
	CheckSuccess(err)
	if st.Mode&0111 == 0 {
		t.Errorf("1st attr error %o", st.Mode)
	}
	time.Sleep(entryTtl * 11/10)
	err = syscall.Lstat(fn, &st)
	CheckSuccess(err)
	if st.Mode&0111 == 0 {
		t.Errorf("uncached attr error %o", st.Mode)
	}
}

func TestMemUnionFsTruncateTimestamp(t *testing.T) {
	wd, _, clean := setupMemUfs(t)
	defer clean()

	contents := "hello"
	fn := wd + "/mnt/y"
	err := ioutil.WriteFile(fn, []byte(contents), 0644)
	CheckSuccess(err)
	time.Sleep(200 * time.Millisecond)

	truncTs := time.Now().UnixNano()
	err = os.Truncate(fn, 3)
	CheckSuccess(err)

	st := syscall.Stat_t{}
	err = syscall.Lstat(fn, &st)
	CheckSuccess(err)

	if abs(truncTs-st.Mtim.Nano()) > 0.1e9 {
		t.Error("timestamp drift", truncTs, st.Mtim)
	}
}

func abs(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}

func TestMemUnionFsRemoveAll(t *testing.T) {
	wd, _, clean := setupMemUfs(t)
	defer clean()

	err := os.MkdirAll(wd+"/ro/dir/subdir", 0755)
	CheckSuccess(err)

	contents := "hello"
	fn := wd + "/ro/dir/subdir/y"
	err = ioutil.WriteFile(fn, []byte(contents), 0644)
	CheckSuccess(err)

	err = os.RemoveAll(wd + "/mnt/dir")
	if err != nil {
		t.Error("Should delete all")
	}

	for _, f := range []string{"dir/subdir/y", "dir/subdir", "dir"} {
		if fi, _ := os.Lstat(filepath.Join(wd, "mount", f)); fi != nil {
			t.Errorf("file %s should have disappeared: %v", f, fi)
		}
	}
}

func TestMemUnionFsRmRf(t *testing.T) {
	wd, _, clean := setupMemUfs(t)
	defer clean()

	err := os.MkdirAll(wd+"/ro/dir/subdir", 0755)
	CheckSuccess(err)

	contents := "hello"
	fn := wd + "/ro/dir/subdir/y"
	err = ioutil.WriteFile(fn, []byte(contents), 0644)
	CheckSuccess(err)
	bin, err := exec.LookPath("rm")
	CheckSuccess(err)
	cmd := exec.Command(bin, "-rf", wd+"/mnt/dir")
	err = cmd.Run()
	if err != nil {
		t.Fatal("rm -rf returned error:", err)
	}

	for _, f := range []string{"dir/subdir/y", "dir/subdir", "dir"} {
		if fi, _ := os.Lstat(filepath.Join(wd, "mount", f)); fi != nil {
			t.Errorf("file %s should have disappeared: %v", f, fi)
		}
	}
}

func TestMemUnionFsDeletedGetAttr(t *testing.T) {
	wd, _, clean := setupMemUfs(t)
	defer clean()

	err := ioutil.WriteFile(wd+"/ro/file", []byte("blabla"), 0644)
	CheckSuccess(err)

	f, err := os.Open(wd + "/mnt/file")
	CheckSuccess(err)
	defer f.Close()

	err = os.Remove(wd + "/mnt/file")
	CheckSuccess(err)

	st := syscall.Stat_t{}
	if err := syscall.Fstat(int(f.Fd()), &st); err != nil || st.Mode & syscall.S_IFREG == 0 {
		t.Fatalf("stat returned error or non-file: %v %v", err, st)
	}
}

func TestMemUnionFsDoubleOpen(t *testing.T) {
	wd, _, clean := setupMemUfs(t)
	defer clean()
	err := ioutil.WriteFile(wd+"/ro/file", []byte("blablabla"), 0644)
	CheckSuccess(err)

	roFile, err := os.Open(wd + "/mnt/file")
	CheckSuccess(err)
	defer roFile.Close()
	rwFile, err := os.OpenFile(wd+"/mnt/file", os.O_WRONLY|os.O_TRUNC, 0666)
	CheckSuccess(err)
	defer rwFile.Close()

	output, err := ioutil.ReadAll(roFile)
	CheckSuccess(err)
	if len(output) != 0 {
		t.Errorf("After r/w truncation, r/o file should be empty too: %q", string(output))
	}

	want := "hello"
	_, err = rwFile.Write([]byte(want))
	CheckSuccess(err)

	b := make([]byte, 100)

	roFile.Seek(0, 0)
	n, err := roFile.Read(b)
	CheckSuccess(err)
	b = b[:n]

	if string(b) != "hello" {
		t.Errorf("r/w and r/o file are not synchronized: got %q want %q", string(b), want)
	}
}

func TestMemUnionFsUpdate(t *testing.T) {
	wd, ufs, clean := setupMemUfs(t)
	defer clean()

	err := ioutil.WriteFile(wd+"/ro/file1", []byte("blablabla"), 0644)
	CheckSuccess(err)

	_, err = os.Lstat(wd + "/mnt/file1")
	CheckSuccess(err)
	if fi, _ := os.Lstat(wd + "/mnt/file2"); fi != nil {
		t.Fatal("file2 should not exist", fi)
	}
	if fi, _ := os.Lstat(wd + "/mnt/symlink"); fi != nil {
		t.Fatal("symlink should not exist", fi)
	}

	err = os.Remove(wd + "/ro/file1")
	CheckSuccess(err)
	err = ioutil.WriteFile(wd+"/ro/file2", []byte("foobar"), 0644)
	CheckSuccess(err)
	err = os.Symlink("target", wd+"/ro/symlink")
	CheckSuccess(err)

	// Still have cached attributes.
	fi, err := os.Lstat(wd + "/mnt/file1")
	CheckSuccess(err)
	if fi, _ := os.Lstat(wd + "/mnt/file2"); fi != nil {
		t.Fatal("file2 should not exist")
	}
	if fi, _ := os.Lstat(wd + "/mnt/symlink"); fi != nil {
		t.Fatal("symlink should not exist", fi)
	}

	roF2fi, err := os.Lstat(wd + "/ro/file2")
	CheckSuccess(err)
	roF2 := fuse.ToAttr(roF2fi)

	roSymlinkFi, err := os.Lstat(wd + "/ro/symlink")
	CheckSuccess(err)
	roSymlink := fuse.ToAttr(roSymlinkFi)

	updates := map[string]*Result{
		"file1": {
			nil, "", "", "",
		},
		"file2": {
			roF2, "", "", "",
		},
		"symlink": {
			roSymlink, "", "", "target",
		},
	}

	ufs.Update(updates)

	// Cached attributes flushed.
	if fi, _ := os.Lstat(wd + "/mnt/file1"); fi != nil {
		t.Fatal("file1 should have disappeared", fi)
	}

	fi, err = os.Lstat(wd + "/mnt/file2")
	CheckSuccess(err)
	if roF2.ModTime().UnixNano() != fi.ModTime().UnixNano() {
		t.Fatalf("file2 attribute mismatch: got %v want %v", fi, roF2)
	}

	val, err := os.Readlink(wd + "/mnt/symlink")
	CheckSuccess(err)
	if val != "target" {
		t.Error("symlink value got %q want %v", val, "target")
	}
}

func TestMemUnionFsFdLeak(t *testing.T) {
	beforeEntries, err := ioutil.ReadDir("/proc/self/fd")
	CheckSuccess(err)

	wd, _, clean := setupMemUfs(t)
	err = ioutil.WriteFile(wd+"/ro/file", []byte("blablabla"), 0644)
	CheckSuccess(err)

	contents, err := ioutil.ReadFile(wd + "/mnt/file")
	CheckSuccess(err)

	err = ioutil.WriteFile(wd+"/mnt/file", contents, 0644)
	CheckSuccess(err)

	clean()

	afterEntries, err := ioutil.ReadDir("/proc/self/fd")
	CheckSuccess(err)

	if len(afterEntries) != len(beforeEntries) {
		t.Errorf("/proc/self/fd changed size: after %v before %v", len(beforeEntries), len(afterEntries))
	}
}

func TestMemUnionFsStatFs(t *testing.T) {
	wd, _, clean := setupMemUfs(t)
	defer clean()

	s1 := syscall.Statfs_t{}
	err := syscall.Statfs(wd+"/mnt", &s1)
	if err != nil {
		t.Fatal("statfs mnt", err)
	}
	if s1.Bsize == 0 {
		t.Fatal("Expect blocksize > 0")
	}
}

func TestMemUnionFsFlushSize(t *testing.T) {
	wd, _, clean := setupMemUfs(t)
	defer clean()

	fn := wd + "/mnt/file"
	f, err := os.OpenFile(fn, os.O_WRONLY|os.O_CREATE, 0644)
	CheckSuccess(err)
	fi, err := f.Stat()
	CheckSuccess(err)

	n, err := f.Write([]byte("hello"))
	CheckSuccess(err)

	f.Close()
	fi, err = os.Lstat(fn)
	CheckSuccess(err)
	if fi.Size() != int64(n) {
		t.Errorf("got %d from Stat().Size, want %d", fi.Size(), n)
	}
}

func TestMemUnionFsFlushRename(t *testing.T) {
	wd, _, clean := setupMemUfs(t)
	defer clean()

	err := ioutil.WriteFile(wd+"/mnt/file", []byte("x"), 0644)

	fn := wd + "/mnt/tmp"
	f, err := os.OpenFile(fn, os.O_WRONLY|os.O_CREATE, 0644)
	CheckSuccess(err)
	fi, err := f.Stat()
	CheckSuccess(err)

	n, err := f.Write([]byte("hello"))
	CheckSuccess(err)
	f.Close()

	dst := wd + "/mnt/file"
	err = os.Rename(fn, dst)
	CheckSuccess(err)

	fi, err = os.Lstat(dst)
	CheckSuccess(err)
	if fi.Size() != int64(n) {
		t.Errorf("got %d from Stat().Size, want %d", fi.Size(), n)
	}
}

func TestMemUnionFsTruncGetAttr(t *testing.T) {
	wd, _, clean := setupMemUfs(t)
	defer clean()

	c := []byte("hello")
	f, err := os.OpenFile(wd+"/mnt/file", os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	CheckSuccess(err)
	_, err = f.Write(c)
	CheckSuccess(err)
	err = f.Close()
	CheckSuccess(err)

	fi, err := os.Lstat(wd + "/mnt/file")
	if fi.Size() != int64(len(c)) {
		t.Fatalf("Length mismatch got %d want %d", fi.Size(), len(c))
	}
}

func TestMemUnionFsRenameDirBasic(t *testing.T) {
	wd, ufs, clean := setupMemUfs(t)
	defer clean()

	err := os.MkdirAll(wd+"/ro/dir/subdir", 0755)
	CheckSuccess(err)

	err = os.Rename(wd+"/mnt/dir", wd+"/mnt/renamed")
	CheckSuccess(err)

	if fi, _ := os.Lstat(wd + "/mnt/dir"); fi != nil {
		t.Fatalf("%s/mnt/dir should have disappeared: %v", wd, fi)
	}

	if fi, _ := os.Lstat(wd + "/mnt/renamed"); fi == nil || !fi.IsDir() {
		t.Fatalf("%s/mnt/renamed should be directory: %v", wd, fi)
	}

	entries, err := ioutil.ReadDir(wd + "/mnt/renamed")
	if err != nil || len(entries) != 1 || entries[0].Name() != "subdir" {
		t.Errorf("readdir(%s/mnt/renamed) should have one entry: %v, err %v", wd, entries, err)
	}

	r := ufs.Reap()
	if r["dir"] == nil || r["dir"].Attr != nil || r["renamed/subdir"] == nil || !r["renamed/subdir"].Attr.IsDir() {
		t.Errorf("Reap should del dir, and add renamed/subdir: %v", r)
	}

	if err = os.Mkdir(wd+"/mnt/dir", 0755); err != nil {
		t.Errorf("mkdir should succeed %v", err)
	}

}

func TestMemUnionFsRenameDirAllSourcesGone(t *testing.T) {
	wd, ufs, clean := setupMemUfs(t)
	defer clean()

	err := os.MkdirAll(wd+"/ro/dir", 0755)
	CheckSuccess(err)

	err = ioutil.WriteFile(wd+"/ro/dir/file.txt", []byte{42}, 0644)
	CheckSuccess(err)

	err = os.Rename(wd+"/mnt/dir", wd+"/mnt/renamed")
	CheckSuccess(err)

	r := ufs.Reap()
	if r["dir"] == nil || r["dir"].Attr != nil || r["dir/file.txt"] == nil || r["dir/file.txt"].Attr != nil {
		t.Errorf("Expected 2 deletion entries in %v", r)
	}
}

func TestMemUnionFsRenameDirWithDeletions(t *testing.T) {
	wd, _, clean := setupMemUfs(t)
	defer clean()

	err := os.MkdirAll(wd+"/ro/dir/subdir", 0755)
	CheckSuccess(err)

	err = ioutil.WriteFile(wd+"/ro/dir/file.txt", []byte{42}, 0644)
	CheckSuccess(err)

	err = ioutil.WriteFile(wd+"/ro/dir/subdir/file.txt", []byte{42}, 0644)
	CheckSuccess(err)

	if fi, _ := os.Lstat(wd + "/mnt/dir/subdir/file.txt"); fi == nil || fi.IsDir() {
		t.Fatalf("%s/mnt/dir/subdir/file.txt should be file: %v", wd, fi)
	}

	err = os.Remove(wd + "/mnt/dir/file.txt")
	CheckSuccess(err)

	err = os.Rename(wd+"/mnt/dir", wd+"/mnt/renamed")
	CheckSuccess(err)

	if fi, _ := os.Lstat(wd + "/mnt/dir/subdir/file.txt"); fi != nil {
		t.Fatalf("%s/mnt/dir/subdir/file.txt should have disappeared: %v", wd, fi)
	}

	if fi, _ := os.Lstat(wd + "/mnt/dir"); fi != nil {
		t.Fatalf("%s/mnt/dir should have disappeared: %v", wd, fi)
	}

	if fi, _ := os.Lstat(wd + "/mnt/renamed"); fi == nil || !fi.IsDir() {
		t.Fatalf("%s/mnt/renamed should be directory: %v", wd, fi)
	}

	if fi, _ := os.Lstat(wd + "/mnt/renamed/file.txt"); fi != nil {
		t.Fatalf("%s/mnt/renamed/file.txt should have disappeared %#v", wd, fi)
	}

	if err = os.Mkdir(wd+"/mnt/dir", 0755); err != nil {
		t.Errorf("mkdir should succeed %v", err)
	}

	if fi, _ := os.Lstat(wd + "/mnt/dir/subdir"); fi != nil {
		t.Fatalf("%s/mnt/dir/subdir should have disappeared %#v", wd, fi)
	}
}

func TestMemUnionGc(t *testing.T) {
	wd, ufs, clean := setupMemUfs(t)
	defer clean()

	writeToFile(wd+"/mnt/file1", "other-content")
	writeToFile(wd+"/mnt/file2", "other-content")
	err := os.Remove(wd + "/mnt/file1")
	CheckSuccess(err)
	ufs.Reset()

	entries, err := ioutil.ReadDir(wd + "/backing")
	CheckSuccess(err)
	if len(entries) != 0 {
		t.Fatalf("should have 1 file after backing store gc: %v", entries)
	}
}

func testEq(t *testing.T, got interface{}, want interface{}, expectEq bool) {
	gots := fmt.Sprintf("%v", got)
	wants := fmt.Sprintf("%v", want)
	if (gots == wants) != expectEq {
		op := "must differ from"
		if expectEq {
			op = "want"
		}
		t.Fatalf("Got %s %s %s.", gots, op, wants)
	}
}

func TestMemUnionResetAttr(t *testing.T) {
	wd, ufs, clean := setupMemUfs(t)
	defer clean()

	ioutil.WriteFile(wd+"/ro/fileattr", []byte{42}, 0644)
	before, _ := os.Lstat(wd + "/mnt/fileattr")
	err := os.Chmod(wd+"/mnt/fileattr", 0606)
	CheckSuccess(err)
	after, _ := os.Lstat(wd + "/mnt/fileattr")
	testEq(t, printFileInfo(after), printFileInfo(before), false)
	ufs.Reset()
	afterReset, _ := os.Lstat(wd + "/mnt/fileattr")
	testEq(t, printFileInfo(afterReset), printFileInfo(before), true)
}

func TestMemUnionResetContent(t *testing.T) {
	wd, ufs, clean := setupMemUfs(t)
	defer clean()
	ioutil.WriteFile(wd+"/ro/filecontents", []byte{42}, 0644)
	before, _ := ioutil.ReadFile(wd + "/mnt/filecontents")
	ioutil.WriteFile(wd+"/mnt/filecontents", []byte{43}, 0644)
	after, _ := ioutil.ReadFile(wd + "/mnt/filecontents")
	testEq(t, after, before, false)
	ufs.Reset()
	afterReset, err := ioutil.ReadFile(wd + "/mnt/filecontents")
	CheckSuccess(err)
	testEq(t, afterReset, before, true)
}

func TestMemUnionResetDelete(t *testing.T) {
	wd, ufs, clean := setupMemUfs(t)
	defer clean()
	ioutil.WriteFile(wd+"/ro/todelete", []byte{42}, 0644)

	var before, after, afterReset syscall.Stat_t
	syscall.Lstat(wd + "/mnt/todelete", &before)
	before.Ino = 0
	os.Remove(wd + "/mnt/todelete")
	syscall.Lstat(wd + "/mnt/todelete", &after)
	testEq(t, after, before, false)
	ufs.Reset()
	syscall.Lstat(wd + "/mnt/todelete", &afterReset)
	afterReset.Ino = 0
	testEq(t, afterReset, before, true)
}

func printFileInfo(fi os.FileInfo) string {
	return fmt.Sprintf("nm %s sz %d m %v time %v dir %v",
		fi.Name(), fi.Size(), fi.Mode(), fi.ModTime(), fi.IsDir())
}

func serialize(infos []os.FileInfo) string {
	names := []string{}
	vals := map[string]os.FileInfo{}
	for _, i := range infos {
		names = append(names, i.Name())
		vals[i.Name()] = i
	}
	sort.Strings(names)

	result := []string{}
	for _, n := range names {
		result = append(result, printFileInfo(vals[n]))
	}
	return strings.Join(result, ", ")
}

func TestMemUnionResetDirEntry(t *testing.T) {
	wd, ufs, clean := setupMemUfs(t)
	defer clean()
	os.Mkdir(wd+"/ro/dir", 0755)
	ioutil.WriteFile(wd+"/ro/dir/todelete", []byte{42}, 0644)
	before, _ := ioutil.ReadDir(wd + "/mnt/dir")

	ioutil.WriteFile(wd+"/mnt/dir/newfile", []byte{42}, 0644)
	os.Remove(wd + "/mnt/dir/todelete")
	after, _ := ioutil.ReadDir(wd + "/mnt/dir")

	testEq(t, serialize(after), serialize(before), false)
	ufs.Reset()
	reset, _ := ioutil.ReadDir(wd + "/mnt/dir")

	testEq(t, serialize(reset), serialize(before), true)
}

func TestMemUnionResetRename(t *testing.T) {
	wd, ufs, clean := setupMemUfs(t)
	defer clean()
	os.Mkdir(wd+"/ro/dir", 0755)
	ioutil.WriteFile(wd+"/ro/dir/movesrc", []byte{42}, 0644)
	before, _ := ioutil.ReadDir(wd + "/mnt/dir")
	os.Rename(wd+"/mnt/dir/movesrc", wd+"/mnt/dir/dest")
	after, _ := ioutil.ReadDir(wd + "/mnt/dir")
	testEq(t, serialize(after), serialize(before), false)
	ufs.Reset()
	reset, _ := ioutil.ReadDir(wd + "/mnt/dir")
	testEq(t, serialize(reset), serialize(before), true)
}

func TestMemUnionFsTruncateOpen(t *testing.T) {
	wd, _, clean := setupMemUfs(t)
	defer clean()

	fn := wd + "/mnt/test"
	f, err := os.OpenFile(fn, os.O_CREATE|os.O_WRONLY, 0644)
	CheckSuccess(err)
	defer f.Close()

	err = f.Truncate(4096)
	CheckSuccess(err)
	fi, err := os.Lstat(fn)
	CheckSuccess(err)
	if fi.Size() != 4096 {
		t.Errorf("Size should be 4096 after Truncate: %d", fi.Size())
	}
}

