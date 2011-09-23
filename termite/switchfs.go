package termite

import (
	"github.com/hanwen/go-fuse/fuse"
	"sync"
)

/*
 A FS for the worker.

 - We have to reroute statfs, so it goes to the writable part.

 - We must wait for the last file to be closed before reading.

*/
type SwitchFileSystem struct {
	*fuse.SwitchFileSystem
	statFsFs fuse.FileSystem

	openMutex sync.Mutex
	openCond  *sync.Cond
	openCount int32
}

type countFile struct {
	fuse.File
	switchFs *SwitchFileSystem
}

func (me *countFile) InnerFile() fuse.File {
	return me.File
}

func (me *countFile) Release() {
	me.File.Release()
	fs := me.switchFs
	fs.openMutex.Lock()
	defer fs.openMutex.Unlock()
	fs.openCount--
	fs.openCond.Broadcast()
}

func NewSwitchFileSystem(fs *fuse.SwitchFileSystem, statFsFs fuse.FileSystem) *SwitchFileSystem {
	me := &SwitchFileSystem{
		SwitchFileSystem: fs,
		statFsFs:         statFsFs,
	}
	me.openCond = sync.NewCond(&me.openMutex)
	return me
}

func (me *SwitchFileSystem) StatFs() *fuse.StatfsOut {
	return me.statFsFs.StatFs()
}

func (me *SwitchFileSystem) Create(name string, flags uint32, mode uint32, context *fuse.Context) (file fuse.File, code fuse.Status) {
	file, code = me.SwitchFileSystem.Create(name, flags, mode, context)
	if code.Ok() {
		file = me.newFile(file)
	}

	return
}

func (me *SwitchFileSystem) newFile(file fuse.File) *countFile {
	me.openMutex.Lock()
	defer me.openMutex.Unlock()
	me.openCount++
	return &countFile{file, me}
}

func (me *SwitchFileSystem) Open(name string, flags uint32, context *fuse.Context) (file fuse.File, code fuse.Status) {
	file, code = me.SwitchFileSystem.Open(name, flags, context)
	if code.Ok() && flags&fuse.O_ANYWRITE != 0 {
		file = me.newFile(file)
	}
	return
}

func (me *SwitchFileSystem) WaitClose() {
	me.openMutex.Lock()
	defer me.openMutex.Unlock()
	for me.openCount > 0 {
		me.openCond.Wait()
	}
}
