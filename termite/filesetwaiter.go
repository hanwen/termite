package termite

import (
	"log"
	"os"
	"sync"
)

var _ = log.Println

type fileSetWaiter struct {
	process func(fset FileSet) os.Error
	sync.Mutex
	channels map[int]chan int
}

func newFileSetWaiter(proc func(FileSet) os.Error) *fileSetWaiter {
	return &fileSetWaiter{
		process:  proc,
		channels: make(map[int]chan int),
	}
}

func (me *fileSetWaiter) newChannel(id int) chan int {
	me.Lock()
	defer me.Unlock()

	c := make(chan int, 1)
	me.channels[id] = c
	return c
}

func (me *fileSetWaiter) findChannel(id int) chan int {
	me.Lock()
	defer me.Unlock()
	return me.channels[id]
}

func (me *fileSetWaiter) signal(id int) {
	me.Lock()
	defer me.Unlock()
	ch := me.channels[id]
	if ch != nil {
		ch <- 1
		close(ch)
		delete(me.channels, id)
	}
}

func (me *fileSetWaiter) flush(id int) {
	me.Lock()
	defer me.Unlock()
	ch := me.channels[id]
	close(ch)
	delete(me.channels, id)
}

func (me *fileSetWaiter) drop(id int) {
	me.Lock()
	defer me.Unlock()
	delete(me.channels, id)
}

func (me *fileSetWaiter) wait(rep *WorkResponse, waitId int) (err os.Error) {
	if rep.FileSet != nil {
		log.Println("Got data for tasks: ", rep.TaskIds, rep.FileSet.Files)

		err = me.process(*rep.FileSet)
		for _, id := range rep.TaskIds {
			if id == waitId {
				continue
			}
			if err == nil {
				me.signal(id)
			} else {
				me.flush(id)
			}
		}
	} else {
		completion := me.findChannel(waitId)
		if completion != nil {
			// completion may be nil if the response
			// already came in.
			_, ok := <-completion
			if !ok {
				return os.NewError("files were never sent.")
			}
		}
	}
	me.drop(waitId)
	return err
}
