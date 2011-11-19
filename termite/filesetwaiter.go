package termite

import (
	"errors"
	"github.com/hanwen/termite/attr"
	"log"
	"sync"
)

var _ = log.Println

type FileSetWaiter struct {
	process func(fset attr.FileSet) error
	sync.Mutex
	channels map[int]chan int
}

func NewFileSetWaiter(proc func(attr.FileSet) error) *FileSetWaiter {
	return &FileSetWaiter{
		process:  proc,
		channels: make(map[int]chan int),
	}
}

func (me *FileSetWaiter) NewChannel(id int) chan int {
	me.Lock()
	defer me.Unlock()

	c := make(chan int, 1)
	me.channels[id] = c
	return c
}

func (me *FileSetWaiter) findChannel(id int) chan int {
	me.Lock()
	defer me.Unlock()
	return me.channels[id]
}

func (me *FileSetWaiter) signal(id int) {
	me.Lock()
	defer me.Unlock()
	ch := me.channels[id]
	if ch != nil {
		ch <- 1
		close(ch)
		delete(me.channels, id)
	}
}

func (me *FileSetWaiter) flush(id int) {
	me.Lock()
	defer me.Unlock()
	ch := me.channels[id]
	close(ch)
	delete(me.channels, id)
}

func (me *FileSetWaiter) drop(id int) {
	me.Lock()
	defer me.Unlock()
	delete(me.channels, id)
}

func (me *FileSetWaiter) Wait(rep *WorkResponse, waitId int) (err error) {
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
				return errors.New("files were never sent.")
			}
		}
	}
	me.drop(waitId)
	return err
}
