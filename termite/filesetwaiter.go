package termite

import (
	"log"
	"os"
	"sync"
)

var _ = log.Println

type fileSetWaiter struct {
	master *Master
	mirror *mirrorConnection
	sync.Mutex
	channels map[int]chan int
}

func newFileSetWaiter(master *Master, mc *mirrorConnection) *fileSetWaiter {
	return &fileSetWaiter{
		mirror:   mc,
		master:   master,
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

func (me *fileSetWaiter) broadcast(id int) {
	me.Lock()
	defer me.Unlock()
	ch := me.channels[id]
	if ch != nil {
		ch <- 1 
		close(ch)
		me.channels[id] = nil, false
	}
}

func (me *fileSetWaiter) flush(id int) {
	me.Lock()
	defer me.Unlock()
	ch := me.channels[id] 
	close(ch)
	me.channels[id] = nil, false
}

func (me *fileSetWaiter) drop(id int) {
	me.Lock()
	defer me.Unlock()
	me.channels[id] = nil, false
}

func (me *fileSetWaiter) wait(rep *WorkResponse, waitId int) (err os.Error) {
	log.Println("Got data for tasks: ", rep.TaskIds)
	
	if rep.FileSet != nil {
		err = me.master.replayFileModifications(me.mirror.rpcClient, rep.FileSet.Files)
		if err == nil {
			me.master.fileServer.updateFiles(rep.FileSet.Files)
			me.master.mirrors.queueFiles(me.mirror, *rep.FileSet)
			for _, id := range rep.TaskIds {
				if id != waitId {
					me.broadcast(id)
				}
			}
		} else {
			for _, id := range rep.TaskIds {
				if id != waitId {
					me.flush(waitId)
				}
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
