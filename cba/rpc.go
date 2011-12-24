package cba

import "fmt"

type Request struct {
	Hash       string
	Start, End int
}

func (me *Request) String() string {
	return fmt.Sprintf("%x [%d, %d]", me.Hash, me.Start, me.End)
}

type Response struct {
	Size  int
	Have  bool
	Chunk []byte
}
