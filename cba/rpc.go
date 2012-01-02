package cba

import (
	"fmt"
)

type Request struct {
	Hash  string
	Start int
}

func (me *Request) String() string {
	return fmt.Sprintf("%x [%d]", me.Hash, me.Start)
}

type Response struct {
	Size  int
	Have  bool
	Last  bool
	Chunk []byte
}
