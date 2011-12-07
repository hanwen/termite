package attr

import (
	"bytes"
	"encoding/gob"
)

func init () {
	gob.Register(&FileAttr{})
}

func (a *FileAttr) Encode() ([]byte, error) {
	b := bytes.NewBuffer(make([]byte, 0, 512))
	enc := gob.NewEncoder(b)
	err := enc.Encode(a)
	return b.Bytes(), err
}

func (a *FileAttr) Decode(b []byte) (error) {
	buf := bytes.NewBuffer(b)
	enc := gob.NewDecoder(buf)
	err := enc.Decode(a)
	return err
}
