package termite

import (
	"io"
	"os"
)

func ServeFileContent(cache *ContentCache, req *ContentRequest, rep *ContentResponse) error {
	if c := cache.ContentsIfLoaded(req.Hash); c != nil {
		end := req.End
		if end > len(c) {
			end = len(c)
		}
		rep.Chunk = c[req.Start:end]
		return nil
	}

	f, err := os.Open(cache.Path(req.Hash))
	if err != nil {
		return err
	}
	defer f.Close()

	rep.Chunk = make([]byte, req.End-req.Start)
	n, err := f.ReadAt(rep.Chunk, int64(req.Start))
	rep.Chunk = rep.Chunk[:n]

	if err == io.EOF {
		err = nil
	}
	return err
}
