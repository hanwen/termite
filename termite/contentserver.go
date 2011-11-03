package termite

import (
	"io"
	"log"
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

func (me *ContentCache) FetchFromServer(fetcher func(req *ContentRequest, rep *ContentResponse) error,
	hash string) error {
	if me.HasHash(hash) {
		return nil
	}
	chunkSize := 1 << 18

	output := me.NewHashWriter()
	written := 0
	for {
		req := &ContentRequest{
			Hash:  hash,
			Start: written,
			End:   written + chunkSize,
		}

		rep := &ContentResponse{}
		err := fetcher(req, rep)
		if err != nil {
			log.Println("FileContent error:", err)
			return err
		}

		if len(rep.Chunk) < chunkSize && written == 0 {
			output.Close()
			saved := me.Save(rep.Chunk)
			if saved != hash {
				log.Fatalf("Corruption: savedHash %x != requested hash %x.", saved, hash)
			}
			return nil
		}

		n, err := output.Write(rep.Chunk)
		written += n
		if err != nil {
			return err
		}
		if len(rep.Chunk) < chunkSize {
			break
		}
	}

	output.Close()
	saved := string(output.hasher.Sum())
	if saved != hash {
		log.Fatalf("Corruption: savedHash %x != requested hash %x.", saved, hash)
	}
	return nil
}
