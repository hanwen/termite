package cba

func (c *Store) newServer() Server {
	return newSpliceServer(c)
}
