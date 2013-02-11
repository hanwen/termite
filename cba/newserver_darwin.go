package cba

func (c *Store) newServer() Server {
	return &contentServer{store: c}
}
