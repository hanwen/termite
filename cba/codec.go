package cba

import (
	"encoding/gob"
	"encoding/binary"
	"log"
	"net/rpc"
	"io"
)

type CbaCodec struct {
	conn     io.ReadWriteCloser
	enc      *gob.Encoder
	dec      *gob.Decoder
}

func NewCbaCodec(conn io.ReadWriteCloser) *CbaCodec {
	c := CbaCodec{
		conn: conn,
		enc: gob.NewEncoder(conn),
		dec: gob.NewDecoder(conn),
	}
	return &c
}

const METHOD = "Server.ServeChunk"

func (c *CbaCodec) WriteRequest(rReq *rpc.Request, serviceReq interface{}) (err error) {
	err = binary.Write(c.conn, binary.BigEndian, rReq.Seq)
	if err != nil {
		return err
	}
	if rReq.ServiceMethod != METHOD {
		log.Panicf("invalid method %q", rReq.ServiceMethod)
	}

	req := serviceReq.(*Request)
	err = c.enc.Encode(req)
	return err
}

func (c *CbaCodec) ReadRequestHeader(req *rpc.Request) (err error) {
	err = binary.Read(c.conn, binary.BigEndian, &req.Seq)
	req.ServiceMethod = METHOD
	return err
}

func (c *CbaCodec) ReadRequestBody(body interface{}) (err error) {
	req := body.(*Request)
	err = c.dec.Decode(req)
	return err
}

func (c *CbaCodec) WriteResponse(rRep *rpc.Response, serviceRep interface{}) (err error) {
	err = binary.Write(c.conn, binary.BigEndian, rRep.Seq)
	if err != nil {
		return err
	}
	if rRep.ServiceMethod != METHOD {
		log.Panicf("invalid method %q", rRep.ServiceMethod)
	}

	rep := serviceRep.(*Response)
	err = c.enc.Encode(rep)
	return err	
}

func (c *CbaCodec) ReadResponseHeader(rep *rpc.Response) (err error) {
	err = binary.Read(c.conn, binary.BigEndian, &rep.Seq)
	rep.ServiceMethod = METHOD
	return err
}

func (c *CbaCodec) ReadResponseBody(body interface{}) (err error) {
	rep := body.(*Response)
	err = c.dec.Decode(rep)
	return err
}

func (c *CbaCodec) Close() error {
	return c.conn.Close()
}

