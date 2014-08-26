package termite

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"
)

const _SOCKET = ".termite-socket"

var Hostname string

func init() {
	var err error
	Hostname, err = os.Hostname()
	if err != nil {
		log.Println("hostname", err)
	}
}

// ids:
//
const (
	RPC_CHANNEL = "rpc......"
	HEADER_LEN  = 9
)

var nextConnectionId uint64

func ConnectionId() string {
	id := atomic.AddUint64(&nextConnectionId, 1)
	id--
	encoded := make([]byte, 9)
	encoded[0] = 'i'
	binary.BigEndian.PutUint64(encoded[1:], id)
	return string(encoded)
}

func OpenSocketConnection(socket string, channel string, timeout time.Duration) net.Conn {
	delay := time.Duration(0)
	conn, err := net.Dial("unix", socket)
	for try := 0; err != nil && delay < timeout; try++ {
		delay = time.Duration(1.5+0.5*rand.Float64()*float64(delay)) + 20*time.Nanosecond
		time.Sleep(delay)
		conn, err = net.Dial("unix", socket)
		continue
	}
	if err != nil {
		log.Fatal("OpenSocketConnection: ", err)
	}

	if len(channel) != HEADER_LEN {
		panic(channel)
	}
	_, err = io.WriteString(conn, channel)
	if err != nil {
		log.Fatal("WriteString", err)
	}
	return conn
}

func FindSocket() string {
	socket := os.Getenv("TERMITE_SOCKET")
	if socket == "" {
		dir, _ := os.Getwd()
		for dir != "" && dir != "/" {
			cand := filepath.Join(dir, _SOCKET)
			fi, _ := os.Lstat(cand)
			if fi != nil && fi.Mode()&os.ModeSocket != 0 {
				socket = cand
				break
			}
			dir = filepath.Clean(filepath.Join(dir, ".."))
		}
	}
	return socket
}

func portRangeListener(port int, retryCount int) net.Listener {
	var err error
	for i := 0; i <= retryCount; i++ {
		p := port + i
		addr := fmt.Sprintf(":%d", p)
		listener, e := net.Listen("tcp", addr)
		if e == nil {
			log.Println("Listening to", listener.Addr())
			return listener
		}
		err = e
	}
	log.Fatal("net.Listen:", err)
	return nil
}
