package termite

import (
	"fmt"
	"log"
	"strings"
	"time"
)

// Timer is a simple aid to time different steps in a function.
type Timer struct {
	start    int64
	messages []string
}

func NewTimer() *Timer {
	return &Timer{start: time.Nanoseconds()}
}

func (me *Timer) Clock(msg string) {
	t := time.Nanoseconds()
	dt := t - me.start
	me.start = t
	me.messages = append(me.messages, fmt.Sprintf("%6.2fms %s",
		float64(dt)*1.0e-6, msg))
}

func (me *Timer) Message() string {
	return strings.Join(me.messages, "\n")
}

func (me *Timer) Log() {
	log.Printf("\n%s", strings.Join(me.messages, "\n"))
}
