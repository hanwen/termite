package stats

import (
	"fmt"
	"log"
	"runtime"
	"syscall"
	"time"
)

// TODO should be in syscall package.
const RUSAGE_SELF = 0
const RUSAGE_CHILDREN = -1

func sampleTime() interface{} {
	c := TotalCpuStat()
	return c
}

type CpuStat struct {
	SelfCpu  time.Duration
	SelfSys  time.Duration
	ChildCpu time.Duration
	ChildSys time.Duration
}

type MemCounter uint64

func (mc MemCounter) String() string {
	unit := ""
	switch {
	case mc > (1 << 31):
		mc >>= 30
		unit = "G"
	case mc > (1 << 21):
		mc >>= 20
		unit = "M"
	case mc > (1 << 11):
		mc >>= 10
		unit = "K"
	}

	return fmt.Sprintf("%d%s", uint64(mc), unit)
}

type MemStat struct {
	HeapIdle  MemCounter
	HeapInuse MemCounter
}

func (m *MemStat) Total() uint64 {
	return uint64(m.HeapIdle + m.HeapInuse)
}

func GetMemStat() *MemStat {
	r := runtime.MemStats
	return &MemStat{
		MemCounter(r.HeapIdle),
		MemCounter(r.HeapInuse),
	}
}

func TimevalToDuration(tv syscall.Timeval) time.Duration {
	ns := syscall.TimevalToNsec(tv)
	return time.Duration(ns) * time.Nanosecond
}

func TotalCpuStat() *CpuStat {
	c := CpuStat{}
	r := syscall.Rusage{}
	err := syscall.Getrusage(RUSAGE_SELF, &r)
	if err != nil {
		log.Println("Getrusage:", err)
		return nil
	}

	c.SelfCpu = TimevalToDuration(r.Utime)
	c.SelfSys = TimevalToDuration(r.Stime)

	err = syscall.Getrusage(RUSAGE_CHILDREN, &r)
	if err != nil {
		return nil
	}
	c.ChildCpu = TimevalToDuration(r.Utime)
	c.ChildSys = TimevalToDuration(r.Stime)

	return &c
}

func (me *CpuStat) Add(x CpuStat) CpuStat {
	return CpuStat{
		SelfSys:  me.SelfSys + x.SelfSys,
		SelfCpu:  me.SelfCpu + x.SelfCpu,
		ChildSys: me.ChildSys + x.ChildSys,
		ChildCpu: me.ChildCpu + x.ChildCpu,
	}
}

func (me *CpuStat) Diff(x CpuStat) CpuStat {
	return CpuStat{
		SelfSys:  me.SelfSys - x.SelfSys,
		SelfCpu:  me.SelfCpu - x.SelfCpu,
		ChildSys: me.ChildSys - x.ChildSys,
		ChildCpu: me.ChildCpu - x.ChildCpu,
	}
}

func (me *CpuStat) String() string {
	return fmt.Sprintf("me %d ms/%d ms, child %d ms/%d ms",
		me.SelfCpu/time.Millisecond, me.SelfSys/time.Millisecond,
		me.ChildCpu/time.Millisecond, me.ChildSys/time.Millisecond)
}

func (me *CpuStat) Percent() string {
	t := me.Total()
	if t == 0 {
		return "(no data)"
	}
	return fmt.Sprintf("%d %% self cpu, %d %% self sys, %d %% child cpu, %d %% child sys",
		(100*me.SelfCpu)/t, (me.SelfSys*100)/t, (me.ChildCpu*100)/t, (me.ChildSys*100)/t)
}

func (me *CpuStat) Total() time.Duration {
	return me.SelfSys + me.SelfCpu + me.ChildSys + me.ChildCpu
}

type CpuStatSampler struct {
	sampler *PeriodicSampler
}

func NewCpuStatSampler() *CpuStatSampler {
	me := &CpuStatSampler{
		sampler: NewPeriodicSampler(time.Second, 60, sampleTime),
	}
	return me
}

func (me *CpuStatSampler) CpuStats() (out []CpuStat) {
	vals := me.sampler.Values()
	var last *CpuStat
	for _, v := range vals {
		s := v.(*CpuStat)
		if last != nil {
			out = append(out, s.Diff(*last))
		}
		last = s
	}
	return out
}
