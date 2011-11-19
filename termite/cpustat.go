package termite

import (
	"fmt"
	"log"
	"syscall"
)

// TODO should be in syscall package.
const RUSAGE_SELF = 0
const RUSAGE_CHILDREN = -1

func sampleTime() interface{} {
	c := TotalCpuStat()
	return c
}

func TotalCpuStat() *CpuStat {
	c := CpuStat{}
	r := syscall.Rusage{}
	err := syscall.Getrusage(RUSAGE_SELF, &r)
	if err != nil {
		log.Println("Getrusage:", err)
		return nil
	}

	c.SelfCpu = syscall.TimevalToNsec(r.Utime)
	c.SelfSys = syscall.TimevalToNsec(r.Stime)

	err = syscall.Getrusage(RUSAGE_CHILDREN, &r)
	if err != nil {
		return nil
	}
	c.ChildCpu = syscall.TimevalToNsec(r.Utime)
	c.ChildSys = syscall.TimevalToNsec(r.Stime)

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
		me.SelfCpu/1e6, me.SelfSys/1e6, me.ChildCpu/1e6, me.ChildSys/1e6)
}

func (me *CpuStat) Percent() string {
	t := me.Total()
	if t == 0 {
		return "(no data)"
	}
	return fmt.Sprintf("%d %% self cpu, %d %% self sys, %d %% child cpu, %d %% child sys",
		(100*me.SelfCpu)/t, (me.SelfSys*100)/t, (me.ChildCpu*100)/t, (me.ChildSys*100)/t)
}

func (me *CpuStat) Total() int64 {
	return me.SelfSys + me.SelfCpu + me.ChildSys + me.ChildCpu
}

type cpuStatSampler struct {
	sampler *PeriodicSampler
}

func newCpuStatSampler() *cpuStatSampler {
	me := &cpuStatSampler{
		sampler: NewPeriodicSampler(1.0, 60, sampleTime),
	}
	return me
}

func (me *cpuStatSampler) CpuStats() (out []CpuStat) {
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
