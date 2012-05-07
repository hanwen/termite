package stats

import (
	"fmt"
	"io/ioutil"
	"strings"
	"time"
)

// See http://www.kernel.org/doc/Documentation/iostats.txt
type DiskStat struct {
	Name                 string
	MajorDev             int
	MinorDev             int
	ReadsCompleted       uint
	MergedReadsCompleted uint
	SectorsRead          uint
	MsSpentReading       uint
	WritesCompleted      uint
	unused               uint
	SectorsWritten       uint
	MsSpentWriting       uint
	IOsInProgress        uint
	MsDoingIO            uint
	MsWeightedIO         uint
}

func (d *DiskStat) String() string {
	return fmt.Sprintf("iops %d(%d)/%d sectors %d/%d ms spent %d/%d",
		d.ReadsCompleted, d.MergedReadsCompleted, d.WritesCompleted,
		d.SectorsRead, d.SectorsWritten, d.MsSpentReading, d.MsSpentWriting)
}

func (d *DiskStat) Add(other *DiskStat) {
	d.ReadsCompleted += other.ReadsCompleted
	d.MergedReadsCompleted += other.MergedReadsCompleted
	d.SectorsWritten += other.SectorsWritten
	d.MsSpentWriting += other.MsSpentWriting
	d.WritesCompleted += other.WritesCompleted
	d.SectorsRead += other.SectorsRead
	d.MsSpentReading += other.MsSpentReading
	d.IOsInProgress += other.IOsInProgress
	d.MsDoingIO += other.MsDoingIO
	d.MsWeightedIO += other.MsWeightedIO
}

func (d *DiskStat) Sub(other *DiskStat) {
	d.ReadsCompleted -= other.ReadsCompleted
	d.MergedReadsCompleted -= other.MergedReadsCompleted
	d.SectorsWritten -= other.SectorsWritten
	d.MsSpentWriting -= other.MsSpentWriting
	d.WritesCompleted -= other.WritesCompleted
	d.SectorsRead -= other.SectorsRead
	d.MsSpentReading -= other.MsSpentReading
	d.IOsInProgress -= other.IOsInProgress
	d.MsDoingIO -= other.MsDoingIO
	d.MsWeightedIO -= other.MsWeightedIO
}

func parseLine(line string) (*DiskStat, error) {
	line = strings.Trim(line, " \n\t")

	st := &DiskStat{}
	n, err := fmt.Sscan(line,
		&st.MajorDev,
		&st.MinorDev,
		&st.Name,
		&st.ReadsCompleted,
		&st.MergedReadsCompleted,
		&st.SectorsRead,
		&st.MsSpentReading,
		&st.WritesCompleted,
		&st.unused,
		&st.SectorsWritten,
		&st.MsSpentWriting,
		&st.IOsInProgress,
		&st.MsDoingIO,
		&st.MsWeightedIO)
	if err != nil {
		return nil, fmt.Errorf("Sscan failed on %q after %d: %v", line, n, err)
	}
	return st, nil
}

func TotalDiskStats() (result DiskStat, err error) {
	ds, err := AllDiskStats()

	st := DiskStat{}
	if err != nil {
		return st, err
	}

	for _, d := range ds {
		if d.Name == "" {
			continue
		}
		
		part := d.Name[len(d.Name)-1]
		if part >= '0' &&  part <= '9' {
			continue
		}
		st.Add(d)
	}
	return st, nil
}

func AllDiskStats() (result []*DiskStat, err error) {
	content, err := ioutil.ReadFile("/proc/diskstats")
	if err != nil {
		return nil, fmt.Errorf("ReadFile failed: %v", err)
	}

	lines := strings.Split(string(content), "\n")
	for _, l := range lines {
		if len(l) == 0 {
			continue
		}
		r, err := parseLine(l)
		if err != nil {
			return nil, err
		}

		result = append(result, r)
	}

	return result, nil
}

type DiskStatSampler struct {
	sampler *PeriodicSampler
}

func sampleDisk() Sample {
	ds, _ := TotalDiskStats()
	return &diskSample{ds}
}

func NewDiskStatSampler() *DiskStatSampler {
	return &DiskStatSampler{
		sampler: NewPeriodicSampler(time.Second, 60, sampleDisk),
	}
}

type diskSample struct {
	DiskStat
}

func (d *diskSample) SubtractSample(s Sample) {
	other := s.(*diskSample)
	d.Sub(&other.DiskStat)
}

func (d *diskSample) CopySample() Sample {
	ds := *d
	return &ds
}

func (d *diskSample) TableHeader() string {
	return fmt.Sprintf("<tr><th>Read</th><th>Write</th></tr>")
}

func (d *diskSample) TableRow() string {
	return fmt.Sprintf("<tr><td>%d</td><td>%d</td></tr>", d.MergedReadsCompleted, d.WritesCompleted)
}

func (s *DiskStatSampler) Stats() (out []DiskStat) {
	diffs := s.sampler.Diffs()
	for _, d := range diffs {
		samp := d.(*diskSample)
		out = append(out, samp.DiskStat)
	}
	return out
}
