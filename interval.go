package irelate

import (
	"github.com/brentp/ififo"
	"strconv"
	"strings"
)

const empty = ""

// Interval satisfies the Relatable interface.
type Interval struct {
	// chrom, start, end, line, source, related[]
	chrom   string
	start   uint32
	end     uint32
	line    string
	source  uint32
	related []Relatable
}

func (i *Interval) Chrom() string        { return i.chrom }
func (i *Interval) Start() uint32        { return i.start }
func (i *Interval) End() uint32          { return i.end }
func (i *Interval) Related() []Relatable { return i.related }
func (i *Interval) AddRelated(b Relatable) {
	if i.related == nil {
		i.related = make([]Relatable, 1, 48)
		i.related[0] = b
	} else {
		i.related = append(i.related, b)
	}
}
func (i *Interval) Source() uint32       { return i.source }
func (i *Interval) SetSource(src uint32) { i.source = src }

// Interval.Less() determines the order of intervals
func (i *Interval) Less(other Relatable) bool {
	a, b := i.Chrom(), other.Chrom()
	if a[len(a)-1] != b[len(b)-1] {
		return i.Chrom() < other.Chrom()
	}
	if i.Start() == other.Start() {
		return i.End() < other.End()
	}
	return i.Start() < other.Start()
}

func IntervalFromBedLine(line string, cache *ififo.IFifo) Relatable {
	fields := strings.SplitN(line, "\t", 4)
	start, err := strconv.ParseUint(fields[1], 10, 32)
	if err != nil {
		panic(err)
	}
	end, err := strconv.ParseUint(fields[2], 10, 32)
	if err != nil {
		panic(err)
	}

	i := cache.Get().(*Interval)
	i.chrom = fields[0]
	i.start = uint32(start)
	i.end = uint32(end)
	return i
}
