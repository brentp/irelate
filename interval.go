package irelate

import (
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
		i.related = make([]Relatable, 1, 4)
		i.related[0] = b
	} else {
		i.related = append(i.related, b)
	}
}
func (i *Interval) Source() uint32       { return i.source }
func (i *Interval) SetSource(src uint32) { i.source = src }

// Interval.Less() determines the order of intervals
func (i *Interval) Less(other Relatable) bool {
	if i.Chrom() != other.Chrom() {
		return i.Chrom() < other.Chrom()
	}
	return i.Start() < other.Start()
}

func IntervalFromBedLine(line string) Relatable {
	fields := strings.SplitN(line, "\t", 4)
	start, err := strconv.ParseUint(fields[1], 10, 32)
	if err != nil {
		panic(err)
	}
	end, err := strconv.ParseUint(fields[2], 10, 32)
	if err != nil {
		panic(err)
	}

	i := Interval{chrom: fields[0], start: uint32(start), end: uint32(end), related: nil}
	return &i
}
