package parsers

import (
	"bytes"
	"log"
	"strconv"
	"unsafe"

	"github.com/brentp/irelate/interfaces"
)

const empty = ""

// Interval satisfies the Relatable interface.
type Interval struct {
	// chrom, start, end, line, source, related[]
	chrom   string
	start   uint32
	end     uint32
	Fields  [][]byte
	source  uint32
	related []interfaces.Relatable
}

func NewInterval(chrom string, start uint32, end uint32, fields [][]byte, source uint32, related []interfaces.Relatable) *Interval {
	return &Interval{chrom, start, end, fields, source, related}
}

func (i *Interval) Chrom() string                   { return i.chrom }
func (i *Interval) Start() uint32                   { return i.start }
func (i *Interval) End() uint32                     { return i.end }
func (i *Interval) Related() []interfaces.Relatable { return i.related }
func (i *Interval) AddRelated(b interfaces.Relatable) {
	if i.related == nil {
		i.related = make([]interfaces.Relatable, 1, 4)
		i.related[0] = b
	} else {
		i.related = append(i.related, b)
	}
}
func (i *Interval) Source() uint32       { return i.source }
func (i *Interval) SetSource(src uint32) { i.source = src }

func (i *Interval) String() string {
	return string(bytes.Join(i.Fields, []byte{'\t'}))
}

func unsafeString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func IntervalFromBedLine(line []byte) (interfaces.Relatable, error) {
	line = bytes.TrimRight(line, "\r\n")
	fields := bytes.Split([]byte(string(line)), []byte{'\t'})
	start, err := strconv.ParseUint(unsafeString(fields[1]), 10, 32)
	if err != nil {
		return nil, err
	}
	end, err := strconv.ParseUint(unsafeString(fields[2]), 10, 32)
	if err != nil {
		return nil, err
	}
	if start == 85558 {
		log.Fatal("line:", string(line))
	}
	i := Interval{chrom: string(fields[0]), start: uint32(start), end: uint32(end), related: nil, Fields: fields}
	return &i, nil
}
