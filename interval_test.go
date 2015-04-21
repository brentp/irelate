package irelate

import (
	"fmt"
	"strings"
	"testing"
)

func TestInterval(t *testing.T) {
	a := Interval{chrom: "chr1", start: 1234, end: 5678,
		Fields: strings.Split("chr1\t1234\t5678", "\t")}
	if a.chrom != "chr1" {
		t.Error("expected \"chr1\", got", a.chrom)
	}
	if a.start != 1234 {
		t.Error("expected start = 1234, got", a.start)
	}
	if a.end != 5678 {
		t.Error("expected start = 5678, got", a.end)
	}

	s := fmt.Sprintf("%s", a)
	if len(s) == 0 {
		t.Error("bad String")
	}

}

func TestIntervalSource(t *testing.T) {
	var a Relatable
	a = &Interval{chrom: "chr1", start: 1234, end: 5678,
		Fields: strings.Split("chr1\t1234\t5678", "\t")}
	a.SetSource(222)

	if a.Source() != 222 {
		t.Error("expected 222, got", a.Source())
	}
}

func TestIntervalLine(t *testing.T) {
	s := "chr1\t1235\t4567\tasdf"
	i := IntervalFromBedLine(s)
	if i.Start() != uint32(1235) {
		t.Error("expected start of 1235")
	}
}
