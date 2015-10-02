package irelate

import (
	"bytes"
	"fmt"
	"testing"

	. "github.com/brentp/irelate/interfaces"
	"github.com/brentp/irelate/parsers"
)

func TestInterval(t *testing.T) {
	a := parsers.NewInterval("chr1", 1234, 5678, bytes.Split([]byte("chr1\t1234\t5678"), []byte("\t")), 0, nil)
	if a.Chrom() != "chr1" {
		t.Error("expected \"chr1\", got", a.Chrom())
	}
	if a.Start() != 1234 {
		t.Error("expected start = 1234, got", a.Start())
	}
	if a.End() != 5678 {
		t.Error("expected start = 5678, got", a.End())
	}

	s := fmt.Sprintf("%v", a)
	if len(s) == 0 {
		t.Error("bad String")
	}

}

func TestIntervalSource(t *testing.T) {
	var a Relatable
	a = parsers.NewInterval("chr1", 1234, 5678, bytes.Split([]byte("chr1\t1234\t5678"), []byte("\t")), 0, nil)
	a.SetSource(222)

	if a.Source() != 222 {
		t.Error("expected 222, got", a.Source())
	}
}

func TestIntervalLine(t *testing.T) {
	s := []byte("chr1\t1235\t4567\tasdf")
	i, _ := parsers.IntervalFromBedLine(s)
	if i.Start() != uint32(1235) {
		t.Error("expected start of 1235")
	}
}
