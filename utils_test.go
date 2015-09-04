package irelate

import (
	"io"
	"os"
	"testing"
)

func TestOpenScanFile(t *testing.T) {
	f, e := os.Open("utils_test.go")
	if e != nil {
		t.Error("couldn't open file")
	}

	s, fh := OpenScanFile(f)

	if !s.Scan() {
		t.Error("should have a scanner")
	}
	if f, ok := fh.(io.ReadCloser); ok {
		f.Close()
	}
}

func TestImin(t *testing.T) {
	if !(Imin(uint32(2), uint32(3)) == uint32(2)) {
		t.Error("bad Min function")
	}
	if !(Imin(uint32(3), uint32(2)) == uint32(2)) {
		t.Error("bad Min function")
	}
}

func TestImax(t *testing.T) {
	if !(Imax(uint32(2), uint32(3)) == uint32(3)) {
		t.Error("bad Min function")
	}
	if !(Imax(uint32(3), uint32(2)) == uint32(3)) {
		t.Error("bad Min function")
	}
}

func TestStreamer(t *testing.T) {
	for _, f := range []string{"data/a.bed", "data/ex.gff", "data/ex.bam"} {
		s, e := Streamer(f, "")
		if e != nil {
			t.Errorf("got error: %s\n", e)
		}
		for r := range s {
			_ = r.Start()
		}
	}
}
