package irelate

import (
	"bufio"
	"compress/gzip"
	"github.com/brentp/ififo"
	"io"
	"os"
	"strings"
)

// check if a buffered Reader is gzipped.
func IsGzip(r *bufio.Reader) (bool, error) {
	m, err := r.Peek(2)
	if err != nil {
		return false, err
	}
	return m[0] == 0x1f && m[1] == 0x8b, nil
}

func Xopen(file string) (io.ReadCloser, error) {
	// TODO: clean this up to not use finalizer.
	var fh io.ReadCloser
	var err error
	if file == "-" {
		fh = os.Stdin
		err = nil
	} else {
		fh, err = os.Open(file)
	}
	if err != nil {
		return nil, err
	}
	if strings.HasSuffix(file, ".gz") {
		var fz io.ReadCloser
		fz, err = gzip.NewReader(fh)
		if err != nil {
			fh.Close()
			return nil, err
		}
		//runtime.SetFinalizer(fh, os.Close)
		return fz, err
	}
	return fh, err
}

// OpenScanFile sets up a (possibly gzipped) file for line-wise reading.
func OpenScanFile(file string) (scanner *bufio.Scanner, fh io.ReadCloser) {
	fh, err := Xopen(file)
	if err != nil {
		panic(err)
	}
	scanner = bufio.NewScanner(fh)
	scanner.Split(bufio.ScanLines)
	return scanner, fh

}

// ScanToRelatable makes is easy to create a chan Relatable from a file of intervals.
func ScanToRelatable(file string, fn func(line string, cache *ififo.IFifo) Relatable, cache *ififo.IFifo) RelatableChannel {
	scanner, fh := OpenScanFile(file)
	ch := make(chan Relatable, 32)
	go func() {
		var i Relatable
		defer fh.Close()
		for scanner.Scan() {
			line := scanner.Text()
			i = fn(line, cache)
			ch <- i
		}

		close(ch)
	}()
	return ch
}

func Imin(a uint32, b uint32) uint32 {
	if b < a {
		return b
	}
	return a
}

func Imax(a uint32, b uint32) uint32 {
	if b > a {
		return b
	}
	return a
}

func Streamer(f string, cache *ififo.IFifo) RelatableChannel {
	var stream chan Relatable
	if strings.HasSuffix(f, ".bam") {
		stream = BamToRelatable(f)
	} else if strings.HasSuffix(f, ".gff") {
		stream = GFFToRelatable(f)
	} else {
		stream = ScanToRelatable(f, IntervalFromBedLine, cache)
	}
	return stream
}
