package irelate

import (
	"bufio"
	"io"
	"log"
	"strings"

	"github.com/brentp/xopen"
)

// OpenScanFile sets up a (possibly gzipped) file for line-wise reading.
func OpenScanFile(file string) (scanner *bufio.Scanner, fh io.ReadCloser) {
	fh, err := xopen.Ropen(file)
	check(err)
	scanner = bufio.NewScanner(fh)
	scanner.Split(bufio.ScanLines)
	return scanner, fh
}

// ScanToRelatable makes is easy to create a chan Relatable from a file of intervals.
func ScanToRelatable(file string, fn func(line string) (Relatable, error)) RelatableChannel {
	scanner, fh := OpenScanFile(file)
	ch := make(chan Relatable, 32)
	go func() {
		for scanner.Scan() {
			v, err := fn(scanner.Text())
			if err != nil {
				log.Println(err)
				break
			}
			ch <- v
		}
		fh.Close()
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

func Streamer(f string) (RelatableChannel, error) {
	var stream chan Relatable
	var err error
	if strings.HasSuffix(f, ".bam") {
		stream, err = BamToRelatable(f)
	} else if strings.HasSuffix(f, ".gff") {
		stream, err = GFFToRelatable(f)
	} else if strings.HasSuffix(f, ".vcf") || strings.HasSuffix(f, ".vcf.gz") {
		v := Vopen(f)
		stream = StreamVCF(v)
	} else {
		stream = ScanToRelatable(f, IntervalFromBedLine)
	}
	return stream, err
}
