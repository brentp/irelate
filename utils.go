package irelate

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/brentp/bix"
	"github.com/brentp/irelate/interfaces"
	"github.com/brentp/irelate/parsers"
	"github.com/brentp/vcfgo"
	"github.com/brentp/xopen"
)

const MaxUint32 = ^uint32(0)
const MaxInt32 = int(MaxUint32 >> 1)

// OpenScanFile sets up a (possibly gzipped) file for line-wise reading.
func OpenScanFile(fh io.Reader) (*bufio.Scanner, io.Reader) {
	scanner := bufio.NewScanner(fh)
	scanner.Split(bufio.ScanLines)
	return scanner, fh
}

type iterable struct {
	*bufio.Scanner
	fn func(line []byte) (interfaces.Relatable, error)
	fh io.Reader
}

func (it iterable) Next() (interfaces.Relatable, error) {
	v := it.Bytes()
	return it.fn(v)
}

func (it iterable) Close() error {
	if rc, ok := it.fh.(io.ReadCloser); ok {
		return rc.Close()
	}
	return nil
}

func ScanToIterator(file io.Reader, fn func(line []byte) (interfaces.Relatable, error)) interfaces.RelatableIterator {
	scanner, fh := OpenScanFile(file)
	return iterable{scanner, fn, fh}
}

// ScanToRelatable makes is easy to create a chan Relatable from a file of intervals.
func ScanToRelatable(file io.Reader, fn func(line []byte) (interfaces.Relatable, error)) interfaces.RelatableChannel {
	scanner, fh := OpenScanFile(file)
	ch := make(chan interfaces.Relatable, 32)
	go func() {
		i := 0
		for scanner.Scan() {
			v, err := fn(scanner.Bytes())
			if err != nil {
				if i > 0 { // break on the header.
					log.Println(err)
					break
				}
			} else {
				ch <- v
				i += 1
			}
		}
		if c, ok := fh.(io.ReadCloser); ok {
			c.Close()
		} else if c, ok := file.(io.ReadCloser); ok {
			c.Close()
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

func RegionToParts(region string) (string, int, int, error) {
	parts := strings.Split(region, ":")
	// e.g. just "chr"
	if len(parts) == 1 {
		parts = append(parts, fmt.Sprintf("1-%d", MaxInt32))
	}

	se := strings.Split(parts[1], "-")
	if len(se) != 2 {
		return "", 0, 0, errors.New(fmt.Sprintf("unable to parse region: %s", region))
	}
	s, err := strconv.Atoi(se[0])
	if err != nil {
		return "", 0, 0, errors.New(fmt.Sprintf("unable to parse region: %s", region))
	}
	e, err := strconv.Atoi(se[1])
	if err != nil {
		return "", 0, 0, errors.New(fmt.Sprintf("unable to parse region: %s", region))
	}
	return parts[0], s, e, nil
}

type location struct {
	chrom string
	start int
	end   int
}

func (l location) RefName() string {
	return l.chrom
}

func (l location) Start() int {
	return l.start
}
func (l location) End() int {
	return l.end
}

func getReader(f, region string) (io.Reader, error) {
	var err error

	var rdr io.Reader
	if region != "" {
		bx, err := bix.New(f, 2)
		if err != nil {
			return nil, err
		}
		chrom, start, end, err := RegionToParts(region)
		if err != nil {
			return nil, err
		}
		rdr, err = bx.ChunkedReader(location{chrom, start, end}, true)
	} else {
		rdr, err = os.Open(f)
	}
	if err != nil {
		return nil, err
	}
	var buf io.Reader
	if !strings.HasSuffix(f, ".bam") {
		/*
			bufr := bufio.NewReader(rdr)
			used := false
			if region == "" {
				if is, err := xopen.IsGzip(bufr); is {
					buf, err = gzip.NewReader(bufr)
					used = true
					if err != nil {
						return nil, err
					}
				}
			}
			if !used {
				buf = bufr
			}*/
		buf = xopen.Buf(rdr)

	} else {
		buf = rdr
	}
	return buf, nil
}

func Iterator(f string, region string) (interfaces.RelatableIterator, error) {
	var iterator interfaces.RelatableIterator
	buf, err := getReader(f, region)
	if err != nil {
		return nil, err
	}
	// TODO: gff, bam
	if strings.HasSuffix(f, ".vcf") || strings.HasSuffix(f, ".vcf.gz") {
		iterator, err = parsers.VCFIterator(buf)
		if err != nil {
			return nil, err
		}

	} else {
		iterator = ScanToIterator(buf, parsers.IntervalFromBedLine)
	}
	return iterator, nil
}

func Streamer(f string, region string) (interfaces.RelatableChannel, error) {
	var stream chan interfaces.Relatable

	buf, err := getReader(f, region)
	if err != nil {
		return nil, err
	}

	if strings.HasSuffix(f, ".bam") {
		stream, err = parsers.BamToRelatable(buf)
	} else if strings.HasSuffix(f, ".gff") {
		stream, err = parsers.GFFToRelatable(buf)
	} else if strings.HasSuffix(f, ".vcf") || strings.HasSuffix(f, ".vcf.gz") {
		var v *vcfgo.Reader
		v, err = parsers.Vopen(buf, nil)
		stream = parsers.StreamVCF(v)
	} else {
		stream = ScanToRelatable(buf, parsers.IntervalFromBedLine)
	}
	return stream, err
}
