package irelate

import (
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/brentp/bix"
	"github.com/brentp/irelate/interfaces"
	"github.com/brentp/xopen"
)

// OpenScanFile sets up a (possibly gzipped) file for line-wise reading.
func OpenScanFile(fh io.Reader) (*bufio.Scanner, io.Reader) {
	scanner := bufio.NewScanner(fh)
	scanner.Split(bufio.ScanLines)
	return scanner, fh
}

// ScanToRelatable makes is easy to create a chan Relatable from a file of intervals.
func ScanToRelatable(file io.Reader, fn func(line string) (interfaces.Relatable, error)) RelatableChannel {
	scanner, fh := OpenScanFile(file)
	ch := make(chan interfaces.Relatable, 32)
	go func() {
		i := 0
		for scanner.Scan() {
			v, err := fn(scanner.Text())
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

func Streamer(f string, region string) (RelatableChannel, error) {
	var stream chan interfaces.Relatable
	var err error

	var rdr io.Reader
	var bx *bix.Bix
	if region != "" {
		bx, err = bix.New(f)
		if err != nil {
			return nil, err
		}
		chrom, start, end, err := RegionToParts(region)
		if err != nil {
			return nil, err
		}
		rdr, err = bx.Query(chrom, start, end)
	} else {
		rdr, err = os.Open(f)
	}
	if err != nil {
		return nil, err
	}
	var buf io.Reader
	if !strings.HasSuffix(f, ".bam") {
		bufr := bufio.NewReaderSize(rdr, 2^17)
		if is, err := xopen.IsGzip(bufr); is {
			buf, err = gzip.NewReader(bufr)
			if err != nil {
				return nil, err
			}
		} else {
			buf = bufr
		}
	} else {
		buf = rdr
	}

	if strings.HasSuffix(f, ".bam") {
		stream, err = BamToRelatable(buf)
	} else if strings.HasSuffix(f, ".gff") {
		stream, err = GFFToRelatable(buf)
	} else if strings.HasSuffix(f, ".vcf") || strings.HasSuffix(f, ".vcf.gz") {
		v := Vopen(buf)
		stream = StreamVCF(v)
	} else {
		stream = ScanToRelatable(buf, IntervalFromBedLine)
	}
	return stream, err
}
