// Implements Relatable for Bam

package irelate

import (
	"github.com/biogo/hts/bam"
	"github.com/biogo/hts/sam"
	"io"
	"os"
)

type Bam struct {
	*sam.Record
	source  uint32
	related []Relatable
	chrom   string
}

func (a *Bam) Chrom() string {
	return a.chrom
}

// cast to 32 bits.
func (a *Bam) Start() uint32 {
	return uint32(a.Record.Start())
}

func (a *Bam) End() uint32 {
	return uint32(a.Record.End())
}

func (a *Bam) Source() uint32 {
	return a.source
}

func (a *Bam) SetSource(src uint32) {
	a.source = src
}

func (a *Bam) AddRelated(b Relatable) {
	if a.related == nil {
		a.related = make([]Relatable, 1, 2)
		a.related[0] = b
	} else {
		a.related = append(a.related, b)
	}
}
func (a *Bam) Related() []Relatable {
	return a.related
}

func (a *Bam) Less(other Relatable) bool {
	if a.Chrom() != other.Chrom() {
		return a.Chrom() < other.Chrom()
	}
	return a.Start() < other.Start()
}

func (a *Bam) MapQ() int {
	return int(a.Record.MapQ)
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func BamToRelatable(file string) RelatableChannel {

	ch := make(chan Relatable, 16)

	go func() {
		f, err := os.Open(file)
		check(err)
		b, err := bam.NewReader(f, 0)
		if err != nil {
			panic(err)
		}
		for {
			rec, err := b.Read()
			if err != nil {
				if err == io.EOF {
					break
				} else {
					panic(err)
				}
			}
			if rec.RefID() == -1 { // unmapped
				continue
			}
			// TODO: see if keeping the list of chrom names and using a ref is better.
			bam := Bam{Record: rec, chrom: rec.Ref.Name(), related: nil}
			ch <- &bam
		}
		close(ch)
		b.Close()
		f.Close()
	}()
	return ch
}
