// Implements Relatable for Bam

package irelate

import (
	"io"

	boom "code.google.com/p/biogo.boom"
)

type Bam struct {
	*boom.Record
	source  uint32
	related []Relatable
	chrom   *string
}

func (self *Bam) Chrom() string {
	return *(self.chrom)
}

// cast to 32 bits.
func (self *Bam) Start() uint32 {
	return uint32(self.Record.Start())
}

func (self *Bam) End() uint32 {
	return uint32(self.Record.End())
}

func (self *Bam) Source() uint32 {
	return self.source
}

func (self *Bam) SetSource(src uint32) {
	self.source = src
}

func (self *Bam) AddRelated(b Relatable) {
	self.related = append(self.related, b)
}
func (self *Bam) Related() []Relatable {
	return self.related
}

func (self *Bam) Less(other Relatable) bool {
	if self.Chrom() != other.Chrom() {
		return self.Chrom() < other.Chrom()
	}
	return self.Start() < other.Start()
}

func BamToRelatable(file string) RelatableChannel {

	ch := make(chan Relatable, 16)

	go func() {
		b, err := boom.OpenBAM(file)
		if err != nil {
			panic(err)
		}
		defer b.Close()
		names := b.RefNames()
		for {
			rec, _, err := b.Read()
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
			bam := Bam{Record: rec, chrom: &(names[rec.RefID()]),
				related: make([]Relatable, 0, 2)}
			ch <- &bam
		}
		close(ch)
	}()
	return ch
}
