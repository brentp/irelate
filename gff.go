// Implements Relatable for Gff

package irelate

import (
	"io"

	"code.google.com/p/biogo/io/featio/gff"
)

type Gff struct {
	*gff.Feature
	_source uint32
	related []Relatable
}

func (self *Gff) Chrom() string {
	return self.Feature.SeqName
}

func (self *Gff) Start() uint32 {
	return uint32(self.Feature.Start() - 1)
}
func (self *Gff) End() uint32 {
	return uint32(self.Feature.End())
}
func (self *Gff) Related() []Relatable {
	return self.related
}
func (self *Gff) AddRelated(r Relatable) {
	self.related = append(self.related, r)
}

func (self *Gff) SetSource(src uint32) {
	self._source = src
}
func (self *Gff) Source() uint32 {
	return self._source
}

func (self *Gff) Less(other Relatable) bool {
	if self.Chrom() != other.Chrom() {
		return self.Chrom() < other.Chrom()
	}
	return self.Start() < other.Start()
}

func GFFToRelatable(file string) RelatableChannel {

	ch := make(chan Relatable, 16)

	go func() {
		fh, err := Xopen(file)
		if err != nil {
			panic(err)
		}
		var g *gff.Reader
		g = gff.NewReader(fh)
		defer fh.Close()

		for {
			feat, err := g.Read()
			if err != nil {
				if err == io.EOF {
					close(ch)
					break
				} else {
					panic(err)
				}
			}
			// since Read returns the interface, first cast back
			// to gff.Feature so we have the needed Attributes.
			gfeat := feat.(*gff.Feature)
			f := Gff{Feature: gfeat, related: make([]Relatable, 0, 7)}
			ch <- &f
		}
	}()
	return ch
}
