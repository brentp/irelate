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

func (g *Gff) Chrom() string {
	return g.Feature.SeqName
}

func (g *Gff) Start() uint32 {
	return uint32(g.Feature.Start() - 1)
}
func (g *Gff) End() uint32 {
	return uint32(g.Feature.End())
}
func (g *Gff) Related() []Relatable {
	return g.related
}

func (g *Gff) Clear() {
	g.related = g.related[:0]
}

func (g *Gff) AddRelated(r Relatable) {
	g.related = append(g.related, r)
}

func (g *Gff) SetSource(src uint32) {
	g._source = src
}
func (g *Gff) Source() uint32 {
	return g._source
}

func (g *Gff) Less(other Relatable) bool {
	if g.Chrom() != other.Chrom() {
		return g.Chrom() < other.Chrom()
	}
	return g.Start() < other.Start()
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
