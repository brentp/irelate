// Implements Relatable for Gff

package irelate

import (
	"io"
	"log"

	"github.com/biogo/biogo/io/featio/gff"
	"github.com/brentp/irelate/interfaces"
)

type Gff struct {
	*gff.Feature
	_source uint32
	related []interfaces.Relatable
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
func (g *Gff) Related() []interfaces.Relatable {
	return g.related
}
func (g *Gff) AddRelated(r interfaces.Relatable) {
	g.related = append(g.related, r)
}

func (g *Gff) SetSource(src uint32) {
	g._source = src
}
func (g *Gff) Source() uint32 {
	return g._source
}

func GFFToRelatable(fh io.Reader) (RelatableChannel, error) {

	ch := make(chan interfaces.Relatable, 16)

	go func() {
		var g *gff.Reader
		g = gff.NewReader(fh)

		for {
			feat, err := g.Read()
			if err != nil {
				if err == io.EOF {
					break
				} else {
					log.Println(err)
					break
				}
			}
			// since Read returns the interface, first cast back
			// to gff.Feature so we have the needed Attributes.
			gfeat := feat.(*gff.Feature)
			f := Gff{Feature: gfeat, related: make([]interfaces.Relatable, 0, 7)}
			ch <- &f
		}
		close(ch)
	}()
	return ch, nil
}
