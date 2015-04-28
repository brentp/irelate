package irelate

import (
	"github.com/brentp/vcfgo"
	"github.com/brentp/xopen"
	"log"
)

type Variant struct {
	*vcfgo.Variant
	source  uint32
	related []Relatable
}

func (v *Variant) AddRelated(r Relatable) {
	v.related = append(v.related, r)
}

func (v *Variant) Related() []Relatable {
	return v.related
}

func (v *Variant) SetSource(src uint32) { v.source = src }
func (v *Variant) Source() uint32       { return v.source }

func Vopen(f string) *vcfgo.Reader {
	rdr, err := xopen.Ropen(f)
	check(err)
	vcf, err := vcfgo.NewReader(rdr, true)
	if err != nil {
		log.Fatal(err)
	}
	return vcf
}

func StreamVCF(vcf *vcfgo.Reader) RelatableChannel {
	ch := make(RelatableChannel, 256)
	go func() {
		for {
			v := vcf.Read()
			if v == nil {
				break
			}
			ch <- &Variant{v, 0, make([]Relatable, 0, 40)}
			vcf.Clear()
		}
		close(ch)
	}()
	return ch
}
