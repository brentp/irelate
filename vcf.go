package irelate

import (
	"bufio"
	"compress/gzip"
	"github.com/brentp/vcfgo"
	"io"
	"log"
	"os"
	"strings"
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

func (v *Variant) Less(o Relatable) bool {
	if v.Chrom() != o.Chrom() {
		return v.Chrom() < o.Chrom()
	}
	return v.Start() < o.Start()
}

func Vopen(f string) *vcfgo.Reader {
	var rdr io.Reader
	rdr, err := os.Open(f)
	if err != nil {
		panic(err)
	}
	if strings.HasSuffix(f, ".gz") {
		rdr, err = gzip.NewReader(rdr)
	}
	if err != nil {
		panic(err)
	}
	rdr = bufio.NewReader(rdr)
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
