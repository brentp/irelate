package irelate

import (
	"bufio"
	"compress/gzip"
	"github.com/mendelics/vcf"
	"io"
	"log"
	"os"
	"strings"
)

type Vcf struct {
	*vcf.Variant
	related []Relatable
	source  uint32
}

func (v *Vcf) Source() uint32 {
	return v.source
}
func (v *Vcf) SetSource(src uint32) {
	v.source = src
}

func (v *Vcf) Start() uint32 {
	return uint32(v.Pos - 1)
}

// TODO: check this
func (v *Vcf) End() uint32 {
	return uint32(v.Pos + len(v.Alt) - len(v.Ref))
}

func (v *Vcf) Chrom() string {
	return v.Variant.Chrom
}

func (v *Vcf) AddRelated(o Relatable) {
	v.related = append(v.related, o)
}

func (v *Vcf) Related() []Relatable {
	return v.related
}

func (v *Vcf) Less(o Relatable) bool {
	if v.Chrom() != o.Chrom() {
		return v.Chrom() < o.Chrom()
	}
	return v.Start() < o.Start()
}

func VCFToRelatable(file string) RelatableChannel {

	ch := make(chan Relatable, 16)

	och := make(chan *vcf.Variant, 5)
	ech := make(chan vcf.InvalidLine, 2)
	var f io.ReadCloser
	var err error

	if file == "-" {
		f = os.Stdin
	} else {
		f, err = os.Open(file)
		if err != nil {
			panic(err)
		}
		if strings.HasSuffix(file, ".gz") {
			f, err = gzip.NewReader(f)
			if err != nil {
				panic(err)
			}
		}
	}

	go func() {
		defer f.Close()
		r := bufio.NewReader(f)
		err = vcf.ToChannel(r, och, ech)
		if err != nil {
			panic(err)
		}
		go func() {
			for invalid := range ech {
				log.Println("failed to parse", invalid.Line)
			}
			close(ech)
		}()
		for o := range och {
			v := Vcf{Variant: o, related: make([]Relatable, 0, 5)}
			ch <- &v
		}
		close(ch)
	}()
	return ch
}
