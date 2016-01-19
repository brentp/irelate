// Implements Relatable for Bam

package parsers

import (
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/biogo/hts/bam"
	"github.com/biogo/hts/bgzf"
	"github.com/biogo/hts/bgzf/index"
	"github.com/biogo/hts/sam"
	"github.com/brentp/irelate/interfaces"
)

type Bam struct {
	*sam.Record
	source     uint32
	related    []interfaces.Relatable
	Chromosome string
	_end       uint32
}

func (a *Bam) Chrom() string {
	return a.Chromosome
}

// cast to 32 bits.
func (a *Bam) Start() uint32 {
	return uint32(a.Record.Start())
}

func (a *Bam) End() uint32 {
	if a._end != 0 {
		return a._end
	}
	a._end = uint32(a.Record.End())
	return a._end
}

func (a *Bam) Source() uint32 {
	return a.source
}

func (a *Bam) SetSource(src uint32) {
	a.source = src
}

func (a *Bam) AddRelated(b interfaces.Relatable) {
	if a.related == nil {
		a.related = make([]interfaces.Relatable, 1, 2)
		a.related[0] = b
	} else {
		a.related = append(a.related, b)
	}
}
func (a *Bam) Related() []interfaces.Relatable {
	return a.related
}

func (a *Bam) MapQ() int {
	return int(a.Record.MapQ)
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func BamToRelatable(f io.Reader) (interfaces.RelatableChannel, error) {

	ch := make(chan interfaces.Relatable, 64)
	b, err := bam.NewReader(f, 0)
	if err != nil {
		return nil, err
	}

	go func() {
		for {
			rec, err := b.Read()
			if err != nil {
				if err == io.EOF {
					break
				} else {
					log.Println(err)
					break
				}
			}
			if rec.RefID() == -1 { // unmapped
				continue
			}
			// TODO: see if keeping the list of chrom names and using a ref is better.
			bam := Bam{Record: rec, Chromosome: rec.Ref.Name(), related: nil}
			ch <- &bam
		}
		close(ch)
		b.Close()
		f.(io.ReadCloser).Close()
	}()
	return ch, nil
}

type BamQueryable struct {
	idx  *bam.Index
	path string
	file io.Reader
	bgzf *bgzf.Reader
	refs map[string]*sam.Reference
}

func NewBamQueryable(path string, workers ...int) (*BamQueryable, error) {
	f, err := os.Open(path + ".bai")
	if err != nil {
		return nil, err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer gz.Close()
	idx, err := bam.ReadIndex(gz)

	b, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	n := 1
	if len(workers) > 0 {
		n = workers[0]
	}

	bgz, err := bgzf.NewReader(b, n)
	if err != nil {
		return nil, err
	}

	br, err := bam.NewReader(bgz, 1)
	if err != nil {
		return nil, err
	}
	hdr := br.Header()
	refs := make(map[string]*sam.Reference, 40)
	for _, r := range hdr.Refs() {
		refs[r.Name()] = r
	}
	br.Close()

	return &BamQueryable{idx: idx, path: path, file: b, bgzf: bgz, refs: refs}, nil

}

func newShort(old *BamQueryable) (*BamQueryable, error) {
	b := &BamQueryable{
		idx:  old.idx,
		path: old.path,
		refs: old.refs,
	}
	var err error
	b.file, err = os.Open(b.path)
	if err != nil {
		return nil, err
	}
	b.bgzf, err = bgzf.NewReader(b.file, 2)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (b *BamQueryable) Query(region interfaces.IPosition) (interfaces.RelatableIterator, error) {
	bn, err := newShort(b) // make a copy since we're messing with the file-pointer
	if err != nil {
		return nil, err
	}

	ref, ok := b.refs[region.Chrom()]
	if !ok {
		if !strings.HasPrefix(region.Chrom(), "chr") {
			ref, ok = b.refs["chr"+region.Chrom()]
		} else if strings.HasPrefix(region.Chrom(), "chr") {
			ref, ok = b.refs[region.Chrom()[3:]]
		}
	}
	if !ok {
		return nil, fmt.Errorf("%s not found in %s", region.Chrom(), bn.path)
	}

	ch := make(chan interfaces.Relatable, 100)
	go func() {
		chrom := ref.Name()
		chunks, err := bn.idx.Chunks(ref, int(region.Start()), int(region.End()))
		if err != nil {
			if err != io.EOF {
				log.Println(err)
			}
			close(ch)
			return
		}
		cr, err := index.NewChunkReader(bn.bgzf, chunks)
		if err != nil {
			if err != io.EOF {
				log.Println(err)
			}
			close(ch)
			return
		}
		br, err := bam.NewReader(cr, 2)
		if err != nil {
			if err != io.EOF {
				log.Println(err)
			}
			close(ch)
			return
		}
		for {
			rec, err := br.Read()
			if err != nil {
				if err != io.EOF {
					log.Println(err)
				}
				break
			}
			ch <- &Bam{Record: rec, Chromosome: chrom, related: nil}
		}
		br.Close()
		close(ch)
	}()
	return &BamIterator{ch, bn}, nil
}

func (b *BamQueryable) Close() error {
	b.bgzf.Close()
	if cr, ok := b.file.(io.ReadCloser); ok {
		cr.Close()
	}
	return nil
}

type BamIterator struct {
	ch interfaces.RelatableChannel
	b  *BamQueryable
}

func NewBamIterator(f string) (*BamIterator, error) {
	fh, err := os.Open(f)
	if err != nil {
		return nil, err
	}
	ch, err := BamToRelatable(fh)

	b := &BamIterator{ch: ch}

	return b, err
}

func (b *BamIterator) Close() error {
	if b.b != nil {
		return b.Close()
	}
	return nil
}

func (b *BamIterator) Next() (interfaces.Relatable, error) {
	rec, ok := <-b.ch
	if !ok {
		return nil, io.EOF
	}
	return rec, nil
}
