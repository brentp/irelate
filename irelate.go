// Streaming relation (overlap, distance, KNN) testing of (any number of) sorted files of intervals.
package irelate

import (
	"container/heap"
	"fmt"
	"io"
	"log"
	"os"

	. "github.com/brentp/irelate/interfaces"
)

func relate(a Relatable, b Relatable, relativeTo int) {
	if a.Source() != b.Source() {
		if relativeTo == -1 {
			a.AddRelated(b)
			b.AddRelated(a)
		} else {
			if uint32(relativeTo) == a.Source() {
				a.AddRelated(b)
			}
			if uint32(relativeTo) == b.Source() {
				b.AddRelated(a)
			}
		}
	}
}

func Less(a Relatable, b Relatable) bool {
	if a.Chrom() != b.Chrom() {
		return a.Chrom() < b.Chrom()
	}
	return a.Start() < b.Start() // || (a.Start() == b.Start() && a.End() < b.End())
}

// 1, 2, 3 ... 9, 10, 11...
func NaturalLessPrefix(a Relatable, b Relatable) bool {
	if !SameChrom(a.Chrom(), b.Chrom()) {
		return NaturalLess(StripChr(a.Chrom()), StripChr(b.Chrom()))
	}
	return a.Start() < b.Start() || (a.Start() == b.Start() && a.End() < b.End())

}

// 1, 10, 11... 19, 2, 20, 21 ...
func LessPrefix(a Relatable, b Relatable) bool {
	if !SameChrom(a.Chrom(), b.Chrom()) {
		return StripChr(a.Chrom()) < StripChr(b.Chrom())
	}
	return a.Start() < b.Start() || (a.Start() == b.Start() && a.End() < b.End())
}

// CheckRelatedByOverlap returns true if Relatables overlap.
func CheckRelatedByOverlap(a Relatable, b Relatable) bool {
	return (b.Start() < a.End()) && (b.Chrom() == a.Chrom())
	// note with distance == 0 this just overlap.
	//distance := uint32(0)
	//return (b.Start()-distance < a.End()) && (b.Chrom() == a.Chrom())
}

// handles chromomomes like 'chr1' from one org and '1' from another.
func CheckOverlapPrefix(a Relatable, b Relatable) bool {
	if b.Start() < a.End() {
		return SameChrom(a.Chrom(), b.Chrom())
	}
	return false
}

// CheckKNN relates an interval to its k-nearest neighbors.
// The reporting function will have to do some filtering since this is only
// guaranteed to associate *at least* k neighbors, but it could be returning extra.
func CheckKNN(a Relatable, b Relatable) bool {
	// the first n checked would be the n_closest, but need to consider ties
	// the report function can decide what to do with them.
	k := 4
	r := a.Related()
	if len(r) >= k {
		// TODO: double-check this.
		return r[len(r)-1].Start()-a.End() < b.Start()-a.End()
	}
	return true
}

// filter rewrites the input-slice to remove nils.
func filter(s []Relatable, nils int) []Relatable {
	j := 0
	if len(s) != nils {

		for _, v := range s {
			if v != nil {
				s[j] = v
				j++
			}
		}
	}
	for k := j; k < len(s); k++ {
		s[k] = nil
	}
	return s[:j]
}

type irelate struct {
	checkRelated func(a, b Relatable) bool
	relativeTo   int
	less         func(a, b Relatable) bool
	//streams      []RelatableIterator
	cache       []Relatable
	sendQ       *relatableQueue
	mergeStream RelatableIterator
	//merger RelatableChannel
	nils int
}

// IRelate provides the basis for flexible overlap/proximity/k-nearest neighbor
// testing. IRelate receives merged, ordered Relatables via stream and takes
// function that checks if they are related (see CheckRelatedByOverlap).
// It is guaranteed that !Less(b, a) is true (we can't guarantee that Less(a, b)
// is true since they may have the same start). Once checkRelated returns false,
// it is assumed that no other `b` Relatables could possibly be related to `a`
// and so `a` is sent to the returnQ.
// streams are a variable number of channels that send intervals.
func IRelate(checkRelated func(a, b Relatable) bool,
	relativeTo int,
	less func(a, b Relatable) bool,
	streams ...RelatableIterator) RelatableIterator {

	mergeStream := newMerger(less, relativeTo, streams...)
	//merger := Merge(less, relativeTo, streams...)

	ir := &irelate{checkRelated: checkRelated, relativeTo: relativeTo,
		mergeStream: mergeStream,
		//merger: merger,
		cache: make([]Relatable, 0, 1024), sendQ: &relatableQueue{make([]Relatable, 0, 1024), less},
		less: less}
	return ir
}

func (ir *irelate) Close() error {
	return nil
}

func (ir *irelate) Next() (Relatable, error) {

	for {
		interval, err := ir.mergeStream.Next()
		if err == io.EOF {
			break
		}
		for i, c := range ir.cache {
			// tried using futures for checkRelated to parallelize... got slower
			if c == nil {
				continue
			}
			if ir.checkRelated(c, interval) {
				relate(c, interval, ir.relativeTo)
			} else {
				if ir.relativeTo == -1 || int(c.Source()) == ir.relativeTo {
					heap.Push(ir.sendQ, c)
				}
				ir.cache[i] = nil
				ir.nils++
			}
		}

		// only do this when we have a lot of nils as it's expensive to create a new slice.
		if ir.nils > 0 {
			// remove nils from the cache (must do this before sending)
			ir.cache, ir.nils = filter(ir.cache, ir.nils), 0
			var o Relatable
			if len(ir.sendQ.rels) > 0 {
				o = ir.sendQ.rels[0]
			} else {
				o = nil
			}
			if o != nil && (len(ir.cache) == 0 || ir.less(o, ir.cache[0])) {
				ir.cache = append(ir.cache, interval)
				return heap.Pop(ir.sendQ).(Relatable), nil
			}
		}
		ir.cache = append(ir.cache, interval)
	}
	if len(ir.cache) > 0 {
		for _, c := range ir.cache {
			if ir.relativeTo == -1 || int(c.Source()) == ir.relativeTo {
				heap.Push(ir.sendQ, c)
			}
		}
		ir.cache = ir.cache[:0]
	}
	if len(ir.sendQ.rels) > 0 {
		return heap.Pop(ir.sendQ).(Relatable), nil
	}
	return nil, io.EOF
}

type merger struct {
	less       func(a, b Relatable) bool
	relativeTo int
	streams    []RelatableIterator
	q          relatableQueue
	seen       map[string]struct{}
	j          int
	lastChrom  string
	verbose    bool
}

func newMerger(less func(a, b Relatable) bool, relativeTo int, streams ...RelatableIterator) *merger {
	q := relatableQueue{make([]Relatable, 0, len(streams)), less}
	verbose := os.Getenv("IRELATE_VERBOSE") == "TRUE"

	for i, stream := range streams {
		interval, err := stream.Next()
		if interval != nil {
			interval.SetSource(uint32(i))
			heap.Push(&q, interval)
		}
		if err == io.EOF {
			stream.Close()
		}
	}
	m := &merger{less: less, relativeTo: relativeTo, streams: streams, q: q, seen: make(map[string]struct{}), j: -1000, lastChrom: "", verbose: verbose}

	return m
}

func (m *merger) Close() error {
	return nil
}

func (m *merger) Next() (Relatable, error) {
	if len(m.q.rels) == 0 {
		return nil, io.EOF
	}
	interval := heap.Pop(&m.q).(Relatable)
	source := interval.Source()
	if !SameChrom(interval.Chrom(), m.lastChrom) {
		m.lastChrom = StripChr(interval.Chrom())
		if _, ok := m.seen[m.lastChrom]; ok {
			log.Println("warning: chromosomes must be in different order between files or the chromosome sort order is not as expected.")
			log.Printf("warning: overlaps will likely be missed after this chrom: %s from source: %d\n", m.lastChrom, interval.Source())
		}
		m.seen[m.lastChrom] = struct{}{}
		if m.verbose {
			log.Printf("on chromosome: %s\n", m.lastChrom)
		}
	}
	// pull the next interval from the same source.
	next_interval, err := m.streams[source].Next()
	if err == nil {
		if next_interval.Start() < interval.Start() {
			if SameChrom(next_interval.Chrom(), interval.Chrom()) {
				panic(fmt.Sprintf("intervals out of order within file: starts at: %d and %d from source: %d", interval.Start(), next_interval.Start(), source))
			}
		}
		next_interval.SetSource(source)
		heap.Push(&m.q, next_interval)
		m.j--
		if m.j == 0 {
			return nil, io.EOF
		}
	} else {
		if int(source) == m.relativeTo {
			// we pull in 200K more records and then stop. to make sure we get anything that might
			// relate to last query
			m.j = 200000
		}
	}
	if err == io.EOF {
		m.streams[source].Close()
	}
	return interval, nil
}
