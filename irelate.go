// Streaming relation (overlap, distance, KNN) testing of (any number of) sorted files of intervals.
package irelate

import (
	"container/heap"
	"fmt"
	"log"
	"os"
	"strings"

	"vbom.ml/util/sortorder"
)

// Relatable provides all the methods for irelate to function.
// See Interval in interval.go for a class that satisfies this interface.
// Related() likely returns and AddRelated() likely appends to a slice of
// relatables. Note that for performance reasons, Relatable should be implemented
// as a pointer to your data-structure (see Interval).

type IPosition interface {
	Chrom() string
	Start() uint32
	End() uint32
}

type Relatable interface {
	IPosition
	Related() []Relatable // A slice of related Relatable's filled by IRelate
	AddRelated(Relatable) // Adds to the slice of relatables
	Source() uint32       // Internally marks the source (file/stream) of the Relatable
	SetSource(source uint32)
}

type IVariant interface {
	IPosition
	Ref() string
	Alt() []string
}

func SamePosition(a, b IPosition) bool {
	return a.Start() == b.Start() && a.End() == b.End() && a.Chrom() == b.Chrom()
}

func SameVariant(a, b IVariant) bool {
	if a.Start() != b.Start() || a.End() != b.End() || a.Chrom() != b.Chrom() || a.Ref() != b.Ref() {
		return false
	}
	for _, aalt := range a.Alt() {
		for _, balt := range b.Alt() {
			if aalt == balt {
				return true
			}
		}
	}
	return false
}

func Same(a, b IPosition, strict bool) bool {
	if av, ok := a.(IVariant); ok {
		if bv, ok := b.(IVariant); ok {
			return SameVariant(av, bv)
		}
		if strict {
			return false
		}
		return SamePosition(a, b)
	}
	if _, ok := b.(IVariant); ok {
		if strict {
			return false
		}
		return SamePosition(a, b)
	}
	return SamePosition(a, b)
}

// RelatableChannel
type RelatableChannel chan Relatable

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

func SameChrom(a, b string) bool {
	if a == b {
		return true
	}
	return stripChr(a) == stripChr(b)
}

func stripChr(c string) string {
	if strings.HasPrefix(c, "chr") {
		return c[3:]
	}
	return c
}

// 1, 2, 3 ... 9, 10, 11...
func NaturalLessPrefix(a Relatable, b Relatable) bool {
	if !SameChrom(a.Chrom(), b.Chrom()) {
		return sortorder.NaturalLess(stripChr(a.Chrom()), stripChr(b.Chrom()))
	}
	return a.Start() < b.Start() || (a.Start() == b.Start() && a.End() < b.End())

}

// 1, 10, 11... 19, 2, 20, 21 ...
func LessPrefix(a Relatable, b Relatable) bool {
	if !SameChrom(a.Chrom(), b.Chrom()) {
		return stripChr(a.Chrom()) < stripChr(b.Chrom())
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

// Send the relatables to the channel in sorted order.
// Check that we couldn't later get an item with a lower start from the current cache.
func sendSortedRelatables(sendQ *relatableQueue, cache []Relatable, out chan Relatable, less func(a, b Relatable) bool) {
	var j int
	for j = 0; j < len((*sendQ).rels) && (len(cache) == 0 || less((*sendQ).rels[j].(Relatable), cache[0])); j++ {
	}
	for i := 0; i < j; i++ {
		out <- heap.Pop(sendQ).(Relatable)
	}
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
	streams ...RelatableChannel) chan Relatable {

	// we infer the chromosome order by the order that we see from source 0.
	stream := Merge(less, relativeTo, streams...)
	out := make(chan Relatable, 64)
	go func() {

		// use the cache to keep relatables to test against.
		cache := make([]Relatable, 1, 1024)
		cache[0] = <-stream

		// Use sendQ to make sure we output in sorted order.
		// We know we can print something when sendQ.minStart < cache.minStart
		sendQ := relatableQueue{make([]Relatable, 0, 1024), less}
		nils := 0

		// TODO:if we know the ends are sorted (in addition to start) then we have some additional
		// optimizations. As soon as checkRelated is false, then all others in the cache before that
		// should be true... binary search if endSorted and len(cache) > 20?
		//endSorted := true
		for interval := range stream {

			for i, c := range cache {
				// tried using futures for checkRelated to parallelize... got slower
				if c == nil {
					continue
				}
				if checkRelated(c, interval) {
					relate(c, interval, relativeTo)
				} else {
					if relativeTo == -1 || c.Source() == uint32(relativeTo) {
						heap.Push(&sendQ, c)
					}
					cache[i] = nil
					nils++
				}
			}

			// only do this when we have a lot of nils as it's expensive to create a new slice.
			if nils > 1 {
				// remove nils from the cache (must do this before sending)
				cache, nils = filter(cache, nils), 0
				// send the elements from cache in order.
				// use heuristic to minimize the sending.
				if len(sendQ.rels) > 8 {
					sendSortedRelatables(&sendQ, cache, out, less)
				}
			}
			cache = append(cache, interval)

		}
		for _, c := range filter(cache, nils) {
			if c.Source() == uint32(relativeTo) || relativeTo == -1 {
				heap.Push(&sendQ, c)
			}
		}
		for i := 0; i < len(sendQ.rels); i++ {
			out <- sendQ.rels[i]
		}
		close(out)
	}()
	return out
}

// Merge accepts channels of Relatables and merges them in order.
// Streams of Relatable's from different source must be merged to send
// to IRelate.
// This uses a priority queue and acts like python's heapq.merge.
func Merge(less func(a, b Relatable) bool, relativeTo int, streams ...RelatableChannel) RelatableChannel {
	verbose := os.Getenv("IRELATE_VERBOSE") == "TRUE"
	q := relatableQueue{make([]Relatable, 0, len(streams)), less}
	seen := make(map[string]struct{})
	for i, stream := range streams {
		interval := <-stream
		if interval != nil {
			interval.SetSource(uint32(i))
			heap.Push(&q, interval)
		}
	}

	ch := make(chan Relatable, 8)
	go func() {
		var interval Relatable
		sentinel := struct{}{}
		lastChrom := ""
		// heuristic to use this to stop when end of query records is reached.
		j := -1000
		for len(q.rels) > 0 {
			interval = heap.Pop(&q).(Relatable)
			source := interval.Source()
			ch <- interval
			if SameChrom(interval.Chrom(), lastChrom) {
				lastChrom = stripChr(interval.Chrom())
				if _, ok := seen[lastChrom]; ok {
					log.Println("warning: chromosomes must be in different order between files or the chromosome sort order is not as expected.")
					log.Printf("warning: overlaps will likely be missed after this chrom: %s from source: %d\n", lastChrom, interval.Source())
				}
				seen[lastChrom] = sentinel
				if verbose {
					log.Printf("on chromosome: %s\n", lastChrom)
				}
			}
			// pull the next interval from the same source.
			next_interval, ok := <-streams[source]
			if ok {
				if next_interval.Start() < interval.Start() {
					if SameChrom(next_interval.Chrom(), interval.Chrom()) {
						panic(fmt.Sprintf("intervals out of order within file: starts at: %d and %d from source: %d", interval.Start(), next_interval.Start(), source))
					}
				}
				next_interval.SetSource(source)
				heap.Push(&q, next_interval)
				j--
				if j == 0 {
					break
				}
			} else {
				if int(source) == relativeTo {
					// we pull in 200K more records and then stop. to make sure we get anything that might
					// relate to last query
					j = 200000
				}
			}
		}
		close(ch)
	}()
	return ch
}
