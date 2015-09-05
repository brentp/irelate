// Streaming relation (overlap, distance, KNN) testing of (any number of) sorted files of intervals.
package irelate

import (
	"container/heap"
	"fmt"
	"log"
	"os"

	. "github.com/brentp/irelate/interfaces"

	"vbom.ml/util/sortorder"
)

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

// 1, 2, 3 ... 9, 10, 11...
func NaturalLessPrefix(a Relatable, b Relatable) bool {
	if !SameChrom(a.Chrom(), b.Chrom()) {
		return sortorder.NaturalLess(StripChr(a.Chrom()), StripChr(b.Chrom()))
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

// Send the relatables to the channel in sorted order.
// Check that we couldn't later get an item with a lower start from the current cache.
func sendSortedRelatables(sendQ *relatableQueue, minStart uint32, out chan Relatable, less func(a, b Relatable) bool) {
	var j int
	for j = 0; j < len((*sendQ).rels) && ((*sendQ).rels[j].(Relatable).Start() < minStart); j++ {
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

		// (lowest ends at start of q) highest ends get popped first.
		cache := NewPriorityQueue(1024, less)
		v := <-stream
		if v == nil {
			close(out)
			return
		}
		cache.Put(v)
		lastChrom := v.Chrom()

		// Use sendQ to make sure we output in sorted order.
		// We know we can print something when sendQ.minStart < cache.minStart
		sendQ := relatableQueue{make([]Relatable, 0, 1024), less}
		j := 0

		for interval := range stream {
			// cache.rels orded with highest ends last.
			// as soon as we are related, break:
			for j = 0; j < len(cache.items); j++ {
				c := cache.Peek()
				if checkRelated(c, interval) {
					break
				}

				if relativeTo == -1 || c.Source() == uint32(relativeTo) {
					// ignore the error since we must have enough.
					v, _ := cache.Get(1)
					heap.Push(&sendQ, v[0])

				}
			}

			// all these have been sent to sendQ
			minStart := ^uint32(0)

			if interval.Chrom() != lastChrom {
				sendSortedRelatables(&sendQ, minStart, out, less)
				lastChrom = interval.Chrom()
				if len(cache.items) > 0 {
					log.Fatalf("shouldn't have any overlaps across chromosomes")
				}
				if len(sendQ.rels) > 0 {
					log.Fatalf("sendQ should be empty across chromosomes")
				}
			} else {
				for _, c := range cache.items {
					if c.Start() < minStart {
						minStart = c.Start()
					}
					relate(c, interval, relativeTo)
				}
				if len(sendQ.rels) > 5 {
					sendSortedRelatables(&sendQ, minStart, out, less)
				}
			}
			cache.Put(interval)
		}
		for _, c := range cache.items {
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
				lastChrom = StripChr(interval.Chrom())
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
