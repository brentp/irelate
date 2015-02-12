// Streaming relation (overlap, distance, KNN) testing of (any number of) sorted files of intervals.
package irelate

import (
	"container/heap"
)

// Relatable provides all the methods for irelate to function.
// See Interval in interval.go for a class that satisfies this interface.
// Note that for performance reasons, Relatable should be implemented
// as a pointer to your data-structure (see Interval).
type Relatable interface {
	Chrom() string
	Start() uint32
	End() uint32
	Source() uint32 // Internally marks the source (file/stream) of the Relatable
	SetSource(source uint32)
	Less(other Relatable) bool // Determines order of the relatables (chrom, start)
	Index() *uint32
}

type Relatables struct {
	Relatable
	Related []Relatable
}

// RelatableChannel
type RelatableChannel chan Relatable
type RelatablesChannel chan Relatables

type RelatableMap map[uint32][]Relatable

func relate(a Relatable, b Relatable, related RelatableMap, includeSameSourceRelations bool, relativeTo int) {
	if (a.Source() != b.Source()) || includeSameSourceRelations {
		if relativeTo != -1 {
			if uint32(relativeTo) == a.Source() {
				related[*a.Index()] = append(related[*a.Index()], b)
			}
			if uint32(relativeTo) == b.Source() {
				related[*b.Index()] = append(related[*b.Index()], a)
			}
		} else {
			related[*a.Index()] = append(related[*a.Index()], b)
			related[*b.Index()] = append(related[*b.Index()], a)
		}
	}
}

// CheckRelatedByOverlap returns true if Relatables overlap.
func CheckRelatedByOverlap(a Relatable, b Relatable) bool {
	distance := uint32(0)
	// note with distance == 0 this just overlap.
	return (b.Start()-distance < a.End()) && (b.Chrom() == a.Chrom())
}

// filter rewrites the input-slice to remove nils.
func filter(s []Relatable, nils int) []Relatable {
	if len(s) == nils {
		return s[:0]
	}

	j := 0
	for _, v := range s {
		if v != nil {
			s[j] = v
			j++
		}
	}
	return s[:j]
}

// Send the relatables to the channel in sorted order.
// Check that we couldn't later get an item with a lower start from the current cache.
func sendSortedRelatables(sendQ *relatableQueue, related RelatableMap, cache []Relatable, out chan Relatables) {
	var j int
	for j = 0; j < len(*sendQ) && (len(cache) == 0 || (*sendQ)[j].(Relatable).Less(cache[0])); j++ {
	}
	var r Relatable
	for i := 0; i < j; i++ {
		r = heap.Pop(sendQ).(Relatable)
		out <- Relatables{r, related[*r.Index()]}
		delete(related, *r.Index())
	}
}

// IRelate provides the basis for flexible overlap/proximity/k-nearest neighbor
// testing. IRelate receives merged, ordered Relatables via stream and takes
// function that checks if they are related (see CheckRelatedByOverlap).
// It is guaranteed that !b.Less(a) is true (we can't guarantee that a.Less(b)
// is true since they may have the same start). Once checkRelated returns false,
// it is assumed that no other `b` Relatables could possibly be related to `a`
// and so `a` is sent to the returnQ. It is likely that includeSameSourceRelations
// will only be set to true if one is doing something like a merge.
func IRelate(stream RelatableChannel,
	checkRelated func(a Relatable, b Relatable) bool,
	includeSameSourceRelations bool,
	relativeTo int) chan Relatables {

	out := make(chan Relatables, 64)
	go func() {

		// TODO: merge cache and related to reduce mem use.
		var related = make(RelatableMap)
		// use the cache to keep relatables to test against.
		cache := make([]Relatable, 1, 256)
		cache[0] = <-stream

		// Use sendQ to make sure we output in sorted order.
		// We know we can print something when sendQ.minStart < cache.minStart
		sendQ := make(relatableQueue, 0, 256)
		nils := 0

		for interval := range stream {

			// TODO: reverse cache so that removing the last element (most common case)
			// is simply a matter of setting len(cache) = len(cache) - 1
			for i, c := range cache {
				// tried using futures for checkRelated to parallelize... got slower
				// TODO: try again since relate() is now expensive as it contains all allocations.
				if checkRelated(c, interval) {
					relate(c, interval, related, includeSameSourceRelations, relativeTo)
				} else {
					if relativeTo == -1 || c.Source() == uint32(relativeTo) {
						heap.Push(&sendQ, c)
					}
					cache[i] = nil
					nils++
				}
			}

			// only do this when we have a lot of nils as it's expensive to create a new slice.
			if nils > 0 {
				// remove nils from the cache (must do this before sending)
				cache, nils = filter(cache, nils), 0
				// send the elements from cache in order.
				// use heuristic to minimize the sending.
				if len(sendQ) > 128 {
					sendSortedRelatables(&sendQ, related, cache, out)
				}
			}
			cache = append(cache, interval)

		}
		for _, c := range filter(cache, nils) {
			if relativeTo == -1 || c.Source() == uint32(relativeTo) {
				heap.Push(&sendQ, c)
			}
		}
		for i := 0; i < len(sendQ); i++ {
			r := sendQ[i].(Relatable)
			out <- Relatables{r, related[*r.Index()]}
		}
		close(out)
	}()
	return out
}

// Merge accepts channels of Relatables and merges them in order.
// Streams of Relatable's from different source must be merged to send
// to IRelate.
// This uses a priority queue and acts like python's heapq.merge.
func Merge(streams ...RelatableChannel) RelatableChannel {
	q := make(relatableQueue, 0, len(streams))
	j := uint32(0)
	for i, stream := range streams {
		interval := <-stream
		if interval != nil {
			interval.SetSource(uint32(i))
			*interval.Index() = j
			heap.Push(&q, interval)
			j++
		}
	}
	ch := make(chan Relatable, 8)
	go func() {
		var interval Relatable
		for len(q) > 0 {
			interval = heap.Pop(&q).(Relatable)
			source := interval.Source()
			ch <- interval
			// need the case/select stmt here to handle end of each stream
			//select {
			// pull the next interval from the same source.
			next_interval, ok := <-streams[source]
			if ok {
				next_interval.SetSource(source)
				*next_interval.Index() = j
				j++
				heap.Push(&q, next_interval)
			}
			//}
		}
		close(ch)
	}()
	return ch
}
