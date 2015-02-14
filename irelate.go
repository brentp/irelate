// Streaming relation (overlap, distance, KNN) testing of (any number of) sorted files of intervals.
package irelate

import (
	"container/heap"
	_ "fmt"
	_ "os"
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
	Index() *int
}

type Relatables struct {
	Relatable
	Related []Relatable
}

// RelatableChannel
type RelatableChannel chan Relatable
type RelatablesChannel chan Relatables

type RelatableMap map[int][]Relatable

func relate(a Relatable, b Relatable, cache RelatableMap, includeSameSourceRelations bool, relativeTo int) {
	if (a.Source() != b.Source()) || includeSameSourceRelations {
		if *a.Index() < 0 {
			panic("b shouldn't happen")
		}
		if *b.Index() < 0 {
			panic("a shouldn't happen")
		}
		if relativeTo != -1 {
			if uint32(relativeTo) == a.Source() {
				cache[*a.Index()] = append(cache[*a.Index()], b)
				//fmt.Fprintf(os.Stderr, "%d\n", len((*cache)[*a.Index()]))
			}
			if uint32(relativeTo) == b.Source() {
				cache[*b.Index()] = append(cache[*b.Index()], a)
			}
		} else {
			cache[*a.Index()] = append(cache[*a.Index()], b)
			cache[*b.Index()] = append(cache[*b.Index()], a)
		}
	}
}

// CheckRelatedByOverlap returns true if Relatables overlap.
func CheckRelatedByOverlap(a Relatable, b Relatable) bool {
	distance := uint32(0)
	// note with distance == 0 this just overlap.
	return (b.Start()-distance < a.End()) && (b.Chrom() == a.Chrom())
}

// Send the relatables to the channel in sorted order.
// Check that we couldn't later get an item with a lower start from the current cache.
func sendSortedRelatables(sendQ *relatableQueue, cache RelatableMap, out chan Relatables) {
	// TODO: need to skip things in cache that are in sendQ. This should happen by clist[0].Index() > 0...
	kmin := int(^uint32(0))
	for k, clist := range cache {
		if k < kmin && *(clist[0].Index()) > 0 {
			kmin = k
		}
		//fmt.Fprintf(os.Stderr, "%d\t", *clist[0].Index())
	}
	//fmt.Fprintf(os.Stderr, "\n")
	if kmin == int(^uint32(0)) {
		return
	}
	//fmt.Fprintf(os.Stderr, "kmin: %d\t", kmin)
	//fmt.Fprintf(os.Stderr, "%v", cache[kmin][0])
	//fmt.Fprintf(os.Stderr, "KMIN Index: %d\n", *cache[kmin][0].Index())
	//fmt.Fprintf(os.Stderr, "sendQ[0] %v\n", (*sendQ)[0].(Relatable))
	var j int
	for j = 0; j < len(*sendQ) && (len(cache) == 0 || (*sendQ)[j].(Relatable).Less(cache[kmin][0])); j++ {
	}
	var r Relatable
	//fmt.Fprintf(os.Stderr, "j: %d\n", j)
	//fmt.Fprintf(os.Stderr, "len(sendQ): %d\n", len(*sendQ))
	for i := 0; i < j; i++ {
		r = heap.Pop(sendQ).(Relatable)
		//c := cache[*r.Index()]
		//print(len(c))
		out <- Relatables{r, cache[-*r.Index()]}
		delete(cache, -*r.Index())
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

		var cache = make(RelatableMap)
		// use the cache to keep relatables to test against.
		interval := <-stream
		cache[*(interval.Index())] = make([]Relatable, 1)
		// the self is alwasy the first element in the slice.
		cache[*interval.Index()][0] = interval

		// Use sendQ to make sure we output in sorted order.
		// We know we can print something when sendQ.minStart < cache.minStart
		sendQ := make(relatableQueue, 0, 256)

		for interval = range stream {

			// TODO: reverse cache so that removing the last element (most common case)
			// is simply a matter of setting len(cache) = len(cache) - 1
			for _, clist := range cache {
				if *clist[0].Index() < 0 {
					continue
				}
				c := clist[0]
				// tried using futures for checkRelated to parallelize... got slower
				// TODO: try again since relate() is now expensive as it contains all allocations.
				if checkRelated(c, interval) {
					relate(c, interval, cache, includeSameSourceRelations, relativeTo)
				} else {
					*c.Index() = -(*c.Index())
					if relativeTo == -1 || c.Source() == uint32(relativeTo) {
						// if it's negative, then it has been used.
						heap.Push(&sendQ, c)
					}
				}
			}
			//fmt.Fprintf(os.Stderr, "len Q: %d; len cache: %d\n", len(sendQ), len(cache))

			// only do this when we have a lot of nils as it's expensive to create a new slice.
			if len(sendQ) > 128 {
				sendSortedRelatables(&sendQ, cache, out)
				for ckey, clist := range cache {
					if *clist[0].Index() < 0 {
						delete(cache, ckey)
					}
				}
			}
			cache[*(interval.Index())] = make([]Relatable, 1)
			cache[*(interval.Index())][0] = interval

		}
		for _, clist := range cache {
			c := clist[0]
			if *c.Index() < 0 {
				continue
			}
			if relativeTo == -1 || c.Source() == uint32(relativeTo) {
				heap.Push(&sendQ, c)
			}
		}
		for i := 0; i < len(sendQ); i++ {
			r := sendQ[i].(Relatable)
			out <- Relatables{r, cache[*r.Index()]}
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
	j := int(1)
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
