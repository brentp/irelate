package irelate

// parallel implements a parallel chrom-sweep.
// broad design is covered in design.md in the irelate package directory.
// In actual fact, there are a number of complexities; most of them relate to
// maintaining intervals in sorted order (and keeping chunks in sorted order)
// while allowing a good level of parallelism.

// more detailed explanations are provided whenever a channel is initialized
// as channels are our main means of keeping order.
// For example
//     tochannels := make(chan chan chan []interfaces.Relatable, 2+nprocs/2)
// Seems to have excessive use of channels, but we actually do need this since
// we have 2 levels of parallelization.
// One level is by chunk of query intervals.
// The next is by sub-chunk within the query chunks.
// The 3rd chan is a place-holder so that the work() function, which calls
// the user-defined fn() can be done concurrently (in a go routine).

// The broad pattern used throughout is to send a channel (K) into another
// channel (PARENT) to keep order and then send K into a worker goroutine
// that sends intervals or []intervals into K.

// I have done much tuning; the areas that affect performance are how the work()
// is parallelized (see the code-block that calls work()). And how the query
// chunks are determined. If the query chunks are too small (< 100 intervals),
// we have a lot of overhead in tracking that chunk that only requires a little
// computation. Unless the databases are very dense, then having the query chunks
// quite large helps parallelization. This is an area of potential optimization,
// though no obvious candidates have emerged.

import (
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"sort"

	"github.com/brentp/irelate/interfaces"
)

func getStart(v interfaces.Relatable, s int) int {
	if ci, ok := v.(interfaces.CIFace); ok {
		a, _, ok := ci.CIPos()
		if ok && int(a) < s {
			return int(a)
		}
	}
	return s
}

func getEnd(v interfaces.Relatable, e int) int {
	if ci, ok := v.(interfaces.CIFace); ok {
		_, b, ok := ci.CIEnd()
		if ok && int(b) > e {
			return int(b)
		}
	}
	return e
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

type sliceIt struct {
	slice []interfaces.Relatable
	i     int
}

func (s *sliceIt) Next() (interfaces.Relatable, error) {
	if s.i < len(s.slice) {
		v := s.slice[s.i]
		s.i += 1
		return v, nil
	}
	s.slice = nil
	return nil, io.EOF

}
func (s *sliceIt) Close() error {
	return nil
}

func sliceToIterator(A []interfaces.Relatable) interfaces.RelatableIterator {
	return &sliceIt{A, 0}
}

// islice makes []interfaces.Relatable sortable.
type islice []interfaces.Relatable

func (i islice) Len() int {
	return len(i)
}

func (i islice) Less(a, b int) bool {
	if i[a].Start() < i[b].Start() {
		return true
	}
	if i[a].Start() == i[b].Start() && i[a].End() <= i[b].End() {
		return true
	}
	return false
}

func (is islice) Swap(i, j int) {
	is[i], is[j] = is[j], is[i]
}

type pos struct {
	chrom string
	start int
	end   int
}

func (p pos) Chrom() string {
	return p.chrom
}
func (p pos) Start() uint32 {
	return uint32(p.start)
}
func (p pos) End() uint32 {
	return uint32(p.end)
}

// make a set of streams ready to be sent to irelate.
func makeStreams(receiver chan []interfaces.RelatableIterator, mustSort bool, A []interfaces.Relatable, lastChrom string, minStart int, maxEnd int, dbs ...interfaces.Queryable) {

	if mustSort {
		sort.Sort(islice(A))
	}

	streams := make([]interfaces.RelatableIterator, 0, len(dbs)+1)
	streams = append(streams, sliceToIterator(A))
	p := pos{lastChrom, minStart, maxEnd}

	for _, db := range dbs {
		stream, err := db.Query(p)
		if err != nil {
			log.Fatal(err)
		}
		streams = append(streams, stream)
	}
	receiver <- streams
	close(receiver)
}

func checkOverlap(a, b interfaces.Relatable) bool {
	return b.Start() < a.End()
}

func less(a, b interfaces.Relatable) bool {
	return a.Start() < b.Start() || (a.Start() == b.Start() && a.End() < b.End())
}

type ciRel struct {
	interfaces.Relatable
	index int
}

func (ci ciRel) Start() uint32 {
	return uint32(getStart(ci.Relatable, int(ci.Relatable.Start())))
}

func (ci ciRel) End() uint32 {
	return uint32(getEnd(ci.Relatable, int(ci.Relatable.End())))
}

// PIRelate implements a parallel IRelate
func PIRelate(chunk int, maxGap int, qstream interfaces.RelatableIterator, ciExtend bool, fn func(interfaces.Relatable), dbs ...interfaces.Queryable) interfaces.RelatableChannel {
	nprocs := runtime.GOMAXPROCS(-1)
	// final interval stream sent back to caller.
	intersected := make(chan interfaces.Relatable, 2048)

	// receivers keeps the interval chunks in order.
	receivers := make(chan chan []interfaces.RelatableIterator, 1)

	// to channels recieves channels that accept intervals from IRelate to be sent for merging.
	// we send slices of intervals to reduce locking.
	tochannels := make(chan chan chan []interfaces.Relatable, 2+nprocs/2)

	verbose := os.Getenv("IRELATE_VERBOSE") == "TRUE"

	// the user-defined callback runs int it's own goroutine.
	// call on the relatable itself. but with all of the associated intervals.
	work := func(rels []interfaces.Relatable, fn func(interfaces.Relatable)) chan []interfaces.Relatable {
		ch := make(chan []interfaces.Relatable, 0)
		go func() {
			if fn != nil {
				for _, r := range rels {
					fn(r)
				}
			}
			ch <- rels
			close(ch)
		}()
		return ch
	}
	if ciExtend {

		work = func(rels []interfaces.Relatable, fn func(interfaces.Relatable)) chan []interfaces.Relatable {
			ch := make(chan []interfaces.Relatable, 0)
			go func() {
				if fn != nil {
					for _, r := range rels {
						fn(r.(ciRel).Relatable)
					}
				}
				ch <- rels
				close(ch)
			}()
			return ch

		}
	}

	// pull the intervals from IRelate, call fn() and (via work()) send chunks to be merged.
	// calling fn() is a bottleneck. so we make sub-chunks and process them in a separate go-routine
	// in work()
	// inner channel keeps track of the order for each big chunk
	go func() {

		for streamsChan := range receivers {

			inner := make(chan chan []interfaces.Relatable, nprocs)
			tochannels <- inner

			// push a channel to to channels out here
			// and then push to that channel inside this goroutine.
			// this maintains order of the intervals.
			go func(streams []interfaces.RelatableIterator) {
				N := 400
				//saved := make([]interfaces.Relatable, N)
				iterator := IRelate(checkOverlap, 0, less, streams...)
				saved := make([]interfaces.Relatable, N)
				k := 0

				for {
					interval, err := iterator.Next()
					if err == io.EOF {
						iterator.Close()
						break
					}
					saved[k] = interval
					k++

					if k == N {
						inner <- work(saved, fn)
						k = 0
						saved = make([]interfaces.Relatable, N)
					}

				}
				if k > 0 {
					inner <- work(saved[:k], fn)
				}
				close(inner)
			}(<-streamsChan) // only one, just used a chan for ordering.
		}
		close(tochannels)
	}()

	go mergeIntervals(tochannels, intersected, ciExtend)

	// split the query intervals into chunks and send for processing to irelate.
	go func() {

		A := make([]interfaces.Relatable, 0, chunk/2)

		lastStart := -10
		lastChrom := ""
		minStart := int(^uint32(0) >> 1)
		maxEnd := 0
		var totalParsed, totalSkipped, c, idx int
		for {
			v, err := qstream.Next()
			if err == io.EOF {
				qstream.Close()
			}
			if v == nil {
				break
			}

			if ciExtend {
				// turn it into an object that will return the ci bounds for Start(), End()
				v = ciRel{v, idx}
				idx++
			}

			// these will be based on CIPOS, CIEND if ciExtend is true
			s, e := int(v.Start()), int(v.End())

			// end chunk when:
			// 1. switch chroms
			// 2. see maxGap bases between adjacent intervals (currently looks at start only)
			// 3. reaches chunkSize (and has at least a gap of 2 bases from last interval).
			if v.Chrom() != lastChrom || (len(A) > 2048 && s-lastStart > maxGap) || ((s-lastStart > 25 && len(A) >= chunk) || len(A) >= chunk+200) || s-lastStart > 10*maxGap {
				if len(A) > 0 {
					// we push a channel onto a queue (another channel) and use that as the output order.
					ch := make(chan []interfaces.RelatableIterator, 0)
					receivers <- ch
					// send work to IRelate
					go makeStreams(ch, ciExtend, A, lastChrom, minStart, maxEnd, dbs...)
					c++
					if verbose {
						if lastChrom == v.Chrom() {
							totalSkipped += s - lastStart
						}
						totalParsed += maxEnd - minStart
						var mem runtime.MemStats
						runtime.ReadMemStats(&mem)
						log.Println("intervals in current chunk:", len(A), fmt.Sprintf("%s:%d-%d", lastChrom, minStart, maxEnd), "gap:", s-lastStart)
						log.Println("\tc:", c, "receivers:", len(receivers), "tochannels:", len(tochannels), "intersected:", len(intersected))
						log.Printf("\tmemory use: %dMB , heap in use: %dMB\n", mem.Alloc/uint64(1000*1000),
							mem.HeapInuse/uint64(1000*1000))
						log.Printf("\ttotal bases skipped / parsed: %d / %d (%.2f)\n", totalSkipped, totalParsed, float64(totalSkipped)/float64(totalParsed))
					}

				}
				lastStart = s
				lastChrom, minStart, maxEnd = v.Chrom(), s, e
				A = make([]interfaces.Relatable, 0, chunk/2)
			} else {
				lastStart = s
				maxEnd = max(e, maxEnd)
				minStart = min(s, minStart)
			}

			A = append(A, v)
		}

		if len(A) > 0 {
			ch := make(chan []interfaces.RelatableIterator, 0)
			receivers <- ch
			go makeStreams(ch, ciExtend, A, lastChrom, minStart, maxEnd, dbs...)
			c++
		}
		close(receivers)
	}()
	return intersected
}

func mergeIntervals(tochannels chan chan chan []interfaces.Relatable, intersected chan interfaces.Relatable, ciExtend bool) {
	// merge the intervals from different channels keeping order.
	// 2 separate function code-blocks so there is no performance hit when they don't
	// care about the cipos.
	if ciExtend {
		nextPrint := 0
		q := make(map[int]ciRel, 100)
		for och := range tochannels {
			for ch := range och {
				for intervals := range ch {
					for _, interval := range intervals {
						ci := interval.(ciRel)
						if ci.index == nextPrint {
							intersected <- ci.Relatable
							nextPrint++
						} else {
							q[ci.index] = ci
							for {
								n, ok := q[nextPrint]
								if !ok {
									break
								}
								delete(q, nextPrint)
								intersected <- n.Relatable
								nextPrint++
							}
						}
					}
					// empty out the q
					for {
						n, ok := q[nextPrint]
						if !ok {
							break
						}
						delete(q, nextPrint)
						intersected <- n.Relatable
						nextPrint++
					}
				}
			}
		}
	} else {
		for och := range tochannels {
			for ch := range och {
				for intervals := range ch {
					for _, interval := range intervals {
						intersected <- interval
					}
				}
			}
		}
	}
	close(intersected)
}
