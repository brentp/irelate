package irelate

import (
	"fmt"
	"io"
	"log"
	"runtime"
	"sort"
	"sync"

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
			return int(e)
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

// make []interfaces.Relatable sortable.
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

// make a set of streams ready to be sent to irelate.
func makeStreams(sem chan int, fromchannels chan []interfaces.RelatableIterator, mustSort bool, A []interfaces.Relatable, lastChrom string, minStart int, maxEnd int, paths ...string) {

	if mustSort {
		sort.Sort(islice(A))
	}

	streams := make([]interfaces.RelatableIterator, 0, len(paths)+1)
	streams = append(streams, sliceToIterator(A))

	region := fmt.Sprintf("%s:%d-%d", lastChrom, minStart, maxEnd)

	for _, path := range paths {
		stream, err := Iterator(path, region)
		if err != nil {
			log.Fatal(err)
		}
		streams = append(streams, stream)
	}
	fromchannels <- streams
	<-sem
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
	return uint32(getStart(ci, int(ci.Relatable.Start())))
}

func (ci ciRel) End() uint32 {
	return uint32(getEnd(ci, int(ci.Relatable.End())))
}

// PIRelate implements a parallel IRelate
func PIRelate(chunk int, maxGap int, qstream interfaces.RelatableIterator, ciExtend bool, fn func(interfaces.Relatable), paths ...string) interfaces.RelatableChannel {

	// final interval stream sent back to caller.
	intersected := make(chan interfaces.Relatable, 1024)
	// fromchannels receives lists of relatables ready to be sent to IRelate
	fromchannels := make(chan []interfaces.RelatableIterator, 8)

	// to channels recieves channels to accept intervals from IRelate to be sent for merging.
	// we send slices of intervals to reduce locking.
	tochannels := make(chan chan []interfaces.Relatable, 8)

	// in parallel (hence the nested go-routines) run IRelate on chunks of data.
	sem := make(chan int, max(runtime.GOMAXPROCS(-1)/2, 1))

	work := func(rels []interfaces.Relatable, fn func(interfaces.Relatable), wg *sync.WaitGroup) {
		for _, r := range rels {
			fn(r)
		}
		wg.Done()
	}
	// call on the relatable itself. but with all of the associated intervals.
	if ciExtend {
		work = func(rels []interfaces.Relatable, fn func(interfaces.Relatable), wg *sync.WaitGroup) {
			for _, r := range rels {
				fn(r.(ciRel).Relatable)
			}
			wg.Done()
		}
	}

	// pull the intervals from IRelate, call fn() and  send chunks to be merged.
	go func() {
		// fwg keeps the work from the internal goroutines synchronized.
		// so that the intervals are sent in order.

		var fwg sync.WaitGroup
		// outerWg waits for all inner goroutines to finish so we know that w can
		// close tochannels
		var outerWg sync.WaitGroup
		N := 800
		kMAX := runtime.GOMAXPROCS(-1)
		for {
			streams, ok := <-fromchannels
			if !ok {
				break
			}
			// number of intervals stuck at this pahse will be kMAX * N

			saved := make([]interfaces.Relatable, N)
			outerWg.Add(1)
			go func(streams []interfaces.RelatableIterator) {
				fwg.Wait()
				fwg.Add(1)
				j := 0
				var wg sync.WaitGroup
				ochan := make(chan []interfaces.Relatable, kMAX)
				k := 0

				for interval := range IRelate(checkOverlap, 0, less, streams...) {
					saved[j] = interval
					j += 1
					if j == N {
						wg.Add(1)
						k += 1
						// send to channel then modify in parallel, then Wait()
						// this way we know that the intervals were sent to ochan
						// in order and we just wait untill all of them are procesessed
						// before sending to tochannels
						ochan <- saved

						go work(saved, fn, &wg)
						saved = make([]interfaces.Relatable, N)

						j = 0
						// only have 4 of these running at once because they are all in memory.
						if k == kMAX {
							wg.Wait()
							tochannels <- ochan
							close(ochan)
							ochan = make(chan []interfaces.Relatable, kMAX)
							k = 0
						}
					}
				}
				if j != 0 {
					wg.Add(1)
					// send to channel then modify in parallel, then Wait()
					ochan <- saved[:j]
					go work(saved[:j], fn, &wg)
				}
				wg.Wait()
				tochannels <- ochan
				close(ochan)
				for i := range streams {
					streams[i].Close()
				}
				fwg.Done()
				outerWg.Done()
			}(streams)
		}
		outerWg.Wait()
		close(tochannels)
	}()

	// merge the intervals from different channels keeping order.
	go func() {
		// 2 separate function code-blocks so there is no performance hit when they don't
		// care about the cipos.
		if ciExtend {
			// we need to track that the intervals come out in the order they went in
			// since we sort()'ed them based on the CIPOS.
			nextPrint := 0
			q := make(map[int]ciRel, 100)
			for {
				ch, ok := <-tochannels
				if !ok {
					break
				}

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
								intersected <- n.Relatable
								nextPrint++
							}
						}
					}
				}
			}
		} else {
			for {
				ch, ok := <-tochannels
				if !ok {
					break
				}

				for intervals := range ch {
					for _, interval := range intervals {
						intersected <- interval
					}
				}
			}
		}
		close(intersected)
	}()

	A := make([]interfaces.Relatable, 0, chunk+100)

	lastStart := -10
	lastChrom := ""
	minStart := int(^uint32(0) >> 1)
	maxEnd := 0
	idx := 0

	go func() {

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
			if v.Chrom() != lastChrom || (len(A) > 2048 && s-lastStart > maxGap) || ((s-lastStart > 15 && len(A) >= chunk) || len(A) >= chunk+100) || s-lastStart > 20*maxGap {
				if len(A) > 0 {
					sem <- 1
					// if ciExtend is true, we have to sort A by the new start which incorporates CIPOS
					go makeStreams(sem, fromchannels, ciExtend, A, lastChrom, minStart, maxEnd, paths...)
					// send work to IRelate
					log.Println("work unit:", len(A), fmt.Sprintf("%s:%d-%d", v.Chrom(), A[0].Start(), A[len(A)-1].End()), "gap:", int(v.Start())-lastStart)
					log.Println("\tfromchannels:", len(fromchannels), "tochannels:", len(tochannels), "intersected:", len(intersected))

				}
				lastStart = s
				lastChrom, minStart, maxEnd = v.Chrom(), s, e
				A = make([]interfaces.Relatable, 0, chunk+100)
			} else {
				lastStart = s
				maxEnd = max(e, maxEnd)
				minStart = min(s, minStart)
			}

			A = append(A, v)
		}

		if len(A) > 0 {
			sem <- 1
			makeStreams(sem, fromchannels, ciExtend, A, lastChrom, minStart, maxEnd, paths...)
		}
		close(fromchannels)
	}()

	return intersected
}
