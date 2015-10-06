package irelate

import (
	"fmt"
	"io"
	"log"
	"runtime"
	"sync"

	"github.com/brentp/irelate/interfaces"
)

func getStartEnd(v interfaces.Relatable) (int, int) {
	s, e := int(v.Start()), int(v.End())
	if ci, ok := v.(interfaces.CIFace); ok {
		a, b, ok := ci.CIEnd()
		if ok && int(b) > e {
			e = int(b)
		}
		a, b, ok = ci.CIPos()
		if ok && int(a) < s {
			s = int(a)
		}
	}
	return s, e
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

// make a set of streams ready to be sent to irelate.
func makeStreams(fromchannels chan []interfaces.RelatableIterator, A []interfaces.Relatable, lastChrom string, minStart int, maxEnd int, paths ...string) {

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
}

func checkOverlap(a, b interfaces.Relatable) bool {
	return b.Start() < a.End()
}

func less(a, b interfaces.Relatable) bool {
	return a.Start() < b.Start() || (a.Start() == b.Start() && a.End() < b.End())
}

// PIRelate implements a parallel IRelate
func PIRelate(chunk int, maxGap int, qstream interfaces.RelatableIterator, fn func(interfaces.Relatable), paths ...string) interfaces.RelatableChannel {

	// final interval stream sent back to caller.
	intersected := make(chan interfaces.Relatable, 1024)
	// fromchannels receives lists of relatables ready to be sent to IRelate
	fromchannels := make(chan []interfaces.RelatableIterator, 8)

	// to channels recieves channels to accept intervals from IRelate to be sent for merging.
	// we send slices of intervals to reduce locking.
	tochannels := make(chan chan []interfaces.Relatable, 8)

	// in parallel (hence the nested go-routines) run IRelate on chunks of data.
	sem := make(chan int, runtime.GOMAXPROCS(-1)+1)

	work := func(rels []interfaces.Relatable, fn func(interfaces.Relatable), wg *sync.WaitGroup) {
		for _, r := range rels {
			fn(r)
		}
		wg.Done()
	}

	// pull the intervals from IRelate, call fn() and  send chunks to be merged.
	go func() {
		// fwg keeps the work from the internal goroutines synchronized.
		// so that the intervals are sent in order.

		var fwg sync.WaitGroup
		// outerWg waits for all inner goroutines to finish so we know that w can
		// close tochannels
		var outerWg sync.WaitGroup
		for {
			streams, ok := <-fromchannels
			if !ok {
				break
			}
			<-sem
			N := 400
			kMAX := runtime.GOMAXPROCS(-1)
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
					//fn(interval)
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
		// wait for all of the sending to finish before we close this channel
		close(intersected)
	}()

	A := make([]interfaces.Relatable, 0, chunk+100)

	lastStart := -10
	lastChrom := ""
	minStart := int(^uint32(0) >> 1)
	maxEnd := 0

	go func() {

		for {
			v, err := qstream.Next()
			if err == io.EOF {
				qstream.Close()
			}
			if v == nil {
				break
			}
			s, e := getStartEnd(v)
			// end chunk when:
			// 1. switch chroms
			// 2. see maxGap bases between adjacent intervals (currently looks at start only)
			// 3. reaches chunkSize (and has at least a gap of 2 bases from last interval).
			if v.Chrom() != lastChrom || (len(A) > 2048 && int(v.Start())-lastStart > maxGap) || ((int(v.Start())-lastStart > 15 && len(A) >= chunk) || len(A) >= chunk+100) || int(v.Start())-lastStart > 20*maxGap {
				if len(A) > 0 {
					sem <- 1
					go makeStreams(fromchannels, A, lastChrom, minStart, maxEnd, paths...)
					// send work to IRelate
					log.Println("work unit:", len(A), fmt.Sprintf("%s:%d-%d", v.Chrom(), A[0].Start(), A[len(A)-1].End()), "gap:", int(v.Start())-lastStart)
					log.Println("\tfromchannels:", len(fromchannels), "tochannels:", len(tochannels), "intersected:", len(intersected))

				}
				lastStart = int(v.Start())
				lastChrom, minStart, maxEnd = v.Chrom(), s, e
				A = make([]interfaces.Relatable, 0, chunk+100)
			} else {
				lastStart = int(v.Start())
				maxEnd = max(e, maxEnd)
				minStart = min(s, minStart)
			}

			A = append(A, v)
		}

		if len(A) > 0 {
			// TODO: move the semaphare into makestreams to avoid a race with the makestreams call above.
			sem <- 1
			makeStreams(fromchannels, A, lastChrom, minStart, maxEnd, paths...)
			// TODO: send to goroutine and block here until it returns so we don't send on closed channel.
		}
		close(fromchannels)
	}()

	return intersected
}
