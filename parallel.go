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
	sem := make(chan int, runtime.GOMAXPROCS(-1))

	work := func(rels []interfaces.Relatable, fn func(interfaces.Relatable), ochan chan []interfaces.Relatable,
		wg *sync.WaitGroup) {
		for _, r := range rels {
			fn(r)
		}
		ochan <- rels
		wg.Done()
	}

	// pull the intervals from IRelate, call fn() and  send chunks to be merged.
	go func() {
		for {
			streams, ok := <-fromchannels
			if !ok {
				break
			}
			<-sem
			N := 200
			ochan := make(chan []interfaces.Relatable, 3)
			tochannels <- ochan

			saved := make([]interfaces.Relatable, N)
			go func(streams []interfaces.RelatableIterator) {
				j := 0
				var wg sync.WaitGroup

				for interval := range IRelate(checkOverlap, 0, less, streams...) {
					//fn(interval)
					saved[j] = interval
					j += 1
					if j == N {
						//ochan <- saved
						wg.Add(1)
						go work(saved, fn, ochan, &wg)
						saved = make([]interfaces.Relatable, N)
						j = 0
					}
				}
				if j != 0 {
					wg.Add(1)
					go work(saved[:j], fn, ochan, &wg)
					//ochan <- saved[:j]
				}
				wg.Wait()
				close(ochan)
				for i := range streams {
					streams[i].Close()
				}
			}(streams)
		}
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
			log.Println("XXXXXXXXXXXXXXX ending", len(A), len(fromchannels))
			makeStreams(fromchannels, A, lastChrom, minStart, maxEnd, paths...)
			log.Println("XXXXXXXXXXXXXXX ended")
		}
		close(fromchannels)
	}()

	return intersected
}
