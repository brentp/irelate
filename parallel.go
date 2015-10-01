package irelate

import (
	"fmt"
	"log"

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

func sliceToChan(A []interfaces.Relatable) interfaces.RelatableChannel {
	m := make(interfaces.RelatableChannel, 24)
	go func() {
		for _, r := range A {
			m <- r
		}
		close(m)
	}()
	return m
}

// make a set of streams ready to be sent to irelate.
func makeStreams(A []interfaces.Relatable, lastChrom string, minStart int, maxEnd int, paths ...string) []interfaces.RelatableChannel {

	streams := make([]interfaces.RelatableChannel, 0, len(paths)+1)
	streams = append(streams, sliceToChan(A))

	region := fmt.Sprintf("%s:%d-%d", lastChrom, minStart, maxEnd)

	for _, path := range paths {
		stream, err := Streamer(path, region)
		if err != nil {
			log.Fatal(err)
		}
		streams = append(streams, stream)
	}

	return streams
}

func checkOverlap(a, b interfaces.Relatable) bool {
	return b.Start() < a.End()
}

func less(a, b interfaces.Relatable) bool {
	return a.Start() < b.Start() || (a.Start() == b.Start() && a.End() < b.End())
}

// PIRelate implements a parallel IRelate
func PIRelate(chunk int, maxGap int, qstream interfaces.RelatableChannel, paths ...string) interfaces.RelatableChannel {

	// final interval stream sent back to caller.
	intersected := make(chan interfaces.Relatable, 2048)
	// fromchannels receives lists of relatables ready to be sent to IRelate
	fromchannels := make(chan []interfaces.RelatableChannel, 2)

	// to channels recieves channels to accept intervals from IRelate to be sent for merging.
	// we send slices of intervals to reduce locking.
	tochannels := make(chan chan []interfaces.Relatable, 2)

	// in parallel (hence the nested go-routines) run IRelate on chunks of data.
	go func() {
		for {
			streams, ok := <-fromchannels
			if !ok {
				break
			}
			N := 500
			ochan := make(chan []interfaces.Relatable, 2)
			tochannels <- ochan
			saved := make([]interfaces.Relatable, N)
			go func(streams []interfaces.RelatableChannel) {
				j := 0

				for interval := range IRelate(checkOverlap, 0, less, streams...) {
					saved[j] = interval
					j += 1
					if j > 0 && j%N == 0 {
						ochan <- saved
						saved = make([]interfaces.Relatable, N)
						j = 0
					}
				}
				if j != 0 {
					ochan <- saved[:j]
				}
				close(ochan)
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

	A := make([]interfaces.Relatable, 0, chunk+10)

	lastStart := -10
	lastChrom := ""
	minStart := int(^uint32(0) >> 1)
	maxEnd := 0

	go func() {

		for v := range qstream {
			s, e := getStartEnd(v)
			// end chunk when:
			// 1. switch chroms
			// 2. see maxGap bases between adjacent intervals (currently looks at start only)
			// 3. reaches chunkSize (and has at least a gap of 2 bases from last interval).
			if v.Chrom() != lastChrom || (len(A) > 2048 && int(v.Start())-lastStart > maxGap) || ((int(v.Start())-lastStart > 5 && len(A) >= chunk) || len(A) >= chunk+100) || int(v.Start())-lastStart > 20*maxGap {
				if len(A) > 0 {
					streams := makeStreams(A, lastChrom, minStart, maxEnd, paths...)
					// send work to IRelate
					log.Println("work unit:", len(A), fmt.Sprintf("%s:%d-%d", v.Chrom(), A[0].Start(), A[len(A)-1].End()), "gap:", int(v.Start())-lastStart)
					fromchannels <- streams

				}
				lastStart = int(v.Start())
				lastChrom, minStart, maxEnd = v.Chrom(), s, e
				A = make([]interfaces.Relatable, 0, chunk+10)
			} else {
				lastStart = int(v.Start())
				maxEnd = max(e, maxEnd)
				minStart = min(s, minStart)
			}

			A = append(A, v)
		}

		if len(A) > 0 {
			streams := makeStreams(A, lastChrom, minStart, maxEnd, paths...)
			// send work to IRelate
			fromchannels <- streams
		}
		close(fromchannels)
	}()

	return intersected
}
