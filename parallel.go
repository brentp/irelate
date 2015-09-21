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
	m := make(interfaces.RelatableChannel, 512)
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
func PIRelate(chunk int, maxGap int, region string, query string, paths ...string) interfaces.RelatableChannel {

	qstream, err := Streamer(query, region)
	if err != nil {
		panic(err)
	}

	// final interval stream sent back to caller.
	intersected := make(chan interfaces.Relatable, 512)
	// fromchannels receives lists of relatables ready to be sent to IRelate
	fromchannels := make(chan []interfaces.RelatableChannel, 5)
	// to channels recieves channels to accept intervals from IRelate
	tochannels := make(chan chan interfaces.Relatable, 5)

	// in parallel (hence the nested go-routines) run IRelate on chunks of data.
	go func() {
		for {
			streams, ok := <-fromchannels
			if !ok {
				break
			}
			ochan := make(chan interfaces.Relatable, 2)
			tochannels <- ochan
			go func(streams []interfaces.RelatableChannel) {
				j := 0

				for interval := range IRelate(checkOverlap, 0, less, streams...) {
					j += 1
					ochan <- interval
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

			for interval := range ch {
				intersected <- interval
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
			if v.Chrom() != lastChrom || (len(A) > 0 && int(v.Start())-lastStart > maxGap) || ((int(v.Start())-lastStart > 2 && len(A) >= chunk) || len(A) >= chunk+10) {
				if len(A) > 0 {
					streams := makeStreams(A, lastChrom, minStart, maxEnd, paths...)
					// send work to IRelate
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
