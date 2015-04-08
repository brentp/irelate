package irelate

import (
	"sync"
	"testing"
)

func benchmarkStreams(nStreams int, b *testing.B) {

	for n := 0; n < b.N; n++ {
		streams := make([]RelatableChannel, 0)
		f := "data/test.bed.gz"
		s := &sync.Pool{New: func() interface{} { return &Interval{} }}

		for i := 0; i < nStreams; i++ {
			streams = append(streams, Streamer(f, s))
		}

		merged := Merge(streams...)

		for interval := range IRelate(merged, CheckRelatedByOverlap, false, 0) {
			Recycle(s, interval)
		}

	}
}

func Benchmark2Streams(b *testing.B) { benchmarkStreams(2, b) }
func Benchmark3Streams(b *testing.B) { benchmarkStreams(3, b) }
