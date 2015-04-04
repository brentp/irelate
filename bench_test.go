package irelate

import (
	"github.com/brentp/ififo"
	"testing"
)

func BenchmarkFull(b *testing.B) {

	for n := 0; n < b.N; n++ {
		streams := make([]RelatableChannel, 0)
		f := "data/test.bed.gz"
		s := ififo.NewIFifo(1000, func() interface{} { return &Interval{} })

		streams = append(streams, Streamer(f, s))
		streams = append(streams, Streamer(f, s))
		streams = append(streams, Streamer(f, s))

		merged := Merge(streams...)

		for interval := range IRelate(merged, CheckRelatedByOverlap, false, 0) {
			s.Put(interval)
		}

	}
}
