package irelate

import (
	"testing"
)

func benchmarkStreams(nStreams int, b *testing.B) {

	for n := 0; n < b.N; n++ {
		streams := make([]RelatableChannel, 0)
		f := "data/test.bed.gz"

		for i := 0; i < nStreams; i++ {
			streams = append(streams, Streamer(f))
		}

		//for a := range IRelate(CheckOverlapPrefix, 0, LessPrefix, streams...) {
		for a := range IRelate(CheckRelatedByOverlap, 0, Less, streams...) {
			a.Start()
		}

	}
}

func Benchmark2Streams(b *testing.B) { benchmarkStreams(2, b) }
func Benchmark3Streams(b *testing.B) { benchmarkStreams(3, b) }
