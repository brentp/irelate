package irelate

import (
	"testing"

	"github.com/brentp/bix"
	"github.com/brentp/irelate/interfaces"
)

func benchmarkStreams(nStreams int, b *testing.B) {

	for n := 0; n < b.N; n++ {
		streams := make([]interfaces.RelatableIterator, 0)
		f := "data/test.bed.gz"

		for i := 0; i < nStreams; i++ {
			s, e := bix.New(f)
			if e != nil {
				panic(e)
			}
			q, e := s.Query(nil)
			streams = append(streams, q)
		}

		//for a := range IRelate(CheckRelatedByOverlap, 0, Less, streams...) {
		iter := IRelate(CheckOverlapPrefix, 0, LessPrefix, streams...)
		for {
			a, err := iter.Next()
			if err != nil {
				break
				a.Start()
			}
		}

	}
}

func Benchmark2Streams(b *testing.B) { benchmarkStreams(2, b) }
func Benchmark3Streams(b *testing.B) { benchmarkStreams(3, b) }
