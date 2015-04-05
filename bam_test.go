package irelate

import (
	"github.com/brentp/ififo"
	"testing"
)

func TestBam(t *testing.T) {
	var g RelatableChannel
	g = BamToRelatable("data/ex.bam")
	q := ififo.NewIFifo(100, func() interface{} { return &Interval{} })
	for i := range IRelate(g, CheckRelatedByOverlap, false, 0, q) {
		if len(i.Related()) != 0 {
			t.Errorf("should have another relation: %d", len(i.Related()))

		}
		i.SetSource(0)
		i.AddRelated(i)
	}

}
