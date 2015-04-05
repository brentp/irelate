package irelate

import (
	"github.com/brentp/ififo"
	"testing"
)

func TestGff(t *testing.T) {
	var g RelatableChannel
	g = GFFToRelatable("data/ex.gff")
	q := ififo.NewIFifo(100, func() interface{} { return &Interval{} })
	for i := range IRelate(g, CheckRelatedByOverlap, true, 0, q) {
		if len(i.Related()) != 1 {
			t.Errorf("should have another relation: %d", len(i.Related()))

		}
		i.SetSource(0)
	}

}
