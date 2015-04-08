package irelate

import (
	"testing"
)

func TestBam(t *testing.T) {
	var g RelatableChannel
	g = BamToRelatable("data/ex.bam")
	for i := range IRelate(g, CheckRelatedByOverlap, false, 0) {
		if len(i.Related()) != 0 {
			t.Errorf("should have another relation: %d", len(i.Related()))

		}
		i.SetSource(0)
		i.AddRelated(i)
	}

}
