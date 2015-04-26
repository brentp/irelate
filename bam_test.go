package irelate

import (
	"testing"
)

func TestBam(t *testing.T) {
	var g RelatableChannel
	g = BamToRelatable("data/ex.bam")
	for i := range IRelate(CheckRelatedByOverlap, 0, g) {
		if len(i.Related()) != 0 {
			t.Errorf("should not have another relation: %d", len(i.Related()))

		}
		i.SetSource(0)
		i.AddRelated(i)
	}
	for i := range IRelate(CheckRelatedByOverlap, 0, g) {
		if len(i.Related()) == 0 {
			t.Errorf("should have another relation: %d", len(i.Related()))

		}
	}
}
