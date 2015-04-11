package irelate

import (
	"testing"
)

func TestGff(t *testing.T) {
	var g RelatableChannel
	g = GFFToRelatable("data/ex.gff")
	for i := range IRelate(CheckRelatedByOverlap, true, 0, g) {
		if len(i.Related()) != 1 {
			t.Errorf("should have another relation: %d", len(i.Related()))

		}
		i.SetSource(0)
	}

}
