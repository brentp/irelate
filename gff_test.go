package irelate

import (
	"testing"
)

func TestGff(t *testing.T) {
	g1 := GFFToRelatable("data/ex.gff")
	g2 := GFFToRelatable("data/ex.gff")
	for i := range IRelate(CheckRelatedByOverlap, 0, LessPrefix, g1, g2) {
		if len(i.Related()) == 0 {
			t.Errorf("should have another relation: %d", len(i.Related()))

		}
		i.SetSource(0)
	}

}
