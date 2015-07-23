package irelate

import (
	"testing"
)

func TestGff(t *testing.T) {
	g1, e := GFFToRelatable("data/ex.gff")
	if e != nil {
		t.Errorf("got error: %s\n", e)
	}
	g2, e := GFFToRelatable("data/ex.gff")
	if e != nil {
		t.Errorf("got error: %s\n", e)
	}
	for i := range IRelate(CheckRelatedByOverlap, 0, LessPrefix, g1, g2) {
		if len(i.Related()) == 0 {
			t.Errorf("should have another relation: %d", len(i.Related()))

		}
		i.SetSource(0)
	}

}
