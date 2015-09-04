package irelate

import (
	"os"
	"testing"
)

func TestGff(t *testing.T) {

	f1, e := os.Open("data/ex.gff")
	if e != nil {
		t.Errorf("got error: %s\n", e)
	}
	g1, e := GFFToRelatable(f1)
	if e != nil {
		t.Errorf("got error: %s\n", e)
	}

	f2, e := os.Open("data/ex.gff")
	if e != nil {
		t.Errorf("got error: %s\n", e)
	}

	g2, e := GFFToRelatable(f2)
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
