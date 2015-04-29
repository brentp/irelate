package irelate

import (
	"testing"
)

func TestBam(t *testing.T) {
	var g RelatableChannel
	g = BamToRelatable("data/ex.bam")
	for i := range IRelate(CheckRelatedByOverlap, 0, Less, g) {
		if len(i.Related()) != 0 {
			t.Errorf("should not have another relation: %d", len(i.Related()))

		}
		i.SetSource(0)
		i.AddRelated(i)
		m := i.(*Bam).MapQ()
		if !(0 <= m && m <= 60) {
			t.Errorf("bad mapping quality: %d", m)
		}
	}
	for i := range IRelate(CheckOverlapPrefix, 0, LessPrefix, g) {
		if len(i.Related()) == 0 {
			t.Errorf("should have another relation: %d", len(i.Related()))

		}
	}
}

func TestRemoteBam(t *testing.T) {
	b1 := BamToRelatable("https://github.com/brentp/irelate/raw/master/data/ex.bam")
	b2 := BamToRelatable("data/ex.bam")
	for interval := range IRelate(CheckRelatedByOverlap, 0, Less, b1, b2) {
		if len(interval.Related()) == 0 {
			t.Errorf("should not have other relation: %s", interval)

		}
		break
	}
}
