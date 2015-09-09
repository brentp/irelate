package irelate

import (
	"testing"

	"github.com/brentp/irelate/interfaces"
	"github.com/brentp/irelate/parsers"
	"github.com/brentp/xopen"
)

func TestBam(t *testing.T) {
	var g interfaces.RelatableChannel
	g, _ = Streamer("data/ex.bam", "")
	for i := range IRelate(CheckRelatedByOverlap, 0, Less, g) {
		if len(i.Related()) != 0 {
			t.Errorf("should not have another relation: %d", len(i.Related()))

		}
		i.SetSource(0)
		i.AddRelated(i)
		m := i.(*parsers.Bam).MapQ()
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

	f1, err := xopen.XReader("https://github.com/brentp/irelate/raw/master/data/ex.bam")
	if err != nil {
		t.Errorf("couldn't open remote bam")
	}

	//f2, err := os.Open("data/ex.bam")
	f2, err := xopen.XReader("data/ex.bam")
	if err != nil {
		t.Errorf("couldn't open local bam")
	}

	b1, _ := parsers.BamToRelatable(f1)
	b2, _ := parsers.BamToRelatable(f2)
	for interval := range IRelate(CheckRelatedByOverlap, 0, Less, b1, b2) {
		if len(interval.Related()) == 0 {
			t.Errorf("should not have other relation: %s", interval)

		}
		break
	}
}
