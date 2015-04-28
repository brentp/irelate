package irelate

import (
	"testing"
)

func TestVCF(t *testing.T) {
	g1 := Vopen("https://raw.githubusercontent.com/brentp/vcfgo/master/examples/test.query.vcf")
	g2 := Vopen("https://raw.githubusercontent.com/brentp/vcfgo/master/examples/test.query.vcf")

	v1 := StreamVCF(g1)
	v2 := StreamVCF(g2)
	for i := range IRelate(CheckRelatedByOverlap, 0, Less, v1, v2) {
		if len(i.Related()) == 0 {
			t.Errorf("should have another relation: %d", len(i.Related()))

		}
		i.SetSource(0)
	}

}
