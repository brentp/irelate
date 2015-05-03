package irelate

import (
	"testing"

	"github.com/brentp/vcfgo"
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

var v1 = &vcfgo.Variant{
	Chromosome: "chr1",
	Pos:        uint64(234),
	Id:         "id",
	Ref:        "A",
	Alt:        []string{"T", "G"},
	Quality:    float32(555.5),
	Filter:     "PASS",
	Info: map[string]interface{}{
		"DP":      uint32(35),
		"__order": []string{"DP"},
	},
}

func TestNewVariant(t *testing.T) {

	iv := NewVariant(v1, uint32(1), []Relatable{})
	if len(iv.Related()) != 0 {
		t.Errorf("shouldn't have any relateds")
	}
	if iv.Source() != uint32(1) {
		t.Errorf("shouldn't have source of 1")
	}

}
