package irelate

import (
	"testing"

	"github.com/brentp/irelate/interfaces"
	"github.com/brentp/irelate/parsers"
	"github.com/brentp/vcfgo"
	"github.com/brentp/xopen"
)

func TestVCF(t *testing.T) {
	r1, err := xopen.Ropen("https://raw.githubusercontent.com/brentp/vcfgo/master/examples/test.query.vcf")
	if err != nil {
		t.Error("couldn't open remote file")
	}
	r2, err := xopen.Ropen("https://raw.githubusercontent.com/brentp/vcfgo/master/examples/test.query.vcf")
	if err != nil {
		t.Error("couldn't open remote file")
	}

	g1 := parsers.Vopen(r1, nil)
	g2 := parsers.Vopen(r2, nil)

	v1 := parsers.StreamVCF(g1)
	v2 := parsers.StreamVCF(g2)
	for i := range IRelate(CheckRelatedByOverlap, 0, Less, v1, v2) {
		if len(i.Related()) == 0 {
			t.Errorf("should have another relation: %d", len(i.Related()))

		}
		i.SetSource(0)
	}

}

var v1 = vcfgo.Variant{
	Chromosome: "chr1",
	Pos:        uint64(234),
	Id_:        "id",
	Reference:  "A",
	Alternate:  []string{"T", "G"},
	Quality:    float32(555.5),
	Filter:     "PASS",
	Info_:      vcfgo.NewInfoByte([]byte("DP=35"), nil),
}

func TestNewVariant(t *testing.T) {

	iv := parsers.NewVariant(&v1, uint32(1), []interfaces.Relatable{})
	if len(iv.Related()) != 0 {
		t.Errorf("shouldn't have any relateds")
	}
	if iv.Source() != uint32(1) {
		t.Errorf("shouldn't have source of 1")
	}

}
