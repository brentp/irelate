package parsers_test

import (
	"testing"

	"github.com/brentp/irelate/parsers"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type BamSuite struct{}

var _ = Suite(&BamSuite{})

type ip struct {
	chrom string
	start uint32
	end   uint32
}

func (p ip) Chrom() string {
	return p.chrom
}
func (p ip) Start() uint32 {
	return p.start
}
func (p ip) End() uint32 {
	return p.end
}

func (s *BamSuite) TestBamQuery(c *C) {
	b, err := parsers.NewBamQueryable("../data/ex.bam")
	c.Assert(err, IsNil)
	reg := ip{"chr1", 3048448, 3049340}

	q, err := b.Query(reg)
	c.Assert(err, IsNil)

	j := 0
	for rec, e := q.Next(); e == nil; rec, e = q.Next() {
		c.Assert(rec.Chrom(), Equals, "chr1")
		j++
	}
	c.Assert(j, Equals, 2)

}
