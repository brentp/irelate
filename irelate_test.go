package irelate

import (
	"container/heap"
	"fmt"
	"strings"
	"testing"
)

const data = `chr1_gl000191_random	50281	52281
chr1_gl000192_random	55678	79327
chr1_gl000192_random	55678	79327
chr1_gl000192_random	79326	79327
chr1_gl000192_random	79327	81327
chr2	38813	41607
chr2	38813	41627
chr2	38813	46588
chr2	41607	41627
chr2	41627	45439
chr2	45439	46385
chr2	45439	46588
chr2	46385	46588
chr2	46587	46588
chr2	46588	48588`

func TestFunctional(t *testing.T) {
	dats := strings.Split(data, "\n")
	mk := func(dats []string) RelatableChannel {
		ch := make(RelatableChannel, 4)
		go func() {
			for _, d := range dats {
				i := IntervalFromBedLine(d)
				ch <- i
			}
			close(ch)
		}()
		return ch
	}
	a := mk(dats)
	b := mk(dats)

	seen2 := false
	highest := uint32(0)
	for v := range IRelate(CheckRelatedByOverlap, 0, Less, a, b) {
		if seen2 {
			if v.Chrom() != "chr2" || v.Start() < highest {
				t.Error("out of order")
			}
			highest = v.Start()
		}
		seen2 = v.Chrom() == "chr2"
	}
}

func Example() {
	var a, b Relatable
	a = &Interval{chrom: "chr1", start: 1234, end: 5678,
		Fields: strings.Split("chr1\t1234\t5678", "\t"), source: 1}
	b = &Interval{chrom: "chr1", start: 9234, end: 9678,
		Fields: strings.Split("chr1\t9234\t9678", "\t"), source: 0}
	fmt.Printf("%s\t%d\t%d\n", a.Chrom(), a.Start(), a.End())
	fmt.Printf("%s\t%d\t%d\n", b.Chrom(), b.Start(), b.End())
	fmt.Println(CheckRelatedByOverlap(a, b))

	fmt.Println("\nIt's possible to define your own check_related function")
	fmt.Println("where b is always >= a by position.")

	CheckRelatedByDistance := func(a Relatable, b Relatable) bool {
		return b.Start()-a.End() < 5000
	}

	fmt.Println(CheckRelatedByDistance(a, b))

	// Output:
	// chr1	1234	5678
	// chr1	9234	9678
	// false
	//
	// It's possible to define your own check_related function
	// where b is always >= a by position.
	// true
}

func TestRelate(t *testing.T) {
	var a, b Relatable
	a = &Interval{chrom: "chr1", start: 1234, end: 5678,
		Fields: strings.Split("chr1\t1234\t5678", "\t"), source: 1}
	b = &Interval{chrom: "chr1", start: 9234, end: 9678,
		Fields: strings.Split("chr1\t9234\t9678", "\t"), source: 0}

	if len(a.Related()) != 0 {
		t.Error("a.related should be empty")
	}
	if len(b.Related()) != 0 {
		t.Error("b.related should be empty")
	}
	relate(a, b, -1)
	if len(a.Related()) != 1 {
		t.Error("a.related should have 1 interval")
	}
	if len(b.Related()) != 1 {
		t.Error("b.related should have 1 interval")
	}

	if a.Related()[0] != b {
		t.Error("a.related[0] should be b")
	}
	a.(*Interval).related = a.Related()[:0]
	b.(*Interval).related = b.Related()[:0]

	relate(a, b, int(a.Source()))
	if len(b.Related()) != 0 {
		t.Error("b shouldn't get a added")
	}
	if len(a.Related()) != 1 {
		t.Error("a should have b added")
	}

	relate(a, b, int(b.Source()))
	if len(b.Related()) != 1 {
		t.Error("b should get a added")
	}
	if len(a.Related()) != 1 {
		t.Error("a should get b re-added")
	}
	bf := filter(b.Related(), 0)
	if len(bf) != len(b.Related()) {
		t.Error("b shouldn't have been filtered")
	}
	bf[0] = nil
	if len(filter(bf, 1)) == len(b.Related()) {
		t.Error("should have been filtered")
	}

}

func TestQ(t *testing.T) {
	a := &Interval{chrom: "chr1", start: 1234, end: 5678}
	b := &Interval{chrom: "chr1", start: 9234, end: 9678}
	c := &Interval{chrom: "chr2", start: 9234, end: 9678}

	q := relatableQueue{rels: make([]Relatable, 0), less: Less}
	heap.Init(&q)
	heap.Push(&q, b)
	heap.Push(&q, a)
	heap.Push(&q, c)

	first := heap.Pop(&q)
	if first != a {
		t.Error("first interval off q should be a")
	}
	second := heap.Pop(&q)
	if second != b {
		t.Error("2nd interval off q should be b")
	}
	third := heap.Pop(&q)
	if third != c {
		t.Error("third interval off q should be c")
	}
	if heap.Pop(&q) != nil {
		t.Error("empty q should return nil")
	}
}

func TestMerge(t *testing.T) {
	var a, b, c Relatable
	a = &Interval{chrom: "chr1", start: 1234, end: 5678}
	b = &Interval{chrom: "chr1", start: 9234, end: 9678}
	c = &Interval{chrom: "chr2", start: 9234, end: 9678}

	nexta := func() RelatableChannel {
		ch := make(chan Relatable, 2)
		go func() {
			for ai := 0; ai < 1; ai += 1 {
				ch <- a
			}
			close(ch)
		}()
		return ch
	}
	nextb := func() RelatableChannel {
		ch := make(chan Relatable)
		go func() {
			for bi := 0; bi < 1; bi += 1 {
				ch <- b
			}
			close(ch)
		}()
		return ch
	}

	nextc := func() RelatableChannel {
		ch := make(chan Relatable)
		go func() {
			for ci := 0; ci < 2; ci += 1 {
				ch <- c
			}
			close(ch)
		}()
		return ch
	}

	merged := Merge(Less, 0, nextc(), nexta(), nextb())

	first := <-merged
	if first != a {
		t.Error("first interval off merge should be a")
	}
	second := <-merged
	if second != b {
		t.Error("2nd interval off merge should be b")
	}
	third := <-merged
	if third != c {
		t.Error("third interval off merge should be c", third)
	}
	fourth := <-merged
	if fourth != c {
		t.Error("fourth interval off merge should be c")
	}

	if <-merged != nil {
		t.Error("empty Merge should return nil")
	}

}

func TestLessRelatableQueue(t *testing.T) {
	var a, b Relatable
	a = &Interval{chrom: "chr1", start: 3077640, end: 3080640, source: 0}
	b = &Interval{chrom: "chr1", start: 2985741, end: 3355185, source: 1}

	q := relatableQueue{rels: make([]Relatable, 0), less: Less}
	heap.Push(&q, a)
	heap.Push(&q, b)
	if len(q.rels) != 2 {
		t.Error("Q should have lenght 2")
	}
	bb := heap.Pop(&q)
	if bb != b {
		t.Error("popped interval should match b")
	}
}

func TestOverlapCheck(t *testing.T) {
	a := &Interval{chrom: "chr1", start: 3077640, end: 3080640, source: 0}
	b := &Interval{chrom: "chr1", start: 2985741, end: 3355185, source: 1}

	if CheckRelatedByOverlap(a, b) != true {
		t.Error("intervals should overlap")
	}
}

func TestSameChrom(t *testing.T) {
	if !SameChrom("1", "chr1") {
		t.Error("chr1 should == 1")
	}
	if !SameChrom("chr1", "1") {
		t.Error("chr1 should == 1")
	}
	if !SameChrom("chr1", "chr1") {
		t.Error("chr1 should == chr1")
	}
	if !SameChrom("1", "1") {
		t.Error("1 should == 1")
	}
	if SameChrom("1", "2") {
		t.Error("1 should not == 2")
	}
	if SameChrom("2", "1") {
		t.Error("1 should not == 2")
	}
	if SameChrom("chr2", "chr1") {
		t.Error("chr1 should not == chr2")
	}
	if SameChrom("1", "11") {
		t.Error("11 should not == 1")
	}
	if SameChrom("11", "1") {
		t.Error("11 should not == 1")
	}
}
