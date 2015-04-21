package irelate

import (
	"container/heap"
	"fmt"
	"strings"
	"testing"
)

func Example() {
	var a, b Relatable
	a = &Interval{chrom: "chr1", start: 1234, end: 5678,
		fields: strings.Split("chr1\t1234\t5678", "\t"), source: 1}
	b = &Interval{chrom: "chr1", start: 9234, end: 9678,
		fields: strings.Split("chr1\t9234\t9678", "\t"), source: 0}
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
		fields: strings.Split("chr1\t1234\t5678", "\t"), source: 1}
	b = &Interval{chrom: "chr1", start: 9234, end: 9678,
		fields: strings.Split("chr1\t9234\t9678", "\t"), source: 0}

	if len(a.Related()) != 0 {
		t.Error("a.related should be empty")
	}
	if len(b.Related()) != 0 {
		t.Error("b.related should be empty")
	}
	relate(a, b, false, -1)
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

	relate(a, b, false, int(a.Source()))
	if len(b.Related()) != 0 {
		t.Error("b shouldn't get a added")
	}
	if len(a.Related()) != 1 {
		t.Error("a should have b added")
	}

	relate(a, b, false, int(b.Source()))
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

	q := make(relatableQueue, 0)
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

	merged := Merge(nextc(), nexta(), nextb())

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

	q := make(relatableQueue, 0)
	heap.Push(&q, a)
	heap.Push(&q, b)
	if len(q) != 2 {
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
