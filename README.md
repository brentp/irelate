irelate.go
==========

Streaming relation (overlap, distance, KNN) testing of (any number of) sorted files of intervals.

Currently supports BED, BAM, GFF, VCF.

[![GoDoc] (https://godoc.org/github.com/brentp/irelate?status.png)](https://godoc.org/github.com/brentp/irelate)
[![Build Status](https://travis-ci.org/brentp/irelate.svg?branch=master)](https://travis-ci.org/brentp/irelate)
[![Coverage Status](https://coveralls.io/repos/brentp/irelate/badge.svg?branch=master)](https://coveralls.io/r/brentp/irelate?branch=master)

Motivation
----------

We want to relate (e.g. intersect or by distance) sets of intervals. For example, we may want
to report the nearest gene to a set of ChIP-Seq peaks. BEDTools does this extremely well, *irelate*
is an attempt to provide an API so that users can write their own tools with little effort in
[go](https://golang.org).

Design
------

+ data-sources must support the *Relatable* Interface. (we provide parsers for common formats).
+ a user-defined function returns true if 2 *Relatable*'s are related. (only a small number of interval-pairs
  are sent to be tested--this is handled automatically by `IRelate`.). We provide `CheckRelatedByOverlap`
  to perform overlap testing.
+ i.Related() gives access to all of the related intervals (after they are added internally by `IRelate`)
+ the "API" is a for loop

Example
-------

(also see [main/main.go](https://github.com/brentp/irelate/blob/master/main/main.go) which
is similar to bedtools intersect -sorted -sortout -c)

print the number of `b` alignments that overlap an interval in `a`

```go

// CheckRelatedByOverlap returns true if Relatables overlap.
func CheckRelatedByOverlap(a Relatable, b Relatable) bool {
        // note with distance == 0 this just overlap.
        return (b.Start() < a.End()) && (b.Chrom() == a.Chrom())
}

// determine ordering of Relatables.
func Less(a Relatable, b Relatable) bool {
    if a.Chrom() != b.Chrom() {
        return a.Chrom() < b.Chrom()
    }
    return a.Start() < b.Start() // || (a.Start() == b.Start() && a.End() < b.End())
}



// a and b are channels that send Relatables.
a := ScanToRelatable('intervals.bed', IntervalFromBedLine)
b := BamToRelatable('some.bam')
for interval := range IRelate(CheckRelatedByOverlap, 0, Less, a, b) {
    fmt.Fprintf("%s\t%d\t%d\t%d\n", interval.Chrom(), interval.Start(), interval.End(), len(interval.Related()))
}
```

The 2nd argument determines the *query* set of intervals. So,
only intervals from `a` (the 0th) source will be sent from IRelate. If this is set to -1, then
all intervals from all sources will be sent. After this, any number of interval streams
can be passed to `IRelate`

If we only want to count alignments with a given mapping quality, the loop becomes:

```go
for interval := range IRelate(CheckRelatedByOverlap, 0, Less, a, b) {
    n := 0
    for _, b := range interval.Related() {
         // cast to a bam to ge the mapping quality.
         if int(b.(*Bam).Score()) > 20 {
             n += 1
         }
    }
    fmt.Fprintf("%s\t%d\t%d\t%d\n", interval.Chrom(), interval.Start(), interval.End(), n))
}


```

*note* that *any number* of interval sources are supported even though the example is with 2.
We can see the source of each interval with: `interval.Source()`. That value is set automatically in `Merge`.


This is a very simple example, but the point of this is that since the interface is a simple function (as in
CheckRelatedByOverlap) and a for loop, it is easy to create custom applications.

For example, here is the function to relate all intervals within 2KB:
```go
// CheckRelatedBy2KB returns true if intervals are within 2KB.
func CheckRelatedBy2KB(a Relatable, b Relatable) bool {
        distance := uint32(2000)
        // note with distance == 0 this just overlap.
        return (b.Start()-distance < a.End()) && (b.Chrom() == a.Chrom())
}

```

Note that we are guaranteed that b.Start() >= a.Start() so the check is quite
simple.

Relatable
---------

the only interface in *irelated* is:

```go
// Relatable provides all the methods for irelate to function.
// See Interval in interval.go for a class that satisfies this interface.
// Related() likely returns and AddRelated() likely appends to a slice of
// relatables. Note that for performance reasons, Relatable should be implemented
// as a pointer to your data-structure (see Interval).
type Relatable interface {
        Chrom() string
        Start() uint32
        End() uint32
        Related() []Relatable // A slice of related Relatable's filled by IRelate
        AddRelated(Relatable) // Adds to the slice of relatables
        SetSource() uint32    // Internally marks the source (file/stream) of the Relatable
}
```

Performance
-----------

There has been little done in the way of optimizations. Currently, *irelate* is within ~3X of
BEDTools. With a large number of files, it gets closer because *irelate* parses the files
concurrently.

```Shell
$ zless $A | wc -l
1572178
$ zless $B | wc -l
31739

$ time bedtools intersect -sorted -a $A -b $B -u -sortout | wc -l
792339

real 1.486s
user 1.488s
sys  0.048s

$ GOMAXPROCS=2 time ./relate $A $B | wc -l
792339

real 5.214s
user 5.164s
sys  0.144s
```
More benefit is seen when one or more of the files is a BAM file.

