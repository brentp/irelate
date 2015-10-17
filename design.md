This will serve as documentation for the parallel chromosome-sweep algorithm implemented 
in [irelate](https://github.com/brentp/irelate).

Implementation
--------------

`chrom-sweep` is a means of intersecting sorted sets of intervals without using an
interval tree or bin structure (e.g., the UCSC binning algorithm) whose memory 
footprint scales poorly with input size. As implemented here in `irelate`, there 
are the following steps:

+ create an iterator of parsed intervals (BED/GFF/BAM) from 
  all (sorted) interval sets (sources)
+ merge the intervals via a priority queue, which maintains sort order
+ request an interval from the priority queue and insert it into a cache
+ check for overlaps with the newest interval and all items in the cache (and add
  overlaps to a list of pointers associated with each interval).
+ eject a given interval from the cache when it does not intersect the newest
  interval to the cache and send that interval to the caller if it was a "query" interval.

An assumption here is that as soon as an interval in the cache does not overlap the in-coming
interval, then it can not be related to any later intervals that would come in to the cache.
This holds true since the intervals are sorted by chromosome and then by start coordinate. 
It also means that we can test, not only for overlaps, but for K-nearest neighbors or 
for intervals within a certain distance. For this reason the "overlaps" function can be 
specified by the user making the implementation quite flexible.

Limitations
-----------

This algorithm is very fast, as evidenced by the [bedtools2 -sorted behavior](http://bedtools.readthedocs.org/en/latest/#performance)
but it suffers from 3 major problems:

1. It relies on having all chromosomes in the same order. This is especially onerous since
   VCFs coming out of GATK have order of `1,2,...21,X,Y,MT` while most other sorted files
   would put MT before X and Y. Of course sorting the numeric chromosomes as characters or
   integers also result in different sort orders.

2. It parses many unneeded intervals. Given a sparse query, for example, variants from a few
   target genes, and dense databases of whole-genome coverage, the chromosome sweep algorithm
   will have to parse *every interval* in the whole-genome databases, even though the areas of
   interest are comprised by less than 1% of the regions in the file. In short, sparse queries
   with dense databases are a bad-case for chrom-sweep.

3. Due to the serial nature, it is not possible to parallelize the intersections.

Parallel Chromosome Sweep
-------------------------

To address these shortcomings, we have developed the parallel chrom-sweep algorithm.
It relies on the algorithm described above, but operates on chunks of the query in
parallel. It does this by parsing the query into an array. It breaks off into a new
array and sends the current one to be processed when one of these is true:

+ the array contains the number of intervals requested by the user (the chunk-size) 
+ a chromosome change is detected
+ a gap (current start minus previous end) of a user-specified size is seen.

The chunk-size helps to send reasonably even amounts of work to the user in support
of efficient load balancing (i.e., to avoid task divergence). The gap cutoff avoids 
putting distant query intervals together in the same array; this helps to parse fewer 
database intervals.

Once an array is complete, it is sent off for the chrom-sweep in parallel as a new array
accumulates; the bounds of the intervals it contains are determined and
those are the basis for a tabix request to each database (or any indexed query). Those
requested regions result in streams of intervals that are sent, along with the query array to
chrom-sweep. This means that only the query chunk is in memory and the database intervals
are retreived from their iterators. This parallelizes quite well up to about a dozen processes
because multiple chromosome-sweeps can be operating as the arrays accumulate. One difficulty
is that we want the output to be sorted; consequently, even though each chunk may finish in any 
order, we must restore sorted order to send the intersections back to the caller. 
This is likely the reason that we seem to asymptote in speed at about 10-12 processes.

Implementation
--------------

The concepts described above, with some modifications to improve parallelization and
to deal with reality are implemented in the [go programming language](https://golang.org) here in [parallel.go](https://github.com/brentp/irelate/blob/master/parallel.go).

The function signature is essentially:

```go
func PIRelate(arraySize int, maxGap int, qstream interfaces.RelatableIterator, dbs ...interfaces.Queryable) interfaces.RelatableChannel {
```

where:

+ arraySize is the requested chunk size
+ maxGap will start a new chunk if a gap that size is seen
+ qstram is an iterable of query intervals
+ dbs... is any number of databases that are "queryable" (see below).
+ and the function returns a channel of intervals (RelatableChannel) where each interval has a list of pointers to database intervals that overlap it.

Queryable is a golang interface. It is:

```go
// Queryable allows querying by genomic position. Anything that meets this interface
// can be used in irelate.
type Queryable interface {
	Query(region IPosition) (RelatableIterator, error)
}
```

and a `RelatableIterator` must have these methods:

```go
// RelatableIterator provides a method to iterate over Relatables
type RelatableIterator interface {
	Next() (Relatable, error)
	Close() error
}
```

So, we are not bound to tabix or any particular file format, we simply have to write go code
for any particular file format that implements those interfaces.

Using the same `golang` concept of interfaces, we can support VCF, BAM, GFF, etc intervals as long
as a parser can generate `IPositions`--structs that have these methods available:

```go
type IPosition interface {
	Chrom() string
	Start() uint32
	End() uint32
}
```

It's easy to actually implement these 3 methods for any genomic interval so adding additional file
formats is trivial; we arent tied to any particular set.
