// Streaming relation (overlap, distance, KNN) testing of (any number of) sorted files of intervals.
package interfaces

import "strings"

// RelatableChannel
type RelatableChannel chan Relatable

type RelatableGetter interface {
	Next() Relatable
}

// IPosition allows accessing positional interface for genomic types.
type IPosition interface {
	Chrom() string
	Start() uint32
	End() uint32
}

type RandomGetter interface {
	Get(query IPosition) []IPosition
}

// Interface to get the CIPos and CIEND from a VCF. Returns start, end, ok.
type CIFace interface {
	CIPos() (uint32, uint32, bool)
	CIEnd() (uint32, uint32, bool)
}

// A RandomChannel accepts a single IPosition and returns a slice of all overlapping positions.
type RandomChannel interface {
	Relate(chan IPosition) chan []IPosition
}

// Relatable provides all the methods for irelate to function.
// See Interval in interval.go for a class that satisfies this interface.
// Related() likely returns and AddRelated() likely appends to a slice of
// relatables. Note that for performance reasons, Relatable should be implemented
// as a pointer to your data-structure (see Interval).
type Relatable interface {
	IPosition
	Related() []Relatable // A slice of related Relatable's filled by IRelate
	AddRelated(Relatable) // Adds to the slice of relatables
	Source() uint32
	SetSource(source uint32)
}

// Info must implement stuff to get info out of a variant info field.
type Info interface {
	Get(key string) (interface{}, error)
	Set(key string, val interface{}) error
	Delete(key string)
	Keys() []string
	String() string
	Bytes() []byte
}

// IVariant must implement IPosition as well as Ref, Alt, and Inof() methods for genetic variants
type IVariant interface {
	IPosition
	Ref() string
	Alt() []string
	Info() Info
	Id() string
	String() string
}

func SameChrom(a, b string) bool {
	if a == b {
		return true
	}
	return StripChr(a) == StripChr(b)
}

func StripChr(c string) string {
	if strings.HasPrefix(c, "chr") {
		return c[3:]
	}
	return c
}

func SamePosition(a, b IPosition) bool {
	return a.Start() == b.Start() && a.End() == b.End() && SameChrom(a.Chrom(), b.Chrom())
}

func OverlapsPosition(a, b IPosition) bool {
	return (b.Start() < a.End() && b.End() > a.Start()) && SameChrom(a.Chrom(), b.Chrom())
}

func SameVariant(a, b IVariant) bool {
	if !SamePosition(a, b) || a.Ref() != b.Ref() {
		return false
	}
	for _, aalt := range a.Alt() {
		for _, balt := range b.Alt() {
			if aalt == balt {
				return true
			}
		}
	}
	return false
}

func Same(a, b IPosition, strict bool) bool {
	// strict only applies if both are IVariants, otherwise, we just check for overlap.
	if !strict {
		return OverlapsPosition(a, b)
	}
	if av, ok := a.(IVariant); ok {
		if bv, ok := b.(IVariant); ok {
			if strict {
				return SameVariant(av, bv)
			}
		}
		return OverlapsPosition(a, b)
	}
	// at most one of them is a variant, just check overlap.
	return OverlapsPosition(a, b)
}
