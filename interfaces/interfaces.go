// Streaming relation (overlap, distance, KNN) testing of (any number of) sorted files of intervals.
package interfaces

import "strings"

type IPosition interface {
	Chrom() string
	Start() uint32
	End() uint32
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
	Source() uint32       // Internally marks the source (file/stream) of the Relatable
	SetSource(source uint32)
}

type Info interface {
	Get(key string) (interface{}, error)
	Set(key string, val interface{}) error
	Delete(key string)
	Keys() []string
	String() string
}

type IVariant interface {
	IPosition
	Ref() string
	Alt() []string
	Info() Info
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
	return (b.Start() < a.End() && b.End() >= a.Start()) && SameChrom(a.Chrom(), b.Chrom())
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
	if av, ok := a.(IVariant); ok {
		if bv, ok := b.(IVariant); ok {
			if strict {
				return SameVariant(av, bv)
			}
			return OverlapsPosition(a, b)
		}
		return OverlapsPosition(a, b)
	}
	// at most one of them is a variant, just check overlap.
	return OverlapsPosition(a, b)
}
