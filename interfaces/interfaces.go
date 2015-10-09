// Streaming relation (overlap, distance, KNN) testing of (any number of) sorted files of intervals.
package interfaces

import "strings"

// RelatableChannel
type RelatableChannel chan Relatable

type RelatableIterator interface {
	Next() (Relatable, error)
	Close() error
}

// Queryable allows querying by genomic position. Anything that meets this interface
// can be used in irelate.
type Queryable interface {
	Query(region IPosition) (RelatableIterator, error)
}

// IPosition allows accessing positional interface for genomic types.
type IPosition interface {
	Chrom() string
	Start() uint32
	End() uint32
}

// Interface to get the CIPos and CIEND from a VCF. Returns start, end, ok.
type CIFace interface {
	CIPos() (uint32, uint32, bool)
	CIEnd() (uint32, uint32, bool)
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

type SIPosition interface {
	IPosition
	String() string
}

// trun an IPosition into an IRelatalbe
type RelWrap struct {
	related []Relatable
	source  uint32
}

func (w *RelWrap) Source() uint32 {
	return w.source
}
func (w *RelWrap) SetSource(s uint32) {
	w.source = s
}
func (w *RelWrap) AddRelated(r Relatable) {
	if w.related == nil {
		w.related = make([]Relatable, 0, 2)
	}
	w.related = append(w.related, r)
}

func (w *RelWrap) Related() []Relatable {
	return w.related
}

type VarWrap struct {
	IVariant
	*RelWrap
}
type PosWrap struct {
	SIPosition
	*RelWrap
}

// turn a position thingy into a Relatable
func AsRelatable(p SIPosition) Relatable {
	if v, ok := p.(IVariant); ok {
		return VarWrap{IVariant: v, RelWrap: &RelWrap{}}
	}
	return &PosWrap{SIPosition: p, RelWrap: &RelWrap{}}
}

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

func AsIPosition(chrom string, start int, end int) IPosition {
	return ip{chrom, uint32(start), uint32(end)}
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
