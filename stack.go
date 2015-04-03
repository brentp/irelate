package irelate

import (
	"sync"
)

type Stack struct {
	sync.Mutex
	stack []*Interval
	max   int
}

// NewStack creates a new stack of Intervals.
func NewStack(max int) *Stack {
	return &Stack{
		stack: make([]*Interval, 0, max),
		max:   max,
	}
}

// Borrow a Interval from the stack.
func (p *Stack) Get() *Interval {
	p.Lock()
	if len(p.stack) == 0 {
		p.Unlock()
		r := &Interval{}
		r.related = make([]Relatable, 0, 2)
		return r
	} else {
		r := p.stack[len(p.stack)-1]
		p.stack = p.stack[:len(p.stack)-1]
		p.Unlock()
		return r
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Return returns a Interval to the stack.
func (p *Stack) Put(r *Interval) {
	p.Lock()
	if len(p.stack) < p.max {
		r.related = r.related[:0]
		p.stack = append(p.stack, r)
		p.Unlock()
	} else {
		p.Unlock()
		r.related = nil
	}
}
