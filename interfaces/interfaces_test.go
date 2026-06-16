package interfaces

import "testing"

func TestEqualFoldASCII(t *testing.T) {
	cases := []struct {
		s, t string
		want bool
	}{
		{"A", "A", true},
		{"A", "a", true},
		{"a", "A", true},
		{"ACG", "acg", true},
		{"acg", "ACG", true},
		{"ACG", "ACG", true},
		{"ACG", "ACT", false},
		{"ACG", "AC", false},
		{"AC", "ACG", false},
		{"", "", true},
		// Non-letter bytes that differ by 0x20 must not fold.
		// '0' (0x30) ^ 0x10 (0x10) = 0x20, but neither is a letter.
		{"0", "\x10", false},
		// Digits are equal only when identical.
		{"1", "1", true},
		{"1", "2", false},
	}
	for _, tc := range cases {
		if got := equalFoldASCII(tc.s, tc.t); got != tc.want {
			t.Errorf("equalFoldASCII(%q, %q) = %v, want %v", tc.s, tc.t, got, tc.want)
		}
	}
}

type mockRefAlt struct {
	chrom string
	start uint32
	end   uint32
	ref   string
	alt   []string
}

func (m mockRefAlt) Chrom() string { return m.chrom }
func (m mockRefAlt) Start() uint32 { return m.start }
func (m mockRefAlt) End() uint32   { return m.end }
func (m mockRefAlt) Ref() string   { return m.ref }
func (m mockRefAlt) Alt() []string { return m.alt }

func TestSameVariant(t *testing.T) {
	base := mockRefAlt{"chr1", 100, 101, "A", []string{"T"}}

	cases := []struct {
		desc string
		a, b mockRefAlt
		want bool
	}{
		{
			desc: "identical uppercase",
			a:    base,
			b:    base,
			want: true,
		},
		{
			desc: "ref lowercase vs uppercase",
			a:    mockRefAlt{"chr1", 100, 101, "a", []string{"T"}},
			b:    base,
			want: true,
		},
		{
			desc: "alt lowercase vs uppercase",
			a:    mockRefAlt{"chr1", 100, 101, "A", []string{"t"}},
			b:    base,
			want: true,
		},
		{
			desc: "both ref and alt lowercase",
			a:    mockRefAlt{"chr1", 100, 101, "a", []string{"t"}},
			b:    base,
			want: true,
		},
		{
			desc: "different ref",
			a:    mockRefAlt{"chr1", 100, 101, "C", []string{"T"}},
			b:    base,
			want: false,
		},
		{
			desc: "different alt",
			a:    mockRefAlt{"chr1", 100, 101, "A", []string{"G"}},
			b:    base,
			want: false,
		},
		{
			desc: "different position",
			a:    mockRefAlt{"chr1", 200, 201, "A", []string{"T"}},
			b:    base,
			want: false,
		},
		{
			desc: "different chrom",
			a:    mockRefAlt{"chr2", 100, 101, "A", []string{"T"}},
			b:    base,
			want: false,
		},
		{
			desc: "NON_REF on left matches any ref-matching variant",
			a:    mockRefAlt{"chr1", 100, 101, "A", []string{"<NON_REF>"}},
			b:    base,
			want: true,
		},
		{
			desc: "NON_REF on right matches any ref-matching variant",
			a:    base,
			b:    mockRefAlt{"chr1", 100, 101, "A", []string{"<NON_REF>"}},
			want: true,
		},
		{
			desc: "multi-alt: one matching alt (mixed case)",
			a:    mockRefAlt{"chr1", 100, 101, "A", []string{"G", "t"}},
			b:    mockRefAlt{"chr1", 100, 101, "A", []string{"T", "C"}},
			want: true,
		},
		{
			desc: "multi-alt: no matching alt",
			a:    mockRefAlt{"chr1", 100, 101, "A", []string{"G", "C"}},
			b:    mockRefAlt{"chr1", 100, 101, "A", []string{"T"}},
			want: false,
		},
		{
			desc: "empty alt on left",
			a:    mockRefAlt{"chr1", 100, 101, "A", []string{}},
			b:    base,
			want: false,
		},
		{
			desc: "empty alt on right",
			a:    base,
			b:    mockRefAlt{"chr1", 100, 101, "A", []string{}},
			want: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			if got := SameVariant(tc.a, tc.b); got != tc.want {
				t.Errorf("SameVariant: got %v, want %v", got, tc.want)
			}
		})
	}
}
