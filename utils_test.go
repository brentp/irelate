package irelate

import (
	"testing"
)

func TestOpenScanFile(t *testing.T) {
	s, fh := OpenScanFile("utils_test.go")

	if !s.Scan() {
		t.Error("should have a scanner")
	}
	fh.Close()
}
