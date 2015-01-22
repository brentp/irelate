package irelate

import (
	"testing"
)

func TestXopen(t *testing.T) {

	f, err := Xopen("utils_test.go")
	if err != nil {
		t.Error("should be able to open file")
	}

	f.Close()

}

func TestOpenScanFile(t *testing.T) {
	s, fh := OpenScanFile("utils_test.go")

	if !s.Scan() {
		t.Error("should have a scanner")
	}
	fh.Close()
}
