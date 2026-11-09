package control

import (
	"errors"
	"testing"
)

func TestErrUnsupportedIsStdlibErrUnsupported(t *testing.T) {
	if !errors.Is(ErrUnsupported, errors.ErrUnsupported) {
		t.Fatalf("errors.Is(%v, errors.ErrUnsupported) = false", ErrUnsupported)
	}
}
