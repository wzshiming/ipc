package ring

import (
	"encoding/binary"
	"testing"
)

const testCap = 64 << 10

func newRegion(t testing.TB, capacity int) []byte {
	t.Helper()
	region := make([]byte, Size(capacity))
	if err := Format(region, capacity); err != nil {
		t.Fatalf("Format(%d): %v", capacity, err)
	}
	return region
}

func TestHeaderFieldsOnDistinctCacheLines(t *testing.T) {
	if HeaderSize != 4096 {
		t.Fatalf("HeaderSize = %d, want 4096", HeaderSize)
	}
	offs := map[string]int{
		"meta":           offMeta,
		"head":           offHead,
		"tail":           offTail,
		"consumerParked": offConsumerParked,
		"producerParked": offProducerParked,
	}
	lines := map[int]string{}
	for name, off := range offs {
		if off%128 != 0 {
			t.Errorf("%s offset %d is not 128-byte aligned", name, off)
		}
		if off+128 > HeaderSize {
			t.Errorf("%s offset %d leaves the header", name, off)
		}
		if other, ok := lines[off/128]; ok {
			t.Errorf("%s shares cache line %d with %s", name, off/128, other)
		}
		lines[off/128] = name
	}
}

func TestSizeIsHeaderPlusCapacity(t *testing.T) {
	if got := Size(testCap); got != HeaderSize+testCap {
		t.Fatalf("Size(%d) = %d, want %d", testCap, got, HeaderSize+testCap)
	}
	if got := MaxRecord(testCap); got != testCap/2-8 {
		t.Fatalf("MaxRecord(%d) = %d, want %d", testCap, got, testCap/2-8)
	}
}

func TestFormatThenVerifyReturnsCapacity(t *testing.T) {
	region := newRegion(t, testCap)
	got, err := Verify(region)
	if err != nil || got != testCap {
		t.Fatalf("Verify = %d, %v; want %d, nil", got, err, testCap)
	}
}

func TestFormatRejectsBadCapacityAndShortRegion(t *testing.T) {
	for _, c := range []int{0, -testCap, 32 << 10, 64<<10 + 8, 3 << 16} {
		if err := Format(make([]byte, HeaderSize+64<<10), c); err == nil {
			t.Errorf("Format(capacity=%d) succeeded, want error", c)
		}
	}
	if err := Format(make([]byte, Size(testCap)-1), testCap); err == nil {
		t.Error("Format on a short region succeeded, want error")
	}
}

func TestVerifyRejectsTamperedHeader(t *testing.T) {
	cases := []struct {
		name   string
		tamper func(r []byte)
	}{
		{"magic", func(r []byte) { r[offMeta] ^= 0xff }},
		{"version", func(r []byte) { binary.LittleEndian.PutUint32(r[offMeta+4:], 99) }},
		{"capacity not power of two", func(r []byte) { binary.LittleEndian.PutUint64(r[offMeta+8:], testCap+8) }},
		{"capacity too small", func(r []byte) { binary.LittleEndian.PutUint64(r[offMeta+8:], 32<<10) }},
		{"capacity too large", func(r []byte) { binary.LittleEndian.PutUint64(r[offMeta+8:], 1<<40) }},
		{"capacity beyond region", func(r []byte) { binary.LittleEndian.PutUint64(r[offMeta+8:], 2*testCap) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			region := newRegion(t, testCap)
			tc.tamper(region)
			if c, err := Verify(region); err == nil {
				t.Fatalf("Verify accepted tampered header (capacity %d)", c)
			}
		})
	}
	if _, err := Verify(make([]byte, HeaderSize-1)); err == nil {
		t.Error("Verify accepted a region shorter than the header")
	}
}
