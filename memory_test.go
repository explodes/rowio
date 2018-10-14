package rowio

import "testing"

func TestMemoryRowIO(t *testing.T) {
	testRowIO(t, "MemoryRowIO", NewMemoryRowIO, func(RowIO) error { return nil })
}
