package rowio

import (
	"fmt"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
)

func testRowIO(t *testing.T, name string, factory func() (RowIO, error), cleanup func(RowIO) error) {
	t.Helper()
	t.Parallel()

	tests := []struct {
		name string
		f    func(t *testing.T, db RowIO)
	}{
		{"setAndGet", test_SetGet},
		{"scan", test_Scan},
	}

	for _, test := range tests {
		test := test
		t.Run(fmt.Sprintf("%s/%s", name, test.name), func(t *testing.T) {
			t.Parallel()
			db, err := factory()
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				err := db.Close()
				if err != nil {
					t.Errorf("error during close: %v", err)
				}
			}()
			defer func() {
				err := cleanup(db)
				if err != nil {
					t.Errorf("error during cleanup: %v", err)
				}
			}()
			test.f(t, db)
		})
	}
}

func test_SetGet(t *testing.T, db RowIO) {
	const largeKeySize = 5
	valueA := &meatyproto{value: 10}
	keyA := []byte{5}
	valueB := &meatyproto{value: 50000}
	keyB := make([]byte, largeKeySize)
	for i := 0; i < largeKeySize; i++ {
		keyB[i] = byte(i)
	}
	must(t, db.Set(testContext(), keyA, valueA))
	must(t, db.Set(testContext(), keyB, valueB))

	outA := &meatyproto{}
	must(t, db.Get(testContext(), keyA, outA))

	outB := &meatyproto{}
	must(t, db.Get(testContext(), keyB, outB))

	assert.Equal(t, *valueA, *outA)
	assert.Equal(t, *valueB, *outB)
}

func test_Scan(t *testing.T, db RowIO) {
	first := []byte{0}
	beforeA := []byte{4}
	keyA := []byte{5}
	keyB := []byte{6}
	keyC := []byte{8}
	afterC := []byte{9}
	last := []byte{13}
	must(t, db.Set(testContext(), keyA, someProto()))
	must(t, db.Set(testContext(), keyB, someProto()))
	must(t, db.Set(testContext(), keyC, someProto()))

	any := someProto()
	factory := func(b []byte) (proto.Message, error) { return any, proto.Unmarshal(b, any) }
	predicate := Predicate(func(pb proto.Message) bool { return true })

	tests := []struct {
		name          string
		from, to      []byte
		expectedCount int
	}{
		{"exact", keyA, keyC, 3},
		{"before", first, beforeA, 0},
		{"after", afterC, last, 0},
		{"start", keyA, keyA, 1},
		{"middle", keyB, keyB, 1},
		{"end", keyC, keyC, 1},
		{"partial", keyA, keyB, 2},
		{"partialSkip", keyB, keyC, 2},
		{"borderStart", first, keyA, 1},
		{"borderEnd", keyC, last, 1},
		{"coveringOuter", first, last, 3},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			iter := db.Scan(testContext(), test.from, test.to, factory, predicate)
			assert.Equal(t, test.expectedCount, countIterations(t, iter))
		})

	}

}
