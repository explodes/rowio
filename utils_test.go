package rowio

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

// GENERIC

var (
	anyError = errors.New("any error")
)

func must(t *testing.T, err error) {
	t.Helper()

	assert.NoError(t, err)
}

func errEqual(t *testing.T, expected, err error) {
	t.Helper()

	switch expected {
	case anyError:
		assert.Error(t, expected)
	case nil:
		assert.NoError(t, expected)
	default:
		assert.Equal(t, expected, err)
	}
}

// PROTOBUF

type fakeproto struct {
	marshalErr error
}

func someProto() proto.Message {
	return &fakeproto{}
}

func (f *fakeproto) ProtoMessage()          {}
func (f *fakeproto) Reset()                 {}
func (f *fakeproto) String() string         { return "" }
func (f *fakeproto) Unmarshal([]byte) error { return f.marshalErr }

type meatyproto struct {
	value int64
}

func (m *meatyproto) ProtoMessage()  {}
func (m *meatyproto) Reset()         { *m = meatyproto{} }
func (m *meatyproto) String() string { return "" }
func (m *meatyproto) Unmarshal(b []byte) error {
	value, _ := binary.Varint(b)
	m.value = value
	return nil
}
func (m *meatyproto) Marshal() ([]byte, error) {
	b := make([]byte, 8)
	binary.PutVarint(b, m.value)
	return b, nil
}

// ROWIO

func someKey() []byte {
	return []byte("some-key")
}

// ITERATOR

func bytesIterator(bytes ...[]byte) keyValueIteratorFunc {
	index := 0
	return keyValueIteratorFunc(func() ([]byte, []byte, bool, error) {
		if index >= len(bytes) {
			return nil, nil, false, nil
		}
		index++
		return bytes[index-1], bytes[index-1], index < len(bytes), nil
	})
}

func countIterations(t *testing.T, iter Iterator) int {
	t.Helper()

	count := 0
	for iter.Next() {
		_, _, err := iter.Value()
		if !assert.NoError(t, err) {
			break
		}
		count++
	}
	return count
}

func assertIteration(t *testing.T, iter Iterator, expectedNext bool, expectedKey []byte, expectedValue proto.Message, expectedErr error) {
	t.Helper()

	next := iter.Next()
	key, value, err := iter.Value()

	assert.Equal(t, expectedNext, next)
	assert.Equal(t, expectedKey, key)
	assert.Equal(t, expectedValue, value)
	errEqual(t, expectedErr, err)
}

func assertFuncIteration(t *testing.T, iter *funcIterator, expectedNext bool, expectedKey []byte, expectedValue []byte, expectedErr error) {
	t.Helper()

	next := iter.next()
	key, value, err := iter.value()

	assert.Equal(t, expectedNext, next)
	assert.Equal(t, expectedKey, key)
	assert.Equal(t, expectedValue, value)
	errEqual(t, expectedErr, err)
}

// CONTEXT

const (
	testTimeout = 5 * time.Second
)

func cancelledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func testContext() context.Context {
	ctx, _ := context.WithTimeout(context.Background(), testTimeout)
	return ctx
}
