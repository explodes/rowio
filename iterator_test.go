package rowio

import (
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestErrIterator(t *testing.T) {
	err := errors.New("test")
	iter := newErrorIterator(err)

	assertIteration(t, iter, false, nil, nil, err)
}

func TestFuncIterator(t *testing.T) {
	a := []byte{12}
	b := []byte{34}
	c := []byte{56}
	f := bytesIterator(a, b, c)
	iter := newFuncIterator(testContext(), f)

	assertFuncIteration(t, iter, true, a, a, nil)
	assertFuncIteration(t, iter, true, b, b, nil)
	assertFuncIteration(t, iter, true, c, c, nil)
	assertFuncIteration(t, iter, false, nil, nil, ErrIteratorDone)
}

func TestFuncIterator_Error(t *testing.T) {
	expectedErr := errors.New("expected")
	f := keyValueIteratorFunc(func() (key []byte, value []byte, more bool, err error) {
		return nil, nil, false, expectedErr
	})
	iter := newFuncIterator(testContext(), f)

	assertFuncIteration(t, iter, true, nil, nil, expectedErr)
	assertFuncIteration(t, iter, false, nil, nil, expectedErr)
}

func TestFuncIterator_ContextCanceled(t *testing.T) {
	ctx := cancelledContext()
	f := bytesIterator()
	iter := newFuncIterator(ctx, f)

	aNext := iter.next()
	aKey, aValue, aErr := iter.value()
	assert.True(t, aNext)
	assert.Nil(t, aKey)
	assert.Nil(t, aValue)
	assert.EqualError(t, aErr, "context canceled")

}

func TestPredicateIterator(t *testing.T) {
	fakePb := someProto()
	factory := Factory(func(b []byte) (proto.Message, error) {
		err := proto.Unmarshal(b, fakePb)
		return fakePb, err
	})
	predicate := Predicate(func(proto.Message) bool { return true })
	a := []byte{12}
	b := []byte{34}
	c := []byte{56}
	f := bytesIterator(a, b, c)
	iter := newPredicateIterator(testContext(), predicate, factory, f)

	assertIteration(t, iter, true, a, fakePb, nil)
	assertIteration(t, iter, true, b, fakePb, nil)
	assertIteration(t, iter, true, c, fakePb, nil)
	assertIteration(t, iter, false, nil, proto.Message(nil), nil)
}

func TestPredicateIterator_error(t *testing.T) {
	var (
		expectedErr = errors.New("expected")
	)
	fakePb := someProto()
	factoryIndex := 0
	factory := Factory(func(b []byte) (proto.Message, error) {
		if factoryIndex > 1 {
			return nil, expectedErr
		}
		factoryIndex++
		err := proto.Unmarshal(b, fakePb)
		return fakePb, err
	})
	predicate := Predicate(func(proto.Message) bool { return true })
	a := []byte{12}
	b := []byte{34}
	c := []byte{56}
	f := bytesIterator(a, b, c)
	iter := newPredicateIterator(testContext(), predicate, factory, f)

	assertIteration(t, iter, true, a, fakePb, nil)
	assertIteration(t, iter, true, b, fakePb, nil)
	assertIteration(t, iter, false, nil, proto.Message(nil), expectedErr)
	assertIteration(t, iter, false, nil, proto.Message(nil), expectedErr)
}
