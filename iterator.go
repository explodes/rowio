package rowio

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

var (
	ErrIteratorDone = errors.New("iterator complete")
)

type Iterator interface {
	Next() bool
	Value() (key []byte, value proto.Message, err error)
}

type errIterator struct {
	err error
}

func newErrorIterator(err error) Iterator                   { return errIterator{err: err} }
func (e errIterator) Value() ([]byte, proto.Message, error) { return nil, nil, e.err }
func (e errIterator) Next() bool                            { return false }

type keyValueIteratorFunc func() (key []byte, value []byte, more bool, err error)

type funcIterator struct {
	ctx  context.Context
	f    keyValueIteratorFunc
	err  error
	done bool
}

func newFuncIterator(ctx context.Context, f keyValueIteratorFunc) *funcIterator {
	return &funcIterator{
		ctx: ctx,
		f:   f,
	}
}

func (f *funcIterator) value() (key []byte, value []byte, err error) {
	if f.err != nil {
		return nil, nil, f.err
	}
	if f.done {
		return nil, nil, ErrIteratorDone
	}
	select {
	case <-f.ctx.Done():
		f.done = true
		f.err = f.ctx.Err()
		return nil, nil, f.err
	default:
	}
	key, value, more, err := f.f()
	if !more {
		f.done = true
	}
	if err != nil {
		f.done = true
		f.err = err
	}
	return key, value, f.err
}

func (f *funcIterator) next() bool {
	return !f.done
}

type predicateIterator struct {
	baseIterator *funcIterator
	predicate    Predicate
	factory      Factory
	err          error
	done         bool
	key          []byte
	value        proto.Message
}

func newPredicateIterator(ctx context.Context, predicate Predicate, factory Factory, f keyValueIteratorFunc) Iterator {
	iter := &predicateIterator{
		baseIterator: newFuncIterator(ctx, f),
		predicate:    predicate,
		factory:      factory,
	}
	iter.getNext()
	return iter
}

func (p *predicateIterator) getNext() {
	if p.done || p.err != nil {
		return
	}
	for p.baseIterator.next() {
		key, next, err := p.baseIterator.value()
		if err != nil {
			p.setErr(err)
			return
		}
		pb, err := p.factory(next)
		if err != nil {
			p.setErr(err)
			return
		}
		if p.predicate(pb) {
			p.key = key
			p.value = pb
			return
		}
	}
	p.setErr(p.baseIterator.err)
}

func (p *predicateIterator) setErr(err error) {
	p.done = true
	p.err = err
	p.value = nil
}

func (p *predicateIterator) Value() ([]byte, proto.Message, error) {
	if p.err != nil {
		return nil, nil, p.err
	}
	if p.done {
		return nil, nil, ErrIteratorDone
	}
	key := p.key
	value := p.value
	p.getNext()
	return key, value, p.err
}

func (p *predicateIterator) Next() bool {
	return !p.done
}
