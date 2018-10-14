package rowio

import (
	"context"

	"github.com/golang/protobuf/proto"

	"github.com/pkg/errors"
)

var (
	ErrKeyDoesNotExist = errors.New("key does not exist")
)

type RowIO interface {
	Set(ctx context.Context, key []byte, value proto.Message) error
	Get(ctx context.Context, key []byte, value proto.Message) error
	Scan(ctx context.Context, fromKey, toKey []byte, factory Factory, predicate Predicate) Iterator
	Close() error
}
