package rowio

import (
	"bytes"
	"context"
	"os"
	"time"

	"github.com/boltdb/bolt"
	"github.com/golang/protobuf/proto"
)

const (
	fileLockTimeout = 10 * time.Second
)

var _ RowIO = (*fileRowIO)(nil)

type fileRowIO struct {
	db     *bolt.DB
	bucket []byte
}

func NewFileRowIO(bucket string, path string, mode os.FileMode) (RowIO, error) {
	db, err := bolt.Open(path, mode, &bolt.Options{
		Timeout: fileLockTimeout,
	})
	if err != nil {
		return nil, err
	}
	f := &fileRowIO{
		db:     db,
		bucket: []byte(bucket),
	}
	if err := f.ensureBucket(); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

func (db *fileRowIO) ensureBucket() error {
	return db.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(db.bucket)
		return err
	})
}

func (db *fileRowIO) Set(ctx context.Context, key []byte, value proto.Message) error {
	valueBytes, err := proto.Marshal(value)
	if err != nil {
		return err
	}
	return db.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(db.bucket)
		return b.Put(key, valueBytes)
	})
}

func (db *fileRowIO) Get(ctx context.Context, key []byte, value proto.Message) error {
	return db.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(db.bucket)
		valueBytes := b.Get(key)
		return proto.Unmarshal(valueBytes, value)
	})
}

func (db *fileRowIO) Scan(ctx context.Context, fromKey, toKey []byte, factory Factory, predicate Predicate) Iterator {

	type iteration struct {
		key   []byte
		value []byte
		more  bool
	}
	iterations := make(chan iteration)
	done := make(chan struct{})
	iterFunc := keyValueIteratorFunc(func() (key []byte, value []byte, more bool, err error) {
		select {
		case <-ctx.Done():
			return nil, nil, false, ctx.Err()
		case i := <-iterations:
			return i.key, i.value, i.more, nil
		case <-done:
			return nil, nil, false, ErrIteratorDone
		}
	})

	go func() {
		db.db.View(func(tx *bolt.Tx) error {
			defer close(done)
			c := tx.Bucket(db.bucket).Cursor()
			for k, v := c.Seek(fromKey); k != nil && bytes.Compare(k, toKey) <= 0; {
				nextK, nextV := c.Next()

				more := nextK != nil && bytes.Compare(nextK, toKey) <= 0
				iterations <- iteration{key: k, value: v, more: more}

				k, v = nextK, nextV
			}
			return nil
		})
	}()

	return newPredicateIterator(ctx, predicate, factory, iterFunc)
}

func (db *fileRowIO) Close() error {
	return db.db.Close()
}
