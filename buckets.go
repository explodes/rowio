package rowio

import (
	"fmt"
	"os"

	"github.com/pkg/errors"
)

var (
	errInvalidBucket = errors.New("invalid bucket")
)

type Buckets interface {
	Get(name string) (RowIO, error)
	Close() error
}

type bucketMap map[string]RowIO

func (m bucketMap) Get(name string) (RowIO, error) {
	db, ok := m[name]
	if !ok {
		return nil, errInvalidBucket
	}
	return db, nil
}

func (m bucketMap) Close() error {
	var err error
	for bucketName, db := range m {
		closeErr := db.Close()
		if err == nil && closeErr != nil {
			err = closeErr
		}
		delete(m, bucketName)
	}
	return err
}

func NewMemoryBuckets(bucketNames ...string) (Buckets, error) {
	b := make(bucketMap)
	for _, bucketName := range bucketNames {
		db, err := NewMemoryRowIO()
		if err != nil {
			b.Close()
			return nil, err
		}
		b[bucketName] = db
	}
	return b, nil
}

func NewFileBuckets(directory string, mode os.FileMode, bucketNames ...string) (Buckets, error) {
	b := make(bucketMap)
	for _, bucketName := range bucketNames {
		path := fmt.Sprintf("%s%c%s", directory, os.PathSeparator, bucketName)
		db, err := NewFileRowIO(bucketName, path, mode)
		if err != nil {
			b.Close()
			return nil, err
		}
		b[bucketName] = db
	}
	return b, nil
}
