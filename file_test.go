package rowio

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestFileRowIO(t *testing.T) {
	type testFileRowIO struct {
		RowIO
		file *os.File
	}
	factory := func() (RowIO, error) {
		f, err := ioutil.TempFile("", "rowio_test")
		if err != nil {
			return nil, err
		}

		io, err := NewFileRowIO("defaultBucket", f.Name(), 0600)
		if err != nil {
			return nil, firstError(err, destroyFile(f))
		}

		wrapped := &testFileRowIO{
			RowIO: io,
			file:  f,
		}
		return wrapped, err
	}
	cleanup := func(db RowIO) error {
		testDB := db.(*testFileRowIO)
		return destroyFile(testDB.file)
	}
	testRowIO(t, "FileRowIO", factory, cleanup)
}

func destroyFile(file *os.File) error {
	errA := file.Close()
	errB := os.Remove(file.Name())
	return firstError(errA, errB)
}

func firstError(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}
