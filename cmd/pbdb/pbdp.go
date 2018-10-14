package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/explodes/rowio"
	"github.com/explodes/rowio/cmd/pbdb/protos"
	"github.com/golang/protobuf/proto"
)

func main() {
	testDb(rowio.NewMemoryRowIO())
	testDb(rowio.NewFileRowIO("sample", "/tmp/rowio", 0600))
}

func testDb(db rowio.RowIO, err error) {
	const max = 2
	noerr(err)
	wg := new(sync.WaitGroup)
	for i := 0; i < max; i++ {
		wg.Add(1)
		go func(i int) {
			set(db, numberedString("user", i), &protos.User{Username: numberedString("explodes", i)})
			wg.Done()
		}(i)
		wg.Add(1)
		go func(i int) {
			set(db, numberedString("log", i), &protos.Log{Message: numberedString("hello world", i)})
			wg.Done()
		}(i)
	}

	wg.Wait()

	for i := 0; i < max; i++ {
		user := getincompat(db, fmt.Sprintf("user%d", i))
		logpb := getincompat(db, fmt.Sprintf("log%d", i))
		fmt.Println(user)
		fmt.Println(logpb)
	}

	userIter := db.Scan(context.Background(), []byte(numberedString("user", 0)), []byte(numberedString("user", max)), asUser, has100)
	dumpIterator(userIter)

	logIter := db.Scan(context.Background(), []byte(numberedString("log", 0)), []byte(numberedString("log", max)), asLog, has100)
	dumpIterator(logIter)

	noerr(db.Close())
}

func numberedString(base string, n int) string {
	return fmt.Sprintf("%s%06d", base, n)
}

func dumpIterator(iter rowio.Iterator) {
	for iter.Next() {
		_, value, err := iter.Value()
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Println(value)
	}
}

func asUser(b []byte) (proto.Message, error) {
	pb := &protos.User{}
	err := proto.Unmarshal(b, pb)
	return pb, err
}

func asLog(b []byte) (proto.Message, error) {
	pb := &protos.Log{}
	err := proto.Unmarshal(b, pb)
	return pb, err
}

func has100(pb proto.Message) bool {
	switch t := pb.(type) {
	case *protos.User:
		return strings.Contains(t.Username, "100")
	case *protos.Log:
		return strings.Contains(t.Message, "100")
	default:
		return false
	}
}

func set(db rowio.RowIO, key string, pb proto.Message) {
	err := db.Set(context.Background(), []byte(key), pb)
	noerr(err)
}

func getuser(db rowio.RowIO, key string) *protos.User {
	pb := &protos.User{}
	err := db.Get(context.Background(), []byte(key), pb)
	noerr(err)
	return pb
}

func getlog(db rowio.RowIO, key string) *protos.Log {
	pb := &protos.Log{}
	err := db.Get(context.Background(), []byte(key), pb)
	noerr(err)
	return pb
}

func getincompat(db rowio.RowIO, key string) *protos.Incompat {
	pb := &protos.Incompat{}
	err := db.Get(context.Background(), []byte(key), pb)
	noerr(err)
	return pb
}

func noerr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
