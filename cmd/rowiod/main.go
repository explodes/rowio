package main

import (
	"flag"
	"log"
	"net"
	"strings"

	"google.golang.org/grpc"

	"github.com/explodes/rowio"
)

const (
	memoryDirectory = ":memory:"

	defaultFileMode = 0600
)

var (
	bucketsFlag     = flag.String("buckets", "default", "comma-separated bucket names to serve")
	directoryFlag   = flag.String("dir", memoryDirectory, "file system directory to serve from, or :memory: for in-memory storage")
	scanTimeoutFlag = flag.Duration("timeout", 0, "timeout to use for scanning, 0 for no timeout")
	bindFlag        = flag.String("bind", "0.0.0.0:8234", "bind address")
)

func main() {
	flag.Parse()
	bucketNames := parseBucketNames(*bucketsFlag)
	buckets := createBuckets(bucketNames, *directoryFlag)
	service := rowio.NewService(buckets, &rowio.ServiceOptions{
		ScanTimeout: *scanTimeoutFlag,
	})
	lis, err := net.Listen("tcp", *bindFlag)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	rowio.RegisterRowIOServiceServer(grpcServer, service)
	log.Printf("serving on %s...", *bindFlag)
	grpcServer.Serve(lis)
}

func createBuckets(bucketNames []string, directory string) rowio.Buckets {
	var buckets rowio.Buckets
	var err error

	if *directoryFlag == memoryDirectory {
		buckets, err = rowio.NewMemoryBuckets(bucketNames...)
	} else {
		buckets, err = rowio.NewFileBuckets(directory, defaultFileMode, bucketNames...)
	}

	if err != nil {
		log.Fatalf("unable to create buckets: %v", err)
	}

	return buckets
}

func parseBucketNames(s string) []string {
	bucketNames := strings.Split(s, ",")
	for _, bucketName := range bucketNames {
		for _, r := range bucketName {
			if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')) {
				log.Fatalf("invalid bucket name %s: must contain a-z A-Z", bucketName)
			}
		}
	}
	return bucketNames
}
