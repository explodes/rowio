package rowio

import (
	"time"

	"github.com/golang/protobuf/ptypes/any"
	"github.com/golang/protobuf/ptypes/empty"
	"golang.org/x/net/context"
)

var (
	_theEmpty = &empty.Empty{}
)

type serviceImpl struct {
	buckets Buckets

	scanTimeout time.Duration
}

type ServiceOptions struct {
	// ScanTimeout is the timeout allowed for scanning. A duration of 0 means there is not timeout.
	ScanTimeout time.Duration
}

func NewService(buckets Buckets, opts *ServiceOptions) RowIOServiceServer {
	service := &serviceImpl{
		buckets: buckets,
	}
	if opts != nil {
		service.scanTimeout = opts.ScanTimeout
	}
	return service
}

func (s *serviceImpl) Set(ctx context.Context, r *SetRequest) (*empty.Empty, error) {
	db, err := s.buckets.Get(r.Bucket)
	if err != nil {
		return nil, err
	}
	err = db.Set(ctx, r.Key, r.Value)
	return _theEmpty, err
}

func (s *serviceImpl) Get(ctx context.Context, r *GetRequest) (*GetResponse, error) {
	db, err := s.buckets.Get(r.Bucket)
	if err != nil {
		return nil, err
	}
	value := &any.Any{}
	err = db.Get(ctx, r.Key, value)
	if err != nil {
		return nil, err
	}
	response := &GetResponse{
		Value: value,
	}
	return response, nil
}

func (s *serviceImpl) scanContext() context.Context {
	var ctx context.Context
	if s.scanTimeout == 0 {
		ctx = context.Background()
	} else {
		ctx, _ = context.WithTimeout(context.Background(), s.scanTimeout)
	}
	return ctx
}

func (s *serviceImpl) Scan(r *ScanRequest, stream RowIOService_ScanServer) error {
	db, err := s.buckets.Get(r.Bucket)
	if err != nil {
		return err
	}
	ctx := s.scanContext()
	iter := db.Scan(ctx, r.FromKey, r.ToKey, AnyFactory, AllPredicate)

	out := &ScanStream{}

	for iter.Next() {
		key, value, err := iter.Value()
		if err != nil {
			return err
		}
		out.Reset()
		out.Key = key
		out.Value = value.(*any.Any)
		if err := stream.Send(out); err != nil {
			return err
		}
	}

	return nil
}
