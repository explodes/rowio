package rowio

import (
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/any"
)

type Factory func([]byte) (proto.Message, error)

func AnyFactory(b []byte) (proto.Message, error) {
	value := &any.Any{}
	if err := proto.Unmarshal(b, value); err != nil {
		return nil, err
	}
	return value, nil
}
