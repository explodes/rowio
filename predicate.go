package rowio

import "github.com/golang/protobuf/proto"

type Predicate func(proto.Message) bool

func AllPredicate(proto.Message) bool { return true }
